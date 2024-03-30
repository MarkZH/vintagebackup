import os
import shutil
import datetime
import platform
import argparse
import sys
import logging
import filecmp
import stat
import itertools
import re
import textwrap
import math
from collections import Counter
from typing import Iterator, Any
from pathlib import Path

backup_date_format = "%Y-%m-%d %H-%M-%S"

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.setLevel(logging.INFO)

new_backup_directory_created = False


class CommandLineError(ValueError):
    """An exception class to catch invalid command line parameters."""

    pass


storage_prefixes = ["", "k", "M", "G", "T", "P", "E", "Z", "Y", "R", "Q"]


def byte_units(size: float) -> str:
    """
    Display a number of bytes with four significant figures with byte units.

    >>> byte_units(12345)
    '12.35 kB'
    """
    for index, prefix in enumerate(storage_prefixes):
        prefix_size = 10**(3*index)
        size_in_units = size/prefix_size
        if size_in_units < 1000:
            break

    decimal_digits = 4 - math.floor(math.log10(size_in_units) + 1)
    return f"{size_in_units:.{decimal_digits}f} {prefix}B"


def all_backups(backup_location: Path) -> list[Path]:
    """Return a sorted list of all backups at the given location."""
    year_pattern = re.compile(r"\d\d\d\d")
    backup_pattern = re.compile(r"\d\d\d\d-\d\d-\d\d \d\d-\d\d-\d\d (.*)")

    def is_valid_directory(dir: os.DirEntry[str], pattern: re.Pattern[str]) -> bool:
        return not dir.is_symlink() and dir.is_dir() and bool(pattern.fullmatch(dir.name))

    all_backup_list: list[Path] = []
    with os.scandir(backup_location) as year_scan:
        for year in (y for y in year_scan if is_valid_directory(y, year_pattern)):
            with os.scandir(year) as dated_backup_scan:
                all_backup_list.extend(Path(dated_backup)
                                       for dated_backup in dated_backup_scan
                                       if is_valid_directory(dated_backup, backup_pattern))

    return sorted(all_backup_list)


def find_previous_backup(backup_location: Path) -> Path | None:
    """Return the most recent backup at the given location."""
    try:
        return all_backups(backup_location)[-1]
    except IndexError:
        return None


def glob_file(glob_file_path: Path | None,
              category: str,
              user_data_location: Path) -> Iterator[tuple[int, str, Iterator[Path]]]:
    """
    Read a file of glob patterns and return an iterator over matching paths.

    The line number and line in the file are included in the iterator for error reporting.
    """
    if not glob_file_path:
        return

    logger.info(f"Reading {category} file: {glob_file_path}")
    with open(glob_file_path) as glob_file:
        for line_number, line in enumerate(glob_file, 1):
            line = line.rstrip("\n")
            glob_path = Path(line)
            if glob_path.is_absolute():
                try:
                    pattern = str(glob_path.relative_to(user_data_location))
                except ValueError:
                    logger.info(f"Ignoring {category} line #{line_number} outside of user folder:"
                                f" {glob_path}")
                    continue
            else:
                pattern = str(glob_path)

            yield line_number, line, user_data_location.glob(pattern)


def create_exclusion_list(exclude_file: Path | None, user_data_location: Path) -> set[Path]:
    """Create a set of files and folders to excluded from backups from glob patterns in a file."""
    exclusions: set[Path] = set()
    for line_number, line, exclusion_set in glob_file(exclude_file, "exclude", user_data_location):
        original_count = len(exclusions)
        exclusions.update(exclusion_set)
        if len(exclusions) == original_count:
            logger.info(f"Nothing found for exclude line #{line_number}: {line}")
    return exclusions


def filter_excluded_paths(exclusions: set[Path],
                          current_dir: Path,
                          name_list: list[str]) -> list[str]:
    """Remove excluded files and folders from the data being backed up."""
    current_set = set(current_dir/name for name in name_list)
    return [path.name for path in current_set - exclusions]


def get_user_location_record(backup_location: Path) -> Path:
    """Return the file that contains the user directory that is backed up at the given location."""
    return backup_location/"vintagebackup.source.txt"


def record_user_location(user_location: Path, backup_location: Path) -> None:
    """Write the user directory being backed up to a file in the base backup directory."""
    user_folder_record = get_user_location_record(backup_location)
    with open(user_folder_record, "w") as user_record:
        user_record.write(str(user_location) + "\n")


def backup_source(backup_location: Path) -> Path:
    """Read the user directory that was backed up to the given backup location."""
    user_folder_record = get_user_location_record(backup_location)
    with open(user_folder_record) as user_record:
        return Path(user_record.read().rstrip("\n"))


def confirm_user_location_is_unchanged(user_data_location: Path, backup_location: Path) -> None:
    """
    Make sure the user directory being backed up is the same as the previous backup run.

    An exception will be thrown when attempting to back up a different user directory to the one
    that was backed up previously. Backing up multiple different directories to the same backup
    location negates the hard linking functionality.
    """
    try:
        recorded_user_folder = backup_source(backup_location)
        if not os.path.samefile(recorded_user_folder, user_data_location):
            raise RuntimeError("Previous backup stored a different user folder."
                               f" Previously: {recorded_user_folder}; Now: {user_data_location}")
    except FileNotFoundError:
        # This is probably the first backup, hence no user folder record.
        pass


def shallow_stats(stats: os.stat_result) -> tuple[int, int, int]:
    """
    Return simple file information for quicker checks for file changes since the last bacukp.

    When not inspecting file contents, only look at the file size, type, and modification time--in
    that order.

    Parameter:
    stats: File information retrieved from a DirEntry.stat() call.
    """
    return (stats.st_size, stat.S_IFMT(stats.st_mode), stats.st_mtime_ns)


def compare_to_backup(user_directory: Path,
                      backup_directory: Path | None,
                      file_names: list[str],
                      examine_whole_file: bool) -> tuple[list[str], list[str], list[str]]:
    """
    Sort a list of files according to whether they have changed since the last backup.

    Parameters:
    user_directory: The subfolder of the user's data currently being walked through
    backup_direcotry: The backup folder that corresponds with the user_directory
    file_names: A list of regular files (not symlinks) in the user directory.
    examine_whole_file: Whether the contents of the file should be examined, or just file
    attributes.

    The file names will be sorted into three lists and returned in this order: (1) matching files
    that have not changed since the last backup, (2) mismatched files that have changed, (3) error
    files that could not be compared for some reason (usually because it is a new file with no
    previous backup). This is the same behavior as filecmp.cmpfiles().
    """
    assert all(not (user_directory/file_name).is_symlink() for file_name in file_names)

    if not backup_directory:
        return [], [], file_names
    elif examine_whole_file:
        return filecmp.cmpfiles(user_directory, backup_directory, file_names, shallow=False)
    else:
        def scan_directory(directory: Path) -> dict[str, os.stat_result]:
            with os.scandir(directory) as scan:
                return {entry.name: entry.stat() for entry in scan}

        try:
            backup_files = scan_directory(backup_directory)
        except OSError:
            return [], [], file_names

        matches: list[str] = []
        mismatches: list[str] = []
        errors: list[str] = []
        user_files = scan_directory(user_directory)
        for file_name in file_names:
            try:
                user_file_stats = shallow_stats(user_files[file_name])
                backup_file_stats = shallow_stats(backup_files[file_name])
                if user_file_stats == backup_file_stats:
                    matches.append(file_name)
                else:
                    mismatches.append(file_name)
            except Exception:
                errors.append(file_name)

        return matches, mismatches, errors


def create_hard_link(previous_backup: Path, new_backup: Path) -> bool:
    """
    Create a hard link between unchanged backup files.

    Return True if successful, False if hard linked failed.
    """
    try:
        os.link(previous_backup, new_backup, follow_symlinks=False)
        return True
    except Exception as error:
        logger.debug(f"Could not create hard link due to error: {error}")
        logger.debug(f"Previous backed up file: {previous_backup}")
        logger.debug(f"Attempted link         : {new_backup}")
        return False


def include_walk(include_file_name: Path | None,
                 user_directory: Path) -> Iterator[tuple[str, list[str], list[str]]]:
    """Create an iterator similar to os.walk() through glob patterns in a file."""
    for line_number, line, inclusions in glob_file(include_file_name, "include", user_directory):
        found_paths = False
        for path in inclusions:
            found_paths = True
            if not os.path.islink(path) and os.path.isdir(path):
                yield from os.walk(path)
            else:
                yield str(path.parent), [], [path.name]
        if not found_paths:
            logger.info(f"Nothing found for include line #{line_number}: {line}")


def separate_symlinks(directory: Path, file_names: list[str]) -> tuple[list[str], list[str]]:
    """
    Separate regular files from symlinks.

    Parameters:
    directory: The directory containing all the files.
    file_names: A list of files in the directory.

    Returns:
    Two lists: the first a list of regular files, the second a list of symlinks.
    """
    def is_symlink(file_name: str) -> bool:
        return (directory/file_name).is_symlink()

    return list(itertools.filterfalse(is_symlink, file_names)), list(filter(is_symlink, file_names))


def backup_directory(user_data_location: Path,
                     new_backup_path: Path,
                     last_backup_path: Path | None,
                     current_user_path: Path,
                     user_dir_names: list[str],
                     user_file_names: list[str],
                     exclusions: set[Path],
                     examine_whole_file: bool,
                     action_counter: Counter[str],
                     is_include_backup: bool) -> None:
    """
    Backup the files in a subfolder in the user's directory.

    Parameters:
    user_data_location: The base directory that is being backed up
    new_backup_path: The base directory of the new dated backup
    last_backup_path: The base directory of the previous dated backup
    current_user_path: The user directory currently being walked through
    user_dir_names: The names of directories contained in the current_user_path
    user_file_names: The names of files contained in the current_user_path
    exclusions: A set of files and folders to exclude from the backup
    examine_whole_file: Whether to examine file contents to check for changes since the last backup
    action_counter: A counter to track how many files have been linked, copied, or failed for both
    is_include_backup: Whether the current directory comes from the include file.
    """
    user_file_names = filter_excluded_paths(exclusions,
                                            current_user_path,
                                            user_file_names)
    user_dir_names[:] = filter_excluded_paths(exclusions,
                                              current_user_path,
                                              user_dir_names)

    user_file_names, user_symlinks = separate_symlinks(current_user_path, user_file_names)
    _, user_directory_symlinks = separate_symlinks(current_user_path, user_dir_names)

    relative_path = current_user_path.relative_to(user_data_location)
    new_backup_directory = new_backup_path/relative_path
    os.makedirs(new_backup_directory, exist_ok=is_include_backup)
    global new_backup_directory_created
    new_backup_directory_created = True
    previous_backup_directory = last_backup_path/relative_path if last_backup_path else None

    matching, mismatching, errors = compare_to_backup(current_user_path,
                                                      previous_backup_directory,
                                                      user_file_names,
                                                      examine_whole_file)

    for file_name in matching:
        assert previous_backup_directory
        previous_backup = previous_backup_directory/file_name
        new_backup = new_backup_directory/file_name
        if os.path.lexists(new_backup):
            logger.debug(f"Skipping backed up include file: {current_user_path/file_name}")
            continue

        if create_hard_link(previous_backup, new_backup):
            action_counter["linked files"] += 1
            logger.debug(f"Linked {previous_backup} to {new_backup}")
        else:
            errors.append(file_name)

    for file_name in itertools.chain(mismatching, errors, user_symlinks, user_directory_symlinks):
        new_backup_file = new_backup_directory/file_name
        user_file = current_user_path/file_name
        try:
            shutil.copy2(user_file, new_backup_file, follow_symlinks=False)
            action_counter["copied files"] += 1
            logger.debug(f"Copied {user_file} to {new_backup_file}")
        except Exception as error:
            logger.warning(f"Could not copy {user_file} to {new_backup_file} ({error})")
            action_counter["failed copies"] += 1


def create_new_backup(user_data_location: Path,
                      backup_location: Path,
                      exclude_file: Path | None,
                      include_file: Path | None,
                      examine_whole_file: bool,
                      force_copy: bool,
                      is_backup_move: bool = False) -> None:
    """
    Create a new dated backup.

    Parameters:
    user_data_location: The folder containing the data to be backed up
    backup_location: The base directory of the backup destination
    exclude_file: A file containg a list of path glob patterns to exclude from the backup
    include_file: A file containg a list of path glob patterns to include in the backup.
    examine_whole_file: Whether to examine file contents to check for changes since the last backup
    force_copy: Whether to always copy files, regardless of whether a previous backup exists.
    """
    if not os.path.isdir(user_data_location):
        raise CommandLineError(f"The user folder path is not a folder: {user_data_location}")

    if not backup_location:
        raise CommandLineError("No backup destination was given.")

    if backup_location.is_relative_to(user_data_location):
        raise CommandLineError("Backup destination cannot be inside user's folder:"
                               f" User data: {user_data_location}"
                               f"; Backup location: {backup_location}")

    if exclude_file and not os.path.isfile(exclude_file):
        raise CommandLineError(f"Exclude file not found: {exclude_file}")

    if include_file and not os.path.isfile(include_file):
        raise CommandLineError(f"Include file not found: {include_file}")

    os.makedirs(backup_location, exist_ok=True)

    now = datetime.datetime.now()
    backup_date = now.strftime(backup_date_format)
    os_name = f"{platform.system()} {platform.release()}".strip()
    new_backup_path = backup_location/str(now.year)/f"{backup_date} ({os_name})"

    if not is_backup_move:
        logger.info("")
        logger.info("=====================")
        logger.info(" Starting new backup")
        logger.info("=====================")
        logger.info("")

    confirm_user_location_is_unchanged(user_data_location, backup_location)
    record_user_location(user_data_location, backup_location)

    if is_backup_move:
        logger.info(f"Original backup  : {user_data_location}")
        logger.info(f"Temporary backup : {new_backup_path}")
    else:
        logger.info(f"User's data      : {user_data_location}")
        logger.info(f"Backup location  : {new_backup_path}")

    last_backup_path = None if force_copy else find_previous_backup(backup_location)
    if last_backup_path:
        logger.info(f"Previous backup  : {last_backup_path}")
    else:
        logger.info("No previous backups. Copying everything ...")

    logger.info("")
    logger.info(f"Reading file contents = {examine_whole_file}")

    action_counter: Counter[str] = Counter()
    exclusions = create_exclusion_list(exclude_file, user_data_location)
    logger.info("Running backup ...")
    for current_user_path, user_dir_names, user_file_names in os.walk(user_data_location):
        backup_directory(user_data_location,
                         new_backup_path,
                         last_backup_path,
                         Path(current_user_path),
                         user_dir_names,
                         user_file_names,
                         exclusions,
                         examine_whole_file,
                         action_counter,
                         False)

    for include_path, _, include_file_list in include_walk(include_file, user_data_location):
        backup_directory(user_data_location,
                         new_backup_path,
                         last_backup_path,
                         Path(include_path),
                         [],
                         include_file_list,
                         set(),
                         examine_whole_file,
                         action_counter,
                         True)

    logger.info("")
    total_files = sum(count for action, count in action_counter.items()
                      if not action.startswith("failed"))
    action_counter["Backed up files"] = total_files
    name_column_size = max(len(name) for name in action_counter.keys())
    count_column_size = len(str(max(action_counter.values())))
    for action, count in action_counter.items():
        logger.info(f"{action.capitalize():<{name_column_size}} : {count:>{count_column_size}}")


def setup_log_file(logger: logging.Logger, log_file_path: str) -> None:
    """Set up logging to write to a file."""
    log_file = logging.FileHandler(log_file_path, encoding="utf8")
    log_file_format = logging.Formatter(fmt="%(asctime)s %(levelname)s    %(message)s")
    log_file.setFormatter(log_file_format)
    logger.addHandler(log_file)


def search_backups(search_directory: Path, backup_folder: Path) -> Path:
    """
    Decide which path to restore among all backups for all items in the given directory.

    The user will pick from a list of all files and folders in search_directory that have ever been
    backed up.

    Parameters:
    search_directory: The directory from which backed up files and folders will be listed
    backup_folder: The backup destination

    Returns:
    The path to a file or folder that will then be searched for among backups.
    """
    if search_directory.is_symlink() or not search_directory.is_dir():
        raise CommandLineError(f"The given search path is not a directory: {search_directory}")
    try:
        user_data_location = backup_source(backup_folder)
    except FileNotFoundError:
        raise CommandLineError(f"There are no backups in {backup_folder}")

    try:
        target_relative_path = search_directory.relative_to(user_data_location)
    except ValueError:
        raise CommandLineError(f"The path {search_directory} is not in the backup at"
                               f" {backup_folder}, which contains {user_data_location}")

    all_paths: set[tuple[str, str]] = set()
    for backup in all_backups(backup_folder):
        backup_search_directory = backup/target_relative_path
        try:
            with os.scandir(backup_search_directory) as backup_scan:
                for item in backup_scan:
                    path_type = ("Symlink" if item.is_symlink()
                                 else "File" if item.is_file()
                                 else "Folder" if item.is_dir()
                                 else "?")
                    all_paths.add((item.name, path_type))
        except FileNotFoundError:
            continue

    menu_list = sorted(all_paths)
    number_column_size = len(str(len(menu_list)))
    for index, (name, path_type) in enumerate(menu_list, 1):
        print(f"{index:>{number_column_size}}: {name} ({path_type})")

    while True:
        try:
            user_choice = int(input("Which path to recover (Ctrl-C to quit): "))
            if user_choice >= 1:
                recovery_target_name = menu_list[user_choice - 1][0]
                return search_directory/recovery_target_name
        except (ValueError, IndexError):
            continue


def recover_path(recovery_path: Path, backup_location: Path) -> None:
    """
    Decide which version of a file to restore to its previous location.

    The user will be presented with a list of backups that contain different versions of the file
    or folder. Any backup that contains a hard-linked copy of a file will be skipped. After the
    user selects a backup date, the file or folder from that backup will be copied to the
    corresponding location in the user's data. The copy from the backup will be renamed with a
    number so as to not overwrite any existing file with the same name.

    Parameters:
    recovery_path: The file or folder that is to be restored.
    backup_location: The folder containing all backups.
    """
    try:
        with open(get_user_location_record(backup_location)) as location_file:
            user_data_location = Path(location_file.readline().rstrip("\n")).resolve(strict=True)
    except FileNotFoundError:
        raise CommandLineError(f"No backups found at {backup_location}")

    if not recovery_path.is_relative_to(user_data_location):
        raise CommandLineError(f"{recovery_path} is not contained in the backup set "
                               f"{backup_location}, which contains {user_data_location}.")

    unique_backups: dict[int, Path] = {}
    recovery_relative_path = recovery_path.relative_to(user_data_location)
    for backup in all_backups(backup_location):
        path = backup/recovery_relative_path
        if path.exists(follow_symlinks=False):
            inode = os.stat(path, follow_symlinks=False).st_ino
            unique_backups.setdefault(inode, path)

    backup_choices = sorted(unique_backups.values())
    number_column_size = len(str(len(backup_choices)))
    for choice, backup_copy in enumerate(backup_choices, 1):
        backup_date = backup_copy.relative_to(backup_location).parts[1]
        path_type = ("Symlink" if backup_copy.is_symlink()
                     else "File" if backup_copy.is_file()
                     else "Folder" if backup_copy.is_dir()
                     else "?")
        print(f"{choice:>{number_column_size}}: {backup_date} ({path_type})")

    while True:
        try:
            user_choice = int(input("Version to recover (Ctrl-C to quit): "))
            if user_choice < 1:
                continue
            chosen_path = backup_choices[user_choice - 1]
            break
        except (ValueError, IndexError):
            pass

    recovered_path = recovery_path
    unique_id = 0
    while os.path.lexists(recovered_path):
        unique_id += 1
        new_file_name = f"{recovery_path.stem}.{unique_id}{recovery_path.suffix}"
        recovered_path = recovery_path.parent/new_file_name

    logger.info(f"Copying {chosen_path} to {recovered_path}")
    if chosen_path.is_symlink() or chosen_path.is_file():
        shutil.copy2(chosen_path, recovered_path, follow_symlinks=False)
    else:
        shutil.copytree(chosen_path, recovered_path, symlinks=True)


def delete_last_backup(backup_location: Path) -> None:
    """Delete the most recent backup."""
    last_backup_directory = find_previous_backup(backup_location)
    if last_backup_directory:
        logger.info(f"Deleting failed backup: {last_backup_directory}")
        shutil.rmtree(last_backup_directory)
    else:
        logger.info("No previous backup to delete")


def delete_oldest_backups_for_space(backup_location: Path, space_requirement: str) -> None:
    """
    Delete backups--starting with the oldest--until enough space is free on the backup destination.

    The most recent backup will never be deleted.

    Parameters:
    backup_location: The folder containing all backups
    space_reuirement: The amount of space that should be free after deleting backups. This may be
    expressed in bytes ("MB", "GB", etc.) or as a percentage ("%") of the total storage space.
    """
    space_text = "".join(space_requirement.lower().split())
    prefixes = [p.lower() for p in storage_prefixes]
    prefix_pattern = "".join(prefixes)
    pattern = rf"\d+(\.\d*)?([{prefix_pattern}]?b?|%)"
    total_storage = shutil.disk_usage(backup_location).total
    if not re.fullmatch(pattern, space_text):
        raise CommandLineError(f"Incorrect format of free-up space: {space_text}")
    if space_text.endswith("%"):
        free_fraction_required = float(space_text[:-1])/100
        if free_fraction_required > 1:
            raise CommandLineError(f"Percent cannot be greater than 100: {space_text}")
        free_storage_required = total_storage*free_fraction_required
    else:
        space_text = space_text.rstrip('b')
        if space_text[-1].isalpha():
            prefix = space_text[-1]
            space_text = space_text[:-1]
        else:
            prefix = ""

        multiplier = 1000**prefixes.index(prefix)
        free_storage_required = float(space_text)*multiplier

    if free_storage_required > shutil.disk_usage(backup_location).total:
        raise CommandLineError(f"Cannot free more storage ({byte_units(free_storage_required)})"
                               f" than exists at {backup_location} ({byte_units(total_storage)})")

    any_deletions = False
    backups = all_backups(backup_location)
    for backup in backups[:-1]:
        if shutil.disk_usage(backup_location).free > free_storage_required:
            break

        if not any_deletions:
            logger.info(f"Deleting old backups until {byte_units(free_storage_required)} is free.")

        logger.info(f"Deleting backup: {backup}")
        shutil.rmtree(backup)
        any_deletions = True

    if any_deletions:
        log_backup_deletions(backup_location)

    if shutil.disk_usage(backup_location).free < free_storage_required:
        logger.warning(f"Could not free up {byte_units(free_storage_required)} of storage"
                       " without deleting most recent backup.")


def parse_time_span_to_timepoint(time_span: str) -> datetime.datetime:
    """
    Parse a string representing a time span into a datetime representing a date that long ago.

    For example, if time_span is "6m", the result is a date six calendar months ago.

    time_span: A string consisting of a positive integer followed by a single letter: "d" for days,
    "w" for weeks, "m" for calendar months, and "y" for calendar years.
    """
    time_span = "".join(time_span.lower().split())
    try:
        number = int(time_span[:-1])
        if number < 1:
            raise ValueError()
    except ValueError:
        raise CommandLineError("Invalid number in time span"
                               f" (must be a positive whole number): {time_span}")

    letter = time_span[-1]
    now = datetime.datetime.now()
    if letter == "d":
        return now - datetime.timedelta(days=number)
    elif letter == "w":
        return now - datetime.timedelta(weeks=number)
    elif letter == "m":
        new_month = now.month - (number % 12)
        new_year = now.year - (number // 12)
        if new_month < 1:
            new_month += 12
            new_year -= 1
        new_day = now.day

        # If new month has fewer days than current month, the return statement may raise.
        while True:
            try:
                return datetime.datetime(new_year, new_month, new_day,
                                         now.hour, now.minute, now.second, now.microsecond)
            except ValueError:
                new_day -= 1
    elif letter == "y":
        return datetime.datetime(now.year - number, now.month, now.day,
                                 now.hour, now.minute, now.second, now.microsecond)
    else:
        raise CommandLineError(f"Invalid time (valid units: {list("dwmy")}): {time_span}")


def delete_backups_older_than(backup_folder: Path, time_span: str) -> None:
    """
    Delete backups older than a given timespan.

    Parameters:
    backup_folder: The folder containing all backups
    time_span: The maximum age of a backup to not be deleted. See parse_time_span_to_timepoint()
    for how the string is formatted.
    """
    timestamp_to_keep = parse_time_span_to_timepoint(time_span)

    any_deletions = False
    backups = all_backups(backup_folder)
    for backup in backups[:-1]:
        backup_timestamp = backup_datetime(backup)
        if backup_timestamp >= timestamp_to_keep:
            break

        if not any_deletions:
            logger.info("Deleting backups prior to"
                        f" {timestamp_to_keep.strftime('%Y-%m-%d %H:%M:%S')}.")

        logger.info(f"Deleting oldest backup: {backup}")
        shutil.rmtree(backup)
        any_deletions = True

    if any_deletions:
        log_backup_deletions(backup_folder)


def backup_datetime(backup: Path) -> datetime.datetime:
    """Get the timestamp of a backup from the backup folder name."""
    timestamp_portion = " ".join(backup.name.split()[:2])
    backup_timestamp = datetime.datetime.strptime(timestamp_portion, backup_date_format)
    return backup_timestamp


def plural_noun(count: int, word: str) -> str:
    """
    Convert a noun to a simple plural form if the count is not one.

    >>> plural_noun(5, "cow")
    'cows'

    >>> plural_noun(1, "cat")
    'cat'

    Irregular nouns that are not pluralized by appending an "s" are not supported.
    >>> plural_noun(3, "fox")
    'foxs'
    """
    return f"{word}{'' if count == 1 else 's'}"


def plural_verb(count: int, word: str) -> str:
    """
    Conjugate a third-person present-tense verb based on the count of subjects.

    >>> count = 4
    >>> f"In the park, {count} people {plural_verb(count, "play")} chess."
    'In the park, 4 people play chess.'
    >>> count = 1
    >>> f"In the house, {count} cat {plural_verb(count, "chase")} a mouse."
    'In the house, 1 cat chases a mouse.'

    Irregular verbs are not supported.
    >>> plural_verb(1, "do")
    'dos'

    "does" would be expected here (i.e., "it does").

    >>> plural_verb(2, "be")
    'be'

    "are" would be expected here (i.e., "they are").
    """
    return f"{word}{'s' if count == 1 else ''}"


def log_backup_deletions(backup_folder: Path) -> None:
    """Log information about the remaining backups after deletions."""
    remaining_backups = all_backups(backup_folder)
    count = len(remaining_backups)
    backups = plural_noun(count, "backup")
    remain = plural_verb(count, "remain")
    logger.info(f"Stopped deletions. {count} {backups} {remain}. Earliest: {remaining_backups[0]}")


def move_backups(old_backup_location: Path,
                 new_backup_location: Path,
                 backups_to_move: list[Path]) -> None:
    """Move a set of backups to a new location."""
    logger.info("=====================")
    move_count = len(backups_to_move)
    logger.info(f"Moving {move_count} {plural_noun(move_count, "backup")}")
    logger.info(f"from {old_backup_location}")
    logger.info(f"to   {new_backup_location}")
    logger.info("=====================")

    for backup in backups_to_move:
        create_new_backup(backup, new_backup_location, None, None, False, False, True)

        dated_backup_path = all_backups(new_backup_location)[-1]
        backup_year_folder = dated_backup_path.parent.parent/backup.parent.name
        correct_backup_path = backup_year_folder/backup.name
        backup_year_folder.mkdir(parents=True, exist_ok=True)

        logger.info("")
        logger.info(f"Renaming {dated_backup_path}")
        logger.info(f"to       {correct_backup_path}")

        dated_backup_path.rename(correct_backup_path)

        backup_source_file = get_user_location_record(new_backup_location)
        backup_source_file.unlink()
        logger.info("---------------------")

    original_backup_source = backup_source(old_backup_location)
    record_user_location(original_backup_source, new_backup_location)


def last_n_backups(backup_location: Path, n: str) -> list[Path]:
    """
    Return a list of the paths of the last n backups.

    backup_location: The location of the backup set.
    n: A positive integer to get the last n backups, or "all" to get all backups.
    """
    backups = all_backups(backup_location)
    return backups if n == "all" else backups[-int(n):]


def backups_since(oldest_backup_date: datetime.datetime, backup_location: Path) -> list[Path]:
    """Return a list of the backups created since a given date."""
    def recent_enough(backup_folder: Path) -> bool:
        return backup_datetime(backup_folder) >= oldest_backup_date

    return list(filter(recent_enough, all_backups(backup_location)))


def print_backup_storage_stats(backup_location: str | Path) -> None:
    """Log information about the storage space of the backup medium."""
    try:
        backup_storage = shutil.disk_usage(backup_location)
        percent_used = round(100*backup_storage.used/backup_storage.total)
        percent_free = round(100*backup_storage.free/backup_storage.total)
        logger.info("Backup storage space: "
                    f"Total = {byte_units(backup_storage.total)}  "
                    f"Used = {byte_units(backup_storage.used)} ({percent_used}%)  "
                    f"Free = {byte_units(backup_storage.free)} ({percent_free}%)")
    except Exception:
        pass


def read_configuation_file(config_file_name: str) -> list[str]:
    """Parse a configuration file into command line arguments."""
    arguments: list[str] = []

    with open(config_file_name) as file:
        for line in file:
            if not line or line.strip().startswith("#"):
                continue
            parameter, value = line.split(":", maxsplit=1)
            arguments.append(f"--{"-".join(parameter.lower().split())}")
            arguments.append(value.strip())

    return list(filter(None, arguments))


def format_paragraphs(lines: str, line_length: int) -> str:
    """Format multiparagaph text in when printing--help."""
    paragraphs: list[str] = []
    needs_paragraph_break = True
    for paragraph in lines.split("\n\n"):
        paragraph = paragraph.strip("\n")
        if paragraph[0].isspace():
            if needs_paragraph_break:
                paragraphs.append(paragraph)
            else:
                paragraphs[-1] = f"{paragraphs[-1]}\n{paragraph}"
            needs_paragraph_break = False
        else:
            needs_paragraph_break = True
            paragraphs.append("\n".join(textwrap.wrap(paragraph, line_length)))

    return "\n\n".join(paragraphs)


def format_text(lines: str) -> str:
    """Format unindented paragraphs (program description and epilogue) in --help."""
    width, _ = shutil.get_terminal_size()
    return format_paragraphs(lines, width)


def format_help(lines: str) -> str:
    """Format indented command line argument descriptions in --help."""
    width, _ = shutil.get_terminal_size()
    return format_paragraphs(lines, width - 24)


def add_no_option(user_input: argparse.ArgumentParser, name: str) -> None:
    """Add negating option for boolean command line arguments."""
    user_input.add_argument(f"--no-{name}", action="store_true", help=format_help(f"""
Disable the --{name} option. This is primarily used if "{name}" appears in a
configuration file. This option has priority even if --{name} is listed later."""))


def toggle_is_set(args: argparse.Namespace, name: str) -> bool:
    """Check that a boolean command line option --X has been selected and not negated by --no-X."""
    options = vars(args)
    return options[name] and not options[f"no_{name}"]


def choice_count(*args: Any) -> int:
    """Count the number of arguments with set values."""
    return len(list(filter(None, args)))


if __name__ == "__main__":
    user_input = argparse.ArgumentParser(add_help=False,
                                         formatter_class=argparse.RawTextHelpFormatter,
                                         allow_abbrev=False,
                                         description=format_text("""
A backup utility that combines the best aspects of full and incremental backups.

Every time Vintage Backup runs, a new folder is created at the backup location
that contains copies of all of the files in the directory being backed up.
If a file in the directory being backed up is unchanged since the last
back up, a hard link to the same file in the previous backup is created.
This way, unchanged files do not take up more storage space in the backup
location, allowing for possible years of daily backups, all while having
each folder in the backup location contain a full backup.

The following options will cause Vinatage Backup to perform an action other than
creating a new backup: --help, --recover, --list. See below for more information.

Technical notes:

- Symbolic links are not followed and are always copied as symbolic links.

- If two files in the user's directory are hard-linked together, these files will be copied/linked
separately (the hard link is not preserved in the backup.)"""))

    user_input.add_argument("-h", "--help", action="store_true", help=format_help("""
Show this help message and exit."""))

    user_input.add_argument("-c", "--config", metavar="FILE_NAME", help=format_help(r"""
Read options from a configuration file instead of command-line arguments. The format
of the file should be one option per line with a colon separating the parameter name
and value. The parameter names have the same names as the double-dashed command line options
(i.e., "user-folder", not "u"). If a parameter does not take a value, like "whole-file",
leave the value blank. Any line starting with a # will be ignored. As an example:

    # Ignored comment
    user-folder: C:\Users\Alice Eve Roberts\
    backup-folder: E:\Backups
    delete-on-error:

The parameter names may also be spelled with spaces instead of the dashes and with mixed case:

    # Ignored comment
    User Folder: C:\Users\Alice Eve Roberts\
    Backup Folder: E:\Backups
    Delete on error:

Values like file and folder names may contain any characters--no escaping or quoting necessary.
Whitespace at the beginning and end of the values will be trimmed off.

If both --config and other command line options are used and they conflict, then the command
line options override the config file options.

A final note: the parameter "config" does nothing inside a config file."""))

    user_input.add_argument("-u", "--user-folder", help=format_help("""
The directory to be backed up. The contents of this
folder and all subfolders will be backed up recursively."""))

    user_input.add_argument("-b", "--backup-folder", help=format_help("""
The destination of the backed up files. This folder will
contain a set of folders labeled by year, and each year's
folder will contain all of that year's backups."""))

    user_input.add_argument("-e", "--exclude", help=format_help("""
The path of a text file containing a list of files and folders
to exclude from backups. Each line in the file should contain
one exclusion. Wildcard characters like * and ? are allowed.
The path should either be an absolute path or one relative to
the directory being backed up (from the -u option)."""))

    user_input.add_argument("-i", "--include", help=format_help("""
The path of a text file containing a list of files and folders
to include in the backups. The entries in this text file
override the exclusions from the --exclude argument. Each line
should contain one file or directory to include. Wildcard
characters like * and ? are allowed. The paths should either
be absolute paths or paths relative to the directory being backed
up (from the -u option). Included paths must be contained within
the directory being backed up."""))

    user_input.add_argument("-w", "--whole-file", action="store_true", help=format_help("""
Examine the entire contents of a file to determine if it has
changed and needs to be copied to the new backup. Without this
option, only the file's size, type, and modification date are
checked for differences. Using this option will make backups
take considerably longer."""))

    add_no_option(user_input, "whole-file")

    user_input.add_argument("--delete-on-error", action="store_true", help=format_help("""
If an error causes a backup to fail to complete, delete that
backup. If this option does not appear, then the incomplete
backup is left in place. Users may want to use this option
so that files that were not part of the failed backup do not
get copied anew during the next backup. NOTE: Individual files
not being copied or linked (e.g., for lack of permission) are
not errors, and will only be noted in the log."""))

    add_no_option(user_input, "delete-on-error")

    user_input.add_argument("--free-up", metavar="SPACE", help=format_help("""
Automatically delete old backups when space runs low on the
backup destination. The SPACE argument can be in one of two forms.
If the argument is a number followed by a percent sign (%%), then
the number is interpreted as a percent of the total storage space
of the destination. Old backups will be deleted until that
percentage of the destination storage space is free.

If the argument ends with one letter or one letter followed by
a 'B', then the number will be interpreted as a number of bytes.
Case does not matter, so all of the following specify 15 megabytes:
15MB, 15Mb, 15mB, 15mb, 15M, 15m. Old backups will be deleted until
at least that much space is free.

In either of the above cases, there should be no space between the
number and subsequent symbol.

No matter what, the most recent backup will not be deleted."""))

    user_input.add_argument("--delete-after", metavar="TIME", help=format_help("""
Delete backups if they are older than the time span in the argument.
The format of the argument is Nt, where N is a whole number and
t is a single letter: d for days, w for weeks, m for calendar months,
or y for calendar years. There should be no space between the number
and letter.

No matter what, the most recent backup will not be deleted."""))

    user_input.add_argument("-r", "--recover", help=format_help("""
Recover a file or folder from the backup. The user will be able
to pick which version to recover by choosing the backup date as
the source. If a file is being recovered, only backup dates where
the file was modified will be presented. If a folder is being
recovered, then all available backup dates will be options.
This option requires the -b option to specify which
backup location to search."""))

    user_input.add_argument("--list", metavar="DIRECTORY", help=format_help("""
Recover a file or folder in the directory specified by the argument
by first choosing what to recover from a list of everything that's
ever been backed up. If no argument is given, the current directory
is used. The backup location argument (-b) is required."""))

    user_input.add_argument("--force-copy", action="store_true", help=format_help("""
Copy all files instead of linking to files previous backups. The
new backup will contain new copies of all of the user's files,
so the backup location will require much more space than a normal
backup."""))

    add_no_option(user_input, "force-copy")

    user_input.add_argument("--debug", action="store_true", help=format_help("""
Log information on all action of a backup."""))

    add_no_option(user_input, "debug")

    default_log_file_name = Path.home()/"vintagebackup.log"
    user_input.add_argument("-l", "--log", default=default_log_file_name, help=format_help(f"""
Where to log the activity of this program. A file of the same
name will be written to the backup folder. The default is
{default_log_file_name.name} in the user's home folder. If no
log file is desired, use the file name NUL on Windows and
/dev/null on Linux, Macs, and similar."""))

    user_input.add_argument("--move-backup", metavar="NEW_BACKUP_LOCATION", help=format_help("""
Move a backup set to a new location. The value of this argument is the new location. The
--backup-folder option is required to specify the current location of the backup set, and one
of --move-count or --move-age is required to specify how many of the most recent backups to
move. Moving each dated backup will take just as long as a normal backup to move since the hard
links to previous backups will be recreated to preserve the space savings, so some planning is
needed when deciding how many backups should be moved."""))

    user_input.add_argument("--move-count", help=format_help("""
Specify the number of the most recent backups to move, or "all" if every backup should be moved
to the new location."""))

    user_input.add_argument("--move-age", help=format_help("""
Specify the maximum age of backups to move. See --delete-after for the time span format to use."""))

    user_input.add_argument("--move-since", help=format_help("""
Move all backups made on or after the specified date (YYYY-MM-DD)."""))

    command_line_options = sys.argv[1:] or ["--help"]
    command_line_args = user_input.parse_args(command_line_options)
    if command_line_args.config:
        file_options = read_configuation_file(command_line_args.config)
        args = user_input.parse_args(file_options + command_line_options)
    else:
        args = command_line_args

    action_count = choice_count(args.help, args.recover, args.list, args.move_backup)
    if action_count > 1:
        print("Up to one of these actions (--help, --recover, --list, --move-backup) "
              "may be performed at one time.")
        print("If none of these options are used, a backup will start,"
              " which requires the -u and -b parameters.")
        user_input.print_usage()
        sys.exit(1)

    if args.help:
        user_input.print_help()
        sys.exit(0)

    exit_code = 1
    delete_last_backup_on_error = False
    action = ""

    try:
        setup_log_file(logger, args.log)
        if toggle_is_set(args, "debug"):
            logger.setLevel(logging.DEBUG)
        logger.debug(args)
        if command_line_args.config:
            logger.info("=====================")
            logger.info("Reading configuration from file: "
                        + os.path.abspath(command_line_args.config))

        if args.recover:
            if not args.backup_folder:
                raise CommandLineError("Backup folder needed to recover file.")

            try:
                backup_folder = Path(args.backup_folder).resolve(strict=True)
            except FileNotFoundError:
                raise CommandLineError(f"Could not find backup folder: {args.backup_folder}")

            action = "recovery"
            recover_path(Path(args.recover).absolute(), backup_folder)
        elif args.list:
            if not args.backup_folder:
                raise CommandLineError("Backup folder needed to list backed up items.")

            try:
                backup_folder = Path(args.backup_folder).resolve(strict=True)
            except FileNotFoundError:
                raise CommandLineError(f"Could not find backup folder: {args.backup_folder}")
            action = "backup listing"
            search_directory = Path(args.list).resolve()
            chosen_recovery_path = search_backups(search_directory, backup_folder)
            recover_path(chosen_recovery_path, backup_folder)
        elif args.move_backup:
            try:
                old_backup_location = Path(args.backup_folder).resolve(strict=True)
            except FileNotFoundError:
                raise CommandLineError(f"Could not find backup folder: {args.backup_folder}")
            action = "move backups"
            new_backup_location = Path(args.move_backup).absolute()

            moving_choices = choice_count(args.move_count, args.move_age, args.move_since)
            if moving_choices != 1:
                print("Exactly one of --move-count, --move-age, or --move-since "
                      "must be used when moving backups.")
                user_input.print_usage()
                sys.exit(1)

            if args.move_count:
                backups_to_move = last_n_backups(old_backup_location, args.move_count)
            elif args.move_age:
                oldest_backup_date = parse_time_span_to_timepoint(args.move_age)
                backups_to_move = backups_since(oldest_backup_date, old_backup_location)
            else:
                assert args.move_since
                oldest_backup_date = datetime.datetime.strptime(args.move_since, "%Y-%m-%d")
                backups_to_move = backups_since(oldest_backup_date, old_backup_location)

            move_backups(old_backup_location, new_backup_location, backups_to_move)
        else:
            def path_or_none(arg: str | None) -> Path | None:
                """Create a Path instance if the input string is valid."""
                return Path(arg).absolute() if arg else None

            try:
                user_folder = Path(args.user_folder).resolve(strict=True)
            except FileNotFoundError:
                raise CommandLineError(f"Could not find users folder: {args.user_folder}")

            backup_folder = Path(args.backup_folder).absolute()

            action = "backup"
            delete_last_backup_on_error = toggle_is_set(args, "delete_on_error")
            create_new_backup(user_folder,
                              backup_folder,
                              path_or_none(args.exclude),
                              path_or_none(args.include),
                              toggle_is_set(args, "whole_file"),
                              toggle_is_set(args, "force_copy"))

            if args.free_up:
                delete_oldest_backups_for_space(backup_folder, args.free_up)

            if args.delete_after:
                delete_backups_older_than(backup_folder, args.delete_after)

        logger.info("")
        print_backup_storage_stats(args.backup_folder)
        exit_code = 0
    except CommandLineError as error:
        logger.error(error)
        logger.info("")
        user_input.print_usage()
    except Exception:
        if action:
            logger.error(f"An error prevented the {action} from completing.")
        else:
            logger.error("An error occurred before any action could take place.")
        logger.exception("Error:")
        if delete_last_backup_on_error and new_backup_directory_created:
            delete_last_backup(args.backup_folder)
        print_backup_storage_stats(args.backup_folder)
    finally:
        sys.exit(exit_code)
