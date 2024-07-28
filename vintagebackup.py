"""A backup utility that uses hardlinks to save space when making fulll backups."""
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
import textwrap
import math
import glob
from collections import Counter
from pathlib import Path
from typing import Callable, Any

backup_date_format = "%Y-%m-%d %H-%M-%S"

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.setLevel(logging.INFO)


class CommandLineError(ValueError):
    """An exception class to catch invalid command line parameters."""

    pass


storage_prefixes = ["", "k", "M", "G", "T", "P", "E", "Z", "Y", "R", "Q"]


def byte_units(size: float) -> str:
    """
    Display a number of bytes with four significant figures with byte units.

    >>> byte_units(12345)
    '12.35 kB'

    >>> byte_units(12)
    '12.00 B'
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
    year_pattern = "%Y"
    backup_pattern = backup_date_format

    def is_valid_directory(dir: os.DirEntry[str], pattern: str) -> bool:
        name = dir.name.split(" (")[0] if pattern == backup_pattern else dir.name
        try:
            datetime.datetime.strptime(name, pattern)
            return dir.is_dir(follow_symlinks=False)
        except ValueError:
            return False

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


def is_real_directory(path: Path) -> bool:
    """Return True if path is a directory and not a symlink."""
    return path.is_dir() and not path.is_symlink()


def backup_paths(user_folder: Path, filter_file: Path | None) -> list[tuple[Path, list[str]]]:
    """Return a list of all paths in a user's folder after filtering it with a filter file."""
    backup_set: set[Path] = set()
    for current_directory_name, dir_names, file_names in os.walk(user_folder):
        current_directory = Path(current_directory_name)
        backup_set.update(current_directory/name for name in file_names + dir_names
                          if not (current_directory/name).is_junction())

    original_backup_set = frozenset(backup_set)

    for line_number, sign, pattern in filter_file_patterns(user_folder, filter_file):
        path_count_before = len(backup_set)
        change_set: set[Path] = set()
        for filter_path_str in glob.iglob(str(pattern), include_hidden=True, recursive=True):
            filter_path = Path(filter_path_str)
            if is_real_directory(filter_path):
                change_set.update(filter(lambda p: p.is_relative_to(filter_path),
                                         original_backup_set))
            else:
                change_set.add(filter_path)

        if sign == "+":
            backup_set.update(change_set)
        else:
            backup_set.difference_update(change_set)
        path_count_after = len(backup_set)

        if path_count_before == path_count_after:
            logger.info(f"{filter_file}: line #{line_number} ({sign} {pattern}) had no effect.")

    backup_tree: dict[Path, list[str]] = {}
    for path in backup_set:
        if is_real_directory(path):
            backup_tree.setdefault(path, [])
        else:
            backup_tree.setdefault(path.parent, []).append(path.name)

    return sorted(backup_tree.items())


PATTERN_ENTRY = tuple[int, str, Path]


def filter_file_patterns(user_folder: Path, filter_file: Path | None) -> list[PATTERN_ENTRY]:
    """
    Read filter patterns from the given filter file.

    Parameters:
    user_folder: The base folder of the user's data.
    filter_file: The file containing filters to the data being backed up.

    Returns:
    A list of tuples of (line number, filter file line, path) where the path may contain glob
    wildcard characters.
    """
    if not filter_file:
        return []

    logger.info(f"Reading filter file: {filter_file}")
    with open(filter_file) as filters:
        entries: list[PATTERN_ENTRY] = []
        for line_number, line in enumerate(filters, 1):
            line = line.lstrip().rstrip("\n")
            if not line:
                continue
            sign = line[0]

            if sign not in "-+#":
                raise ValueError(f"Line #{line_number} ({line}): The first symbol "
                                 "of each line in the filter file must be -, +, or #.")

            if sign == "#":
                continue

            pattern = user_folder/line[1:].lstrip()
            if not pattern.is_relative_to(user_folder):
                raise ValueError(f"Line #{line_number} ({line}): Filter looks at paths "
                                 "outside user folder.")

            entries.append((line_number, sign, pattern))

    return entries


def get_user_location_record(backup_location: Path) -> Path:
    """Return the file that contains the user directory that is backed up at the given location."""
    return backup_location/"vintagebackup.source.txt"


def record_user_location(user_location: Path, backup_location: Path) -> None:
    """Write the user directory being backed up to a file in the base backup directory."""
    user_folder_record = get_user_location_record(backup_location)
    with open(user_folder_record, "w") as user_record:
        user_record.write(str(user_location.resolve(strict=True)) + "\n")


def backup_source(backup_location: Path) -> Path:
    """Read the user directory that was backed up to the given backup location."""
    user_folder_record = get_user_location_record(backup_location)
    with open(user_folder_record) as user_record:
        return Path(user_record.readline().rstrip("\n")).resolve()


def confirm_user_location_is_unchanged(user_data_location: Path, backup_location: Path) -> None:
    """
    Make sure the user directory being backed up is the same as the previous backup run.

    An exception will be thrown when attempting to back up a different user directory to the one
    that was backed up previously. Backing up multiple different directories to the same backup
    location negates the hard linking functionality.
    """
    try:
        recorded_user_folder = backup_source(backup_location)
        if not recorded_user_folder.samefile(user_data_location):
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


def separate_links(directory: Path, path_names: list[str]) -> tuple[list[str], list[str]]:
    """
    Separate regular files and folders from symlinks.

    Directories within the given directory are not traversed.

    Parameters:
    directory: The directory containing all the files.
    path_names: A list of names in the directory.

    Returns:
    Two lists: the first a list of regular files, the second a list of symlinks.
    """
    def is_link(name: str) -> bool:
        return (directory/name).is_symlink()

    return list(itertools.filterfalse(is_link, path_names)), list(filter(is_link, path_names))


def backup_directory(user_data_location: Path,
                     new_backup_path: Path,
                     last_backup_path: Path | None,
                     current_user_path: Path,
                     user_file_names: list[str],
                     examine_whole_file: bool,
                     action_counter: Counter[str]) -> None:
    """
    Backup the files in a subfolder in the user's directory.

    Parameters:
    user_data_location: The base directory that is being backed up
    new_backup_path: The base directory of the new dated backup
    last_backup_path: The base directory of the previous dated backup
    current_user_path: The user directory currently being walked through
    user_file_names: The names of files contained in the current_user_path
    examine_whole_file: Whether to examine file contents to check for changes since the last backup
    action_counter: A counter to track how many files have been linked, copied, or failed for both
    """
    if not current_user_path.is_dir():
        logger.warning(f"Folder disappeared during backup: {current_user_path}")
        return

    relative_path = current_user_path.relative_to(user_data_location)
    new_backup_directory = new_backup_path/relative_path
    new_backup_directory.mkdir(parents=True)
    previous_backup_directory = last_backup_path/relative_path if last_backup_path else None

    user_file_names, user_links = separate_links(current_user_path, user_file_names)
    matching, mismatching, errors = compare_to_backup(current_user_path,
                                                      previous_backup_directory,
                                                      user_file_names,
                                                      examine_whole_file)

    for file_name in matching:
        assert previous_backup_directory
        previous_backup = previous_backup_directory/file_name
        new_backup = new_backup_directory/file_name

        if create_hard_link(previous_backup, new_backup):
            action_counter["linked files"] += 1
            logger.debug(f"Linked {previous_backup} to {new_backup}")
        else:
            errors.append(file_name)

    for file_name in itertools.chain(mismatching, errors, user_links):
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
                      *,
                      filter_file: Path | None,
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
    if not user_data_location.is_dir():
        raise CommandLineError(f"The user folder path is not a folder: {user_data_location}")

    if not backup_location:
        raise CommandLineError("No backup destination was given.")

    if backup_location.is_relative_to(user_data_location):
        raise CommandLineError("Backup destination cannot be inside user's folder:"
                               f" User data: {user_data_location}"
                               f"; Backup location: {backup_location}")

    if filter_file and not filter_file.is_file():
        raise CommandLineError(f"Filter file not found: {filter_file}")

    backup_location.mkdir(parents=True, exist_ok=True)

    now = datetime.datetime.now()
    backup_date = now.strftime(backup_date_format)
    os_name = f"{platform.system()} {platform.release()}".strip()
    new_backup_path = backup_location/str(now.year)/f"{backup_date} ({os_name})"

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
    elif force_copy:
        logger.info("Copying everything.")
    else:
        logger.info("No previous backups. Copying everything.")

    logger.info("")
    logger.info(f"Reading file contents = {examine_whole_file}")

    action_counter: Counter[str] = Counter()
    paths_to_backup = backup_paths(user_data_location, filter_file)
    logger.info("Running backup ...")
    for current_user_path, user_file_names in paths_to_backup:
        backup_directory(user_data_location,
                         new_backup_path,
                         last_backup_path,
                         current_user_path,
                         user_file_names,
                         examine_whole_file,
                         action_counter)

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
    if log_file_path != os.devnull:
        log_file = logging.FileHandler(log_file_path, encoding="utf8")
        log_file_format = logging.Formatter(fmt="%(asctime)s %(levelname)s    %(message)s")
        log_file.setFormatter(log_file_format)
        logger.addHandler(log_file)


def search_backups(search_directory: Path,
                   backup_folder: Path,
                   choice: int | None = None) -> Path | None:
    """
    Decide which path to restore among all backups for all items in the given directory.

    The user will pick from a list of all files and folders in search_directory that have ever been
    backed up.

    Parameters:
    search_directory: The directory from which backed up files and folders will be listed
    backup_folder: The backup destination
    choice: Pre-selected choice of which file to recover (used for testing).

    Returns:
    The path to a file or folder that will then be searched for among backups.
    """
    if not is_real_directory(search_directory):
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

    if not all_paths:
        logger.info(f"No backups found for the folder {search_directory}")
        return None

    menu_list = sorted(all_paths)
    if choice is None:
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
    else:
        return search_directory/menu_list[choice][0]


def recover_path(recovery_path: Path, backup_location: Path, choice: int | None = None) -> None:
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
    choice: Pre-selected choice of which file to recover (used for testing).
    """
    try:
        user_data_location = backup_source(backup_location)
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

    if not unique_backups:
        logger.info(f"No backups found for {recovery_path}")
        return

    backup_choices = sorted(unique_backups.values())
    if choice is None:
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
    else:
        chosen_path = backup_choices[choice]

    recovered_path = recovery_path
    unique_id = 0
    while recovered_path.exists(follow_symlinks=False):
        unique_id += 1
        new_file_name = f"{recovery_path.stem}.{unique_id}{recovery_path.suffix}"
        recovered_path = recovery_path.parent/new_file_name

    logger.info(f"Copying {chosen_path} to {recovered_path}")
    if chosen_path.is_symlink() or chosen_path.is_file():
        shutil.copy2(chosen_path, recovered_path, follow_symlinks=False)
    else:
        shutil.copytree(chosen_path, recovered_path, symlinks=True)


def choose_backup(backup_folder: Path, choice: int | None) -> Path | None:
    """Choose a backup from a numbered list shown in a terminal."""
    backup_choices = all_backups(backup_folder)
    if not backup_choices:
        return None

    if choice is not None:
        return backup_choices[choice]

    number_column_size = len(str(len(backup_choices)))
    for choice, backup in enumerate(backup_choices, 1):
        backup_name = backup.relative_to(backup_folder)
        print(f"{choice:>{number_column_size}}: {backup_name}")

    while True:
        try:
            user_choice = int(input("Backup to restore (Ctrl-C to quit): "))
            if user_choice < 1:
                continue
            return backup_choices[user_choice - 1]
        except (ValueError, IndexError):
            pass


def delete_directory_tree(backup_path: Path) -> None:
    """Delete a single backup."""
    def remove_readonly(func: Callable[..., Any], path: str, _: Any) -> None:
        """
        Clear the readonly bit and reattempt the removal.

        Copied from https://docs.python.org/3/library/shutil.html#rmtree-example
        """
        os.chmod(path, stat.S_IWRITE)
        func(path)

    shutil.rmtree(backup_path, onexc=remove_readonly)


def delete_last_backup(backup_location: Path) -> None:
    """Delete the most recent backup."""
    last_backup_directory = find_previous_backup(backup_location)
    if last_backup_directory:
        logger.info(f"Deleting failed backup: {last_backup_directory}")
        delete_directory_tree(last_backup_directory)
    else:
        logger.info("No previous backup to delete")


def delete_oldest_backups_for_space(backup_location: Path, space_requirement: str) -> None:
    """
    Delete backups--starting with the oldest--until enough space is free on the backup destination.

    The most recent backup will never be deleted.

    Parameters:
    backup_location: The folder containing all backups
    space_requirement: The amount of space that should be free after deleting backups. This may be
    expressed in bytes ("MB", "GB", etc.) or as a percentage ("%") of the total storage space.
    """
    total_storage = shutil.disk_usage(backup_location).total
    free_storage_required = parse_storage_space(space_requirement, total_storage)

    if free_storage_required > shutil.disk_usage(backup_location).total:
        raise CommandLineError(f"Cannot free more storage ({byte_units(free_storage_required)})"
                               f" than exists at {backup_location} ({byte_units(total_storage)})")

    any_deletions = False
    backups = all_backups(backup_location)
    for backup in backups[:-1]:
        if shutil.disk_usage(backup_location).free > free_storage_required:
            break

        if not any_deletions:
            logger.info("")
            logger.info(f"Deleting old backups to free up {byte_units(free_storage_required)}.")

        logger.info(f"Deleting backup: {backup}")
        delete_directory_tree(backup)
        any_deletions = True

    if any_deletions:
        log_backup_deletions(backup_location)

    if shutil.disk_usage(backup_location).free < free_storage_required:
        logger.warning(f"Could not free up {byte_units(free_storage_required)} of storage"
                       " without deleting most recent backup.")


def parse_storage_space(space_requirement: str, total_storage: int) -> float:
    """
    Parse a string into a number of bytes of storage space.

    Parameters:
    space_requirement: A string indicating an amount of space, either as an absolute number of bytes
    or a percentage of the total storage. Byte units and prefixes are allowed. Percents require a
    percent sign.
    total_storage: The total storage space in bytes on the device. Used with percentage values.

    >>> parse_storage_space("152 kB", 0)
    152000.0

    >>> parse_storage_space("15%", 1000)
    150.0
    """
    space_text = "".join(space_requirement.lower().split())
    if space_text.endswith("%"):
        try:
            free_fraction_required = float(space_text[:-1])/100
        except ValueError:
            raise CommandLineError(f"Invalid percentage value: {space_requirement}")

        if free_fraction_required > 1:
            raise CommandLineError(f"Percent cannot be greater than 100: {space_requirement}")

        return total_storage*free_fraction_required
    elif space_text[-1].isalpha():
        space_text = space_text.rstrip('b')
        number, prefix = ((space_text[:-1], space_text[-1])
                          if space_text[-1].isalpha() else
                          (space_text, ""))

        try:
            multiplier: int = 1000**storage_prefixes.index(prefix)
            return float(number)*multiplier
        except ValueError:
            raise CommandLineError(f"Invalid storage space value: {space_requirement}")
    else:
        raise CommandLineError(f"Incorrect format of free-up space: {space_requirement}")


def parse_time_span_to_timepoint(time_span: str) -> datetime.datetime:
    """
    Parse a string representing a time span into a datetime representing a date that long ago.

    For example, if time_span is "6m", the result is a date six calendar months ago.

    time_span: A string consisting of a positive integer followed by a single letter: "d" for days,
    "w" for weeks, "m" for calendar months, and "y" for calendar years.

    >>> import datetime
    >>> today = datetime.date.today()
    >>> yesterday = today - datetime.timedelta(days=1)
    >>> yesterday_parse = parse_time_span_to_timepoint("1d")
    >>> yesterday == yesterday_parse.date()
    True
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

        return fix_end_of_month(new_year, new_month, now.day,
                                now.hour, now.minute, now.second, now.microsecond)
    elif letter == "y":
        return fix_end_of_month(now.year - number, now.month, now.day,
                                now.hour, now.minute, now.second, now.microsecond)
    else:
        raise CommandLineError(f"Invalid time (valid units: {list("dwmy")}): {time_span}")


def fix_end_of_month(year: int, month: int, day: int,
                     hour: int, minute: int, second: int, microsecond: int) -> datetime.datetime:
    """
    Fix day if it is past then end of the month (e.g., Feb. 31).

    >>> fix_end_of_month(2023, 2, 31, 0, 0, 0, 0)
    datetime.datetime(2023, 2, 28, 0, 0)

    >>> fix_end_of_month(2024, 2, 31, 0, 0, 0, 0)
    datetime.datetime(2024, 2, 29, 0, 0)

    >>> fix_end_of_month(2025, 4, 31, 0, 0, 0, 0)
    datetime.datetime(2025, 4, 30, 0, 0)
    """
    new_day = day
    while True:
        try:
            return datetime.datetime(year, month, new_day,
                                     hour, minute, second, microsecond)
        except ValueError:
            new_day -= 1


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
            logger.info("")
            logger.info("Deleting backups prior to"
                        f" {timestamp_to_keep.strftime('%Y-%m-%d %H:%M:%S')}.")

        logger.info(f"Deleting oldest backup: {backup}")
        delete_directory_tree(backup)
        try:
            year_folder = backup.parent
            year_folder.rmdir()
            logger.info(f"Deleted empty year folder {year_folder}")
        except OSError:
            pass
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
    move_count = len(backups_to_move)
    logger.info(f"Moving {move_count} {plural_noun(move_count, "backup")}")
    logger.info(f"from {old_backup_location}")
    logger.info(f"to   {new_backup_location}")

    for backup in backups_to_move:
        create_new_backup(backup,
                          new_backup_location,
                          filter_file=None,
                          examine_whole_file=False,
                          force_copy=False,
                          is_backup_move=True)

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


def verify_last_backup(user_folder: Path,
                       backup_folder: Path,
                       filter_file: Path | None,
                       result_folder: Path) -> None:
    """
    Verify the most recent backup by comparing with the users files.

    Parameters:
    user_folder: The source of the backed up data.
    backup_folder: The location of the backed up data.
    filter_file: The file that filters which files are backed up.
    result_folder: Where the results files will be saved.
    """
    confirm_user_location_is_unchanged(user_folder, backup_folder)
    last_backup_folder = find_previous_backup(backup_folder)

    if last_backup_folder is None:
        raise CommandLineError(f"No backups found in {backup_folder}.")

    logger.info(f"Verifying backup in {backup_folder} by comparing against {user_folder}")

    result_folder.mkdir(parents=True, exist_ok=True)
    prefix = datetime.datetime.now().strftime(backup_date_format)
    matching_file_name = result_folder/f"{prefix} matching files.txt"
    mismatching_file_name = result_folder/f"{prefix} mismatching files.txt"
    error_file_name = result_folder/f"{prefix} error files.txt"

    with (open(matching_file_name, "w", encoding="utf8") as matching_file,
          open(mismatching_file_name, "w", encoding="utf8") as mismatching_file,
          open(error_file_name, "w", encoding="utf8") as error_file):

        for file in (matching_file, mismatching_file, error_file):
            file.write(f"Comparison: {user_folder} <---> {backup_folder}\n")

        for directory, file_names in backup_paths(user_folder, filter_file):
            relative_directory = directory.relative_to(user_folder)
            backup_directory = last_backup_folder/relative_directory
            matches, mismatches, errors = filecmp.cmpfiles(directory,
                                                           backup_directory,
                                                           file_names,
                                                           shallow=False)

            def file_name_line(file_name: str) -> str:
                """Create a relative path for recording to a file."""
                return f"{relative_directory/file_name}\n"

            matching_file.writelines(map(file_name_line, matches))
            mismatching_file.writelines(map(file_name_line, mismatches))
            error_file.writelines(map(file_name_line, errors))


def restore_backup(dated_backup_folder: Path, user_folder: Path, *, delete_new_files: bool) -> None:
    """
    Return a user's folder to a previously backed up state.

    Existing files that were backed up will be overwritten with the backup.

    Parameters:
    backup_folder: The backup from which to restore files and folders
    user_folder: The folder that will be restored to a previous state.
    delete_new_files: Whether to delete files and folders that are not present in the backup.
    """
    for current_backup_folder, folder_names, file_names in os.walk(dated_backup_folder):
        current_backup_path = Path(current_backup_folder)
        current_user_path = user_folder/current_backup_path.relative_to(dated_backup_folder)
        current_user_path.mkdir(parents=True, exist_ok=True)

        for file_name in file_names:
            try:
                source = current_backup_path/file_name
                destination = current_user_path/file_name
                shutil.copy2(source, destination, follow_symlinks=False)
            except Exception as error:
                logger.warning(f"Could not restore {destination} from {source}: {error}")

        if delete_new_files:
            backed_up_paths = set(folder_names) | set(file_names)
            user_paths = set(entry.name for entry in os.scandir(current_user_path))
            for new_name in user_paths - backed_up_paths:
                new_path = current_user_path/new_name
                if is_real_directory(new_path):
                    delete_directory_tree(new_path)
                else:
                    new_path.unlink()


def last_n_backups(backup_location: Path, n: str | int) -> list[Path]:
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
        backup_folder = Path(backup_location)
        backups = all_backups(backup_folder)
        logger.info(f"Backups stored: {len(backups)}")
        logger.info(f"Earliest backup: {backups[0].name}")
    except Exception:
        pass


def read_configuation_file(config_file_name: str) -> list[str]:
    """Parse a configuration file into command line arguments."""
    arguments: list[str] = []

    with open(config_file_name) as file:
        for line in file:
            line = line.strip()
            if not line or line.startswith("#"):
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


def add_no_option(user_input: argparse.ArgumentParser | argparse._ArgumentGroup, name: str) -> None:
    """Add negating option for boolean command line arguments."""
    user_input.add_argument(f"--no-{name}", action="store_true", help=format_help(f"""
Disable the --{name} option. This is primarily used if "{name}" appears in a
configuration file. This option has priority even if --{name} is listed later."""))


def toggle_is_set(args: argparse.Namespace, name: str) -> bool:
    """Check that a boolean command line option --X has been selected and not negated by --no-X."""
    options = vars(args)
    return options[name] and not options[f"no_{name}"]


def path_or_none(arg: str | None) -> Path | None:
    """Create a Path instance if the input string is valid."""
    return Path(arg).absolute() if arg else None


def argument_parser() -> argparse.ArgumentParser:
    """Create the parser for command line arguments."""
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

Vintage Backup can also perform other operations besides backups. See the Actions section below for
more capabilities.

Technical notes:

- Symbolic links are not followed and are always copied as symbolic links.

- Windows junction points (soft links) are excluded by default. They may be added using a filter
file (see --filter below). In that case, all of the contents will be copied.

- If two files in the user's directory are hard-linked together, these files will be copied/linked
separately. The hard link is not preserved in the backup.

- If the user folder and the backup destination are on different drives or partitions with different
file systems (NTFS, ext4, APFS, etc.), hard links may not be created due to differences in how file
modification times are recorded. Using the --whole-file option may mitigate this, but backups will
take much more time."""))

    action_group = user_input.add_argument_group("Actions", format_text("""
The default action when vintage backups is run is to create a new backup. If one of the following
options are chosen, then that action is performed instead."""))

    only_one_action_group = action_group.add_mutually_exclusive_group()

    only_one_action_group.add_argument("-h", "--help", action="store_true", help=format_help("""
Show this help message and exit."""))

    only_one_action_group.add_argument("-r", "--recover", help=format_help("""
Recover a file or folder from the backup. The user will be able
to pick which version to recover by choosing the backup date as
the source. If a file is being recovered, only backup dates where
the file was modified will be presented. If a folder is being
recovered, then all available backup dates will be options.
This option requires the -b option to specify which
backup location to search."""))

    only_one_action_group.add_argument("--list", metavar="DIRECTORY", help=format_help("""
Recover a file or folder in the directory specified by the argument
by first choosing what to recover from a list of everything that's
ever been backed up. If no argument is given, the current directory
is used. The backup location argument (-b) is required."""))

    only_one_action_group.add_argument("--move-backup", metavar="NEW_BACKUP_LOCATION",
                                       help=format_help("""
Move a backup set to a new location. The value of this argument is the new location. The
--backup-folder option is required to specify the current location of the backup set, and one
of --move-count, --move-age, or --move-since is required to specify how many of the most recent
backups tomove. Moving each dated backup will take just as long as a normal backup to move since the
hard links to previous backups will be recreated to preserve the space savings, so some planning is
needed when deciding how many backups should be moved."""))

    only_one_action_group.add_argument("--verify", metavar="RESULT_DIR", help=format_help("""
Verify the latest backup by comparing them against the original files. The result of the comparison
will be placed in the folder RESULT_DIR. The result is three files: a list of files that match, a
list of files that do not match, and a list of files that caused errors during the comparison. The
arguments --user-folder and --backup-folder are required. If a filter file was used to create the
backup, then --filter should be supplied as well."""))

    only_one_action_group.add_argument("--restore", action="store_true", help=format_help("""
This action restores the user's folder to a previous, backed up state. Any existing user files that
have the same name as one in the backup will be overwritten. See the Restore Options section below
for required parameters."""))

    common_group = user_input.add_argument_group("Options needed for all actions")

    common_group.add_argument("-b", "--backup-folder", help=format_help("""
The destination of the backed up files. This folder will
contain a set of folders labeled by year, and each year's
folder will contain all of that year's backups."""))

    backup_group = user_input.add_argument_group("Options for backing up")

    backup_group.add_argument("-u", "--user-folder", help=format_help("""
The directory to be backed up. The contents of this
folder and all subfolders will be backed up recursively."""))

    backup_group.add_argument("-f", "--filter", metavar="FILTER_FILE_NAME", help=format_help("""
Filter the set of files that will be backed up. The value of this argument should be the name of
a text file that contains lines specifying what files to include or exclude. These may contain
wildcard characters like * and ? to allow for matching multiple file names.

Each line should begin with a minus (-), plus (+), or hash (#). Lines with minus signs specify
files and folders to exclude. Lines with plus signs specify files and folders to include. Lines
with hash signs are ignored. All included files must reside within the directory tree of the
--user-folder. For example, if backing up C:\\Users\\Alice Eaves Roberts, the following filter file:

    # Ignore AppData except Firefox
    - AppData
    + AppData/Roaming/Mozilla/Firefox/

will exclude everything in C:\\Users\\Alice Eaves Roberts\\AppData\\ except the
Roaming\\Mozilla\\Firefox subfolder. The order of the lines matters. If the - and + lines above
were reversed, the Firefox folder would be included and then excluded by the following - Appdata
line."""))

    backup_group.add_argument("-w", "--whole-file", action="store_true", help=format_help("""
Examine the entire contents of a file to determine if it has
changed and needs to be copied to the new backup. Without this
option, only the file's size, type, and modification date are
checked for differences. Using this option will make backups
take considerably longer."""))

    add_no_option(backup_group, "whole-file")

    backup_group.add_argument("--free-up", metavar="SPACE", help=format_help("""
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

    backup_group.add_argument("--delete-after", metavar="TIME", help=format_help("""
Delete backups if they are older than the time span in the argument.
The format of the argument is Nt, where N is a whole number and
t is a single letter: d for days, w for weeks, m for calendar months,
or y for calendar years. There should be no space between the number
and letter.

No matter what, the most recent backup will not be deleted."""))

    backup_group.add_argument("--force-copy", action="store_true", help=format_help("""
Copy all files instead of linking to files previous backups. The
new backup will contain new copies of all of the user's files,
so the backup location will require much more space than a normal
backup."""))

    add_no_option(backup_group, "force-copy")

    move_group = user_input.add_argument_group("Move backup options", format_text("""
Use exactly one of these options to specify which backups to move when using --move-backup."""))

    only_one_move_group = move_group.add_mutually_exclusive_group()

    only_one_move_group.add_argument("--move-count", help=format_help("""
Specify the number of the most recent backups to move, or "all" if every backup should be moved
to the new location."""))

    only_one_move_group.add_argument("--move-age", help=format_help("""
Specify the maximum age of backups to move. See --delete-after for the time span format to use."""))

    only_one_move_group.add_argument("--move-since", help=format_help("""
Move all backups made on or after the specified date (YYYY-MM-DD)."""))

    restore_group = user_input.add_argument_group("Restore Options", format_help("""
Exactly one of each of the following option pairs(--use-last-backup/--choose-backup and
--delete-new/--keep-new) is required when restoring a backup."""))

    choose_restore_backup_group = restore_group.add_mutually_exclusive_group()

    choose_restore_backup_group.add_argument("--last-backup", action="store_true",
                                             help=format_help("""
Restore from the most recent backup."""))

    choose_restore_backup_group.add_argument("--choose-backup", action="store_true",
                                             help=format_help("""
Choose which backup to restore from a list."""))

    restore_preservation_group = restore_group.add_mutually_exclusive_group()

    restore_preservation_group.add_argument("--delete-new", action="store_true",
                                            help=format_help("""
Delete any new files that are not in the backup."""))

    restore_preservation_group.add_argument("--keep-new", action="store_true", help=format_help("""
New files not in the backup will be preserved."""))

    other_group = user_input.add_argument_group("Other options")

    other_group.add_argument("-c", "--config", metavar="FILE_NAME", help=format_help(r"""
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

    other_group.add_argument("--debug", action="store_true", help=format_help("""
Log information on all action of a backup."""))

    add_no_option(other_group, "debug")

    default_log_file_name = Path.home()/"vintagebackup.log"
    other_group.add_argument("-l", "--log", default=default_log_file_name, help=format_help(f"""
Where to log the activity of this program. A file of the same
name will be written to the backup folder. The default is
{default_log_file_name.name} in the user's home folder. If no
log file is desired, use the file name {os.devnull}."""))

    # This argument is only used for testing.
    user_input.add_argument("--choice", help=argparse.SUPPRESS)

    return user_input


def main(argv: list[str]) -> int:
    """
    Start the main program.

    argv: A list of command line arguments as from sys.argv
    """
    if argv and argv[0] == sys.argv[0]:
        argv = argv[1:]

    command_line_options = argv or ["--help"]
    user_input = argument_parser()
    command_line_args = user_input.parse_args(command_line_options)
    if command_line_args.config:
        file_options = read_configuation_file(command_line_args.config)
        args = user_input.parse_args(file_options + command_line_options)
    else:
        args = command_line_args

    if args.help:
        user_input.print_help()
        return 0

    exit_code = 1
    action = ""

    try:
        setup_log_file(logger, args.log)
        if toggle_is_set(args, "debug"):
            logger.setLevel(logging.DEBUG)
        logger.debug(args)

        if args.recover:
            if not args.backup_folder:
                raise CommandLineError("Backup folder needed to recover file.")

            try:
                backup_folder = Path(args.backup_folder).resolve(strict=True)
            except FileNotFoundError:
                raise CommandLineError(f"Could not find backup folder: {args.backup_folder}")

            action = "recovery"
            choice = None if args.choice is None else int(args.choice)
            print_run_title(command_line_args, "Recovering from backups")
            recover_path(Path(args.recover).resolve(), backup_folder, choice)
        elif args.list:
            if not args.backup_folder:
                raise CommandLineError("Backup folder needed to list backed up items.")

            try:
                backup_folder = Path(args.backup_folder).resolve(strict=True)
            except FileNotFoundError:
                raise CommandLineError(f"Could not find backup folder: {args.backup_folder}")
            action = "backup listing"
            search_directory = Path(args.list).resolve()
            print_run_title(command_line_args, "Listing recoverable files")
            chosen_recovery_path = search_backups(search_directory, backup_folder)
            if chosen_recovery_path is not None:
                recover_path(chosen_recovery_path, backup_folder)
        elif args.move_backup:
            if not args.backup_folder:
                raise CommandLineError("Current backup folder location (--backup-folder) needed.")

            try:
                old_backup_location = Path(args.backup_folder).resolve(strict=True)
            except FileNotFoundError:
                raise CommandLineError(f"Could not find backup folder: {args.backup_folder}")

            action = "backup location move"
            new_backup_location = Path(args.move_backup).absolute()

            if args.move_count:
                backups_to_move = last_n_backups(old_backup_location, args.move_count)
            elif args.move_age:
                oldest_backup_date = parse_time_span_to_timepoint(args.move_age)
                backups_to_move = backups_since(oldest_backup_date, old_backup_location)
            elif args.move_since:
                oldest_backup_date = datetime.datetime.strptime(args.move_since, "%Y-%m-%d")
                backups_to_move = backups_since(oldest_backup_date, old_backup_location)
            else:
                raise CommandLineError("Exactly one of --move-count, --move-age, or --move-since "
                                       "must be used when moving backups.")

            print_run_title(command_line_args, "Moving backups")
            move_backups(old_backup_location, new_backup_location, backups_to_move)
        elif args.verify:
            try:
                user_folder = Path(args.user_folder).resolve(strict=True)
            except FileNotFoundError:
                raise CommandLineError(f"Could not find users folder: {args.user_folder}")

            try:
                backup_folder = Path(args.backup_folder).resolve(strict=True)
            except FileNotFoundError:
                raise CommandLineError(f"Could not find backup location: {args.backup_folder}")

            action = "verification"
            filter_file = path_or_none(args.filter)
            result_folder = path_or_none(args.verify)
            assert result_folder is not None
            print_run_title(command_line_args, "Verifying last backup")
            verify_last_backup(user_folder, backup_folder, filter_file, result_folder)
        elif args.restore:
            try:
                user_folder = Path(args.user_folder).resolve(strict=True)
            except FileNotFoundError:
                raise CommandLineError(f"Could not find users folder: {args.user_folder}")

            try:
                backup_folder = Path(args.backup_folder).resolve(strict=True)
            except FileNotFoundError:
                raise CommandLineError(f"Could not find backup location: {args.backup_folder}")

            confirm_user_location_is_unchanged(user_folder, backup_folder)

            if not args.delete_new and not args.keep_new:
                raise CommandLineError("One of the following are required: "
                                       "--delete-new or --keep-new")
            delete_new_files = bool(args.delete_new)

            if not args.last_backup and not args.choose_backup:
                raise CommandLineError("One of the following are required: "
                                       "--use-last-backup or --choose-backup")
            choice = None if args.choice is None else int(args.choice)
            restore_source = (find_previous_backup(backup_folder)
                              if args.last_backup else
                              choose_backup(backup_folder, choice))

            if not restore_source:
                raise CommandLineError(f"No backups found in {backup_folder}")

            action = "restoration"
            restore_backup(restore_source, user_folder, delete_new_files=delete_new_files)
        else:
            if not args.user_folder:
                raise CommandLineError("User's folder not specified.")

            try:
                user_folder = Path(args.user_folder).resolve(strict=True)
            except FileNotFoundError:
                raise CommandLineError(f"Could not find user's folder: {args.user_folder}")

            if not args.backup_folder:
                raise CommandLineError("Backup folder not specified.")

            backup_folder = Path(args.backup_folder).absolute()

            action = "backup"
            print_run_title(command_line_args, "Starting new backup")
            create_new_backup(user_folder,
                              backup_folder,
                              filter_file=path_or_none(args.filter),
                              examine_whole_file=toggle_is_set(args, "whole_file"),
                              force_copy=toggle_is_set(args, "force_copy"))

            if args.free_up:
                action = "deletions for freeing up space"
                delete_oldest_backups_for_space(backup_folder, args.free_up)

            if args.delete_after:
                action = "deletion of old backups"
                delete_backups_older_than(backup_folder, args.delete_after)

            logger.info("")
            print_backup_storage_stats(args.backup_folder)

        exit_code = 0
    except CommandLineError as error:
        if __name__ == "__main__":
            user_input.print_usage()
        logger.error(error)
    except Exception:
        if action:
            logger.error(f"An error prevented the {action} from completing.")
        else:
            logger.error("An error occurred before any action could take place.")
        logger.exception("Error:")
        if __name__ == "__main__":
            print_backup_storage_stats(args.backup_folder)
    finally:
        return exit_code


def print_run_title(command_line_args: argparse.Namespace, action_title: str) -> None:
    """Print the action taking place."""
    logger.info("")
    divider = "="*(len(action_title) + 2)
    logger.info(divider)
    logger.info(f" {action_title}")
    logger.info(divider)
    logger.info("")

    if command_line_args.config:
        logger.info("Reading configuration from file: "
                    + os.path.abspath(command_line_args.config))
        logger.info("")


if __name__ == "__main__":
    sys.exit(main(sys.argv))
