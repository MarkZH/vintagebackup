"""A backup utility that uses hardlinks to save space when making full backups."""

import os
import shutil
import datetime
import platform
import argparse
import sys
import logging
import filecmp
import stat
import textwrap
import math
import random
import io
from collections import Counter
from collections.abc import Callable, Iterator, Iterable
from pathlib import Path
from typing import Any, cast

backup_date_format = "%Y-%m-%d %H-%M-%S"

logger = logging.getLogger(__name__)
logger.addHandler(logging.FileHandler(os.devnull))
logger.setLevel(logging.INFO)


class CommandLineError(ValueError):
    """An exception class to catch invalid command line parameters."""


class ConcurrencyError(RuntimeError):
    """An exception thrown when another process is using the same backup location."""


class Backup_Lock:
    """
    Lock out other Vintage Backup instances from accessing the same backup location.

    This class should be used as a context manager like so:
    ```
    with Lock_File(backup_path, "backup"):
        # Code that uses backup path
    ```
    """

    def __init__(self, backup_location: Path, operation: str) -> None:
        """Set up the lock."""
        self.lock_file_path = backup_location/"vintagebackup.lock"
        self.pid = str(os.getpid())
        self.operation = operation

    def __enter__(self) -> None:
        """
        Attempt to take possession of the file lock.

        If unsuccessful, a ConcurrencyError is raised.
        """
        while not self.acquire_lock():
            try:
                other_pid, other_operation = self.read_lock_data()
            except FileNotFoundError:
                continue

            raise ConcurrencyError(
                f"Vintage Backup already running {other_operation} on "
                f"{self.lock_file_path.parent} (PID {other_pid})")

    def __exit__(self, *_: object) -> None:
        """Release the file lock."""
        self.lock_file_path.unlink()

    def acquire_lock(self) -> bool:
        """
        Attempt to create the lock file.

        Returns whether locking was successful.
        """
        try:
            self.create_lock()
            return True
        except FileExistsError:
            return False

    def create_lock(self) -> None:
        """Write PID and operation to the lock file."""
        with self.lock_file_path.open("x", encoding="utf8") as lock_file:
            lock_file.write(f"{self.pid}\n")
            lock_file.write(f"{self.operation}\n")

    def read_lock_data(self) -> tuple[str, str]:
        """Get all data from lock file."""
        with self.lock_file_path.open(encoding="utf8") as lock_file:
            pid = lock_file.readline().strip()
            operation = lock_file.readline().strip()
            return (pid, operation)


storage_prefixes = ["", "k", "M", "G", "T", "P", "E", "Z", "Y", "R", "Q"]


def byte_units(size: float) -> str:
    """
    Display a number of bytes with four significant figures with byte units.

    >>> byte_units(12345)
    '12.35 kB'

    >>> byte_units(12)
    '12.00 B'
    """
    prefix_step = 1000
    for index in range(len(storage_prefixes)):
        prefix_size = prefix_step**index
        size_in_units = size/prefix_size
        if size_in_units < prefix_step:
            break

    prefix = storage_prefixes[index]
    decimal_digits = 4 - math.floor(math.log10(size_in_units) + 1)
    return f"{size_in_units:.{decimal_digits}f} {prefix}B"


def all_backups(backup_location: Path) -> list[Path]:
    """Return a sorted list of all backups at the given location."""

    def is_valid_directory(date_folder: Path) -> bool:
        try:
            year = datetime.datetime.strptime(date_folder.parent.name, "%Y").year
            date = datetime.datetime.strptime(date_folder.name, backup_date_format)
            return year == date.year and is_real_directory(date_folder)
        except ValueError:
            return False

    all_backup_list: list[Path] = []
    for year_folder in filter(is_real_directory, backup_location.iterdir()):
        all_backup_list.extend(filter(is_valid_directory, year_folder.iterdir()))

    return sorted(all_backup_list)


def find_previous_backup(backup_location: Path) -> Path | None:
    """Return the most recent backup at the given location."""
    try:
        return all_backups(backup_location)[-1]
    except IndexError:
        return None


def is_real_directory(path: Path) -> bool:
    """Return True if path is a directory and not a symlink."""
    return path.is_dir(follow_symlinks=False)


class Backup_Set:
    """Generate the list of all paths to be backed up after filtering."""

    def __init__(self, user_folder: Path, filter_file: Path | None) -> None:
        """
        Prepare the path generator by parsing the filter file.

        :param user_folder: The folder to be backed up.
        :param filter_file: The path of the filter file that edits the paths to backup.
        """
        self.entries: list[tuple[int, str, Path]] = []
        self.lines_used: set[int] = set()
        self.user_folder = user_folder
        self.filter_file = filter_file

        if not filter_file:
            return

        with filter_file.open(encoding="utf8") as filters:
            logger.info(f"Filtering items according to {filter_file} ...")
            for line_number, line_raw in enumerate(filters, 1):
                line = line_raw.strip()
                if not line:
                    continue
                sign = line[0]

                if sign not in "-+#":
                    raise ValueError(
                        f"Line #{line_number} ({line}): The first symbol "
                        "of each line in the filter file must be -, +, or #.")

                if sign == "#":
                    continue

                pattern = user_folder/line[1:].strip()
                if not pattern.is_relative_to(user_folder):
                    raise ValueError(
                        f"Line #{line_number} ({line}): Filter looks at paths outside user folder.")

                logger.debug(f"Filter added: {line} --> {sign} {pattern}")
                self.entries.append((line_number, sign, pattern))

    def __iter__(self) -> Iterator[tuple[Path, list[str]]]:
        """Create the iterator that yields the paths to backup."""
        for current_directory, _, files in self.user_folder.walk():
            good_files = list(filter(self.passes, (current_directory/file for file in files)))
            if good_files:
                yield (current_directory, [file.name for file in good_files])

        self.log_unused_lines()

    def passes(self, path: Path) -> bool:
        """Determine if a path should be included in the backup according to the filter file."""
        is_included = not path.is_junction()
        for line_number, sign, pattern in self.entries:
            should_include = (sign == "+")
            if is_included == should_include or not path.full_match(pattern):
                continue

            self.lines_used.add(line_number)
            is_included = should_include
            logger.debug(
                "File: %s %s by line %d: %s %s",
                path,
                "included" if is_included else "excluded",
                line_number,
                sign,
                pattern)

        return is_included

    def log_unused_lines(self) -> None:
        """Warn the user if any of the lines in the filter file had no effect on the backup."""
        for line_number, sign, pattern in self.entries:
            if line_number not in self.lines_used:
                logger.info(
                    f"{self.filter_file}: line #{line_number} ({sign} {pattern}) had no effect.")


def get_user_location_record(backup_location: Path) -> Path:
    """Return the file that contains the user directory that is backed up at the given location."""
    return backup_location/"vintagebackup.source.txt"


def record_user_location(user_location: Path, backup_location: Path) -> None:
    """Write the user directory being backed up to a file in the base backup directory."""
    user_folder_record = get_user_location_record(backup_location)
    resolved_user_location = absolute_path(user_location, strict=True)
    logger.debug(f"Writing {resolved_user_location} to {user_folder_record}")
    user_folder_record.write_text(f"{resolved_user_location}\n", encoding="utf8")


def backup_source(backup_location: Path) -> Path:
    """Read the user directory that was backed up to the given backup location."""
    user_folder_record = get_user_location_record(backup_location)
    return absolute_path(user_folder_record.read_text(encoding="utf8").removesuffix("\n"))


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
            raise RuntimeError(
                "Previous backup stored a different user folder."
                f" Previously: {absolute_path(recorded_user_folder)};"
                f" Now: {absolute_path(user_data_location)}")
    except FileNotFoundError:
        # This is probably the first backup, hence no user folder record.
        pass


def shallow_stats(stats: os.stat_result) -> tuple[int, int, int]:
    """
    Return simple file information for quicker checks for file changes since the last bacukp.

    When not inspecting file contents, only look at the file size, type, and modification time--in
    that order.

    :param stats: File information retrieved from a DirEntry.stat() call.
    """
    return (stats.st_size, stat.S_IFMT(stats.st_mode), stats.st_mtime_ns)


def random_filter(probability: float) -> Callable[[Any], bool]:
    """Create a filter that chooses items with the given probability."""

    def actual_random_filter(_: Any) -> bool:
        return random.random() < probability

    return actual_random_filter


def compare_to_backup(
        user_directory: Path,
        backup_directory: Path | None,
        file_names: list[str],
        *,
        examine_whole_file: bool,
        copy_probability: float) -> tuple[list[str], list[str]]:
    """
    Sort a list of files according to whether they will be hard-linked or copied.

    :param user_directory: The subfolder of the user's data currently being walked through
    :param backup_directory: The backup folder that corresponds with the user_directory
    :param file_names: A list of files in the user directory.
    :param examine_whole_file: Whether the contents of the file should be examined, or just file
    attributes.
    :param copy_probability: Instead of hard-linking a file that hasn't changed since the last
    backup, copy it anyway with a given probability.

    The file names will be sorted into two lists and returned in this order: (1) matching files
    that will be hard-linked, (2) files that will be copied due to being new, changed, unreadable,
    or randomly chosen for copying. Symbolic links will be put in the second list for copying.
    """
    if not backup_directory:
        return [], file_names

    file_names, links = separate_links(user_directory, file_names)
    comparison_function = deep_comparison if examine_whole_file else shallow_comparison
    matches, mismatches, errors = comparison_function(user_directory, backup_directory, file_names)
    move_to_errors, matches = separate(matches, random_filter(copy_probability))
    errors.extend(move_to_errors)
    errors.extend(links)

    return matches, mismatches + errors


def deep_comparison(
        user_directory: Path,
        backup_directory: Path,
        file_names: list[str]) -> tuple[list[str], list[str], list[str]]:
    """Inspect file contents to determine if files match the most recent backup."""
    return filecmp.cmpfiles(user_directory, backup_directory, file_names, shallow=False)


def shallow_comparison(
        user_directory: Path,
        backup_directory: Path,
        file_names: list[str]) -> tuple[list[str], list[str], list[str]]:
    """Decide which files match the previous backup based on quick stat information."""

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
            file_set = matches if user_file_stats == backup_file_stats else mismatches
            file_set.append(file_name)
        except Exception:
            errors.append(file_name)

    return matches, mismatches, errors


def create_hard_link(previous_backup: Path, new_backup: Path) -> bool:
    """
    Create a hard link between unchanged backup files.

    Return True if successful, False if linking failed.
    """
    try:
        new_backup.hardlink_to(previous_backup)
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

    :param directory: The directory containing all the files.
    :param path_names: A list of names in the directory.

    :returns Two lists: the first a list of regular files, the second a list of symlinks.
    """

    def is_not_link(name: str) -> bool:
        return not (directory/name).is_symlink()

    return separate(path_names, is_not_link)


def separate[T](items: Iterable[T], predicate: Callable[[T], bool]) -> tuple[list[T], list[T]]:
    """
    Separate a sequence of items into two lists according to a predicate.

    :param items: A sequence of items to be separated.
    :param predicate: A function returning True or False for each item.
    :returns: Two lists. The first list are items where the predicate is True, the second where the
    predicate is False.
    """
    true_items: list[T] = []
    false_items: list[T] = []
    for item in items:
        destination = true_items if predicate(item) else false_items
        destination.append(item)
    return true_items, false_items


def backup_directory(
        user_data_location: Path,
        new_backup_path: Path,
        last_backup_path: Path | None,
        current_user_path: Path,
        user_file_names: list[str],
        action_counter: Counter[str],
        *,
        examine_whole_file: bool,
        copy_probability: float) -> None:
    """
    Backup the files in a subfolder in the user's directory.

    :param user_data_location: The base directory that is being backed up
    :param new_backup_path: The base directory of the new dated backup
    :param last_backup_path: The base directory of the previous dated backup
    :param current_user_path: The user directory currently being walked through
    :param user_file_names: The names of files contained in the current_user_path
    :param examine_whole_file: Whether to examine file contents to check for changes since the last
    backup
    :param copy_probability: Probability of copying a file when it would normally be hard-linked
    :param action_counter: A counter to track how many files have been linked, copied, or failed for
    both
    """
    relative_path = current_user_path.relative_to(user_data_location)
    new_backup_directory = new_backup_path/relative_path
    new_backup_directory.mkdir(parents=True)
    previous_backup_directory = last_backup_path/relative_path if last_backup_path else None
    files_to_link, files_to_copy = compare_to_backup(
        current_user_path,
        previous_backup_directory,
        user_file_names,
        examine_whole_file=examine_whole_file,
        copy_probability=copy_probability)

    for file_name in files_to_link:
        previous_backup = cast(Path, previous_backup_directory)/file_name
        new_backup = new_backup_directory/file_name

        if create_hard_link(previous_backup, new_backup):
            action_counter["linked files"] += 1
            logger.debug(f"Linked {previous_backup} to {new_backup}")
        else:
            files_to_copy.append(file_name)

    for file_name in files_to_copy:
        new_backup_file = new_backup_directory/file_name
        user_file = current_user_path/file_name
        try:
            shutil.copy2(user_file, new_backup_file, follow_symlinks=False)
            action_counter["copied files"] += 1
            logger.debug(f"Copied {user_file} to {new_backup_file}")
        except Exception as error:
            logger.warning(f"Could not copy {user_file} ({error})")
            action_counter["failed copies"] += 1


def backup_name(backup_datetime: datetime.datetime | str | None) -> Path:
    """Create the name and relative path for the new dated backup."""
    now = (
        datetime.datetime.strptime(backup_datetime, backup_date_format)
        if isinstance(backup_datetime, str)
        else (backup_datetime or datetime.datetime.now()))
    return Path(str(now.year))/now.strftime(backup_date_format)


def create_new_backup(
        user_data_location: Path,
        backup_location: Path,
        *,
        filter_file: Path | None,
        examine_whole_file: bool,
        force_copy: bool,
        copy_probability: float,
        timestamp: datetime.datetime | str | None,
        is_backup_move: bool = False) -> None:
    """
    Create a new dated backup.

    :param user_data_location: The folder containing the data to be backed up
    :param backup_location: The base directory of the backup destination. This directory should
    already exist.
    :param filter_file: A file containg a list of path glob patterns to exclude/include from the
    backup
    :param examine_whole_file: Whether to examine file contents to check for changes since the last
    backup
    :param force_copy: Whether to always copy files, regardless of whether a previous backup exists.
    :param max_average_hard_links: How many times on average a file will be hardlinked before being
    copied.
    :param timestamp: Manually set timestamp of new backup. Used for debugging.
    :param is_backup_move: Used to customize log messages when moving a backup to a new location.
    """
    check_paths_for_validity(user_data_location, backup_location, filter_file)

    new_backup_path = backup_location/backup_name(timestamp)
    staging_backup_path = backup_staging_folder(backup_location)
    if staging_backup_path.exists():
        logger.info("There is a staging folder leftover from previous incomplete backup.")
        logger.info(f"Deleting {staging_backup_path} ...")
        delete_directory_tree(staging_backup_path)

    confirm_user_location_is_unchanged(user_data_location, backup_location)
    record_user_location(user_data_location, backup_location)

    if is_backup_move:
        logger.info(f"Original backup  : {user_data_location}")
        logger.info(f"Temporary backup : {new_backup_path}")
    else:
        logger.info(f"User's data      : {user_data_location}")
        logger.info(f"Backup location  : {new_backup_path}")
    logger.info(f"Staging area     : {staging_backup_path}")

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
    paths_to_backup = Backup_Set(user_data_location, filter_file)
    logger.info("Running backup ...")
    for current_user_path, user_file_names in paths_to_backup:
        backup_directory(
            user_data_location,
            staging_backup_path,
            last_backup_path,
            current_user_path,
            user_file_names,
            action_counter,
            examine_whole_file=examine_whole_file,
            copy_probability=copy_probability)

    if staging_backup_path.is_dir():
        new_backup_path.parent.mkdir(parents=True, exist_ok=True)
        staging_backup_path.rename(new_backup_path)

    report_backup_file_counts(action_counter)


def backup_staging_folder(backup_location: Path) -> Path:
    """Get the name of the staging folder for new backups."""
    return backup_location/"Staging"


def report_backup_file_counts(action_counter: Counter[str]) -> None:
    """Log the number of files that were backed up, hardlinked, copied, and failed to copy."""
    logger.info("")
    total_files = sum(
        count for action, count in action_counter.items() if not action.startswith("failed"))
    action_counter["Backed up files"] = total_files
    name_column_size = max(map(len, action_counter))
    count_column_size = len(str(max(action_counter.values())))
    for action, count in action_counter.items():
        logger.info(f"{action.capitalize():<{name_column_size}} : {count:>{count_column_size}}")

    if total_files == 0:
        logger.warning("No files were backed up!")


def check_paths_for_validity(
        user_data_location: Path,
        backup_location: Path,
        filter_file: Path | None) -> None:
    """Check the given paths for validity and raise an exception for improper inputs."""
    if not user_data_location.is_dir():
        raise CommandLineError(f"The user folder path is not a folder: {user_data_location}")

    if backup_location.exists() and not backup_location.is_dir():
        raise CommandLineError("Backup location exists but is not a folder.")

    if backup_location.is_relative_to(user_data_location):
        raise CommandLineError(
            "Backup destination cannot be inside user's folder:"
            f" User data: {user_data_location}"
            f"; Backup location: {backup_location}")

    if filter_file and not filter_file.is_file():
        raise CommandLineError(f"Filter file not found: {filter_file}")


def setup_log_file(logger: logging.Logger, log_file_path: str) -> None:
    """Set up logging to write to a file."""
    if log_file_path != os.devnull:
        log_file = logging.FileHandler(log_file_path, encoding="utf8")
        log_file_format = logging.Formatter(fmt="%(asctime)s %(levelname)s    %(message)s")
        log_file.setFormatter(log_file_format)
        logger.addHandler(log_file)


def search_backups(
        search_directory: Path,
        backup_folder: Path,
        operation: str,
        choice: int | None = None) -> Path | None:
    """
    Decide which path to restore among all backups for all items in the given directory.

    The user will pick from a list of all files and folders in search_directory that have ever been
    backed up.

    :param search_directory: The directory from which backed up files and folders will be listed
    :param backup_folder: The backup destination
    :param choice: Pre-selected choice of which file to recover (used for testing).

    :returns Path: The path to a file or folder that will then be searched for among backups.
    """
    target_relative_path = directory_relative_to_backup(search_directory, backup_folder)

    all_paths: set[tuple[str, str]] = set()
    for backup in all_backups(backup_folder):
        backup_search_directory = backup/target_relative_path
        try:
            all_paths.update(
                (item.name, classify_path(item)) for item in backup_search_directory.iterdir())
        except FileNotFoundError:
            continue

    if not all_paths:
        logger.info(f"No backups found for the folder {search_directory}")
        return None

    menu_list = sorted(all_paths)
    if choice is None:
        menu_choices = [f"{name} ({path_type})" for (name, path_type) in menu_list]
        choice = choose_from_menu(menu_choices, f"Which path for {operation}")

    return search_directory/menu_list[choice][0]


def directory_relative_to_backup(search_directory: Path, backup_folder: Path) -> Path:
    """Return a path to a user's folder relative to the backups folder."""
    if not is_real_directory(search_directory):
        raise CommandLineError(f"The given search path is not a directory: {search_directory}")

    return path_relative_to_backups(search_directory, backup_folder)


def recover_path(recovery_path: Path, backup_location: Path, choice: int | None = None) -> None:
    """
    Decide which version of a file to restore to its previous location.

    The user will be presented with a list of backups that contain different versions of the file
    or folder. Any backup that contains a hard-linked copy of a file will be skipped. After the
    user selects a backup date, the file or folder from that backup will be copied to the
    corresponding location in the user's data. The copy from the backup will be renamed with a
    number so as to not overwrite any existing file with the same name.

    :param recovery_path: The file or folder that is to be restored.
    :param backup_location: The folder containing all backups.
    :param choice: Pre-selected choice of which file to recover (used for testing).
    """
    recovery_relative_path = path_relative_to_backups(recovery_path, backup_location)
    unique_backups: dict[int, Path] = {}
    for backup in all_backups(backup_location):
        path = backup/recovery_relative_path
        if path.exists(follow_symlinks=False):
            inode = path.stat(follow_symlinks=False).st_ino
            unique_backups.setdefault(inode, path)

    if not unique_backups:
        logger.info(f"No backups found for {recovery_path}")
        return

    backup_choices = sorted(unique_backups.values())
    if choice is None:
        menu_choices: list[str] = []
        for backup_copy in backup_choices:
            backup_date = backup_copy.relative_to(backup_location).parts[1]
            path_type = classify_path(backup_copy)
            menu_choices.append(f"{backup_date} ({path_type})")
        choice = choose_from_menu(menu_choices, "Version to recover")
    chosen_path = backup_choices[choice]

    recovered_path = unique_path_name(recovery_path)
    logger.info(f"Copying {chosen_path} to {recovered_path}")
    if is_real_directory(chosen_path):
        shutil.copytree(chosen_path, recovered_path, symlinks=True)
    else:
        shutil.copy2(chosen_path, recovered_path, follow_symlinks=False)


def unique_path_name(destination_path: Path) -> Path:
    """
    Create a unique name for a path if something already exists at that path.

    If there is nothing at the destination path, it is returned unchanged. Otherwise, a number will
    be inserted between the name and suffix (if any) to prevent clobbering any existing files or
    folders.

    :param destination_path: The path that will be modified if something already exists there.
    """
    unique_path = destination_path
    unique_id = 0
    while unique_path.exists(follow_symlinks=False):
        unique_id += 1
        new_path_name = f"{destination_path.stem}.{unique_id}{destination_path.suffix}"
        unique_path = destination_path.parent/new_path_name
    return unique_path


def path_relative_to_backups(user_path: Path, backup_location: Path) -> Path:
    """Return a path to a user's file or folder relative to the backups folder."""
    try:
        user_data_location = backup_source(backup_location)
    except FileNotFoundError:
        raise CommandLineError(f"No backups found at {backup_location}") from None

    try:
        return user_path.relative_to(user_data_location)
    except ValueError:
        raise CommandLineError(
            f"{user_path} is not contained in the backup set "
            f"{backup_location}, which contains {user_data_location}.") from None


def choose_from_menu(menu_choices: list[str], prompt: str) -> int:
    """
    Let user choose from options presented a numbered list in a terminal.

    :param menu_choices: List of choices
    :param prompt: Message to show user prior to the prompt for a choice.

    :returns int: The returned number is an index into the input list. The interface has the user
    choose a number from 1 to len(menu_list), but returns a number from 0 to len(menu_list) - 1.
    """
    number_column_size = len(str(len(menu_choices)))
    for number, choice in enumerate(menu_choices, 1):
        print(f"{number:>{number_column_size}}: {choice}")

    while True:
        try:
            user_choice = int(input(f"{prompt} ({cancel_key()} to quit): "))
            if 1 <= user_choice <= len(menu_choices):
                return user_choice - 1
        except ValueError:
            pass

        print(f"Enter a number from 1 to {len(menu_choices)}")


def cancel_key() -> str:
    """Return string describing the key combination that emits a SIGINT."""
    action_key = "Cmd" if platform.system() == "Darwin" else "Ctrl"
    return f"{action_key}-C"


def choose_backup(backup_folder: Path, choice: int | None) -> Path | None:
    """Choose a backup from a numbered list shown in a terminal."""
    backup_choices = all_backups(backup_folder)
    if not backup_choices:
        return None

    if choice is not None:
        return backup_choices[choice]

    menu_choices = [str(backup.relative_to(backup_folder)) for backup in backup_choices]
    return backup_choices[choose_from_menu(menu_choices, "Backup to restore")]


def delete_directory_tree(backup_path: Path) -> None:
    """Delete a single backup."""

    def remove_readonly(func: Callable[..., Any], path: str, _: Any) -> None:
        """
        Clear the readonly bit and reattempt the removal.

        Copied from https://docs.python.org/3/library/shutil.html#rmtree-example
        """
        os.chmod(path, stat.S_IWRITE, follow_symlinks=False)
        func(path)

    shutil.rmtree(backup_path, onexc=remove_readonly)


def delete_oldest_backups_for_space(
        backup_location: Path,
        space_requirement: str | None,
        min_backups_remaining: int = 1) -> None:
    """
    Delete backups--starting with the oldest--until enough space is free on the backup destination.

    The most recent backup will never be deleted.

    :param backup_location: The folder containing all backups
    :param space_requirement: How much space should be free after deleting backups. This is
    expressed in bytes with a unit ("MB", etc.) or as a percentage ("%") of the total storage space.
    :param min_backups_remaining: The minimum number of backups remaining after deletions. The most
    recent backup will never be deleted, so the minimum meaningful value is one.
    """
    if not space_requirement:
        return

    total_storage = shutil.disk_usage(backup_location).total
    free_storage_required = parse_storage_space(space_requirement)

    if free_storage_required > total_storage:
        raise CommandLineError(
            f"Cannot free more storage ({byte_units(free_storage_required)})"
            f" than exists at {backup_location} ({byte_units(total_storage)})")

    current_free_space = shutil.disk_usage(backup_location).free
    first_deletion_message = (
        "Deleting old backups to free up "
        f"{byte_units(free_storage_required)}"
        f" ({byte_units(current_free_space)} currently free).")

    def stop(backup: Path) -> bool:
        return shutil.disk_usage(backup).free > free_storage_required

    delete_backups(backup_location, min_backups_remaining, first_deletion_message, stop)

    final_free_space = shutil.disk_usage(backup_location).free
    if final_free_space < free_storage_required:
        backups_remaining = len(all_backups(backup_location))
        if backups_remaining == 1:
            logger.warning(
                f"Could not free up {byte_units(free_storage_required)} of storage"
                " without deleting most recent backup.")
        else:
            logger.info("Stopped after reaching maximum number of deletions.")


def parse_storage_space(space_requirement: str) -> float:
    """
    Parse a string into a number of bytes of storage space.

    :param space_requirement: A string indicating an amount of space as an absolute number of
    bytes. Byte units and prefixes are allowed.

    >>> parse_storage_space("100")
    100.0

    >>> parse_storage_space("152 kB")
    152000.0

    Note that the byte units are case and spacing insensitive.
    >>> parse_storage_space("123gb")
    123000000000.0
    """
    text = "".join(space_requirement.upper().split())
    text = text.replace("K", "k")
    text = text.rstrip("B")
    number, prefix = (text[:-1], text[-1]) if text[-1].isalpha() else (text, "")

    try:
        multiplier: int = 1000**storage_prefixes.index(prefix)
        return float(number)*multiplier
    except ValueError:
        raise CommandLineError(f"Invalid storage space value: {space_requirement}") from None


def parse_time_span_to_timepoint(
        time_span: str,
        now: datetime.datetime | None = None) -> datetime.datetime:
    """
    Parse a string representing a time span into a datetime representing a date that long ago.

    For example, if time_span is "6m", the result is a date six calendar months ago.

    :param time_span: A string consisting of a positive integer followed by a single letter: "d"
    for days, "w" for weeks, "m" for calendar months, and "y" for calendar years.
    :param now: The point from which to calculate the past point. If None, use
    datetime.datetime.now().
    """
    time_span = "".join(time_span.lower().split())
    try:
        number = int(time_span[:-1])
    except ValueError:
        raise CommandLineError(
            f"Invalid number in time span (must be a whole number): {time_span}") from None

    if number < 1:
        raise CommandLineError(f"Invalid number in time span (must be positive): {time_span}")

    letter = time_span[-1]
    now = now or datetime.datetime.now()
    if letter == "d":
        return now - datetime.timedelta(days=number)
    elif letter == "w":
        return now - datetime.timedelta(weeks=number)
    elif letter == "m":
        new_date = months_ago(now, number)
        return datetime.datetime.combine(new_date, now.time())
    elif letter == "y":
        new_date = fix_end_of_month(now.year - number, now.month, now.day)
        return datetime.datetime.combine(new_date, now.time())
    else:
        raise CommandLineError(f"Invalid time (valid units: {list("dwmy")}): {time_span}")


def months_ago(now: datetime.datetime | datetime.date, month_count: int) -> datetime.date:
    """
    Return a date that is a number of calendar months ago.

    The day of the month is not changed unless necessary to produce a valid date
    (see fix_end_of_month()).
    """
    new_month = now.month - (month_count % 12)
    new_year = now.year - (month_count // 12)
    if new_month < 1:
        new_month += 12
        new_year -= 1
    return fix_end_of_month(new_year, new_month, now.day)


def fix_end_of_month(year: int, month: int, day: int) -> datetime.date:
    """
    Replace a day past the end of the month (e.g., Feb. 31) with the last day of the same month.

    >>> fix_end_of_month(2023, 2, 31)
    datetime.date(2023, 2, 28)

    >>> fix_end_of_month(2024, 2, 31)
    datetime.date(2024, 2, 29)

    >>> fix_end_of_month(2025, 4, 31)
    datetime.date(2025, 4, 30)

    All other days are unaffected.

    >>> fix_end_of_month(2025, 5, 23)
    datetime.date(2025, 5, 23)
    """
    while True:
        try:
            return datetime.date(year, month, day)
        except ValueError:
            day -= 1


def delete_backups_older_than(
        backup_folder: Path,
        time_span: str | None,
        min_backups_remaining: int = 1) -> None:
    """
    Delete backups older than a given timespan.

    :param backup_folder: The folder containing all backups
    :param time_span: The maximum age of a backup to not be deleted. See
    parse_time_span_to_timepoint() for how the string is formatted.
    :param min_backups_remaining: The minimum number of backups remaining after deletions. The most
    recent backup will never be deleted, so the minimum meaningful value is one.
    """
    if not time_span:
        return

    timestamp_to_keep = parse_time_span_to_timepoint(time_span)
    first_deletion_message = (
        f"Deleting backups prior to {timestamp_to_keep.strftime('%Y-%m-%d %H:%M:%S')}.")

    def stop(backup: Path) -> bool:
        return backup_datetime(backup) >= timestamp_to_keep

    delete_backups(backup_folder, min_backups_remaining, first_deletion_message, stop)
    oldest_backup_date = backup_datetime(all_backups(backup_folder)[0])
    if oldest_backup_date < timestamp_to_keep:
        backups_remaining = len(all_backups(backup_folder))
        if backups_remaining == 1:
            logger.warning(
                f"Could not delete all backups older than {timestamp_to_keep} without"
                " deleting most recent backup.")
        else:
            logger.info("Stopped after reaching maximum number of deletions.")


def delete_backups(
        backup_folder: Path,
        min_backups_remaining: int,
        first_deletion_message: str,
        stop_deletion_condition: Callable[[Path], bool]) -> None:
    """
    Delete backups until a condition is met.

    :param backup_folder: The base folder containing all backups.
    :param min_backups_remaining: The minimum number of backups that should remain after deletions.
    Defaults to 1 if value is None or less than 1 (at least one backup will always remain).
    :param first_deletion_message: A message to print/log prior to the first deletion if any
    deletions will take place.
    :param stop_deletion_condition: A function that, if it returns True, stops deletions.
    """
    min_backups_remaining = max(1, min_backups_remaining)

    backups_to_delete = all_backups(backup_folder)[:-min_backups_remaining]
    for deletion_count, backup in enumerate(backups_to_delete, 1):
        if stop_deletion_condition(backup):
            break

        if deletion_count == 1:
            logger.info("")
            logger.info(first_deletion_message)

        logger.info(f"Deleting oldest backup: {backup}")
        delete_directory_tree(backup)

        try:
            year_folder = backup.parent
            year_folder.rmdir()
            logger.info(f"Deleted empty year folder {year_folder}")
        except OSError:
            pass

        logger.info(f"Free space: {byte_units(shutil.disk_usage(backup_folder).free)}")


def backup_datetime(backup: Path) -> datetime.datetime:
    """Get the timestamp of a backup from the backup folder name."""
    return datetime.datetime.strptime(backup.name, backup_date_format)


def plural_noun(count: int, word: str) -> str:
    """
    Convert a noun to a simple plural phrase if the count is not one.

    >>> plural_noun(5, "cow")
    '5 cows'

    >>> plural_noun(1, "cat")
    '1 cat'

    Irregular nouns that are not pluralized by appending an "s" are not supported.
    >>> plural_noun(3, "fox")
    '3 foxs'
    """
    return f"{count} {word}{'' if count == 1 else 's'}"


def move_backups(
        old_backup_location: Path,
        new_backup_location: Path,
        backups_to_move: list[Path]) -> None:
    """Move a set of backups to a new location."""
    move_count = len(backups_to_move)
    logger.info(f"Moving {plural_noun(move_count, "backup")}")
    logger.info(f"from {old_backup_location}")
    logger.info(f"to   {new_backup_location}")

    for backup in backups_to_move:
        create_new_backup(
            backup,
            new_backup_location,
            filter_file=None,
            examine_whole_file=False,
            force_copy=False,
            copy_probability=0.0,
            is_backup_move=True,
            timestamp=backup_datetime(backup))

        backup_source_file = get_user_location_record(new_backup_location)
        backup_source_file.unlink()
        logger.info("---------------------")

    original_backup_source = backup_source(old_backup_location)
    record_user_location(original_backup_source, new_backup_location)


def verify_last_backup(backup_folder: Path, filter_file: Path | None, result_folder: Path) -> None:
    """
    Verify the most recent backup by comparing with the user's files.

    :param backup_folder: The location of the backed up data.
    :param filter_file: The file that filters which files are backed up.
    :param result_folder: Where the resulting files will be saved.
    """
    user_folder = backup_source(backup_folder)
    if not user_folder.is_dir():
        raise FileNotFoundError(f"Could not find user folder: {user_folder}")

    last_backup_folder = find_previous_backup(backup_folder)

    if last_backup_folder is None:
        raise CommandLineError(f"No backups found in {backup_folder}.")

    logger.info(f"Verifying backup in {backup_folder} by comparing against {user_folder}")

    result_folder.mkdir(parents=True, exist_ok=True)
    prefix = datetime.datetime.now().strftime(backup_date_format)
    matching_file_name = result_folder/f"{prefix} matching files.txt"
    mismatching_file_name = result_folder/f"{prefix} mismatching files.txt"
    error_file_name = result_folder/f"{prefix} error files.txt"

    with (matching_file_name.open("w", encoding="utf8") as matching_file,
        mismatching_file_name.open("w", encoding="utf8") as mismatching_file,
        error_file_name.open("w", encoding="utf8") as error_file):

        for file in (matching_file, mismatching_file, error_file):
            file.write(f"Comparison: {user_folder} <---> {backup_folder}\n")

        def file_name_line_writer(relative_directory: Path) -> Callable[[str], str]:
            return lambda file_name: f"{relative_directory/file_name}\n"

        for directory, file_names in Backup_Set(user_folder, filter_file):
            relative_directory = directory.relative_to(user_folder)
            backup_directory = last_backup_folder/relative_directory
            matches, mismatches, errors = filecmp.cmpfiles(
                directory,
                backup_directory,
                file_names,
                shallow=False)

            stringifier = file_name_line_writer(relative_directory)
            matching_file.writelines(map(stringifier, matches))
            mismatching_file.writelines(map(stringifier, mismatches))
            error_file.writelines(map(stringifier, errors))


def restore_backup(
        dated_backup_folder: Path,
        destination: Path,
        *,
        delete_extra_files: bool) -> None:
    """
    Return a user's folder to a previously backed up state.

    Existing files that were backed up will be overwritten with the backup.

    :param dated_backup_folder: The backup from which to restore files and folders
    :param destination: The folder that will be restored to a backed up state.
    :param delete_extra_files: Whether to delete files and folders that are not present in the
    backup.
    """
    user_folder = backup_source(dated_backup_folder.parent.parent)
    logger.info(f"Restoring: {user_folder}")
    logger.info(f"From     : {dated_backup_folder}")
    logger.info(f"Deleting extra files: {delete_extra_files}")
    if not user_folder.samefile(destination):
        logger.info(f"Restoring to: {destination}")

    for current_backup_path, folder_names, file_names in dated_backup_folder.walk():
        current_user_folder = destination/current_backup_path.relative_to(dated_backup_folder)
        logger.debug(f"Creating {current_user_folder}")
        current_user_folder.mkdir(parents=True, exist_ok=True)

        for file_name in file_names:
            try:
                file_source = current_backup_path/file_name
                file_destination = current_user_folder/file_name
                logger.debug(
                    f"Copying {file_name} from {current_backup_path} to {current_user_folder}")
                shutil.copy2(file_source, file_destination, follow_symlinks=False)
            except Exception as error:
                logger.warning(f"Could not restore {file_destination} from {file_source}: {error}")

        if delete_extra_files:
            backed_up_paths = set(folder_names) | set(file_names)
            user_paths = {entry.name for entry in current_user_folder.iterdir()}
            for new_name in user_paths - backed_up_paths:
                new_path = current_user_folder/new_name
                logger.debug(f"Deleting extra file {new_path}")
                if is_real_directory(new_path):
                    delete_directory_tree(new_path)
                else:
                    new_path.unlink()


def last_n_backups(backup_location: Path, n: str | int) -> list[Path]:
    """
    Return a list of the paths of the last n backups.

    :param backup_location: The location of the backup set.
    :param n: A positive integer to get the last n backups, or "all" to get all backups.
    """
    backups = all_backups(backup_location)
    if str(n).lower() == "all":
        return backups

    count = int(n)
    if count < 1 or count != float(n):
        raise ValueError(f"Value must be a positive whole number: {n}")

    return backups[-count:]


def backups_since(oldest_backup_date: datetime.datetime, backup_location: Path) -> list[Path]:
    """Return a list of the backups created since a given date."""

    def recent_enough(backup_folder: Path) -> bool:
        return backup_datetime(backup_folder) >= oldest_backup_date

    return list(filter(recent_enough, all_backups(backup_location)))


def print_backup_storage_stats(backup_location: Path) -> None:
    """Log information about the storage space of the backup medium."""
    backup_storage = shutil.disk_usage(backup_location)
    percent_used = round(100*backup_storage.used/backup_storage.total)
    percent_free = round(100*backup_storage.free/backup_storage.total)
    logger.info("")
    logger.info(
        "Backup storage space: "
        f"Total = {byte_units(backup_storage.total)}  "
        f"Used = {byte_units(backup_storage.used)} ({percent_used}%)  "
        f"Free = {byte_units(backup_storage.free)} ({percent_free}%)")
    backups = all_backups(backup_location)
    logger.info(f"Backups stored: {len(backups)}")
    logger.info(f"Earliest backup: {backups[0].name}")


def read_configuation_file(config_file_name: str) -> list[str]:
    """Parse a configuration file into command line arguments."""
    try:
        with open(config_file_name, encoding="utf8") as file:
            arguments: list[str] = []
            for line_raw in file:
                line = line_raw.strip()
                if not line or line.startswith("#"):
                    continue
                parameter_raw, value_raw = line.split(":", maxsplit=1)

                parameter = "-".join(parameter_raw.lower().split())
                if parameter == "config":
                    raise CommandLineError(
                        "The parameter `config` within a configuration file has no effect.")
                arguments.append(f"--{parameter}")

                value = remove_quotes(value_raw)
                if value:
                    arguments.append(value)
            return arguments
    except FileNotFoundError:
        raise CommandLineError(f"Configuation file does not exist: {config_file_name}") from None


def remove_quotes(s: str) -> str:
    """
    After stripping a string of outer whitespace, remove pairs of quotes from the start and end.

    >>> remove_quotes('  "  a b c  "   ')
    '  a b c  '

    Strings without quotes are stripped of outer whitespace.

    >>> remove_quotes(' abc  ')
    'abc'

    All other strings are unchanged.

    >>> remove_quotes('Inner "quoted strings" are not affected.')
    'Inner "quoted strings" are not affected.'

    >>> remove_quotes('"This quote will stay," he said.')
    '"This quote will stay," he said.'

    If a string (usually a file name read from a file) has leading or trailing spaces,
    then the user should surround this file name with quotations marks to make sure the
    spaces are included in the return value.

    If the file name actually does begin and end with quotation marks, then surround the
    file name with another pair of quotation marks. Only one pair will be removed.

    >>> remove_quotes('""abc""')
    '"abc"'
    """
    s = s.strip()
    if len(s) > 1 and (s[0] == s[-1] == '"'):
        return s[1:-1]
    return s


def format_paragraphs(lines: str, line_length: int) -> str:
    """
    Format multiparagraph text in when printing --help.

    :param lines: A string of text where paragraphs are separated by at least two newlines. Indented
    lines will be preserved as-is.
    :param line_length: The length of the line for word wrapping. Indented lines will not be word
    wrapped.

    :returns string: A single string with word-wrapped lines and paragraphs separated by exactly two
    newlines.
    """
    paragraphs: list[str] = []
    for paragraph_raw in lines.split("\n\n"):
        paragraph = paragraph_raw.strip("\n")
        if not paragraph:
            continue

        paragraphs.append(
            paragraph if paragraph[0].isspace() else textwrap.fill(paragraph, line_length))

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
    user_input.add_argument(f"--no-{name}", action="store_true", help=format_help(
f"""Disable the --{name} option. This is primarily used if "{name}" appears in a
configuration file. This option has priority even if --{name} is listed later."""))


def toggle_is_set(args: argparse.Namespace, name: str) -> bool:
    """Check that a boolean command line option --X has been selected and not negated by --no-X."""
    options = vars(args)
    return options[name] and not options[f"no_{name}"]


def path_or_none(arg: str | None) -> Path | None:
    """Create a Path instance if the input string is valid."""
    return absolute_path(arg) if arg else None


def copy_probability_from_hard_link_count(hard_link_count: str) -> float:
    """
    Convert an expected average hard link count into a copy probability.

    Randomly copying files serves two purposes. First, it increases the safety of the backed up
    files. If no files were ever copied, then there would only be one copy of each file in the
    backup location. If that backup become corrupted, then all backups of the file would be lost.
    Randomly copying files ensures that there are multiple independent copies available, even when
    they don't change.

    Second, in some operating systems (including Windows), creating a new hard link to a file that
    already has many hard links takes longer and longer. If most files in a backup set have
    hundreds of hard links, then backups can take multiple hours even if copying everything would
    take under an hour.

    This function returns a probability of copying an unchanged file instead of hard linking. The
    convesion is p = 1/(h + 1), where h is the hard link count and p is the resulting probability.
    The files to copy are chosen randomly since counting the number of hard links requires a file
    system access. For files with many hard links, this can be slow, which negates half the purpose
    of copying files instead of hard-linking them.
    """
    try:
        average_hard_link_count = int(hard_link_count)
    except ValueError:
        raise CommandLineError(f"Invalid value for hard link count: {hard_link_count}") from None

    if average_hard_link_count < 1:
        raise CommandLineError(
            f"Hard link count must be a positive whole number. Got: {hard_link_count}")

    logger.info(f"Maximum average hard link count = {average_hard_link_count}")
    return 1/(average_hard_link_count + 1)


def print_run_title(command_line_args: argparse.Namespace, action_title: str) -> None:
    """Print the action taking place."""
    logger.info("")
    divider = "="*(len(action_title) + 2)
    logger.info(divider)
    logger.info(f" {action_title}")
    logger.info(divider)
    logger.info("")

    if command_line_args.config:
        logger.info(f"Reading configuration from file: {Path(command_line_args.config).absolute()}")
        logger.info("")


def get_existing_path(path: str | None, folder_type: str) -> Path:
    """
    Return the absolute version of the given existing path.

    Raise an exception if the path does not exist.
    """
    if not path:
        raise CommandLineError(f"{folder_type.capitalize()} not specified.")

    try:
        return absolute_path(path, strict=True)
    except FileNotFoundError:
        raise CommandLineError(f"Could not find {folder_type.lower()}: {path}") from None


def start_recovery_from_backup(args: argparse.Namespace) -> None:
    """Recover a file or folder from a backup according to the command line."""
    backup_folder = get_existing_path(args.backup_folder, "backup folder")
    choice = None if args.choice is None else int(args.choice)
    print_run_title(args, "Recovering from backups")
    recover_path(absolute_path(args.recover), backup_folder, choice)


def choose_target_path_from_backups(args: argparse.Namespace) -> Path | None:
    """Choose a path from a list of backed up files and folders from a given directory."""
    operation = "recovery" if args.list else "purging"
    backup_folder = get_existing_path(args.backup_folder, "backup folder")
    search_directory = absolute_path(args.list or args.purge_list)
    print_run_title(args, f"Listing files and directories for {operation}")
    logger.info(f"Searching for everything backed up from {search_directory} ...")
    test_choice = int(args.choice) if args.choice else None
    return search_backups(search_directory, backup_folder, operation, test_choice)


def choose_recovery_target_from_backups(args: argparse.Namespace) -> None:
    """Choose what to recover from a list of everything backed up from a folder."""
    backup_folder = get_existing_path(args.backup_folder, "backup folder")
    chosen_recovery_path = choose_target_path_from_backups(args)
    if chosen_recovery_path is not None:
        recover_path(chosen_recovery_path, backup_folder)


def choose_purge_target_from_backups(
        args: argparse.Namespace,
        confirmation_response: str | None = None) -> None:
    """Choose which path to purge from a list of everything backed up from a folder."""
    backup_folder = get_existing_path(args.backup_folder, "backup folder")
    chosen_purge_path = choose_target_path_from_backups(args)
    if chosen_purge_path is not None:
        purge_path(chosen_purge_path, backup_folder, confirmation_response, args.choice)


def start_move_backups(args: argparse.Namespace) -> None:
    """Parse command line options to move backups to another location."""
    old_backup_location = get_existing_path(args.backup_folder, "current backup location")
    new_backup_location = absolute_path(args.move_backup)

    confirm_choice_made(args, "move_count", "move_age", "move_since")
    if args.move_count:
        backups_to_move = last_n_backups(old_backup_location, args.move_count)
    elif args.move_age:
        oldest_backup_date = parse_time_span_to_timepoint(args.move_age)
        backups_to_move = backups_since(oldest_backup_date, old_backup_location)
    elif args.move_since:
        oldest_backup_date = datetime.datetime.strptime(args.move_since, "%Y-%m-%d")
        backups_to_move = backups_since(oldest_backup_date, old_backup_location)

    new_backup_location.mkdir(parents=True, exist_ok=True)
    with Backup_Lock(new_backup_location, "backup move"):
        print_run_title(args, "Moving backups")
        move_backups(old_backup_location, new_backup_location, backups_to_move)


def start_verify_backup(args: argparse.Namespace) -> None:
    """Parse command line options for verifying backups."""
    backup_folder = get_existing_path(args.backup_folder, "backup folder")
    filter_file = path_or_none(args.filter)
    result_folder = absolute_path(args.verify)
    print_run_title(args, "Verifying last backup")
    verify_last_backup(backup_folder, filter_file, result_folder)


def start_backup_restore(args: argparse.Namespace) -> None:
    """Parse command line arguments for a backup recovery."""
    backup_folder = get_existing_path(args.backup_folder, "backup folder")

    confirm_choice_made(args, "destination", "user_folder")
    destination = (
        absolute_path(args.destination) if args.destination
        else get_existing_path(args.user_folder, "user folder"))

    if args.user_folder:
        confirm_user_location_is_unchanged(destination, backup_folder)

    confirm_choice_made(args, "delete_extra", "keep_extra")
    delete_extra_files = bool(args.delete_extra)

    confirm_choice_made(args, "last_backup", "choose_backup")
    choice = None if args.choice is None else int(args.choice)
    restore_source = (
        find_previous_backup(backup_folder) if args.last_backup
        else choose_backup(backup_folder, choice))

    if not restore_source:
        raise CommandLineError(f"No backups found in {backup_folder}")

    print_run_title(args, "Restoring user data from backup")

    required_response = "yes"
    logger.info(
        f"This will overwrite all files in {destination} and subfolders with files "
        f"in {restore_source}.")
    if delete_extra_files:
        logger.info(
            "Any files that were not backed up, including newly created files and "
            "files not backed up because of --filter, will be deleted.")
    automatic_response = "no" if args.bad_input else required_response
    response = (
        automatic_response if args.skip_prompt
        else input(
            f'Do you want to continue? Type "{required_response}" to proceed '
            f'or press {cancel_key()} to cancel: '))

    if response.strip().lower() == required_response:
        restore_backup(restore_source, destination, delete_extra_files=delete_extra_files)
    else:
        logger.info(
            f'The response was "{response}" and not "{required_response}", '
            'so the restoration is cancelled.')


def classify_path(path: Path) -> str:
    """Return a text description of the item at the given path (file, folder, etc.)."""
    return ("Symlink" if path.is_symlink()
            else "Folder" if path.is_dir()
            else "File" if path.is_file()
            else "Unknown")


def start_backup_purge(args: argparse.Namespace, confirmation_reponse: str | None = None) -> None:
    """Parse command line options to purge file or folder from all backups."""
    backup_folder = get_existing_path(args.backup_folder, "backup folder")
    purge_target = absolute_path(args.purge)
    print_run_title(args, "Purging from backups")
    purge_path(purge_target, backup_folder, confirmation_reponse, args.choice)


def purge_path(
        purge_target: Path,
        backup_folder: Path,
        confirmation_reponse: str | None,
        arg_choice: str | None) -> None:
    """Purge a file/folder by deleting it from all backups."""
    relative_purge_target = path_relative_to_backups(purge_target, backup_folder)

    backup_list = all_backups(backup_folder)
    potential_deletions = (backup/relative_purge_target for backup in backup_list)
    paths_to_delete = list(filter(lambda p: p.exists(follow_symlinks=False), potential_deletions))
    if not paths_to_delete:
        logger.info(f"Could not find any backed up copies of {purge_target}")
        return

    path_type_counts = Counter(map(classify_path, paths_to_delete))
    types_to_delete = choose_types_to_delete(paths_to_delete, path_type_counts, arg_choice)

    type_choice_data = [(path_type_counts[path_type], path_type) for path_type in types_to_delete]
    type_list = [f"{plural_noun(count, path_type)}" for count, path_type in type_choice_data]
    logger.info(f"Path to be purged from backups: {purge_target}")
    prompt = f"The following items will be deleted: {", ".join(type_list)}.\nProceed? [y/n] "
    confirmation = confirmation_reponse or input(prompt)
    if confirmation.lower() != "y":
        return

    for path in paths_to_delete:
        path_type = classify_path(path)
        if path_type in types_to_delete:
            logger.info(f"Deleting {path_type} {path} ...")
            action = delete_directory_tree if path_type == "Folder" else Path.unlink
            action(path)

    last_backup = find_previous_backup(backup_folder)
    if backup_list[-1] != last_backup or backup_staging_folder(backup_folder).exists():
        logger.warning(
            f"A backup to {backup_folder} ran during purging. You may want to rerun the "
            "purge after the backup completes.")
    logger.info("If you want to prevent the purged item from being backed up in the future,")
    logger.info("consider adding the following line to a filter file:")
    filter_line = (
        relative_purge_target/"**" if is_real_directory(purge_target) else relative_purge_target)
    logger.info(f"- {filter_line}")


def choose_types_to_delete(
        paths_to_delete: list[Path],
        path_type_counts: Counter[str],
        test_choice: str | None) -> list[str]:
    """If a purge target has more than one type in backups, choose which type to delete."""
    if len(path_type_counts) == 1:
        return [classify_path(paths_to_delete[0])]
    else:
        menu_choices = [
            f"{path_type}s ({count} items)"
            for path_type, count in sorted(path_type_counts.items())]
        all_choice = f"All ({len(paths_to_delete)} items)"
        menu_choices.append(all_choice)
        prompt = "Multiple types of paths were found. Which one should be deleted?\nChoice"
        choice = choose_from_menu(menu_choices, prompt) if test_choice is None else int(test_choice)
        type_choices = sorted(path_type_counts.keys())
        return type_choices if menu_choices[choice] == all_choice else [type_choices[choice]]


def confirm_choice_made(args: argparse.Namespace, *options: str) -> None:
    """Make sure that exactly one of the argument parameters is present."""
    args_dict = vars(args)
    if len(list(filter(None, map(args_dict.get, options)))) != 1:
        option_list = [f"--{option.replace("_", "-")}" for option in options]
        comma = ", "
        message = "Exactly one of the following is required: " + comma.join(option_list)
        if message.count(comma) == 1:
            message = message.replace(comma, " or ")
        else:
            message = f"{comma}or ".join(message.rsplit(comma, maxsplit=1))
        raise CommandLineError(message)


def start_backup(args: argparse.Namespace) -> None:
    """
    Parse command line arguments to start a backup.

    :returns Path: The base directory where all backups are stored.
    """
    user_folder = get_existing_path(args.user_folder, "user's folder")

    if not args.backup_folder:
        raise CommandLineError("Backup folder not specified.")

    backup_folder = absolute_path(args.backup_folder)
    backup_folder.mkdir(parents=True, exist_ok=True)

    with Backup_Lock(backup_folder, "backup"):
        print_run_title(args, "Starting new backup")
        free_space_before_backup = shutil.disk_usage(backup_folder).free
        create_new_backup(
            user_folder,
            backup_folder,
            filter_file=path_or_none(args.filter),
            examine_whole_file=toggle_is_set(args, "whole_file"),
            force_copy=toggle_is_set(args, "force_copy"),
            copy_probability=copy_probability(args),
            timestamp=args.timestamp)

        logger.info("")
        free_space_after_backup = shutil.disk_usage(backup_folder).free
        backup_space_taken = free_space_before_backup - free_space_after_backup
        log_backup_size(args.free_up, backup_space_taken)

        delete_old_backups(args)


def log_backup_size(free_up_parameter: str | None, backup_space_taken: int) -> None:
    """
    Log size of previous backup and warn user if backup is near or over --free-up parameter.

    This should warn the user that a future backup may not have enough storage space to complete
    sucessfully.
    """
    free_up = parse_storage_space(free_up_parameter or "0")
    free_up_percent = math.ceil(100*backup_space_taken/free_up) if free_up else 0
    free_up_text = f" ({free_up_percent}% of --free-up)" if free_up else ""
    free_up_warning_percent = 90
    is_warning = free_up_percent >= free_up_warning_percent
    log_destination = logger.warning if is_warning else logger.info
    log_destination(f"Backup space used: {byte_units(backup_space_taken)}{free_up_text}")
    if is_warning:
        logger.warning("Consider increasing the size of the --free-up parameter.")


def absolute_path(path: Path | str, *, strict: bool = False) -> Path:
    """
    Return an absolute version of the given path.

    Relative path segments (..) are removed. Symlinks are not resolved.

    :param path: The path to be made absolute.
    :param stict: If True, raise a FileNotFoundError if the path does not exist. Symlinks are
    not followed, so an existing symlink to a non-existent file or folder does not raise an error.
    """
    abs_path = Path(os.path.abspath(path))
    if strict and not abs_path.exists(follow_symlinks=False):
        raise FileNotFoundError(f"The path {abs_path}, resolved from {path} does not exist.")
    return abs_path


def parse_probability(probability_str: str) -> float:
    """Parse probability from --copy-probability argument."""
    divisor = 100 if probability_str.endswith("%") else 1
    number = float(probability_str.rstrip("%"))
    probability = number/divisor
    if probability < 0.0 or probability > 1.0:
        raise CommandLineError(
            "Value of --copy-probability must be between 0.0 and 1.0 "
            f"(or 0% and 100%): {probability_str}")
    return probability


def copy_probability(args: argparse.Namespace) -> float:
    """Calculate the probability of copying unchanged files from command line arguments."""
    if args.hard_link_count:
        return copy_probability_from_hard_link_count(args.hard_link_count)
    elif args.copy_probability:
        return parse_probability(args.copy_probability)
    else:
        return 0.0


def delete_old_backups(args: argparse.Namespace) -> None:
    """Delete the oldest backups by various criteria in the command line options."""
    backup_folder = get_existing_path(args.backup_folder, "backup folder")
    backup_count = len(all_backups(backup_folder))
    max_deletions = int(args.max_deletions or backup_count)
    min_backups_remaining = max(backup_count - max_deletions, 1)
    delete_oldest_backups_for_space(backup_folder, args.free_up, min_backups_remaining)
    delete_backups_older_than(backup_folder, args.delete_after, min_backups_remaining)
    print_backup_storage_stats(backup_folder)


def delete_before_backup(args: argparse.Namespace) -> None:
    """Delete old backups before running a backup process."""
    delete_old_backups(args)
    start_backup(args)


def argument_parser() -> argparse.ArgumentParser:
    """Create the parser for command line arguments."""
    user_input = argparse.ArgumentParser(
        add_help=False,
        formatter_class=argparse.RawTextHelpFormatter,
        allow_abbrev=False,
        description=format_text(
"""A backup utility that combines the best aspects of full and incremental backups.

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

- If a folder contains no files and none of its subfolders contain files, whether because there
were none or all files were filtered out, it will not appear in the backup.

- Symbolic links are not followed and are always copied as symbolic links. On Windows, symbolic
links cannot be created or copied without elevated privileges, so they will be missing from
backups if not run in administrator mode. Backups will be complete for all other files, so an
unprivileged user may user this program and use the logs to restore symbolic links after restoring a
backup.

- Windows junction points (soft links) are excluded by default. They may be added using a filter
file (see --filter below). In that case, all of the contents will be copied.

- If two files in the user's directory are hard-linked together, these files will be copied/linked
separately. The hard link is not preserved in the backup.

- If the user folder and the backup destination are on different drives or partitions with different
file systems (NTFS, ext4, APFS, etc.), hard links may not be created due to differences in how file
modification times are recorded. Using the --whole-file option may mitigate this, but backups will
take much more time."""))

    action_group = user_input.add_argument_group("Actions", format_text(
"""The default action when vintage backups is run is to create a new backup. If one of the following
options are chosen, then that action is performed instead."""))

    only_one_action_group = action_group.add_mutually_exclusive_group()

    only_one_action_group.add_argument("-h", "--help", action="store_true", help=format_help(
"""Show this help message and exit."""))

    only_one_action_group.add_argument("-r", "--recover", help=format_help(
"""Recover a file or folder from the backup. The user will be able
to pick which version to recover by choosing the backup date as
the source. If a file is being recovered, only backup dates where
the file was modified will be presented. If a folder is being
recovered, then all available backup dates will be options.
This option requires the --backup-folder option to specify which
backup location to search."""))

    only_one_action_group.add_argument(
        "--list",
        metavar="DIRECTORY",
        nargs="?",
        const=".",
        help=format_help(
"""Recover a file or folder in the directory specified by the argument by first choosing what to
recover from a list of everything that's ever been backed up. If there is no folder specified
after --list, then the current directory is used. The backup location argument --backup-folder
is required."""))

    only_one_action_group.add_argument(
        "--move-backup",
        metavar="NEW_BACKUP_LOCATION",
        help=format_help(
"""Move a backup set to a new location. The value of this argument is the new location. The
--backup-folder option is required to specify the current location of the backup set, and one
of --move-count, --move-age, or --move-since is required to specify how many of the most recent
backups to move. Moving each dated backup will take just as long as a normal backup to move since
the hard links to previous backups will be recreated to preserve the space savings, so some planning
is needed when deciding how many backups should be moved."""))

    only_one_action_group.add_argument("--verify", metavar="RESULT_DIR", help=format_help(
"""Verify the latest backup by comparing them against the original files. The result of the
comparison will be placed in the folder RESULT_DIR. The result is three files: a list of files that
match, a list of files that do not match, and a list of files that caused errors during the
comparison. The --backup-folder argument is required. If a filter file was used
to create the backup, then --filter should be supplied as well."""))

    only_one_action_group.add_argument("--restore", action="store_true", help=format_help(
"""This action restores the user's folder to a previous, backed up state. Any existing user files
that have the same name as one in the backup will be overwritten. The --backup-folder is required to
specify from where to restore. See the Restore Options section below for the other required
parameters."""))

    only_one_action_group.add_argument("--purge", help=format_help(
"""Delete a file or folder from all backups. The argument is the path to delete. This requires the
--backup-folder argument."""))

    only_one_action_group.add_argument(
        "--purge-list",
        metavar="DIRECTORY",
        nargs="?",
        const=".",
        help=format_help(
"""Purge a file or folder from all backups in the directory specified by the argument by first
choosing what to purge from a list of everything that's ever been backed up. If there is no folder
specified after --purge-list, then the current directory is used. If the file exists in the user's
folder, it is not deleted. The backup location argument --backup-folder is required."""))

    only_one_action_group.add_argument("--delete-only", action="store_true", help=format_help(
"""Delete old backups according to --free-up or --delete-after without running a backup first."""))

    common_group = user_input.add_argument_group("Options needed for all actions")

    common_group.add_argument("-b", "--backup-folder", help=format_help(
"""The destination of the backed up files. This folder will
contain a set of folders labeled by year, and each year's
folder will contain all of that year's backups."""))

    backup_group = user_input.add_argument_group("Options for backing up")

    backup_group.add_argument("-u", "--user-folder", help=format_help(
"""The directory to be backed up. The contents of this
folder and all subfolders will be backed up recursively."""))

    backup_group.add_argument("-f", "--filter", metavar="FILTER_FILE_NAME", help=format_help(
"""Filter the set of files that will be backed up. The value of this argument should be the name of
a text file that contains lines specifying what files to include or exclude.

Each line in the file consists of a symbol followed by a path. The symbol must be a minus (-),
plus (+), or hash (#). Lines with minus signs specify files and folders to exclude. Lines with plus
signs specify files and folders to include. Lines with hash signs are ignored. Prior to reading the
first line, everything in the user's folder is included. The path that follows may contain wildcard
characters like *, **, [], and ? to allow for matching multiple path names. If you want to match a
single name that contains wildcards, put brackets around them: What Is Life[?].pdf, for example.
Since leading and trailing whitespace is normally removed, use brackets around each leading/trailing
space character: - [ ][ ]has_two_leading_and_three_trailing_spaces.txt[ ][ ][ ]

Only files will be matched against each line in this file. If you want to include or exclude an
entire directory, the line must end with a "/**" or "\\**" to match all of its contents. The paths
may be absolute or relative. If a path is relative, it is relative to the user's folder.

All paths must reside within the directory tree of the --user-folder. For example, if backing up
C:\\Users\\Alice, the following filter file:

    # Ignore AppData except Firefox
    - AppData/**
    + AppData/Roaming/Mozilla/Firefox/**

will exclude everything in C:\\Users\\Alice\\AppData\\ except the
Roaming\\Mozilla\\Firefox subfolder. The order of the lines matters. If the - and + lines above
were reversed, the Firefox folder would be included and then excluded by the following - Appdata
line.

Because each line only matches to files, some glob patterns may not do what the user expects. Here
are some examples of such patterns:

    # Assume that dir1 is a folder in the user's --user-folder and dir2 is a folder inside dir1.

    # This line does nothing.
    - dir1

    # This line will exclude all files in dir1, but not folders. dir1/dir2 is still included.
    - dir1/*

    # This line will exclude dir1 and all of its contents.
    - dir1/**"""))

    backup_group.add_argument("-w", "--whole-file", action="store_true", help=format_help(
"""Examine the entire contents of a file to determine if it has
changed and needs to be copied to the new backup. Without this
option, only the file's size, type, and modification date are
checked for differences. Using this option will make backups
take considerably longer."""))

    add_no_option(backup_group, "whole-file")

    backup_group.add_argument("--free-up", metavar="SPACE", help=format_help(
"""After a successful backup, delete old backups until the amount of free space on the
backup destination is at least SPACE.

The argument should be a bare number or a number followed by letters that
indicate a unit in bytes. The number will be interpreted as a number
of bytes. Case does not matter, so all of the following specify
15 megabytes: 15MB, 15Mb, 15mB, 15mb, 15M, and 15m. Old backups
will be deleted until at least that much space is free.

This can be used at the same time as --delete-after.

The most recent backup will not be deleted."""))

    backup_group.add_argument("--delete-after", metavar="TIME", help=format_help(
"""After a successful backup, delete backups if they are older than the time span in the argument.
The format of the argument is Nt, where N is a whole number and t is a single letter: d for days, w
for weeks, m for calendar months, or y for calendar years.

This can be used at the same time as --free-up.

The most recent backup will not be deleted."""))

    backup_group.add_argument("--max-deletions", help=format_help(
"""Specify the maximum number of deletions per program run."""))

    backup_group.add_argument("--delete-first", action="store_true", help=format_help(
"""Delete old backups (according to --free-up, --delete-after, and --max-deletions) to make room
prior to starting a new backup.

The most recent backup will never be deleted."""))

    add_no_option(backup_group, "delete-first")

    backup_group.add_argument("--force-copy", action="store_true", help=format_help(
"""Copy all files instead of linking to files previous backups. The
new backup will contain new copies of all of the user's files,
so the backup location will require much more space than a normal
backup."""))

    add_no_option(backup_group, "force-copy")

    link_copy_probability_group = backup_group.add_mutually_exclusive_group()

    link_copy_probability_group.add_argument("--hard-link-count", help=format_help(
"""Specify the average number of hard links Vintage Backup should create for an unchanged file
before copying it again. The argument HARD_LINK_COUNT should be an integer. If specified, every
unchanged file will be copied with a probability of 1/(HARD_LINK_COUNT + 1)."""))

    link_copy_probability_group.add_argument("--copy-probability", help=format_help(
"""Specify the probability that an unchanged file will be copied instead of hard-linked during a
backup. The probability can be expressed as a decimal (0.1) or as a percent (10%%). This is an
alternate to --hard-link-count and cannot be used together with it."""))

    move_group = user_input.add_argument_group("Move backup options", format_text(
"""Use exactly one of these options to specify which backups to move when using --move-backup."""))

    only_one_move_group = move_group.add_mutually_exclusive_group()

    only_one_move_group.add_argument("--move-count", help=format_help(
"""Specify the number of the most recent backups to move or "all" if every backup should be moved
to the new location."""))

    only_one_move_group.add_argument("--move-age", help=format_help(
"""Specify the maximum age of backups to move. See --delete-after for the time span format to use.
"""))

    only_one_move_group.add_argument("--move-since", help=format_help(
"""Move all backups made on or after the specified date (YYYY-MM-DD)."""))

    restore_group = user_input.add_argument_group("Restore Options", format_help(
"""Exactly one of each of the following option pairs(--last-backup/--choose-backup and
--delete-extra/--keep-extra) is required when restoring a backup. The --destination option is
optional."""))

    choose_restore_backup_group = restore_group.add_mutually_exclusive_group()

    choose_restore_backup_group.add_argument(
        "--last-backup",
        action="store_true",
        help=format_help("""Restore from the most recent backup."""))

    choose_restore_backup_group.add_argument(
        "--choose-backup",
        action="store_true",
        help=format_help("""Choose which backup to restore from a list."""))

    restore_preservation_group = restore_group.add_mutually_exclusive_group()

    restore_preservation_group.add_argument(
        "--delete-extra",
        action="store_true",
        help=format_help("""Delete any extra files that are not in the backup."""))

    restore_preservation_group.add_argument(
        "--keep-extra",
        action="store_true",
        help=format_help("""Preserve any extra files that are not in the backup."""))

    restore_group.add_argument("--destination", help=format_help(
"""Specify a different destination for the backup restoration. Either this or
the --user-folder option is required when recovering from a backup."""))

    other_group = user_input.add_argument_group("Other options")

    other_group.add_argument("-c", "--config", metavar="FILE_NAME", help=format_help(
r"""Read options from a configuration file instead of command-line arguments. The format
of the file should be one option per line with a colon separating the parameter name
and value. The parameter names have the same names as the double-dashed command line options
(i.e., "user-folder", not "u"). If a parameter does not take a value, like "whole-file",
leave the value blank. Any line starting with a # will be ignored. As an example:

    # Ignored comment
    user-folder: C:\Users\Alice\
    backup-folder: E:\Backups
    delete-on-error:

The parameter names may also be spelled with spaces instead of the dashes and with mixed case:

    # Ignored comment
    User Folder: C:\Users\Alice\
    Backup Folder: E:\Backups
    Delete on error:

Values like file and folder names may contain any characters--no escaping or quoting necessary.
Whitespace at the beginning and end of the values will be trimmed off. If a file or folder name
begins or ends with spaces, surrounding the name with double quotes will preserve this space.

    User Folder: "/home/bob/folder that ends with a space "

If a file or folder name is already quoted--that is, starts and ends with double quotes--then
another pair of quotes will preserve these quotes. If the filter file is name
"the alleged file.txt" with quotes in the name, then the configuration file line should look like
this:

    filter file: ""the alleged file.txt""

If both --config and other command line options are used and they conflict, then the command
line options override the config file options.

A final note: recursive configuration files are not supported. Using the parameter "config" inside
a configuration file will cause the program to quit with an error."""))

    other_group.add_argument("--debug", action="store_true", help=format_help(
        """Log information on all actions during a program run."""))

    add_no_option(other_group, "debug")

    default_log_file_name = Path.home()/"vintagebackup.log"
    other_group.add_argument(
        "-l", "--log",
        default=str(default_log_file_name),
        help=format_help(
f"""Where to log the activity of this program. The default is
{default_log_file_name.name} in the user's home folder. If no
log file is desired, use the file name {os.devnull}."""))

    # The following arguments are only used for testing.

    # Bypass keyboard input when testing functions that ask for a choice from a menu.
    user_input.add_argument("--choice", help=argparse.SUPPRESS)

    # Allow for backups to be created more quickly by providing a timestamp instead of using
    # datetime.datetime.now().
    user_input.add_argument("--timestamp", help=argparse.SUPPRESS)

    # Skip confirmation prompt for backup restorations.
    user_input.add_argument("--skip-prompt", action="store_true", help=argparse.SUPPRESS)

    # Give user input that causes errors.
    user_input.add_argument("--bad-input", action="store_true", help=argparse.SUPPRESS)

    return user_input


def parse_command_line(argv: list[str]) -> argparse.Namespace:
    """Parse the command line options and incorporate configuration file options if needed."""
    if argv and argv[0] == sys.argv[0]:
        argv = argv[1:]

    command_line_options = argv or ["--help"]
    user_input = argument_parser()
    command_line_args = user_input.parse_args(command_line_options)
    if command_line_args.config:
        file_options = read_configuation_file(command_line_args.config)
        return user_input.parse_args(file_options + command_line_options)
    else:
        return command_line_args


def print_usage(destination: io.TextIOBase | None = None) -> None:
    """Print short instructions for the command line options."""
    argument_parser().print_usage(destination)


def print_help(destination: io.TextIOBase | None = None) -> None:
    """Print full manual for Vintage Backup."""
    argument_parser().print_help(destination)


def main(argv: list[str]) -> int:
    """
    Start the main program.

    :param argv: A list of command line arguments as from sys.argv
    """
    try:
        args = parse_command_line(argv)
        if args.help:
            print_help()
            return 0

        setup_log_file(logger, args.log)
        logger.setLevel(logging.DEBUG if toggle_is_set(args, "debug") else logging.INFO)
        logger.debug(args)

        action = (
            start_recovery_from_backup if args.recover
            else choose_recovery_target_from_backups if args.list
            else start_move_backups if args.move_backup
            else start_verify_backup if args.verify
            else start_backup_restore if args.restore
            else start_backup_purge if args.purge
            else choose_purge_target_from_backups if args.purge_list
            else delete_old_backups if args.delete_only
            else delete_before_backup if toggle_is_set(args, "delete_first")
            else start_backup)
        action(args)
        return 0
    except CommandLineError as error:
        if __name__ == "__main__":
            print_usage()
        logger.error(error)
    except Exception:
        logger.exception("The program ended unexpectedly with an error:")

    return 1


if __name__ == "__main__":
    try:
        logger.addHandler(logging.StreamHandler(sys.stdout))
        sys.exit(main(sys.argv))
    except KeyboardInterrupt:
        sys.exit(1)
