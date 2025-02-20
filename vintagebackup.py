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
import itertools
import textwrap
import math
import random
import time
from collections import Counter
from collections.abc import Callable, Iterator, Iterable
from pathlib import Path
from multiprocessing import Process, set_start_method, freeze_support
from typing import Any, Literal, cast

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
    with Lock_File(backup_path):
        # Code that uses backup path
    ```
    """

    heartbeat_period = datetime.timedelta(seconds=1)
    stale_timeout = datetime.timedelta(seconds=3)

    def __init__(self, backup_location: Path, operation: str, *, wait: bool) -> None:
        """Set up the lock."""
        self.lock_file_path = backup_location/"vintagebackup.lock"
        self.wait = wait
        self.pid = str(os.getpid())
        self.heartbeat_counter = 0
        self.heartbeat = Process(target=self.heartbeat_writer)
        self.operation = operation
        self.previous_heartbeat_data: tuple[str, str, str] | None = None

    def __enter__(self) -> None:
        """
        Attempt to take possession of the file lock.

        If unsuccessful, wait or fail out according to the --wait choice. Failure is indicated by a
        ConcurrencyError exception.
        """
        last_pid = None
        while not self.acquire_lock():
            try:
                if self.lock_is_stale():
                    logger.info(f"Deleting stale lock file: {self.lock_file_path}")
                    self.lock_file_path.unlink()
                    continue

                other_pid = self.read_blocking_pid()
                other_operation = self.read_blocking_operation()
            except FileNotFoundError:
                continue

            if not self.wait:
                raise ConcurrencyError(f"Vintage Backup already running {other_operation} on "
                                       f"{self.lock_file_path.parent} (PID {other_pid})")

            if last_pid != other_pid:
                logger.info(f"Waiting for another Vintage Backup process (PID: {other_pid})"
                            f" to finish {other_operation} in {self.lock_file_path.parent} ...")
                last_pid = other_pid

        self.heartbeat.start()

    def __exit__(self, *_: object) -> None:
        """Release the file lock."""
        self.heartbeat.terminate()
        self.heartbeat.join()
        self.lock_file_path.unlink()

    def acquire_lock(self) -> bool:
        """
        Attempt to create the lock file.

        Returns whether locking was successful.
        """
        try:
            self.write_heartbeat("x")
            return True
        except FileExistsError:
            return False

    def heartbeat_writer(self) -> None:
        """Write PID and heartbeat counter periodically to file to indicate lock is still valid."""
        while True:
            self.heartbeat_counter += 1
            self.write_heartbeat("w")
            time.sleep(self.heartbeat_period.total_seconds())

    def write_heartbeat(self, mode: Literal["x", "w"]) -> None:
        """
        Write PID and heartbeat counter to the lock file.

        :param mode: Whether to open the file in exclusive mode ("x") or write mode ("w").
        """
        with self.lock_file_path.open(mode) as lock_file:
            lock_file.write(f"{self.pid}\n")
            lock_file.write(f"{self.heartbeat_counter}\n")
            lock_file.write(f"{self.operation}\n")

    def lock_is_stale(self) -> bool:
        """Return True if information in the lock file has not changed in a long time."""
        heartbeat_data_1 = self.recall_heartbeat_data()
        time.sleep(self.stale_timeout.total_seconds())
        heartbeat_data_2 = self.read_heartbeat_data()
        return heartbeat_data_1 == heartbeat_data_2

    def read_heartbeat_data(self) -> tuple[str, str, str]:
        """Get all data from lock file."""
        with self.lock_file_path.open() as lock_file:
            pid = lock_file.readline().strip()
            heartbeat_counter = lock_file.readline().strip()
            operation = lock_file.readline().strip()
            self.previous_heartbeat_data = (pid, heartbeat_counter, operation)
            return self.previous_heartbeat_data

    def read_blocking_pid(self) -> str:
        """Get the PID of the other Vintage Backup process."""
        return self.recall_heartbeat_data()[0]

    def read_blocking_operation(self) -> str:
        """Get the name of the operation that is blocking this run of Vintage Backup."""
        return self.recall_heartbeat_data()[2]

    def recall_heartbeat_data(self) -> tuple[str, str, str]:
        """Return data from previous lock file read if available or read the lock file."""
        return self.previous_heartbeat_data or self.read_heartbeat_data()


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
    year_pattern = "%Y"
    backup_pattern = backup_date_format

    def is_valid_directory(directory: os.DirEntry[str], pattern: str) -> bool:
        name = directory.name.split(" (")[0] if pattern == backup_pattern else directory.name
        try:
            datetime.datetime.strptime(name, pattern)
            return is_real_directory(directory)
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


def is_real_directory(path: Path | os.DirEntry[str]) -> bool:
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

        with filter_file.open() as filters:
            logger.info(f"Filtering items according to {filter_file} ...")
            for line_number, line_raw in enumerate(filters, 1):
                line = line_raw.lstrip().rstrip("\n")
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

                logger.debug(f"Filter added: {line} --> {sign} {pattern}")
                self.entries.append((line_number, sign, pattern))

    def __iter__(self) -> Iterator[tuple[Path, list[str]]]:
        """Generate the paths to backup when used in, for example, a for-loop."""
        return self.filtered_paths()

    def filtered_paths(self) -> Iterator[tuple[Path, list[str]]]:
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
            if is_included == should_include:
                continue

            if path.full_match(pattern):
                self.lines_used.add(line_number)
                is_included = should_include
                logger.debug("File: %s %s by line %d: %s %s",
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
                logger.info(f"{self.filter_file}: line #{line_number}"
                            f" ({sign} {pattern}) had no effect.")


def get_user_location_record(backup_location: Path) -> Path:
    """Return the file that contains the user directory that is backed up at the given location."""
    return backup_location/"vintagebackup.source.txt"


def record_user_location(user_location: Path, backup_location: Path) -> None:
    """Write the user directory being backed up to a file in the base backup directory."""
    user_folder_record = get_user_location_record(backup_location)
    resolved_user_location = user_location.resolve(strict=True)
    logger.debug(f"Writing {resolved_user_location} to {user_folder_record}")
    with user_folder_record.open("w") as user_record:
        user_record.write(str(resolved_user_location) + "\n")


def backup_source(backup_location: Path) -> Path:
    """Read the user directory that was backed up to the given backup location."""
    user_folder_record = get_user_location_record(backup_location)
    with user_folder_record.open() as user_record:
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
                               f" Previously: {recorded_user_folder.resolve()};"
                               f" Now: {user_data_location.resolve()}")
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


def compare_to_backup(user_directory: Path,
                      backup_directory: Path | None,
                      file_names: list[str],
                      examine_whole_file: bool,
                      copy_probability: float) -> tuple[list[str], list[str], list[str]]:
    """
    Sort a list of files according to whether they have changed since the last backup.

    :param user_directory: The subfolder of the user's data currently being walked through
    :param backup_directory: The backup folder that corresponds with the user_directory
    :param file_names: A list of files in the user directory.
    :param examine_whole_file: Whether the contents of the file should be examined, or just file
    attributes.
    :param copy_probability: Instead of hard-linking a file that hasn't changed since the last
    backup, copy it anyway with a given probability.

    The file names will be sorted into three lists and returned in this order: (1) matching files
    that have not changed since the last backup, (2) mismatched files that have changed, (3) error
    files that could not be compared for some reason (usually because it is a new file with no
    previous backup). Files that are symbolic links will be put in the errors list for copying.
    """
    if not backup_directory:
        return [], [], file_names

    file_names, links = separate_links(user_directory, file_names)
    comparison_function = deep_comparison if examine_whole_file else shallow_comparison
    matches, mismatches, errors = comparison_function(user_directory, backup_directory, file_names)
    move_to_errors, matches = separate(matches, random_filter(copy_probability))
    errors.extend(move_to_errors)
    errors.extend(links)

    return matches, mismatches, errors


def deep_comparison(user_directory: Path,
                    backup_directory: Path,
                    file_names: list[str]) -> tuple[list[str], list[str], list[str]]:
    """Inspect file contents to determine if files match the most recent backup."""
    return filecmp.cmpfiles(user_directory, backup_directory, file_names, shallow=False)


def shallow_comparison(user_directory: Path,
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
        if predicate(item):
            true_items.append(item)
        else:
            false_items.append(item)
    return true_items, false_items


def backup_directory(user_data_location: Path,
                     new_backup_path: Path,
                     last_backup_path: Path | None,
                     current_user_path: Path,
                     user_file_names: list[str],
                     examine_whole_file: bool,
                     copy_probability: float,
                     action_counter: Counter[str]) -> None:
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
    if not is_real_directory(current_user_path):
        logger.warning(f"Folder disappeared during backup: {current_user_path}")
        return

    relative_path = current_user_path.relative_to(user_data_location)
    new_backup_directory = new_backup_path/relative_path
    new_backup_directory.mkdir(parents=True)
    previous_backup_directory = last_backup_path/relative_path if last_backup_path else None
    matching, mismatching, errors = compare_to_backup(current_user_path,
                                                      previous_backup_directory,
                                                      user_file_names,
                                                      examine_whole_file,
                                                      copy_probability)

    for file_name in matching:
        previous_backup = cast(Path, previous_backup_directory)/file_name
        new_backup = new_backup_directory/file_name

        if create_hard_link(previous_backup, new_backup):
            action_counter["linked files"] += 1
            logger.debug(f"Linked {previous_backup} to {new_backup}")
        else:
            errors.append(file_name)

    for file_name in itertools.chain(mismatching, errors):
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
    now = (datetime.datetime.strptime(backup_datetime, backup_date_format)
           if isinstance(backup_datetime, str)
           else (backup_datetime or datetime.datetime.now()))
    return Path(str(now.year))/now.strftime(backup_date_format)

def create_new_backup(user_data_location: Path,
                      backup_location: Path,
                      *,
                      filter_file: Path | None,
                      examine_whole_file: bool,
                      force_copy: bool,
                      max_average_hard_links: str | None,
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
    paths_to_backup = Backup_Set(user_data_location, filter_file)
    copy_probability = copy_probability_from_hard_link_count(max_average_hard_links)
    logger.info("Running backup ...")
    for current_user_path, user_file_names in paths_to_backup:
        backup_directory(user_data_location,
                         new_backup_path,
                         last_backup_path,
                         current_user_path,
                         user_file_names,
                         examine_whole_file,
                         copy_probability,
                         action_counter)

    report_backup_file_counts(action_counter)


def report_backup_file_counts(action_counter: Counter[str]) -> None:
    """Log the number of files that were backed up, hardlinked, copied, and failed to copy."""
    logger.info("")
    total_files = sum(count for action, count in action_counter.items()
                      if not action.startswith("failed"))
    action_counter["Backed up files"] = total_files
    name_column_size = max(len(name) for name in action_counter)
    count_column_size = len(str(max(action_counter.values())))
    for action, count in action_counter.items():
        logger.info(f"{action.capitalize():<{name_column_size}} : {count:>{count_column_size}}")

    if total_files == 0:
        logger.warning("No files were backed up!")


def check_paths_for_validity(user_data_location: Path,
                             backup_location: Path,
                             filter_file: Path | None) -> None:
    """Check the given paths for validity and raise an exception for improper inputs."""
    if not user_data_location.is_dir():
        raise CommandLineError(f"The user folder path is not a folder: {user_data_location}")

    if backup_location.exists() and not backup_location.is_dir():
        raise CommandLineError("Backup location exists but is not a folder.")

    if backup_location.is_relative_to(user_data_location):
        raise CommandLineError("Backup destination cannot be inside user's folder:"
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


def search_backups(search_directory: Path,
                   backup_folder: Path,
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
        menu_choices = [f"{name} ({path_type})" for (name, path_type) in menu_list]
        choice = choose_from_menu(menu_choices, "Which path to recover")

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
            path_type = ("Symlink" if backup_copy.is_symlink()
                         else "File" if backup_copy.is_file()
                         else "Folder" if backup_copy.is_dir()
                         else "?")
            menu_choices.append(f"{backup_date} ({path_type})")
        choice = choose_from_menu(menu_choices, "Version to recover")

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


def path_relative_to_backups(user_path: Path, backup_location: Path) -> Path:
    """Return a path to a user's file or folder relative to the backups folder."""
    try:
        user_data_location = backup_source(backup_location)
    except FileNotFoundError:
        raise CommandLineError(f"No backups found at {backup_location}")

    try:
        return user_path.relative_to(user_data_location)
    except ValueError:
        raise CommandLineError(f"{user_path} is not contained in the backup set "
                               f"{backup_location}, which contains {user_data_location}.")


def choose_from_menu(menu_choices: list[str], prompt: str) -> int:
    """
    Let user choose from options presented a numbered list in a terminal.

    :param menu_choices: List of choices
    :param prompt: Message to show user prior to the prompt for a choice.

    :returns int: The returned number is an index into the input list. Note that the user interface
    has the user choose a number from 1 to len(menu_list), but returns a number from 0 to
    len(menu_list) - 1.
    """
    number_column_size = len(str(len(menu_choices)))
    for number, choice in enumerate(menu_choices, 1):
        print(f"{number:>{number_column_size}}: {choice}")

    while True:
        try:
            action_key = "Cmd" if platform.system() == "Darwin" else "Ctrl"
            user_choice = int(input(f"{prompt} ({action_key}-C to quit): "))
            if 1 <= user_choice <= len(menu_choices):
                return user_choice - 1
        except ValueError:
            pass

        print(f"Enter a number from 1 to {len(menu_choices)}")


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


def delete_oldest_backups_for_space(backup_location: Path,
                                    space_requirement: str | None,
                                    min_backups_remaining: int | None = None) -> None:
    """
    Delete backups--starting with the oldest--until enough space is free on the backup destination.

    The most recent backup will never be deleted.

    :param backup_location: The folder containing all backups
    :param space_requirement: The amount of space that should be free after deleting backups. This
    may be expressed in bytes with a unit ("MB", "GB", etc.) or as a percentage ("%") of the total
    storage space.
    """
    if not space_requirement:
        return

    total_storage = shutil.disk_usage(backup_location).total
    free_storage_required = parse_storage_space(space_requirement, total_storage)

    if free_storage_required > total_storage:
        raise CommandLineError(f"Cannot free more storage ({byte_units(free_storage_required)})"
                               f" than exists at {backup_location} ({byte_units(total_storage)})")

    current_free_space = shutil.disk_usage(backup_location).free
    first_deletion_message = ("Deleting old backups to free up "
                              f"{byte_units(free_storage_required)},"
                              f" ({byte_units(current_free_space)} currently free).")

    def stop(backup: Path) -> bool:
        return shutil.disk_usage(backup).free > free_storage_required

    delete_backups(backup_location, min_backups_remaining, first_deletion_message, stop)

    final_free_space = shutil.disk_usage(backup_location).free
    if final_free_space < free_storage_required:
        backups_remaining = len(all_backups(backup_location))
        if backups_remaining == 1:
            logger.warning(f"Could not free up {byte_units(free_storage_required)} of storage"
                           " without deleting most recent backup.")
        else:
            logger.info("Stopped after reaching maximum number of deletions.")


def parse_storage_space(space_requirement: str, total_storage: int) -> float:
    """
    Parse a string into a number of bytes of storage space.

    :param space_requirement: A string indicating an amount of space, either as an absolute number
    of bytes or a percentage of the total storage. Byte units and prefixes are allowed. Percents
    require a percent sign.
    :param total_storage: The total storage space in bytes on the device. Used with percentage
    values.

    >>> parse_storage_space("152 kB", 0)
    152000.0

    >>> parse_storage_space("15%", 1000)
    150.0

    Note that the byte units are case and spacing insensitive.
    >>> parse_storage_space("123gb", 0)
    123000000000.0
    """
    space_text = "".join(space_requirement.upper().split())
    if space_text.endswith("%"):
        try:
            free_fraction_required = float(space_text[:-1])/100
        except ValueError:
            raise CommandLineError(f"Invalid percentage value: {space_requirement}")

        if free_fraction_required > 1:
            raise CommandLineError(f"Percent cannot be greater than 100: {space_requirement}")

        return total_storage*free_fraction_required
    elif space_text[-1].isalpha():
        space_text = space_text.rstrip("B")
        number, prefix = ((space_text[:-1], space_text[-1])
                          if space_text[-1].isalpha() else
                          (space_text, ""))

        try:
            prefix = prefix.lower() if prefix == "K" else prefix
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

    :param time_span: A string consisting of a positive integer followed by a single letter: "d"
    for days, "w" for weeks, "m" for calendar months, and "y" for calendar years.

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
    except ValueError:
        raise CommandLineError(f"Invalid number in time span (must be a whole number): {time_span}")

    if number < 1:
        raise CommandLineError(f"Invalid number in time span (must be positive): {time_span}")

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

        new_date = fix_end_of_month(new_year, new_month, now.day)
        return datetime.datetime.combine(new_date, now.time())
    elif letter == "y":
        new_date = fix_end_of_month(now.year - number, now.month, now.day)
        return datetime.datetime.combine(new_date, now.time())
    else:
        raise CommandLineError(f"Invalid time (valid units: {list("dwmy")}): {time_span}")


def fix_end_of_month(year: int, month: int, day: int) -> datetime.date:
    """
    Fix day if it is past then end of the month (e.g., Feb. 31).

    >>> fix_end_of_month(2023, 2, 31)
    datetime.date(2023, 2, 28)

    >>> fix_end_of_month(2024, 2, 31)
    datetime.date(2024, 2, 29)

    >>> fix_end_of_month(2025, 4, 31)
    datetime.date(2025, 4, 30)
    """
    new_day = day
    while True:
        try:
            return datetime.date(year, month, new_day)
        except ValueError:
            new_day -= 1


def delete_backups_older_than(backup_folder: Path,
                              time_span: str | None,
                              min_backups_remaining: int | None = None) -> None:
    """
    Delete backups older than a given timespan.

    :param backup_folder: The folder containing all backups
    :param time_span: The maximum age of a backup to not be deleted. See
    parse_time_span_to_timepoint() for how the string is formatted.
    """
    if not time_span:
        return

    timestamp_to_keep = parse_time_span_to_timepoint(time_span)
    first_deletion_message = ("Deleting backups prior to "
                              f"{timestamp_to_keep.strftime('%Y-%m-%d %H:%M:%S')}.")

    def stop(backup: Path) -> bool:
        return backup_datetime(backup) >= timestamp_to_keep

    delete_backups(backup_folder, min_backups_remaining, first_deletion_message, stop)
    oldest_backup_date = backup_datetime(all_backups(backup_folder)[0])
    if oldest_backup_date < timestamp_to_keep:
        backups_remaining = len(all_backups(backup_folder))
        if backups_remaining == 1:
            logger.warning(f"Could not delete all backups older than {timestamp_to_keep} without"
                           " deleting most recent backup.")
        else:
            logger.info("Stopped after reaching maximum number of deletions.")


def delete_backups(backup_folder: Path,
                   min_backups_remaining: int | None,
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
    min_backups_remaining = min_backups_remaining if min_backups_remaining else 1
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
                          max_average_hard_links=None,
                          is_backup_move=True,
                          timestamp=backup_datetime(backup))

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
    Verify the most recent backup by comparing with the user's files.

    :param user_folder: The source of the backed up data.
    :param backup_folder: The location of the backed up data.
    :param filter_file: The file that filters which files are backed up.
    :param result_folder: Where the resulting files will be saved.
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

    with (matching_file_name.open("w", encoding="utf8") as matching_file,
          mismatching_file_name.open("w", encoding="utf8") as mismatching_file,
          error_file_name.open("w", encoding="utf8") as error_file):

        for file in (matching_file, mismatching_file, error_file):
            file.write(f"Comparison: {user_folder} <---> {backup_folder}\n")

        for directory, file_names in Backup_Set(user_folder, filter_file):
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


def restore_backup(dated_backup_folder: Path,
                   user_folder: Path,
                   *,
                   delete_extra_files: bool) -> None:
    """
    Return a user's folder to a previously backed up state.

    Existing files that were backed up will be overwritten with the backup.

    :param dated_backup_folder: The backup from which to restore files and folders
    :param user_folder: The folder that will be restored to a previous state.
    :param delete_extra_files: Whether to delete files and folders that are not present in the
    backup.
    """
    logger.info(f"Restoring: {user_folder}")
    logger.info(f"From     : {dated_backup_folder}")
    logger.info(f"Deleting extra files: {delete_extra_files}")
    for current_backup_folder, folder_names, file_names in dated_backup_folder.walk():
        current_backup_path = Path(current_backup_folder)
        current_user_path = user_folder/current_backup_path.relative_to(dated_backup_folder)
        logger.debug(f"Creating {current_user_path}")
        current_user_path.mkdir(parents=True, exist_ok=True)

        for file_name in file_names:
            try:
                source = current_backup_path/file_name
                destination = current_user_path/file_name
                logger.debug(f"Copying {file_name} from {current_backup_path} "
                             f"to {current_user_path}")
                shutil.copy2(source, destination, follow_symlinks=False)
            except Exception as error:
                logger.warning(f"Could not restore {destination} from {source}: {error}")

        if delete_extra_files:
            backed_up_paths = set(folder_names) | set(file_names)
            with os.scandir(current_user_path) as user_data_scan:
                user_paths = {entry.name for entry in user_data_scan}
            for new_name in user_paths - backed_up_paths:
                new_path = current_user_path/new_name
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
    return backups if n == "all" else backups[-int(n):]


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
    logger.info("Backup storage space: "
                f"Total = {byte_units(backup_storage.total)}  "
                f"Used = {byte_units(backup_storage.used)} ({percent_used}%)  "
                f"Free = {byte_units(backup_storage.free)} ({percent_free}%)")
    backups = all_backups(backup_location)
    logger.info(f"Backups stored: {len(backups)}")
    logger.info(f"Earliest backup: {backups[0].name}")


def read_configuation_file(config_file_name: str) -> list[str]:
    """Parse a configuration file into command line arguments."""
    arguments: list[str] = []

    try:
        with open(config_file_name) as file:
            for line_raw in file:
                line = line_raw.strip()
                if not line or line.startswith("#"):
                    continue
                parameter_raw, value_raw = line.split(":", maxsplit=1)
                parameter = parameter_raw.strip().lower()
                value = value_raw.strip()
                if parameter == "config":
                    raise CommandLineError("The parameter `config` within a configuration file"
                                           " has no effect.")
                arguments.append(f"--{"-".join(parameter.split())}")
                if value:
                    arguments.append(value)
    except FileNotFoundError:
        raise CommandLineError(f"Configuation file does not exist: {config_file_name}")

    return arguments


def format_paragraphs(lines: str, line_length: int) -> str:
    """
    Format multiparagraph text in when printing --help.

    :param lines: A string of text with paragraphs are separated by at least two newlines. Indented
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

        if paragraph[0].isspace():
            paragraphs.append(paragraph)
        else:
            paragraphs.append(textwrap.fill(paragraph, line_length))

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
    return Path(arg).resolve() if arg else None


def copy_probability_from_hard_link_count(hard_link_count: str | None) -> float:
    """
    Convert an expected average hard link count into a copy probability.

    In order to prevent the slow increase in time required to make a backup on Windows, this
    function returns a probability of copying an unchanged file instead of hard linking. The
    convesion is p = 1/(h + 1), where h is the hard link count and p is the resulting probability.
    """
    if hard_link_count is None:
        return 0.0

    try:
        average_hard_link_count = int(hard_link_count)
    except ValueError:
        raise CommandLineError(f"Invalid value for hard link count: {hard_link_count}")

    if average_hard_link_count < 1:
        raise CommandLineError("Hard link count must be a positive whole number. "
                               f"Got: {hard_link_count}")

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
        return Path(path).resolve(strict=True)
    except FileNotFoundError:
        raise CommandLineError(f"Could not find {folder_type.lower()}: {path}")


def start_recovery_from_backup(args: argparse.Namespace) -> None:
    """Recover a file or folder from a backup according to the command line."""
    backup_folder = get_existing_path(args.backup_folder, "backup folder")
    choice = None if args.choice is None else int(args.choice)
    with Backup_Lock(backup_folder, "recovery from backup", wait=toggle_is_set(args, "wait")):
        print_run_title(args, "Recovering from backups")
        recover_path(Path(args.recover).resolve(), backup_folder, choice)


def choose_recovery_target_from_backups(args: argparse.Namespace) -> None:
    """Choose what to recover a list of backed up files and folders."""
    backup_folder = get_existing_path(args.backup_folder, "backup folder")
    search_directory = Path(args.list).resolve()
    with Backup_Lock(backup_folder, "recovery from backup", wait=toggle_is_set(args, "wait")):
        print_run_title(args, "Listing recoverable files and directories")
        logger.info(f"Searching for everything backed up from {search_directory} ...")
        chosen_recovery_path = search_backups(search_directory, backup_folder)
        if chosen_recovery_path is not None:
            recover_path(chosen_recovery_path, backup_folder)


def start_move_backups(args: argparse.Namespace) -> None:
    """Parse command line options to move backups to another location."""
    old_backup_location = get_existing_path(args.backup_folder, "current backup location")
    new_backup_location = Path(args.move_backup).resolve()

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

    new_backup_location.mkdir(parents=True, exist_ok=True)
    with (Backup_Lock(old_backup_location, "backup move", wait=toggle_is_set(args, "wait")),
          Backup_Lock(new_backup_location, "backup move", wait=toggle_is_set(args, "wait"))):
        print_run_title(args, "Moving backups")
        move_backups(old_backup_location, new_backup_location, backups_to_move)


def start_verify_backup(args: argparse.Namespace) -> None:
    """Parse command line options for verifying backups."""
    user_folder = get_existing_path(args.user_folder, "user's folder")
    backup_folder = get_existing_path(args.backup_folder, "backup folder")
    filter_file = path_or_none(args.filter)
    result_folder = Path(args.verify).resolve()
    with Backup_Lock(backup_folder, "backup verification", wait=toggle_is_set(args, "wait")):
        print_run_title(args, "Verifying last backup")
        verify_last_backup(user_folder, backup_folder, filter_file, result_folder)


def start_backup_restore(args: argparse.Namespace) -> None:
    """Parse command line arguments for a backup recovery."""
    backup_folder = get_existing_path(args.backup_folder, "backup folder")

    if args.destination:
        destination = Path(args.destination).resolve()
        user_folder = None
    else:
        user_folder = get_existing_path(args.user_folder, "user folder")
        confirm_user_location_is_unchanged(user_folder, backup_folder)
        destination = user_folder

    confirm_choice_made(args, "delete_extra", "keep_extra")
    delete_extra_files = bool(args.delete_extra)

    confirm_choice_made(args, "last_backup", "choose_backup")
    choice = None if args.choice is None else int(args.choice)
    restore_source = (find_previous_backup(backup_folder)
                      if args.last_backup else
                      choose_backup(backup_folder, choice))

    if not restore_source:
        raise CommandLineError(f"No backups found in {backup_folder}")

    with Backup_Lock(backup_folder, "restoration from backup", wait=toggle_is_set(args, "wait")):
        print_run_title(args, "Restoring user data from backup")

        required_response = "yes"
        logger.info(f"This will overwrite all files in {user_folder} and subfolders with files "
                    f"in {restore_source}.")
        if delete_extra_files:
            logger.info("Any files that were not backed up, including newly created files and "
                        "files not backed up because of --filter, will be deleted.")
        automatic_response = "no" if args.bad_input else required_response
        response = (automatic_response if args.skip_prompt
                    else input(f'Do you want to continue? Type "{required_response}" to proceed '
                               'or press Ctrl-C to cancel: '))

        if response.strip().lower() == required_response:
            restore_backup(restore_source, destination, delete_extra_files=delete_extra_files)
        else:
            logger.info(f'The response was "{response}" and not "{required_response}", '
                        'so the restoration is cancelled.')


def confirm_choice_made(args: argparse.Namespace, option1: str, option2: str) -> None:
    """Make sure that at least one of two argument parameters are present."""
    args_dict = vars(args)
    if not args_dict.get(option1) and not args_dict.get(option2):
        raise CommandLineError("One of the following are required: "
                               f"--{option1.replace("_", "-")} or --{option2.replace("_", "-")}")


def start_backup(args: argparse.Namespace) -> None:
    """
    Parse command line arguments to start a backup.

    :returns Path: The base directory where all backups are stored.
    """
    user_folder = get_existing_path(args.user_folder, "user's folder")

    if not args.backup_folder:
        raise CommandLineError("Backup folder not specified.")

    backup_folder = Path(args.backup_folder).resolve()
    backup_folder.mkdir(parents=True, exist_ok=True)

    with Backup_Lock(backup_folder, "backup", wait=toggle_is_set(args, "wait")):
        print_run_title(args, "Starting new backup")
        create_new_backup(user_folder,
                          backup_folder,
                          filter_file=path_or_none(args.filter),
                          examine_whole_file=toggle_is_set(args, "whole_file"),
                          force_copy=toggle_is_set(args, "force_copy"),
                          max_average_hard_links=args.hard_link_count,
                          timestamp=args.timestamp)
        delete_old_backups(args)


def delete_old_backups(args: argparse.Namespace) -> None:
    """Delete the oldest backups by various criteria in the command line options."""
    backup_folder = get_existing_path(args.backup_folder, "backup folder")
    backup_count = len(all_backups(backup_folder))
    max_deletions = None if args.max_deletions is None else int(args.max_deletions)
    min_backups_remaining = None if max_deletions is None else max(backup_count - max_deletions, 1)
    delete_oldest_backups_for_space(backup_folder, args.free_up, min_backups_remaining)
    delete_backups_older_than(backup_folder, args.delete_after, min_backups_remaining)
    print_backup_storage_stats(backup_folder)


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

- If a folder is completely empty, whether because it was already empty or everything inside was
filtered out, it will not appear in the backup.

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
This option requires the --backup-folder option to specify which
backup location to search."""))

    only_one_action_group.add_argument("--list", metavar="DIRECTORY", nargs="?", const=".",
                                       help=format_help("""
Recover a file or folder in the directory specified by the argument by first choosing what to
recover from a list of everything that's ever been backed up. If there is no folder specified
after --list, then the current directory is used. The backup location argument --backup-folder
is required."""))

    only_one_action_group.add_argument("--move-backup", metavar="NEW_BACKUP_LOCATION",
                                       help=format_help("""
Move a backup set to a new location. The value of this argument is the new location. The
--backup-folder option is required to specify the current location of the backup set, and one
of --move-count, --move-age, or --move-since is required to specify how many of the most recent
backups to move. Moving each dated backup will take just as long as a normal backup to move since
the hard links to previous backups will be recreated to preserve the space savings, so some planning
is needed when deciding how many backups should be moved."""))

    only_one_action_group.add_argument("--verify", metavar="RESULT_DIR", help=format_help("""
Verify the latest backup by comparing them against the original files. The result of the comparison
will be placed in the folder RESULT_DIR. The result is three files: a list of files that match, a
list of files that do not match, and a list of files that caused errors during the comparison. The
arguments --user-folder and --backup-folder are required. If a filter file was used to create the
backup, then --filter should be supplied as well."""))

    only_one_action_group.add_argument("--restore", action="store_true", help=format_help("""
This action restores the user's folder to a previous, backed up state. Any existing user files that
have the same name as one in the backup will be overwritten. The --backup-folder is required to
specify from where to restore. See the Restore Options section below for the other required
parameters."""))

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
wildcard characters like *, **, [], and ? to allow for matching multiple path names. If you want to
match a single name that contains wildcards, put brackets around them: What Is Life[?].pdf, for
example. Only files will be matched against each line in this file. If you want to include or
exclude an entire directory, the line must end with a "/**" or "\\**" to match all of its contents.

Each line should begin with a minus (-), plus (+), or hash (#). Lines with minus signs specify
files and folders to exclude. Lines with plus signs specify files and folders to include. Lines
with hash signs are ignored. All paths must reside within the directory tree of the
--user-folder. For example, if backing up C:\\Users\\Alice, the following filter file:

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

    backup_group.add_argument("-w", "--whole-file", action="store_true", help=format_help("""
Examine the entire contents of a file to determine if it has
changed and needs to be copied to the new backup. Without this
option, only the file's size, type, and modification date are
checked for differences. Using this option will make backups
take considerably longer."""))

    add_no_option(backup_group, "whole-file")

    backup_group.add_argument("--free-up", metavar="SPACE", help=format_help("""
After a successful backup, delete old backups until the amount of free space on the
backup destination is at least SPACE. The SPACE argument can be in one of two forms.
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
After a successful backup, delete backups if they are older than the time span in the argument.
The format of the argument is Nt, where N is a whole number and
t is a single letter: d for days, w for weeks, m for calendar months,
or y for calendar years. There should be no space between the number
and letter.

No matter what, the most recent backup will not be deleted."""))

    backup_group.add_argument("--max-deletions", help=format_help("""
Specify the maximum number of deletions per program run."""))

    backup_group.add_argument("--force-copy", action="store_true", help=format_help("""
Copy all files instead of linking to files previous backups. The
new backup will contain new copies of all of the user's files,
so the backup location will require much more space than a normal
backup."""))

    add_no_option(backup_group, "force-copy")

    backup_group.add_argument("--hard-link-count", help=format_help("""
Specify the average number of hard links Vintage Backup should create for a file before copying it
again. The argument HARD_LINK_COUNT should be an integer. If specified, every unchanged file will be
copied with a probability of 1/(HARD_LINK_COUNT + 1).

This is probably only useful for Windows machines. If a lot of files being backed up are not
changing, the backups will gradually slow down as the number of hard links increases. This is due to
peculiarities of the NTFS file system."""))

    move_group = user_input.add_argument_group("Move backup options", format_text("""
Use exactly one of these options to specify which backups to move when using --move-backup."""))

    only_one_move_group = move_group.add_mutually_exclusive_group()

    only_one_move_group.add_argument("--move-count", help=format_help("""
Specify the number of the most recent backups to move or "all" if every backup should be moved
to the new location."""))

    only_one_move_group.add_argument("--move-age", help=format_help("""
Specify the maximum age of backups to move. See --delete-after for the time span format to use."""))

    only_one_move_group.add_argument("--move-since", help=format_help("""
Move all backups made on or after the specified date (YYYY-MM-DD)."""))

    restore_group = user_input.add_argument_group("Restore Options", format_help("""
Exactly one of each of the following option pairs(--last-backup/--choose-backup and
--delete-extra/--keep-extra) is required when restoring a backup. The --destination option is
optional."""))

    choose_restore_backup_group = restore_group.add_mutually_exclusive_group()

    choose_restore_backup_group.add_argument("--last-backup", action="store_true",
                                             help=format_help("""
Restore from the most recent backup."""))

    choose_restore_backup_group.add_argument("--choose-backup", action="store_true",
                                             help=format_help("""
Choose which backup to restore from a list."""))

    restore_preservation_group = restore_group.add_mutually_exclusive_group()

    restore_preservation_group.add_argument("--delete-extra", action="store_true",
                                            help=format_help("""
Delete any extra files that are not in the backup."""))

    restore_preservation_group.add_argument("--keep-extra", action="store_true",
                                            help=format_help("""
Preserve any extra files that are not in the backup."""))

    restore_group.add_argument("--destination", help=format_help("""
Specify a different destination for the backup restoration. Either this or the --user-folder option
is required when recovering from a backup."""))

    other_group = user_input.add_argument_group("Other options")

    other_group.add_argument("--wait", action="store_true", help=format_help("""
By default, if another Vintage Backup process is using the backup location, Vintage Backup will
exit. With this parameter, the program will wait until the other process finishes before
continuing."""))

    add_no_option(other_group, "wait")

    other_group.add_argument("-c", "--config", metavar="FILE_NAME", help=format_help(r"""
Read options from a configuration file instead of command-line arguments. The format
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
Whitespace at the beginning and end of the values will be trimmed off.

If both --config and other command line options are used and they conflict, then the command
line options override the config file options.

A final note: the parameter "config" does nothing inside a config file and will cause the program to
quit with an error."""))

    other_group.add_argument("--debug", action="store_true", help=format_help("""
Log information on all actions during a program run."""))

    add_no_option(other_group, "debug")

    default_log_file_name = Path.home()/"vintagebackup.log"
    other_group.add_argument("-l", "--log", default=str(default_log_file_name),
                             help=format_help(f"""
Where to log the activity of this program. The default is
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


def parse_command_line(argv: list[str], user_input: argparse.ArgumentParser) -> argparse.Namespace:
    """Parse the command line options and incorporate configuration file options if needed."""
    if argv and argv[0] == sys.argv[0]:
        argv = argv[1:]

    command_line_options = argv or ["--help"]
    command_line_args = user_input.parse_args(command_line_options)
    if command_line_args.config:
        file_options = read_configuation_file(command_line_args.config)
        return user_input.parse_args(file_options + command_line_options)
    else:
        return command_line_args


def main(argv: list[str]) -> int:
    """
    Start the main program.

    :param argv: A list of command line arguments as from sys.argv
    """
    try:
        user_input = argument_parser()
        args = parse_command_line(argv, user_input)
        if args.help:
            user_input.print_help()
            return 0

        setup_log_file(logger, args.log)
        logger.setLevel(logging.DEBUG if toggle_is_set(args, "debug") else logging.INFO)
        logger.debug(args)

        action = (start_recovery_from_backup if args.recover
                  else choose_recovery_target_from_backups if args.list
                  else start_move_backups if args.move_backup
                  else start_verify_backup if args.verify
                  else start_backup_restore if args.restore
                  else start_backup)
        action(args)
        return 0
    except CommandLineError as error:
        if __name__ == "__main__":
            user_input.print_usage()
        logger.error(error)
    except Exception as error:
        logger.error(error)

    return 1


if __name__ == "__main__":
    try:
        set_start_method("spawn")
        freeze_support()
        logger.addHandler(logging.StreamHandler(sys.stdout))
        sys.exit(main(sys.argv))
    except KeyboardInterrupt:
        sys.exit(1)
