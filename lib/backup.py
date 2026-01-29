"""The main functions for backing up user files."""

import logging
import os
import shutil
import datetime
import argparse
import filecmp
import stat
import math
import random
from collections import Counter
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import cast

from lib.argument_parser import toggle_is_set
from lib.backup_utilities import all_backups, backup_date_format, find_previous_backup
from lib.backup_info import confirm_user_location_is_unchanged, record_user_location
from lib.backup_lock import Backup_Lock
from lib.backup_set import Backup_Set
from lib.console import print_run_title
from lib.exceptions import CommandLineError
import lib.filesystem as fs

logger = logging.getLogger()


def shallow_stats(stats: os.stat_result) -> tuple[int, int, int]:
    """
    Return simple file information for quicker checks for file changes since the last bacukp.

    When not inspecting file contents, only look at the file size, type, and modification time--in
    that order.

    Arguments:
        stats: File information retrieved from a DirEntry.stat() call.
    """
    return (stats.st_size, stat.S_IFMT(stats.st_mode), stats.st_mtime_ns)


def random_filter(probability: float) -> Callable[[object], bool]:
    """Create a filter that chooses items with the given probability."""

    def actual_random_filter(_: object) -> bool:
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

    Arguments:
        user_directory: The subfolder of the user's data currently being walked through
        backup_directory: The backup folder that corresponds with the user_directory
        file_names: A list of files in the user directory.
        examine_whole_file: Whether the contents of the file should be examined, or just file
            attributes.
        copy_probability: Instead of hard-linking a file that hasn't changed since the last
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
    random_copies, matches = separate(matches, random_filter(copy_probability))
    return matches, mismatches + errors + random_copies + links


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
        logger.debug("Could not create hard link due to error: %s", error)
        logger.debug("Previous backed up file: %s", previous_backup)
        logger.debug("Attempted link         : %s", new_backup)
        return False


def separate_links(directory: Path, path_names: list[str]) -> tuple[list[str], list[str]]:
    """
    Separate regular files and folders from symlinks.

    Directories within the given directory are not traversed.

    Arguments:
        directory: The directory containing all the files.
        path_names: A list of names in the directory.

    Returns:
        lists: Two lists: the first a list of regular files, the second a list of symlinks.
    """

    def is_not_link(name: str) -> bool:
        return not (directory/name).is_symlink()

    return separate(path_names, is_not_link)


def separate[T](items: Iterable[T], predicate: Callable[[T], bool]) -> tuple[list[T], list[T]]:
    """
    Separate a sequence of items into two lists according to a predicate.

    Arguments:
        items: A sequence of items to be separated.
        predicate: A function returning True or False for each item.

    Returns:
        lists: Two lists: The first has items where the predicate is True, the second where the
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
        copy_probability: float) -> int:
    """
    Backup the files in a subfolder in the user's directory.

    Arguments:
        user_data_location: The base directory that is being backed up
        new_backup_path: The base directory of the new dated backup
        last_backup_path: The base directory of the previous dated backup
        current_user_path: The user directory currently being walked through
        user_file_names: The names of files contained in the current_user_path
        examine_whole_file: Whether to examine file contents to check for changes since the last
            backup
        copy_probability: Probability of copying a file when it would normally be hard-linked
        action_counter: A counter to track how many files have been linked, copied, or failed for
            both

    Returns:
        size: Total size of copied files in bytes
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
            logger.debug("Linked %s to %s", previous_backup, new_backup)
        else:
            files_to_copy.append(file_name)

    size_of_copied_files = 0
    for file_name in files_to_copy:
        new_backup_file = new_backup_directory/file_name
        user_file = current_user_path/file_name
        try:
            shutil.copy2(user_file, new_backup_file, follow_symlinks=False)
            action_counter["copied files"] += 1
            size_of_copied_files += user_file.stat().st_size
            logger.debug("Copied %s to %s", user_file, new_backup_file)
        except Exception as error:
            logger.warning("Could not copy %s (%s)", user_file, error)
            action_counter["failed copies"] += 1

    return size_of_copied_files


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
        is_backup_move: bool = False) -> int:
    """
    Create a new dated backup.

    Arguments:
        user_data_location: The folder containing the data to be backed up
        backup_location: The base directory of the backup destination. This directory should
            already exist.
        filter_file: A file containg a list of path glob patterns to exclude/include from the
            backup
        examine_whole_file: Whether to examine file contents to check for changes since the last
            backup
        force_copy: Whether to always copy files, regardless of whether a previous backup exists.
        copy_probability: Probability that an unchanged file will be copied instead of hardlinked.
        timestamp: Manually set timestamp of new backup. Used for debugging.
        is_backup_move: Used to customize log messages when moving a backup to a new location.

    Returns:
        size: Total size of copied files in bytes
    """
    check_paths_for_validity(user_data_location, backup_location, filter_file)

    new_backup_path = backup_location/backup_name(timestamp)
    staging_backup_path = backup_staging_folder(backup_location)
    if staging_backup_path.exists():
        logger.info("There is a staging folder leftover from previous incomplete backup.")
        logger.info("Deleting %s ...", staging_backup_path)
        fs.delete_directory_tree(staging_backup_path)

    confirm_user_location_is_unchanged(user_data_location, backup_location)
    record_user_location(user_data_location, backup_location)

    if is_backup_move:
        logger.info("Original backup  : %s", user_data_location)
        logger.info("Temporary backup : %s", new_backup_path)
    else:
        logger.info("User's data      : %s", user_data_location)
        logger.info("Backup location  : %s", new_backup_path)
    logger.info("Staging area     : %s", staging_backup_path)

    last_backup_path = None if force_copy else find_previous_backup(backup_location)
    if last_backup_path:
        logger.info("Previous backup  : %s", last_backup_path)
    elif force_copy:
        logger.info("Copying everything.")
    else:
        logger.info("No previous backups. Copying everything.")

    logger.info("")
    logger.info("Reading file contents = %s", examine_whole_file)

    action_counter: Counter[str] = Counter()
    logger.info("Filter file: %s", filter_file)
    logger.info("Running backup ...")
    size_of_backup = 0
    for current_user_path, user_file_names in Backup_Set(user_data_location, filter_file):
        size_of_backup += backup_directory(
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
    return size_of_backup


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
        logger.info("%*s : %*d", -name_column_size, action.capitalize(), count_column_size, count)

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
        raise CommandLineError(f"Backup location exists but is not a folder: {backup_location}")

    if backup_location.is_relative_to(user_data_location):
        raise CommandLineError(
            "Backup destination cannot be inside user's folder:"
            f" User data: {user_data_location}"
            f"; Backup location: {backup_location}")

    if filter_file and not filter_file.is_file():
        raise CommandLineError(f"Filter file not found: {filter_file}")


def print_backup_storage_stats(backup_location: Path) -> None:
    """Log information about the storage space of the backup medium."""
    backup_storage = shutil.disk_usage(backup_location)
    percent_used = round(100*backup_storage.used/backup_storage.total)
    percent_free = round(100*backup_storage.free/backup_storage.total)
    logger.info("")
    logger.info(
        "Backup storage space: Total = %s  Used = %s (%d%%)  Free = %s (%d%%)",
        fs.byte_units(backup_storage.total),
        fs.byte_units(backup_storage.used),
        percent_used,
        fs.byte_units(backup_storage.free),
        percent_free)
    backups = all_backups(backup_location)
    logger.info("Backups stored: %d", len(backups))
    logger.info("Earliest backup: %s", backups[0].name)


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

    logger.info("Maximum average hard link count = %d", average_hard_link_count)
    return 1/(average_hard_link_count + 1)


def start_backup(args: argparse.Namespace) -> None:
    """Parse command line arguments to start a backup."""
    user_folder = fs.get_existing_path(args.user_folder, "user's folder")

    if not args.backup_folder:
        raise CommandLineError("Backup folder not specified.")

    backup_folder = fs.absolute_path(args.backup_folder)
    backup_folder.mkdir(parents=True, exist_ok=True)

    with Backup_Lock(backup_folder, "backup"):
        print_run_title(args, "Starting new backup")
        filter_file = fs.path_or_none(args.filter)
        backup_space_taken = create_new_backup(
            user_folder,
            backup_folder,
            filter_file=filter_file,
            examine_whole_file=toggle_is_set(args, "compare_contents"),
            force_copy=toggle_is_set(args, "force_copy"),
            copy_probability=copy_probability(args),
            timestamp=args.timestamp)

        logger.info("")
        log_backup_size(args.free_up, backup_space_taken)


def log_backup_size(free_up_parameter: str | None, backup_space_taken: int) -> None:
    """
    Log size of previous backup and warn user if backup is near or over --free-up parameter.

    This should warn the user that a future backup may not have enough storage space to complete
    sucessfully.
    """
    free_up = fs.parse_storage_space(free_up_parameter or "0")
    free_up_percent = math.ceil(100*backup_space_taken/free_up) if free_up else 0
    free_up_text = f" ({free_up_percent}% of --free-up)" if free_up else ""
    free_up_warning_percent = 90
    is_warning = free_up_percent >= free_up_warning_percent
    log_destination = logger.warning if is_warning else logger.info
    log_destination(f"Backup space used: {fs.byte_units(backup_space_taken)}{free_up_text}")
    if is_warning:
        logger.warning("Consider increasing the size of the --free-up parameter.")


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
