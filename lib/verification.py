"""Functions for verifying the user's data is successfully backed up."""

import argparse
import filecmp
import logging
import hashlib
import datetime
from pathlib import Path

from lib.argument_parser import path_or_none
from lib.backup_utilities import find_previous_backup, all_backups, backup_datetime
from lib.backup_info import backup_source
from lib.backup_set import Backup_Set
from lib.console import print_run_title
from lib.datetime_calculations import parse_time_span_to_timepoint
from lib.exceptions import CommandLineError
import lib.filesystem as fs

logger = logging.getLogger()


def verify_last_backup(result_folder: Path, backup_folder: Path, filter_file: Path | None) -> None:
    """
    Verify the most recent backup by comparing with the user's files.

    :param backup_folder: The location of the backed up data.
    :param filter_file: The file that filters which files are backed up.
    :param result_folder: Where the resulting files will be saved.
    """
    try:
        user_folder = backup_source(backup_folder)
    except FileNotFoundError:
        raise CommandLineError(f"No backups found in {backup_folder}") from None

    if not user_folder.is_dir():
        raise CommandLineError(f"Could not find user folder: {user_folder}")

    last_backup_folder = find_previous_backup(backup_folder)

    if not last_backup_folder:
        raise CommandLineError(f"No backups found in {backup_folder}.")

    logger.info("Filter file: %s", filter_file)
    logger.info("Verifying backup in %s by comparing against %s ...", backup_folder, user_folder)

    result_folder.mkdir(parents=True, exist_ok=True)
    matching_file_name = fs.unique_path_name(result_folder/"matching files.txt")
    mismatching_file_name = fs.unique_path_name(result_folder/"mismatching files.txt")
    error_file_name = fs.unique_path_name(result_folder/"error files.txt")

    with (matching_file_name.open("w", encoding="utf8") as matching_file,
        mismatching_file_name.open("w", encoding="utf8") as mismatching_file,
        error_file_name.open("w", encoding="utf8") as error_file):

        for file in (matching_file, mismatching_file, error_file):
            file.write(f"Comparison: {user_folder} <---> {backup_folder}\n")

        for directory, file_names in Backup_Set(user_folder, filter_file):
            relative_directory = directory.relative_to(user_folder)
            backup_directory = last_backup_folder/relative_directory
            matches, mismatches, errors = filecmp.cmpfiles(
                directory,
                backup_directory,
                file_names,
                shallow=False)

            fs.write_directory(matching_file, directory, matches)
            fs.write_directory(mismatching_file, directory, mismatches)
            fs.write_directory(error_file, directory, errors)


def start_verify_backup(args: argparse.Namespace) -> None:
    """Parse command line options for verifying backups."""
    backup_folder = fs.get_existing_path(args.backup_folder, "backup folder")
    filter_file = path_or_none(args.filter)
    result_folder = fs.absolute_path(args.verify)
    print_run_title(args, "Verifying last backup")
    verify_last_backup(result_folder, backup_folder, filter_file)


hash_function = "sha3_256"
checksum_file_name = "checksums.sha3"


def create_checksum_for_last_backup(backup_folder: Path) -> None:
    """Create a file containing checksums of all files in the latest backup."""
    last_backup = find_previous_backup(backup_folder)
    if not last_backup:
        raise CommandLineError(f"Could not find backup in {backup_folder}")

    create_checksum_for_folder(last_backup)
    logger.info("Done creating checksum file")


def create_checksum_for_folder(folder: Path) -> None:
    """Create a file containing checksums of all files in the given folder."""
    checksum_path = fs.unique_path_name(folder/checksum_file_name)
    logger.info("Creating checksum file: %s ...", checksum_path)
    with checksum_path.open("w", encoding="utf8") as checksum_file:
        for current_directory, _, file_names in folder.walk():
            for file_name in file_names:
                path = current_directory/file_name
                if path == checksum_path:
                    continue
                with path.open("rb") as file:
                    digest = hashlib.file_digest(file, hash_function).hexdigest()
                    relative_path = path.relative_to(folder)
                    checksum_file.write(f"{relative_path} {digest}\n")


def start_checksum(args: argparse.Namespace) -> None:
    """Create checksum file for latest backup if specified by arguments."""
    if args.no_checksum:
        return

    backup_folder = fs.absolute_path(args.backup_folder)
    if args.checksum or time_has_passed(args, "checksum", backup_folder):
        create_checksum_for_last_backup(backup_folder)


def last_checksum(backup_folder: Path) -> datetime.datetime | None:
    """Find the date of the last backup with a checksum file."""
    backup_found = None
    for backup in all_backups(backup_folder):
        if fs.unique_path_exists(backup/checksum_file_name):
            backup_found = backup_datetime(backup)
    return backup_found


def time_has_passed(args: argparse.Namespace, action: str, backup_folder: Path) -> bool:
    """Check whether the action has taken place recently according to --{action}-every argument."""
    options = vars(args)
    time_span = options[f"{action}_every"]
    if not time_span:
        return False

    previous_checksum = last_checksum(backup_folder)
    if not previous_checksum:
        return True

    checksum_deadline = parse_time_span_to_timepoint(time_span)
    return previous_checksum < checksum_deadline
