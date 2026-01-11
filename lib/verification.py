"""Functions for verifying the user's data is successfully backed up."""

import argparse
import filecmp
import logging
import hashlib
import datetime
from pathlib import Path
import tempfile
import shutil

from lib.argument_parser import path_or_none
import lib.backup_utilities as util
from lib.backup_info import backup_source
from lib.backup_set import Backup_Set
from lib.console import print_run_title
from lib.datetime_calculations import parse_time_span_to_timepoint
from lib.exceptions import CommandLineError
import lib.filesystem as fs
from lib import console

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

    last_backup_folder = util.find_previous_backup(backup_folder)

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
verify_checksum_file_name = "checksum_verification.txt"


def create_checksum_for_last_backup(backup_folder: Path) -> None:
    """Create a file containing checksums of all files in the latest backup."""
    last_backup = util.find_previous_backup(backup_folder)
    if not last_backup:
        raise CommandLineError(f"Could not find backup in {backup_folder}")

    create_checksum_for_folder(last_backup)
    logger.info("Done creating checksum file")


def create_checksum_for_folder(folder: Path) -> Path:
    """Create a file containing checksums of all files in the given folder."""
    checksum_path = fs.unique_path_name(folder/checksum_file_name)
    logger.info("Creating checksum file: %s ...", checksum_path)
    with checksum_path.open("w", encoding="utf8") as checksum_file:
        for current_directory, _, file_names in folder.walk():
            for file_name in file_names:
                path = current_directory/file_name
                if path == checksum_path:
                    continue
                digest = get_file_checksum(path)
                relative_path = path.relative_to(folder)
                checksum_file.write(f"{relative_path} {digest}\n")

    return checksum_path


def get_file_checksum(path: Path) -> str:
    """
    Read a file and calculate its checksum.

    Returns a hexadecimal string.
    """
    with path.open("rb") as file:
        return hashlib.file_digest(file, hash_function).hexdigest()


def start_checksum(args: argparse.Namespace) -> None:
    """Create checksum file for latest backup if specified by arguments."""
    backup_folder = fs.absolute_path(args.backup_folder)
    if should_do_periodic_action(args, "checksum", backup_folder):
        create_checksum_for_last_backup(backup_folder)


def last_checksum(backup_folder: Path) -> datetime.datetime | None:
    """Find the date of the last backup with a checksum file."""
    backup_found = None
    for backup in util.all_backups(backup_folder):
        if fs.find_unique_path(backup/checksum_file_name):
            backup_found = util.backup_datetime(backup)
    return backup_found


def verify_backup_checksum(backup_folder: Path, result_directory: Path) -> Path | None:
    """Verify the checksums of backed up files and write changed files to a new file."""
    checksum_path = fs.find_unique_path(backup_folder/checksum_file_name)
    if not checksum_path:
        raise ValueError(f"Could not find checksum file in {backup_folder}")

    with (checksum_path.open(encoding="utf8") as checksum_file,
          tempfile.TemporaryFile("w+", encoding="utf8") as temp):
        temp.write(f"Verifying checksums of {backup_folder}\n")
        write_count = 0
        for line_raw in checksum_file:
            line = line_raw.rstrip()
            if not line:
                continue
            relative_path, checksum = line.rsplit(" ", maxsplit=1)
            backup_path = backup_folder/relative_path
            current_checksum = get_file_checksum(backup_path)
            if current_checksum != checksum:
                logger.warning("File changed in backup: %s", relative_path)
                write_count += temp.write(f"{relative_path} {checksum} {current_checksum}\n")

        checksum_verify_path = None
        if write_count > 0:
            checksum_verify_path = fs.unique_path_name(result_directory/verify_checksum_file_name)
            logger.warning("Writing changed files to %s ...", checksum_verify_path)
            with checksum_verify_path.open("w", encoding="utf8") as checksum_verify_file:
                temp.seek(0)
                shutil.copyfileobj(temp, checksum_verify_file)

        return checksum_verify_path


def start_verify_checksum(args: argparse.Namespace) -> None:
    """Parse command line for verifying backup checksums."""
    backup_folder = fs.absolute_path(args.backup_folder)
    checksummed_backups = [
        backup for backup in util.all_backups(backup_folder)
        if fs.find_unique_path(backup/checksum_file_name)]

    if not checksummed_backups:
        raise CommandLineError(f"No backups with checksums found in {backup_folder}")

    if args.oldest:
        target = checksummed_backups[0]
    elif args.newest:
        target = checksummed_backups[-1]
    else:
        choice = console.choose_from_menu(
            [p.name for p in checksummed_backups],
            "Choose a backup to verify its checksum")
        target = checksummed_backups[choice]

    result_folder = fs.absolute_path(args.verify_checksum)
    verify_backup_checksum(target, result_folder)


def should_do_periodic_action(args: argparse.Namespace, action: str, backup_folder: Path) -> bool:
    """Check whether the action has taken place recently according to --{action}-every argument."""
    options = vars(args)
    if options[f"no_{action}"]:
        return False

    if options[action]:
        return True

    time_span = options[f"{action}_every"]
    if not time_span:
        return False

    previous_action_lookup = last_checksum if action == "checksum" else None
    if not previous_action_lookup:
        raise ValueError(f"No backup info lookup for {action}")

    previous_action_date = previous_action_lookup(backup_folder)
    if not previous_action_date:
        return True

    now = (
        datetime.datetime.strptime(args.timestamp, util.backup_date_format) if args.timestamp
        else datetime.datetime.now())
    required_action_date = parse_time_span_to_timepoint(time_span, now)
    return previous_action_date <= required_action_date
