"""Functions for verifying the user's data is successfully backed up."""

import argparse
import filecmp
import logging
from pathlib import Path

from lib.argument_parser import path_or_none
from lib.backup import find_previous_backup
from lib.backup_info import backup_source
from lib.backup_set import Backup_Set
from lib.console import print_run_title
from lib.exceptions import CommandLineError
from lib.filesystem import absolute_path, get_existing_path, unique_path_name, write_directory

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
        raise CommandLineError(f"No backups found in {backup_folder}")

    if not user_folder.is_dir():
        raise CommandLineError(f"Could not find user folder: {user_folder}")

    last_backup_folder = find_previous_backup(backup_folder)

    if last_backup_folder is None:
        raise CommandLineError(f"No backups found in {backup_folder}.")

    logger.info("Filter file: %s", filter_file)
    logger.info("Verifying backup in %s by comparing against %s ...", backup_folder, user_folder)

    result_folder.mkdir(parents=True, exist_ok=True)
    matching_file_name = unique_path_name(result_folder/"matching files.txt")
    mismatching_file_name = unique_path_name(result_folder/"mismatching files.txt")
    error_file_name = unique_path_name(result_folder/"error files.txt")

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

            write_directory(matching_file, directory, matches)
            write_directory(mismatching_file, directory, mismatches)
            write_directory(error_file, directory, errors)


def start_verify_backup(args: argparse.Namespace) -> None:
    """Parse command line options for verifying backups."""
    backup_folder = get_existing_path(args.backup_folder, "backup folder")
    filter_file = path_or_none(args.filter)
    result_folder = absolute_path(args.verify)
    print_run_title(args, "Verifying last backup")
    verify_last_backup(result_folder, backup_folder, filter_file)
