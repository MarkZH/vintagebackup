"""Functions for finding files missing from a user's data."""

import argparse
from pathlib import Path
import logging

from lib.backup_info import backup_source
from lib.backup_set import Backup_Set
from lib.exceptions import CommandLineError
from lib.backup_utilities import all_backups
from lib.console import print_run_title
from lib.filesystem import unique_path_name, path_or_none

logger = logging.getLogger()


def find_missing_files(
        backup_directory: Path,
        filter_file: Path | None,
        result_directory: Path) -> None:
    """Find files that are missing in the user's folder and exist only in backups."""
    backups = all_backups(backup_directory)
    if not backups:
        raise CommandLineError(f"No backups found in {backup_directory}")

    user_directory = backup_source(backup_directory)
    if not user_directory:
        raise CommandLineError(f"Could not find source of backup for {backup_directory}")

    logger.info("Creating list of user files in %s ...", user_directory)
    user_files: set[Path] = set()
    for directory, file_names in Backup_Set(user_directory, filter_file):
        relative_directory = directory.relative_to(user_directory)
        user_files.update(relative_directory/name for name in file_names)

    logger.info("Searching for missing files in backups: %s ...", backup_directory)
    last_seen: dict[Path, Path] = {}  # last_seen[user file] = backup path
    backup_count = len(backups)
    for index, backup in enumerate(backups, 1):
        logger.info("[%d/%d] %s", index, backup_count, backup.name)
        for directory, _, file_names in backup.walk():
            relative_directory = directory.relative_to(backup)
            last_seen.update({
                relative_directory/name: backup for name in file_names
                if relative_directory/name not in user_files})

    if not last_seen:
        logger.info("No missing user files found.")
        return

    logger.warning("Files missing from user folder %s found in %s",
        user_directory,
        backup_directory)
    result_directory.mkdir(parents=True, exist_ok=True)
    result_file = unique_path_name(result_directory/"missing_files.txt")
    logger.warning("Copying list to %s", result_file)
    current_directory: Path | None = None
    with result_file.open("w", encoding="utf8") as result:
        result.write(f"Missing user files found in {backup_directory}:\n")
        for user_file, backup in sorted(last_seen.items()):
            if user_file.parent != current_directory:
                logger.debug(user_file.parent)
                result.write(f"{user_file.parent}\n")
                current_directory = user_file.parent

            line = f"    {user_file.name}    last seen: {backup.name}"
            logger.debug(line)
            result.write(f"{line}\n")


def start_finding_missing_files(args: argparse.Namespace) -> None:
    """Start finding missing files after parsing command line."""
    print_run_title(args, "Finding missing files")
    find_missing_files(Path(args.backup_folder), path_or_none(args.filter), Path(args.find_missing))
