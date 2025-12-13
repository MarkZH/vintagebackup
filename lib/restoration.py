"""Functions for completely restoring a user's directory from backup."""

import logging
import shutil
import argparse
from pathlib import Path

from lib.argument_parser import confirm_choice_made
from lib.backup_utilities import all_backups
from lib.backup_info import backup_source
from lib.backup_utilities import find_previous_backup
from lib.console import cancel_key, choose_from_menu, print_run_title
from lib.exceptions import CommandLineError
from lib.filesystem import absolute_path, delete_path, get_existing_path

logger = logging.getLogger()


def choose_backup(backup_folder: Path, choice: int | None) -> Path | None:
    """Choose a backup from a numbered list shown in a terminal."""
    backup_choices = all_backups(backup_folder)
    if not backup_choices:
        return None

    if choice is not None:
        return backup_choices[choice]

    menu_choices = [str(backup.relative_to(backup_folder)) for backup in backup_choices]
    return backup_choices[choose_from_menu(menu_choices, "Backup to restore")]


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
    logger.info("Restoring: %s", user_folder)
    logger.info("From     : %s", dated_backup_folder)
    logger.info("Deleting extra files: %s", delete_extra_files)
    if absolute_path(user_folder) != absolute_path(destination):
        logger.info("Restoring to: %s", destination)

    for current_backup_path, folder_names, file_names in dated_backup_folder.walk():
        current_user_folder = destination/current_backup_path.relative_to(dated_backup_folder)
        logger.debug("Creating %s", current_user_folder)
        current_user_folder.mkdir(parents=True, exist_ok=True)

        for file_name in file_names:
            file_source = current_backup_path/file_name
            file_destination = current_user_folder/file_name
            logger.debug(
                "Copying %s from %s to %s",
                file_name,
                current_backup_path,
                current_user_folder)
            try:
                shutil.copy2(file_source, file_destination, follow_symlinks=False)
            except Exception as error:
                logger.warning(
                    "Could not restore %s from %s: %s",
                    file_destination,
                    file_source,
                    error)

        if delete_extra_files:
            backed_up_paths = set(folder_names) | set(file_names)
            user_paths = {entry.name for entry in current_user_folder.iterdir()}
            for new_name in user_paths - backed_up_paths:
                new_path = current_user_folder/new_name
                logger.debug("Deleting extra item %s", new_path)
                delete_path(new_path, ignore_errors=True)


def start_backup_restore(args: argparse.Namespace) -> None:
    """Parse command line arguments for a backup recovery."""
    backup_folder = get_existing_path(args.backup_folder, "backup folder")
    if not args.destination:
        raise CommandLineError("The --destination argument is required for restoring backups.")
    destination = absolute_path(args.destination)
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
        "This will overwrite all files in %s and subfolders with files in %s.",
        destination,
        restore_source)
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
            'The response was "%s" and not "%s", so the restoration is cancelled.',
            response,
            required_response)
