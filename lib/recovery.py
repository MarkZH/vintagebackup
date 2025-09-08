"""Functions for recovering individual files and folders from backups."""

import logging
import shutil
import argparse
import enum
from pathlib import Path
from typing import cast

from lib.backup import all_backups
from lib.backup_info import backup_source
from lib.console import cancel_key, choose_from_menu, print_run_title
from lib.exceptions import CommandLineError
from lib.filesystem import (
    absolute_path,
    get_existing_path,
    is_real_directory,
    unique_path_name,
    classify_path)

logger = logging.getLogger()


def search_backups(
        search_directory: Path,
        backup_folder: Path,
        operation: str,
        choice: int | None = None) -> Path | None:
    """
    Choose a path from among all backups for all items in the given directory.

    The user will pick from a list of all files and folders in search_directory that have ever been
    backed up.

    :param search_directory: The directory from which backed up files and folders will be listed
    :param backup_folder: The backup destination
    :param operation: The name of the operation that called for this search. This will be put into
    the user choice prompt "Which path for {operation}:".
    :param choice: Pre-selected choice of which file to recover (used for testing).

    :returns Path | None: The path to a file or folder that will then be searched for among backups,
    or None if no backed up files are found in the search_directory.
    """
    target_relative_path = directory_relative_to_backup(search_directory, backup_folder)

    all_paths: set[tuple[str, str]] = set()
    for backup in all_backups(backup_folder):
        backup_search_directory = backup/target_relative_path
        try:
            all_paths.update(
                (item.name, classify_path(item))
                for item in backup_search_directory.iterdir())
        except FileNotFoundError:
            continue

    if not all_paths:
        logger.info("No backups found for the folder %s", search_directory)
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


def recover_path(
        recovery_path: Path,
        backup_location: Path,
        *,
        search: bool,
        choice: int | str | None = None) -> None:
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
        logger.info("No backups found for %s", recovery_path)
        return

    backup_choices = sorted(unique_backups.values())
    if search:
        choice = cast(str, choice)
        binary_search_recovery(recovery_path, backup_choices, choice)
    else:
        choice = cast(int, choice)
        recover_from_menu(recovery_path, backup_location, backup_choices, choice)


def recover_from_menu(
        recovery_path: Path,
        backup_location: Path,
        backup_choices: list[Path],
        choice: int | None) -> None:
    """Choose which version of a path to recover from a list of backup dates."""
    if choice is None:
        menu_choices: list[str] = []
        for backup_copy in backup_choices:
            backup_date = backup_copy.relative_to(backup_location).parts[1]
            path_type = classify_path(backup_copy)
            menu_choices.append(f"{backup_date} ({path_type})")
        choice = choose_from_menu(menu_choices, "Version to recover")
    chosen_path = backup_choices[choice]
    recover_path_to_original_location(chosen_path, recovery_path)


def recover_path_to_original_location(backed_up_source: Path, destination: Path) -> None:
    """
    Copy a path from backup without clobbering existing data.

    :param backed_up_source: The file or folder to be copied to its original location.
    :param destination: The full path to the original location. This should include the name of
    the recovered file or folder, not just the destination folder.
    """
    if destination.exists(follow_symlinks=False) and destination.name != backed_up_source.name:
        raise RuntimeError(
            "The path to the backup and the path to the original location must have the same name:"
            f"\n{backed_up_source}\n{destination}")

    recovered_path = unique_path_name(destination)
    logger.info("Copying %s to %s", backed_up_source, recovered_path)
    if is_real_directory(backed_up_source):
        shutil.copytree(backed_up_source, recovered_path, symlinks=True)
    else:
        shutil.copy2(backed_up_source, recovered_path, follow_symlinks=False)


def binary_search_recovery(
        recovery_path: Path,
        backup_choices: list[Path],
        binary_choices: str | None = None) -> None:
    """Choose a version of a path to recover by searching with the user deciding older or newer."""
    binary_choices = binary_choices or ""
    in_testing = bool(binary_choices)
    while True:
        index = len(backup_choices)//2
        path_to_backup = backup_choices[index]
        recover_path_to_original_location(path_to_backup, recovery_path)

        if in_testing and not binary_choices and len(backup_choices) > 1:
            raise RuntimeError("Binary choices for testing exhausted.")

        response = binary_choices[0] if binary_choices else prompt_for_binary_choice(backup_choices)
        binary_choices = binary_choices[1:]

        match response:
            case Binary_Response.CORRECT:
                return
            case Binary_Response.OLDER:
                backup_choices = backup_choices[:index]
            case Binary_Response.NEWER:
                backup_choices = backup_choices[index + 1:]


class Binary_Response(enum.StrEnum):
    """Valid values for user responses during binary search recovery."""

    CORRECT = "c"
    OLDER = "o"
    NEWER = "n"


def prompt_for_binary_choice(backup_choices: list[Path]) -> Binary_Response:
    """Prompt user for which set of backups to search next during binary search."""
    if len(backup_choices) == 1:
        logger.info("Only one choice for recovery.")
        return Binary_Response.CORRECT  # Since there's only one choice, it has to be correct.

    special_list_length = 2
    special_case = len(backup_choices) == special_list_length
    valid_choices = (
        {Binary_Response.CORRECT, Binary_Response.OLDER}
        if special_case else Binary_Response)
    question = (
            "Is the data [C]orrect, or do you want the [O]lder version?"
            if special_case else
            "Is the data [C]orrect, or do you want an [O]lder or [N]ewer version?")
    print(f"Press {cancel_key()} to quit early.")
    prompt = f"{question} [{'/'.join(valid_choices)}]: "
    while True:
        response = input(prompt)
        if not response:
            continue
        response = response[0].lower()
        if response in valid_choices:
            return cast(Binary_Response, response)
        else:
            print("Invalid response")


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


def start_recovery_from_backup(args: argparse.Namespace) -> None:
    """Recover a file or folder from a backup according to the command line."""
    backup_folder = get_existing_path(args.backup_folder, "backup folder")
    try:
        choice: int | str | None = None if args.choice is None else int(args.choice)
    except ValueError:
        choice = str(args.choice)
    print_run_title(args, "Recovering from backups")
    recover_path(absolute_path(args.recover), backup_folder, search=args.search, choice=choice)


def choose_target_path_from_backups(args: argparse.Namespace) -> Path | None:
    """Choose a path from a list of backed up files and folders from a given directory."""
    operation = "recovery" if args.list else "purging"
    backup_folder = get_existing_path(args.backup_folder, "backup folder")
    search_directory = absolute_path(args.list or args.purge_list)
    print_run_title(args, f"Listing files and directories for {operation}")
    logger.info("Searching for everything backed up from %s ...", search_directory)
    test_choice = None if args.choice is None else int(args.choice)
    return search_backups(search_directory, backup_folder, operation, test_choice)


def choose_recovery_target_from_backups(args: argparse.Namespace) -> None:
    """Choose what to recover from a list of everything backed up from a folder."""
    backup_folder = get_existing_path(args.backup_folder, "backup folder")
    chosen_recovery_path = choose_target_path_from_backups(args)
    if chosen_recovery_path:
        recover_path(chosen_recovery_path, backup_folder, search=args.search)
