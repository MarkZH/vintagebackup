"""Functions for purging files and folders from all backups."""

import argparse
import logging
from collections import Counter

from lib.backup import backup_staging_folder
from lib.backup_utilities import all_backups, find_previous_backup
from lib.console import choose_from_menu, plural_noun, print_run_title
from lib.filesystem import Absolute_Path, delete_path, get_existing_path, classify_path
from lib import recovery

logger = logging.getLogger()


def choose_purge_target_from_backups(
        args: argparse.Namespace,
        confirmation_response: str | None = None) -> None:
    """Choose which path to purge from a list of everything backed up from a folder."""
    backup_folder = get_existing_path(args.backup_folder, "backup folder")
    chosen_purge_path = recovery.choose_target_path_from_backups(args)
    if chosen_purge_path:
        purge_path(chosen_purge_path, backup_folder, confirmation_response, args.choice)


def start_backup_purge(args: argparse.Namespace, confirmation_reponse: str | None = None) -> None:
    """Parse command line options to purge file or folder from all backups."""
    backup_folder = get_existing_path(args.backup_folder, "backup folder")
    purge_target = Absolute_Path(args.purge)
    print_run_title(args, "Purging from backups")
    purge_path(purge_target, backup_folder, confirmation_reponse, args.choice)


def purge_path(
        purge_target: Absolute_Path,
        backup_folder: Absolute_Path,
        confirmation_reponse: str | None,
        arg_choice: str | None) -> None:
    """Purge a file/folder by deleting it from all backups."""
    relative_purge_target = recovery.path_relative_to_backups(purge_target, backup_folder)

    backup_list = all_backups(backup_folder)
    potential_deletions = (backup/relative_purge_target for backup in backup_list)
    paths_to_delete = list(filter(lambda p: p.exists(), potential_deletions))
    if not paths_to_delete:
        logger.info("Could not find any backed up copies of %s", purge_target)
        return

    path_type_counts = Counter(map(classify_path, paths_to_delete))
    types_to_delete = choose_types_to_delete(paths_to_delete, path_type_counts, arg_choice)

    type_choice_data = [(path_type_counts[path_type], path_type) for path_type in types_to_delete]
    type_list = [f"{plural_noun(count, path_type)}" for count, path_type in type_choice_data]
    logger.info("Path to be purged from backups: %s", purge_target)
    prompt = f"The following items will be deleted: {", ".join(type_list)}.\nProceed? [y/n] "
    confirmation = confirmation_reponse or input(prompt)
    if confirmation.lower() != "y":
        return

    for path in paths_to_delete:
        path_type = classify_path(path)
        if path_type in types_to_delete:
            logger.info("Deleting %s %s ...", path_type, path)
            delete_path(path, ignore_errors=True)

    last_backup = find_previous_backup(backup_folder)
    if backup_list[-1] != last_backup or backup_staging_folder(backup_folder).exists():
        logger.warning(
            "A backup to %s ran during purging. You may want to rerun the "
            "purge after the backup completes.",
            backup_folder)
    logger.info("If you want to prevent the purged item from being backed up in the future,")
    logger.info("consider adding the following line to a filter file:")
    filter_line = (
        relative_purge_target/"**" if purge_target.is_real_directory() else relative_purge_target)
    logger.info("- %s", filter_line)


def choose_types_to_delete(
        paths_to_delete: list[Absolute_Path],
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
