"""Functions for moving backups from one location to another."""

import datetime
import logging
import argparse
from pathlib import Path

from lib.argument_parser import confirm_choice_made
from lib.backup import create_new_backup
from lib.backup_info import (
    backup_log_file,
    backup_source,
    get_backup_info_file,
    record_backup_log_file,
    record_user_location)
from lib.backup_lock import Backup_Lock
from lib.backup_utilities import all_backups, backup_datetime
from lib.console import plural_noun, print_run_title
from lib.datetime_calculations import parse_time_span_to_timepoint
from lib.filesystem import absolute_path, get_existing_path

logger = logging.getLogger()


def move_backups(
        old_backup_location: Path,
        new_backup_location: Path,
        backups_to_move: list[Path]) -> None:
    """Move a set of backups to a new location."""
    move_count = len(backups_to_move)
    logger.info("Moving %s", plural_noun(move_count, "backup"))
    logger.info("from %s", old_backup_location)
    logger.info("to   %s", new_backup_location)

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

        backup_source_file = get_backup_info_file(new_backup_location)
        backup_source_file.unlink()
        logger.info("---------------------")

    original_backup_source = backup_source(old_backup_location)
    if original_backup_source:
        record_user_location(original_backup_source, new_backup_location)
    else:
        logger.warning("Could not find source of user data in %s", old_backup_location)

    old_log_file = backup_log_file(old_backup_location)
    if old_log_file:
        record_backup_log_file(old_log_file, new_backup_location)


def last_n_backups(n: str | int, backup_location: Path) -> list[Path]:
    """
    Return a list of the paths of the last n backups.

    Arguments:
        backup_location: The location of the backup set.
        n: A positive integer to get the last n backups, or "all" to get all backups.
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


def start_move_backups(args: argparse.Namespace) -> None:
    """Parse command line options to move backups to another location."""
    old_backup_location = get_existing_path(args.backup_folder, "current backup location")
    new_backup_location = absolute_path(args.move_backup)
    backups_to_move = choose_backups_to_move(args, old_backup_location)
    new_backup_location.mkdir(parents=True, exist_ok=True)
    with Backup_Lock(new_backup_location, "backup move"):
        print_run_title(args, "Moving backups")
        move_backups(old_backup_location, new_backup_location, backups_to_move)


def choose_backups_to_move(args: argparse.Namespace, old_backup_location: Path) -> list[Path]:
    """Choose which backups to move based on the command line arguments."""
    confirm_choice_made(args, "move_count", "move_age", "move_since")
    if args.move_count:
        backups_to_move = last_n_backups(args.move_count, old_backup_location)
    elif args.move_age:
        oldest_backup_date = parse_time_span_to_timepoint(args.move_age)
        backups_to_move = backups_since(oldest_backup_date, old_backup_location)
    elif args.move_since:
        oldest_backup_date = datetime.datetime.strptime(args.move_since, "%Y-%m-%d")
        backups_to_move = backups_since(oldest_backup_date, old_backup_location)
    else:
        raise AssertionError("Should never reach here.")

    return backups_to_move
