"""Functions for deleting old backups."""

import logging
import shutil
import argparse
import datetime
from collections.abc import Callable
from pathlib import Path

from lib.backup_utilities import all_backups, backup_datetime
from lib.backup import print_backup_storage_stats, start_backup
from lib.datetime_calculations import parse_time_span_to_timepoint
from lib.exceptions import CommandLineError
import lib.filesystem as fs
from lib.verification import start_checksum, verify_backup_checksum

logger = logging.getLogger()


def delete_oldest_backups_for_space(
        backup_location: Path,
        space_requirement: str | None,
        verify_checksum_result_folder: Path | None,
        min_backups_remaining: int = 1) -> None:
    """
    Delete backups--starting with the oldest--until enough space is free on the backup destination.

    The most recent backup will never be deleted.

    :param backup_location: The folder containing all backups
    :param space_requirement: How much space should be free after deleting backups. This is
    expressed in bytes with a unit ("MB", etc.) or as a percentage ("%") of the total storage space.
    :param min_backups_remaining: The minimum number of backups remaining after deletions. The most
    recent backup will never be deleted, so the minimum meaningful value is one.
    """
    if not space_requirement:
        return

    total_storage = shutil.disk_usage(backup_location).total
    free_storage_required = fs.parse_storage_space(space_requirement)

    if free_storage_required > total_storage:
        raise CommandLineError(
            f"Cannot free more storage ({fs.byte_units(free_storage_required)})"
            f" than exists at {backup_location} ({fs.byte_units(total_storage)})")

    current_free_space = shutil.disk_usage(backup_location).free
    first_deletion_message = (
        "Deleting old backups to free up "
        f"{fs.byte_units(free_storage_required)}"
        f" ({fs.byte_units(current_free_space)} currently free).")

    def stop(backup: Path) -> bool:
        return shutil.disk_usage(backup).free > free_storage_required

    delete_backups(
        backup_location,
        min_backups_remaining,
        first_deletion_message,
        stop,
        verify_checksum_result_folder)


def delete_backups_older_than(
        backup_folder: Path,
        time_span: str | None,
        verify_checksum_result_folder: Path | None,
        min_backups_remaining: int = 1) -> None:
    """
    Delete backups older than a given timespan.

    :param backup_folder: The folder containing all backups
    :param time_span: The maximum age of a backup to not be deleted. See
    parse_time_span_to_timepoint() for how the string is formatted.
    :param min_backups_remaining: The minimum number of backups remaining after deletions. The most
    recent backup will never be deleted, so the minimum meaningful value is one.
    """
    if not time_span:
        return

    timestamp_to_keep = parse_time_span_to_timepoint(time_span)
    first_deletion_message = (
        f"Deleting backups prior to {timestamp_to_keep.strftime('%Y-%m-%d %H:%M:%S')}.")

    def stop(backup: Path) -> bool:
        return backup_datetime(backup) >= timestamp_to_keep

    delete_backups(
        backup_folder,
        min_backups_remaining,
        first_deletion_message,
        stop,
        verify_checksum_result_folder)


def delete_single_backup(backup: Path, verify_checksum_result_folder: Path | None) -> None:
    """Delete a backup and, if it is the last in a year, the year folder that contains it."""
    if verify_checksum_result_folder:
        verify_backup_checksum(backup, verify_checksum_result_folder)

    fs.delete_directory_tree(backup, ignore_errors=True)
    try:
        year_folder = backup.parent
        year_folder.rmdir()
        logger.info("Deleted empty year folder %s", year_folder)
    except OSError:
        pass

    logger.info("Free space: %s", fs.byte_units(shutil.disk_usage(backup.parent.parent).free))


def delete_backups(
        backup_folder: Path,
        min_backups_remaining: int,
        first_deletion_message: str,
        stop_deletion_condition: Callable[[Path], bool],
        verify_checksum_result_folder: Path | None) -> None:
    """
    Delete backups until a condition is met.

    :param backup_folder: The base folder containing all backups.
    :param min_backups_remaining: The minimum number of backups that should remain after deletions.
    Defaults to 1 if value is less than 1 (at least one backup will always remain).
    :param first_deletion_message: A message to print/log prior to the first deletion if any
    deletions will take place.
    :param stop_deletion_condition: A function that, if it returns True, stops deletions.
    """
    min_backups_remaining = max(1, min_backups_remaining)

    backups_to_delete = all_backups(backup_folder)[:-min_backups_remaining]
    for deletion_count, backup in enumerate(backups_to_delete, 1):
        if stop_deletion_condition(backup):
            break

        if deletion_count == 1:
            logger.info("")
            logger.info(first_deletion_message)

        logger.info("Deleting oldest backup: %s", backup)
        delete_single_backup(backup, verify_checksum_result_folder)

    remaining_backups = all_backups(backup_folder)
    oldest_backup = remaining_backups[0]
    if not stop_deletion_condition(oldest_backup):
        if len(remaining_backups) == 1:
            logger.warning("Stopped backup deletions to preserve most recent backup.")
        else:
            logger.info("Stopped after reaching maximum number of deletions.")


def delete_too_frequent_backups(
        backup_folder: Path,
        args: argparse.Namespace,
        min_backups_remaining: int,
        verify_checksum_result_folder: Path | None) -> None:
    """
    Delete backups according to retention arguments.

    This function deletes backups so that only weekly, monthly, and yearly backups are left.
    """
    check_time_span_parameters(args)

    min_backups_remaining = max(1, min_backups_remaining)
    max_deletions = len(all_backups(backup_folder)) - min_backups_remaining
    deletion_count = 0
    now = datetime.datetime.now()

    def old_enough(date_cutoff: datetime.datetime) -> Callable[[Path], bool]:
        return lambda backup: backup_datetime(backup) < date_cutoff

    for period, period_word, time_span_str in (
            ("7d", "weekly", args.keep_weekly_after),
            ("1m", "monthly", args.keep_monthly_after),
            ("1y", "yearly", args.keep_yearly_after)):

        if time_span_str is None:
            continue

        date_cutoff = parse_time_span_to_timepoint(time_span_str, now)
        backups = list(filter(old_enough(date_cutoff), all_backups(backup_folder)))
        while len(backups) > 1:
            if deletion_count >= max_deletions:
                return
            standard = backups[0]
            next_backup = backups[1]
            next_timestamp = backup_datetime(next_backup)
            earliest_standard_timestamp = parse_time_span_to_timepoint(period, next_timestamp)
            if backup_datetime(standard).date() > earliest_standard_timestamp.date():
                logger.info("Deleting backup (%s) %s", period_word, next_backup)
                deletion_count += 1
                delete_single_backup(next_backup, verify_checksum_result_folder)
                backups.remove(next_backup)
            else:
                backups.remove(standard)


def check_time_span_parameters(args: argparse.Namespace) -> None:
    """Make sure less frequent backup retention time spans are longer than more frequent ones."""
    last_date_cutoff: datetime.datetime | None = None
    last_period_word = ""
    last_time_span_str = ""
    now = datetime.datetime.now()
    for period_word, time_span_str in (
        ("weekly", args.keep_weekly_after),
        ("monthly", args.keep_monthly_after),
        ("yearly", args.keep_yearly_after)):
        if time_span_str is None:
            continue

        date_cutoff = parse_time_span_to_timepoint(time_span_str, now)
        if last_date_cutoff and date_cutoff >= last_date_cutoff:
            raise CommandLineError(
                f"The {period_word} time span ({time_span_str}) is not longer than "
                f"the {last_period_word} time span ({last_time_span_str}). "
                "Less frequent backup specs must have longer time spans.")

        last_date_cutoff = date_cutoff
        last_period_word = period_word
        last_time_span_str = time_span_str


def delete_old_backups(args: argparse.Namespace) -> None:
    """Delete the oldest backups by various criteria in the command line options."""
    backup_folder = fs.get_existing_path(args.backup_folder, "backup folder")
    backup_count = len(all_backups(backup_folder))
    verify_checksum_result_folder = fs.path_or_none(args.verify_checksum_before_deletion)
    max_deletions = int(args.max_deletions or backup_count)
    min_backups_remaining = max(backup_count - max_deletions, 1)
    delete_too_frequent_backups(
        backup_folder, args, min_backups_remaining, verify_checksum_result_folder)
    delete_oldest_backups_for_space(
        backup_folder, args.free_up, verify_checksum_result_folder, min_backups_remaining)
    delete_backups_older_than(
        backup_folder, args.delete_after, verify_checksum_result_folder, min_backups_remaining)
    print_backup_storage_stats(backup_folder)


def delete_before_backup(args: argparse.Namespace) -> None:
    """Delete old backups before running a backup process."""
    delete_old_backups(args)
    start_backup(args)
    start_checksum(args)
