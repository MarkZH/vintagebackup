"""Utilities for working with backups."""

import datetime
import argparse
from collections.abc import Callable

from lib.filesystem import Absolute_Path
from lib.datetime_calculations import parse_time_span_to_timepoint


backup_date_format = "%Y-%m-%d %H-%M-%S"


def backup_datetime(backup: Absolute_Path) -> datetime.datetime:
    """Get the timestamp of a backup from the backup folder name."""
    return datetime.datetime.strptime(backup.name, backup_date_format)


def all_backups(backup_location: Absolute_Path) -> list[Absolute_Path]:
    """Return a sorted list of all backups at the given location."""

    def is_valid_directory(date_folder: Absolute_Path) -> bool:
        try:
            year = datetime.datetime.strptime(date_folder.parent.name, "%Y").year
            date = datetime.datetime.strptime(date_folder.name, backup_date_format)
            return year == date.year and date_folder.is_real_directory()
        except ValueError:
            return False

    all_backup_list: list[Absolute_Path] = []
    for year_folder in filter(Absolute_Path.is_real_directory, backup_location.iterdir()):
        all_backup_list.extend(filter(is_valid_directory, year_folder.iterdir()))

    return sorted(all_backup_list)


def find_previous_backup(backup_location: Absolute_Path) -> Absolute_Path | None:
    """Return the most recent backup at the given location."""
    try:
        return all_backups(backup_location)[-1]
    except IndexError:
        return None


def should_do_periodic_action(
        args: argparse.Namespace, action: str,
        backup_folder: Absolute_Path,
        previous_action_lookup: Callable[[Absolute_Path], datetime.datetime | None]) -> bool:
    """Check whether the action has taken place recently according to --{action}-every argument."""
    options = vars(args)
    if options[f"no_{action}"]:
        return False

    if options[action]:
        return True

    time_span = options[f"{action}_every"]
    if not time_span:
        return False

    previous_action_date = previous_action_lookup(backup_folder)
    if not previous_action_date:
        return True

    now = (
        datetime.datetime.strptime(args.timestamp, backup_date_format) if args.timestamp
        else datetime.datetime.now())
    required_action_date = parse_time_span_to_timepoint(time_span, now)
    return previous_action_date <= required_action_date
