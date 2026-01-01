"""Utilities for working with backups."""

import datetime
from pathlib import Path

from lib.filesystem import is_real_directory


backup_date_format = "%Y-%m-%d %H-%M-%S"


def backup_datetime(backup: Path) -> datetime.datetime:
    """Get the timestamp of a backup from the backup folder name."""
    return datetime.datetime.strptime(backup.name, backup_date_format)


def all_backups(backup_location: Path) -> list[Path]:
    """Return a sorted list of all backups at the given location."""

    def is_valid_directory(date_folder: Path) -> bool:
        try:
            year = datetime.datetime.strptime(date_folder.parent.name, "%Y").year
            date = datetime.datetime.strptime(date_folder.name, backup_date_format)
            return year == date.year and is_real_directory(date_folder)
        except ValueError:
            return False

    all_backup_list: list[Path] = []
    for year_folder in filter(is_real_directory, backup_location.iterdir()):
        all_backup_list.extend(filter(is_valid_directory, year_folder.iterdir()))

    return sorted(all_backup_list)


def find_previous_backup(backup_location: Path) -> Path | None:
    """Return the most recent backup at the given location."""
    try:
        return all_backups(backup_location)[-1]
    except IndexError:
        return None
