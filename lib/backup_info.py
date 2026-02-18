"""Functions for reading information about previous backups."""

import datetime
import logging
import os
from typing import TypedDict, Literal

from lib.exceptions import CommandLineError
from lib.filesystem import Absolute_Path, default_log_file_name
from lib.backup_utilities import backup_date_format

logger = logging.getLogger()


def get_backup_info_file(backup_location: Absolute_Path) -> Absolute_Path:
    """
    Return the file that contains information about the backup process at the given location.

    The file will containe the user directory that is backed up at the given location and the log
    file for activity at the backup location.
    """
    return backup_location/"vintagebackup.source.txt"


def record_user_location(user_location: Absolute_Path, backup_location: Absolute_Path) -> None:
    """Write the user directory being backed up to a file in the base backup directory."""
    backup_info = read_backup_information(backup_location)
    backup_info["Source"] = user_location
    write_backup_information(backup_location, backup_info)


def backup_source(backup_location: Absolute_Path) -> Absolute_Path | None:
    """Read the user directory that was backed up to the given backup location."""
    return read_backup_information(backup_location)["Source"]


def confirm_user_location_is_unchanged(
        user_data_location: Absolute_Path,
        backup_location: Absolute_Path) -> None:
    """
    Make sure the user directory being backed up is the same as the previous backup run.

    An exception will be thrown when attempting to back up a different user directory to the one
    that was backed up previously. Backing up multiple different directories to the same backup
    location negates the hard linking functionality.
    """
    recorded_user_folder = backup_source(backup_location)
    if not recorded_user_folder:
        # This is probably the first backup, hence no user folder record.
        return

    if not recorded_user_folder.samefile(user_data_location):
        raise CommandLineError(
            "Previous backup stored a different user folder."
            f" Previously: {recorded_user_folder};"
            f" Now: {Absolute_Path(user_data_location)}")


class Backup_Info(TypedDict):
    """Information about a backup folder."""

    Source: Absolute_Path | None
    Log: Absolute_Path | None
    Compare_Timestamp: datetime.datetime | None


def read_backup_information(backup_folder: Absolute_Path) -> Backup_Info:
    """Get information about a backup folder."""
    info_file = get_backup_info_file(backup_folder)
    try:
        extracted_info = Backup_Info(Source=None, Log=None, Compare_Timestamp=None)
        with info_file.open_text(encoding="utf8") as info:
            for line_raw in info:
                line = line_raw.lstrip().removesuffix("\n")
                if not line:
                    continue

                key, value_string = line.split(": ", maxsplit=1)
                key = backup_info_key(key)
                if key == "Compare_Timestamp":
                    timestamp = datetime.datetime.strptime(value_string, backup_date_format)
                    extracted_info[key] = timestamp
                else:
                    path = Absolute_Path(value_string)
                    extracted_info[key] = path
        return extracted_info
    except FileNotFoundError:
        return Backup_Info(Source=None, Log=None, Compare_Timestamp=None)


def backup_info_key(key: str) -> Literal["Source", "Log", "Compare_Timestamp"]:
    """Verify that backup info keys read from a file are valid keys."""
    key = key.strip()
    if key == "Source":
        return "Source"

    if key == "Log":
        return "Log"

    if key == "Compare_Timestamp":
        return "Compare_Timestamp"

    raise KeyError(f"Unknown key for Backup_Info: {key}")


def write_backup_information(backup_folder: Absolute_Path, backup_info: Backup_Info) -> None:
    """Record backup information to a file in the backup folder."""
    info_file = get_backup_info_file(backup_folder)
    info_file.parent.mkdir(parents=True, exist_ok=True)
    with info_file.open_text("w", encoding="utf8") as info:
        for key in map(backup_info_key, backup_info):
            if key == "Compare_Timestamp":
                timestamp = backup_info[key]
                if timestamp:
                    logger.debug("Writing %s : %s to %s", key, timestamp, info_file)
                    info.write(f"{key}: {timestamp.strftime(backup_date_format)}")
            else:
                path = backup_info[key]
                if path:
                    logger.debug("Writing %s : %s to %s", key, path, info_file)
                    info.write(f"{key}: {path}\n")


def backup_log_file(backup_folder: Absolute_Path) -> Absolute_Path | None:
    """Retreive the log file used in the last backup."""
    return read_backup_information(backup_folder)["Log"]


def primary_log_path(log_file_name: str | None, backup_folder: str | None) -> Absolute_Path | None:
    """Determine which file to use for logging."""
    if log_file_name:
        return Absolute_Path(log_file_name) if log_file_name != os.devnull else None
    elif backup_folder:
        log_file_path = backup_log_file(Absolute_Path(backup_folder))
        return log_file_path or default_log_file_name
    else:
        return None


def record_backup_log_file(log_file_path: Absolute_Path, backup_path: Absolute_Path) -> None:
    """Record location of log file used with a backup folder."""
    backup_info = read_backup_information(backup_path)
    backup_info["Log"] = log_file_path
    write_backup_information(backup_path, backup_info)


def record_compare_contents_timestamp(
        backup_location: Absolute_Path,
        timestamp: datetime.datetime) -> None:
    """Record timestamp of last time file contents were compared during backup."""
    info = read_backup_information(backup_location)
    info["Compare_Timestamp"] = timestamp
    write_backup_information(backup_location, info)
