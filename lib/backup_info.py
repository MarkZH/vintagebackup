"""Functions for reading information about previous backups."""

import datetime
import logging
import os
from pathlib import Path
from typing import TypedDict, Literal

from lib.exceptions import CommandLineError
from lib.filesystem import absolute_path, default_log_file_name
from lib.backup_utilities import backup_date_format

logger = logging.getLogger()


def get_backup_info_file(backup_location: Path) -> Path:
    """
    Find the file that contains information about the backup process at the given location.

    Arguments:
        backup_location: The folder containing all dated backups

    Returns:
        path: The file containing information about previous backups. See the Backup_Info enum.
    """
    return backup_location/"vintagebackup.source.txt"


def record_user_location(user_location: Path, backup_location: Path) -> None:
    """
    Write the user directory being backed up to a file in the base backup directory.

    Arguments:
        user_location: The folder containing the user's data that will be backed up
        backup_location: The folder containing all dated backups
    """
    backup_info = read_backup_information(backup_location)
    backup_info["Source"] = absolute_path(user_location)
    write_backup_information(backup_location, backup_info)


def backup_source(backup_location: Path) -> Path | None:
    """
    Read the user directory that was backed up to the given backup location.

    Arguments:
        backup_location: The folder containing all dated backups

    Returns:
        path: The source of the backed up data, i.e., the user's data
    """
    return read_backup_information(backup_location)["Source"]


def confirm_user_location_is_unchanged(user_data_location: Path, backup_location: Path) -> None:
    """
    Make sure the user directory being backed up is the same as the previous backup run.

    Arguments:
        user_data_location: The folder that will be backed up
        backup_location: The folder containing all dated backups

    Raises:
        CommandLineError: An exception will be thrown when attempting to back up a different user
            directory to the one that was backed up previously. Backing up multiple different
            directories to the same backup location negates the hard linking functionality.
    """
    recorded_user_folder = backup_source(backup_location)
    if not recorded_user_folder:
        # This is probably the first backup, hence no user folder record.
        return

    if not recorded_user_folder.samefile(user_data_location):
        raise CommandLineError(
            "Previous backup stored a different user folder."
            f" Previously: {absolute_path(recorded_user_folder)};"
            f" Now: {absolute_path(user_data_location)}")


class Backup_Info(TypedDict):
    """Information about a backup folder."""

    # The source of backed up data, i.e., the user's files
    Source: Path | None

    # The log file used in the last backup
    Log: Path | None

    # The last time file contents were compared instead of file type, size, and modification time.
    Compare_Timestamp: datetime.datetime | None


def read_backup_information(backup_folder: Path) -> Backup_Info:
    """
    Get information about a backup folder.

    Arguments:
        backup_folder: The folder containing all dated backups

    Returns:
        backup_info: Information about the last backup include the user's folder, log file, and
            the last time file contents were compared.
    """
    info_file = get_backup_info_file(backup_folder)
    try:
        extracted_info = Backup_Info(Source=None, Log=None, Compare_Timestamp=None)
        with info_file.open(encoding="utf8") as info:
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
                    path = absolute_path(value_string)
                    extracted_info[key] = path
        return extracted_info
    except FileNotFoundError:
        return Backup_Info(Source=None, Log=None, Compare_Timestamp=None)


def backup_info_key(key: str) -> Literal["Source", "Log", "Compare_Timestamp"]:
    """
    Verify that backup info keys read from a file are valid keys.

    Arguments:
        key: A string to lookup information about the last backup operation

    Returns:
        literal_str: A literal version of the key that is confirmed valid

    Raises:
        KeyError: If the input string is not used in Backup_Info
    """
    key = key.strip()
    if key == "Source":
        return "Source"

    if key == "Log":
        return "Log"

    if key == "Compare_Timestamp":
        return "Compare_Timestamp"

    raise KeyError(f"Unknown key for Backup_Info: {key}")


def write_backup_information(backup_folder: Path, backup_info: Backup_Info) -> None:
    """
    Record backup information to a file in the backup folder.

    Arguments:
        backup_folder: Folder containing all dated backups
        backup_info: Information about the most recent backup
    """
    info_file = get_backup_info_file(backup_folder)
    info_file.parent.mkdir(parents=True, exist_ok=True)
    with info_file.open("w", encoding="utf8") as info:
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


def backup_log_file(backup_folder: Path) -> Path | None:
    """
    Retreive the log file used in the last backup.

    Arguments:
        backup_folder: The folder containing all dated backups

    Returns:
        path: The path to the last used log file, if any
    """
    return read_backup_information(backup_folder)["Log"]


def primary_log_path(log_file_name: str | None, backup_folder: str | None) -> Path | None:
    """
    Determine which file to use for logging.

    Arguments:
        log_file_name: The name of the log file as read from --log
        backup_folder: Path to the folder containing all dated backups

    Returns:
        path: The path to the log file to use for this run of Vintage Backup, if any.
    """
    if log_file_name:
        return absolute_path(log_file_name) if log_file_name != os.devnull else None
    elif backup_folder:
        backup_path = absolute_path(backup_folder)
        log_file_path = backup_log_file(backup_path)
        return log_file_path or default_log_file_name
    else:
        return None


def record_backup_log_file(log_file_path: Path, backup_path: Path) -> None:
    """Record location of log file used with a backup folder."""
    backup_info = read_backup_information(backup_path)
    backup_info["Log"] = absolute_path(log_file_path)
    write_backup_information(backup_path, backup_info)


def record_compare_contents_timestamp(backup_location: Path, timestamp: datetime.datetime) -> None:
    """Record timestamp of last time file contents were compared during backup."""
    info = read_backup_information(backup_location)
    info["Compare_Timestamp"] = timestamp
    write_backup_information(backup_location, info)
