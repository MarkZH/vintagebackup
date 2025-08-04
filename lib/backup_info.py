"""Functions for reading information about previous backups."""

import os
from pathlib import Path
from typing import TypedDict, Literal

def get_backup_info_file(backup_location: Path) -> Path:
    """
    Return the file that contains information about the backup process at the given location.

    The file will containe the user directory that is backed up at the given location and the log
    file for activity at the backup location.
    """
    return backup_location/"vintagebackup.source.txt"


def record_user_location(user_location: Path, backup_location: Path) -> None:
    """Write the user directory being backed up to a file in the base backup directory."""
    backup_info = read_backup_information(backup_location)
    backup_info["Source"] = absolute_path(user_location, strict=True)
    write_backup_information(backup_location, backup_info)


def backup_source(backup_location: Path) -> Path:
    """Read the user directory that was backed up to the given backup location."""
    user_folder = read_backup_information(backup_location)["Source"]
    if user_folder:
        return user_folder
    else:
        raise FileNotFoundError(f"No source for backups in {backup_location} found.")


def confirm_user_location_is_unchanged(user_data_location: Path, backup_location: Path) -> None:
    """
    Make sure the user directory being backed up is the same as the previous backup run.

    An exception will be thrown when attempting to back up a different user directory to the one
    that was backed up previously. Backing up multiple different directories to the same backup
    location negates the hard linking functionality.
    """
    try:
        recorded_user_folder = backup_source(backup_location)
        if not recorded_user_folder.samefile(user_data_location):
            raise CommandLineError(
                "Previous backup stored a different user folder."
                f" Previously: {absolute_path(recorded_user_folder)};"
                f" Now: {absolute_path(user_data_location)}")
    except FileNotFoundError:
        # This is probably the first backup, hence no user folder record.
        pass


class Backup_Info(TypedDict):
    """Information about a backup folder."""

    Source: Path | None
    Log: Path | None


def read_backup_information(backup_folder: Path) -> Backup_Info:
    """Get information about a backup folder."""
    info_file = get_backup_info_file(backup_folder)
    try:
        extracted_info = Backup_Info(Source=None, Log=None)
        with info_file.open(encoding="utf8") as info:
            for line_raw in info:
                line = line_raw.lstrip().removesuffix("\n")
                if not line:
                    continue
                if any(line.startswith(k) for k in extracted_info):
                    key, value_string = line.split(": ", maxsplit=1)
                else:
                    key = "Source"
                    value_string = line

                key = backup_info_key(key)
                value = absolute_path(value_string)
                extracted_info[key] = value
        return extracted_info
    except FileNotFoundError:
        return Backup_Info(Source=None, Log=None)


def backup_info_key(key: str) -> Literal["Source", "Log"]:
    """Verify that backup info keys read from a file are valid keys."""
    key = key.strip()
    if key == "Source":
        return "Source"

    if key == "Log":
        return "Log"

    raise KeyError(f"Unknown key for Backup_Info: {key}")


def write_backup_information(backup_folder: Path, backup_info: Backup_Info) -> None:
    """Record backup information to a file in the backup folder."""
    info_file = get_backup_info_file(backup_folder)
    with info_file.open("w", encoding="utf8") as info:
        for key, value in backup_info.items():
            if value:
                logger.debug("Writing %s : %s to %s", key, value, info_file)
                info.write(f"{key}: {value}\n")


def backup_log_file(backup_folder: Path) -> Path | None:
    """Retreive the log file used in the last backup."""
    return read_backup_information(backup_folder)["Log"]


def primary_log_path(log_file_name: str | None, backup_folder: str | None) -> Path | None:
    """Determine which file to use for logging."""
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
