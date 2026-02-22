"""Functions for working with logs."""

import logging
import os

from lib.backup_info import primary_log_path, record_backup_log_file
from lib.filesystem import absolute_path


def setup_initial_null_logger() -> None:
    """Reset a logger that outputs to null so that no logs are printed during testing."""
    logger = logging.getLogger()
    while logger.handlers:
        handler = logger.handlers[0]
        handler.close()
        logger.removeHandler(handler)

    logger.addHandler(logging.FileHandler(os.devnull, encoding="utf8"))
    logger.setLevel(logging.INFO)


def setup_log_file(
        log_file_name: str,
        error_log_file_path: str | None,
        backup_folder: str | None,
        *,
        debug: bool) -> None:
    """Set up logging to write to a file."""
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG if debug else logging.INFO)
    log_file_path = primary_log_path(log_file_name, backup_folder)

    if backup_folder and log_file_path:
        record_backup_log_file(log_file_path, absolute_path(backup_folder))

    log_format = "%(asctime)s %(levelname)s    %(message)s"
    if log_file_path:
        log_file = logging.FileHandler(log_file_path, encoding="utf8")
        log_file_format = logging.Formatter(fmt=log_format)
        log_file.setFormatter(log_file_format)
        logger.addHandler(log_file)

    if error_log_file_path:
        error_log = logging.FileHandler(error_log_file_path, encoding="utf8", delay=True)
        error_log.setLevel(logging.WARNING)
        error_log_format = logging.Formatter(fmt=log_format)
        error_log.setFormatter(error_log_format)
        logger.addHandler(error_log)
