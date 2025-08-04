"""Functions for working with logs."""

import logging
import os
import logging
from pathlib import Path

from lib.backup_info import primary_log_path, record_backup_log_file


def setup_initial_null_logger(logger: logging.Logger) -> None:
    """Reset a logger that outputs to null so that no logs are printed during testing."""
    for handler in logger.handlers:
        handler.close()
    logger.handlers.clear()

    logger.addHandler(logging.FileHandler(os.devnull))
    logger.setLevel(logging.INFO)


default_log_file_name = Path.home()/"vintagebackup.log"


def setup_log_file(
        logger: logging.Logger,
        log_file_name: str,
        error_log_file_path: str | None,
        backup_folder: str | None) -> None:
    """Set up logging to write to a file."""
    log_file_path = primary_log_path(log_file_name, backup_folder)

    if backup_folder and log_file_path:
        record_backup_log_file(log_file_path, Path(backup_folder))

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
