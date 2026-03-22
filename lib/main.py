"""The main function where the requested action is initiated."""

import argparse
import logging
import shutil

from lib.argument_parser import parse_command_line, print_help, print_usage, toggle_is_set
from lib.automation import generate_windows_scripts
from lib.backup import start_backup, print_backup_storage_stats
from lib.backup_deletion import delete_old_backups
from lib.backup_set import preview_filter
from lib.backup_utilities import all_backups
from lib.configuration import generate_config
from lib.console import print_run_title
import lib.exceptions as exc
from lib.filesystem import absolute_path, parse_storage_space
from lib.logs import setup_initial_null_logger, setup_log_file
from lib.move_backups import start_move_backups
from lib.purge import choose_purge_target_from_backups, start_backup_purge
from lib.recovery import choose_recovery_target_from_backups, start_recovery_from_backup
from lib.find_missing import start_finding_missing_files
from lib.restoration import start_backup_restore
from lib.verification import start_verify_backup, start_checksum, start_verify_checksum

logger = logging.getLogger()
setup_initial_null_logger()


def default_action(args: argparse.Namespace) -> None:
    """
    If no other action arguments are present, run a backup by default.

    Arguments:
        args: Parsed command line options
    """
    print_run_title(args, "Starting new backup")
    backup_cycle(args)
    delete_old_backups(args)
    start_checksum(args)
    print_backup_storage_stats(absolute_path(args.backup_folder))


def backup_cycle(args: argparse.Namespace) -> None:
    """
    Retry backup creation until success, deleting old backups as needed.

    Arguments:
        args: Parsed command line

    Raises:
        OutOfSpaceError: If the backup storage media runs out of space and --free-up is not used
        CommandLineError: If the backup storage media runs out of space and --free-up cannot delete
            enough old backups to make room
    """
    while True:
        try:
            delete_old_backups(args)
            start_backup(args)
            break
        except exc.OutOfSpaceError as error:
            if not args.free_up:
                raise

            logger.warning("Could not complete backup. %s", error)
            free_up_space = parse_storage_space(args.free_up)
            backup_location = absolute_path(args.backup_folder)
            free_space = shutil.disk_usage(backup_location).free
            if free_up_space < free_space:
                raise exc.CommandLineError(
                    "Cannot free up enough space to complete backup. "
                    f"Increase value of --free-up. Currently: {args.free_up}") from None

            backup_count = len(all_backups(backup_location))
            if backup_count == 1:
                raise exc.CommandLineError(
                    f"Cannot free up enough space at {backup_location} to complete backup "
                    "without deleting the only remaining backup.") from None
            elif backup_count == 0:
                raise exc.CommandLineError(
                    f"There is not enough space at {backup_location} to "
                    "create a backup.") from None


def main(argv: list[str], *, testing: bool) -> int:
    """
    Start the main program.

    Arguments:
        argv: A list of command line arguments as from sys.argv
        testing: Whether this function is being run during testing. If True, some console output
            will be disabled.

    Returns:
        int: Exit code: 0 for success, 1 for failure.
    """
    try:
        args = parse_command_line(argv)
        if args.help:
            print_help()
            return 0

        debug_output = toggle_is_set(args, "debug")
        setup_log_file(args.log, args.error_log, args.backup_folder, debug=debug_output)
        logger.debug(args)

        action = (
            generate_config if args.generate_config
            else generate_windows_scripts if args.generate_windows_scripts
            else start_recovery_from_backup if args.recover
            else choose_recovery_target_from_backups if args.list
            else start_finding_missing_files if args.find_missing
            else start_move_backups if args.move_backup
            else start_verify_backup if args.verify
            else start_verify_checksum if args.verify_checksum
            else start_backup_restore if args.restore
            else start_backup_purge if args.purge
            else choose_purge_target_from_backups if args.purge_list
            else delete_old_backups if args.delete_only
            else preview_filter if args.preview_filter is not None
            else preview_filter if args.preview_filter_exclusions is not None
            else default_action)
        action(args)
        return 0
    except exc.CommandLineError as error:
        if not testing:
            print_usage()
        logger.error(error)
    except (exc.ConcurrencyError, exc.OutOfSpaceError, exc.FilterFileError) as error:
        logger.error(error)
    except Exception:
        logger.exception("The program ended unexpectedly with an error:")

    return 1
