"""The main function where the requested action is initiated."""

import argparse
import logging
import shutil

import lib.argument_parser as argparser
import lib.automation as automate
import lib.backup as bak
import lib.backup_deletion as delete
from lib.backup_set import preview_filter
import lib.backup_utilities as util
import lib.configuration as config
from lib import console
import lib.exceptions as exc
import lib.filesystem as fs
from lib import logs
import lib.move_backups as move
from lib import purge
import lib.recovery as recover
import lib.find_missing as missing
import lib.restoration as restore
import lib.verification as verify

logger = logging.getLogger()
logs.setup_initial_null_logger()


def default_action(args: argparse.Namespace) -> None:
    """
    If no other action arguments are present, run a backup by default.

    Arguments:
        args: Parsed command line options
    """
    console.print_run_title(args, "Starting new backup")
    backup_cycle(args)
    delete.delete_old_backups(args)
    verify.start_verify_backup(args)
    verify.start_checksum(args)
    bak.print_backup_storage_stats(fs.absolute_path(args.backup_folder))


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
    delete_oldest = False

    while True:
        try:
            delete.delete_old_backups(args, delete_oldest=delete_oldest)
            if delete_oldest:
                logger.info("")
                logger.info("Restarting backup")
            delete_oldest = False
            bak.start_backup(args)
            break
        except exc.OutOfSpaceError as error:
            if args.free_up == "auto":
                logger.info("Not enough space to complete backup.")
                delete_oldest = True
                continue

            if not args.free_up:
                raise

            logger.warning("Could not complete backup. %s", error)
            free_up_space = fs.parse_storage_space(args.free_up)
            backup_location = fs.absolute_path(args.backup_folder)
            free_space = shutil.disk_usage(backup_location).free
            if free_up_space < free_space:
                raise exc.CommandLineError(
                    "Cannot free up enough space to complete backup. "
                    f"Increase value of --free-up. Currently: {args.free_up}") from None

            backup_count = len(util.all_backups(backup_location))
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
        args = argparser.parse_command_line(argv)
        if args.help:
            argparser.print_help()
            return 0

        debug_output = argparser.toggle_is_set(args, "debug")
        logs.setup_log_file(args.log, args.error_log, args.backup_folder, debug=debug_output)
        logger.debug(args)

        action = (
            config.generate_config if args.generate_config
            else automate.generate_windows_scripts if args.generate_windows_scripts
            else recover.start_recovery_from_backup if args.recover
            else recover.choose_recovery_target_from_backups if args.list
            else missing.start_finding_missing_files if args.find_missing
            else move.start_move_backups if args.move_backup
            else verify.start_verify_backup if args.verify_only
            else verify.start_verify_checksum if args.verify_checksum
            else restore.start_backup_restore if args.restore
            else purge.start_backup_purge if args.purge
            else purge.choose_purge_target_from_backups if args.purge_list
            else delete.delete_old_backups if args.delete_only
            else preview_filter if args.preview_filter is not None
            else preview_filter if args.preview_filter_exclusions is not None
            else default_action)
        action(args)
        return 0
    except exc.CommandLineError as error:
        if not testing:
            argparser.print_usage()
        logger.error(error)
    except (exc.ConcurrencyError, exc.OutOfSpaceError, exc.FilterFileError) as error:
        logger.error(error)
    except Exception:
        logger.exception("The program ended unexpectedly with an error:")

    return 1
