"""The main function where the requested action is initiated."""

import argparse
import logging

from lib.argument_parser import parse_command_line, print_help, print_usage, toggle_is_set
from lib.automation import generate_windows_scripts
from lib.backup import start_backup
from lib.backup_deletion import delete_before_backup, delete_old_backups
from lib.backup_set import preview_filter
from lib.configuration import generate_config
from lib.exceptions import CommandLineError, ConcurrencyError
from lib.logs import setup_initial_null_logger, setup_log_file
from lib.move_backups import start_move_backups
from lib.purge import choose_purge_target_from_backups, start_backup_purge
from lib.recovery import choose_recovery_target_from_backups, start_recovery_from_backup
from lib.restoration import start_backup_restore
from lib.verification import start_verify_backup, start_checksum, start_verify_checksum

logger = logging.getLogger()
setup_initial_null_logger()


def main(argv: list[str], *, testing: bool) -> int:
    """
    Start the main program.

    :param argv: A list of command line arguments as from sys.argv
    """
    try:
        args = parse_command_line(argv)
        if args.help:
            print_help()
            return 0

        debug_output = toggle_is_set(args, "debug")
        setup_log_file(args.log, args.error_log, args.backup_folder, debug=debug_output)
        logger.debug(args)

        def default_action(args: argparse.Namespace) -> None:
            start_backup(args)
            delete_old_backups(args)
            start_checksum(args)

        action = (
            generate_config if args.generate_config
            else generate_windows_scripts if args.generate_windows_scripts
            else start_recovery_from_backup if args.recover
            else choose_recovery_target_from_backups if args.list
            else start_move_backups if args.move_backup
            else start_verify_backup if args.verify
            else start_verify_checksum if args.verify_checksum
            else start_backup_restore if args.restore
            else start_backup_purge if args.purge
            else choose_purge_target_from_backups if args.purge_list
            else delete_old_backups if args.delete_only
            else delete_before_backup if toggle_is_set(args, "delete_first")
            else preview_filter if args.preview_filter is not None
            else default_action)
        action(args)
        return 0
    except CommandLineError as error:
        if not testing:
            print_usage()
        logger.error(error)
    except ConcurrencyError as error:
        logger.error(error)
    except Exception:
        logger.exception("The program ended unexpectedly with an error:")

    return 1
