import logging

logger = logging.getLogger(__name__)
setup_initial_null_logger(logger)


def main(argv: list[str]) -> int:
    """
    Start the main program.

    :param argv: A list of command line arguments as from sys.argv
    """
    try:
        args = parse_command_line(argv)
        if args.help:
            print_help()
            return 0

        setup_log_file(logger, args.log, args.error_log, args.backup_folder)
        logger.setLevel(logging.DEBUG if toggle_is_set(args, "debug") else logging.INFO)
        logger.debug(args)

        action = (
            generate_config if args.generate_config
            else generate_windows_scripts if args.generate_windows_scripts
            else start_recovery_from_backup if args.recover
            else choose_recovery_target_from_backups if args.list
            else start_move_backups if args.move_backup
            else start_verify_backup if args.verify
            else start_backup_restore if args.restore
            else start_backup_purge if args.purge
            else choose_purge_target_from_backups if args.purge_list
            else delete_old_backups if args.delete_only
            else delete_before_backup if toggle_is_set(args, "delete_first")
            else preview_filter if args.preview_filter is not None
            else start_backup)
        action(args)
        return 0
    except CommandLineError as error:
        if __name__ == "__main__":
            print_usage()
        logger.error(error)
    except ConcurrencyError as error:
        logger.error(error)
    except Exception:
        logger.exception("The program ended unexpectedly with an error:")

    return 1
