"""Function library for parsing arguments from the command line."""

import os
import shutil
import argparse
import sys
import textwrap
import io
from pathlib import Path

from lib.configuration import read_configuation_file
from lib.exceptions import CommandLineError
from lib.filesystem import absolute_path, default_log_file_name


def format_paragraphs(lines: str, line_length: int) -> str:
    """
    Format multiparagraph text in when printing --help.

    :param lines: A string of text where paragraphs are separated by at least two newlines. Indented
    lines will be preserved as-is.
    :param line_length: The length of the line for word wrapping. Indented lines will not be word
    wrapped.

    :returns string: A single string with word-wrapped lines and paragraphs separated by exactly two
    newlines.
    """
    paragraphs: list[str] = []
    for paragraph_raw in lines.split("\n\n"):
        paragraph = paragraph_raw.strip("\n")
        if not paragraph:
            continue

        paragraphs.append(
            paragraph if paragraph[0].isspace() else textwrap.fill(paragraph, line_length))

    return "\n\n".join(paragraphs)


def format_text(lines: str) -> str:
    """Format unindented paragraphs (program description and epilogue) in --help."""
    width, _ = shutil.get_terminal_size()
    return format_paragraphs(lines, width)


def format_help(lines: str) -> str:
    """Format indented command line argument descriptions in --help."""
    width, _ = shutil.get_terminal_size()
    return format_paragraphs(lines, width - 24)


def add_no_option(user_input: argparse.ArgumentParser | argparse._ArgumentGroup, name: str) -> None:
    """Add negating option for boolean command line arguments."""
    user_input.add_argument(f"--no-{name}", action="store_true", help=format_help(
f"""Disable the --{name} option. This is primarily used if "{name}" appears in a
configuration file. This option has priority even if --{name} is listed later."""))


def toggle_is_set(args: argparse.Namespace, name: str) -> bool:
    """Check that a boolean command line option --X has been selected and not negated by --no-X."""
    options = vars(args)
    return options[name] and not options[f"no_{name}"]


def path_or_none(arg: str | None) -> Path | None:
    """Create a Path instance if the input string is valid."""
    return absolute_path(arg) if arg else None


def confirm_choice_made(args: argparse.Namespace, *options: str) -> None:
    """Make sure that exactly one of the argument parameters is present."""
    args_dict = vars(args)
    if len(list(filter(None, map(args_dict.get, options)))) != 1:
        option_list = [f"--{option.replace("_", "-")}" for option in options]
        comma = ", "
        message = "Exactly one of the following is required: " + comma.join(option_list)
        if message.count(comma) == 1:
            message = message.replace(comma, " or ")
        else:
            message = f"{comma}or ".join(message.rsplit(comma, maxsplit=1))
        raise CommandLineError(message)


def argument_parser() -> argparse.ArgumentParser:
    """Create the parser for command line arguments."""
    user_input = argparse.ArgumentParser(
        add_help=False,
        formatter_class=argparse.RawTextHelpFormatter,
        allow_abbrev=False,
        description=format_text(
"""A backup utility that combines the best aspects of full and incremental backups.

Every time Vintage Backup runs, a new folder is created at the backup location
that contains copies of all of the files in the directory being backed up.
If a file in the directory being backed up is unchanged since the last
back up, a hard link to the same file in the previous backup is created.
This way, unchanged files do not take up more storage space in the backup
location, allowing for possible years of daily backups, all while having
each folder in the backup location contain a full backup.

Vintage Backup can also perform other operations besides backups. See the Actions section below for
more capabilities.

Technical notes:

- If a folder contains no files and none of its subfolders contain files, whether because there
were none or all files were filtered out, it will not appear in the backup.

- Symbolic links are not followed and are always copied as symbolic links. On Windows, symbolic
links cannot be created or copied without elevated privileges, so they will be missing from
backups if not run in administrator mode. Backups will be complete for all other files, so an
unprivileged user may user this program and use the logs to restore symbolic links after restoring a
backup.

- Windows junction points (soft links) are excluded by default. They may be added using a filter
file (see --filter below). In that case, all of the contents will be copied.

- If two files in the user's directory are hard-linked together, these files will be copied/linked
separately. The hard link is not preserved in the backup.

- If the user folder and the backup destination are on different drives or partitions with different
file systems (NTFS, ext4, APFS, etc.), hard links may not be created due to differences in how file
modification times are recorded. Using the --whole-file option may mitigate this, but backups will
take much more time."""))

    action_group = user_input.add_argument_group("Actions", format_text(
"""The default action when vintage backups is run is to create a new backup. If one of the following
options are chosen, then that action is performed instead."""))

    only_one_action_group = action_group.add_mutually_exclusive_group()

    only_one_action_group.add_argument("-h", "--help", action="store_true", help=format_help(
"""Show this help message and exit."""))

    only_one_action_group.add_argument("-r", "--recover", help=format_help(
"""Recover a file or folder from the backup. The user will be able
to pick which version to recover by choosing the backup date as
the source. If a file is being recovered, only backup dates where
the file was modified will be presented. If a folder is being
recovered, then all available backup dates will be options.
This option requires the --backup-folder option to specify which
backup location to search."""))

    only_one_action_group.add_argument(
        "--list",
        metavar="DIRECTORY",
        nargs="?",
        const=".",
        help=format_help(
"""Recover a file or folder in the directory specified by the argument by first choosing what to
recover from a list of everything that's ever been backed up. If there is no folder specified
after --list, then the current directory is used. The backup location argument --backup-folder
is required."""))

    only_one_action_group.add_argument(
        "--move-backup",
        metavar="NEW_BACKUP_LOCATION",
        help=format_help(
"""Move a backup set to a new location. The value of this argument is the new location. The
--backup-folder option is required to specify the current location of the backup set, and one
of --move-count, --move-age, or --move-since is required to specify how many of the most recent
backups to move. Moving each dated backup will take just as long as a normal backup to move since
the hard links to previous backups will be recreated to preserve the space savings, so some planning
is needed when deciding how many backups should be moved."""))

    only_one_action_group.add_argument("--verify", metavar="RESULT_DIR", help=format_help(
"""Verify the latest backup by comparing them against the original files. The result of the
comparison will be placed in the folder RESULT_DIR. The result is three files: a list of files that
match, a list of files that do not match, and a list of files that caused errors during the
comparison. The --backup-folder argument is required. If a filter file was used
to create the backup, then --filter should be supplied as well."""))

    only_one_action_group.add_argument(
        "--preview-filter",
        metavar="FILE_NAME",
        nargs="?",
        const=False,
        help=format_help(
"""Create a list of the files and folders that will be backed up after being filtered by the
--filter file argument. The argument is a file name where the list will be written. If there is no
argument, the list will be written to the console. The --user-folder argument is required."""))

    only_one_action_group.add_argument("--restore", action="store_true", help=format_help(
"""This action restores the user's folder to a previous, backed up state. Any existing user files
that have the same name as one in the backup will be overwritten. The --backup-folder is required to
specify from where to restore. See the Restore Options section below for the other required
parameters."""))

    only_one_action_group.add_argument("--purge", help=format_help(
"""Delete a file or folder from all backups. The argument is the path to delete. This requires the
--backup-folder argument."""))

    only_one_action_group.add_argument(
        "--purge-list",
        metavar="DIRECTORY",
        nargs="?",
        const=".",
        help=format_help(
"""Purge a file or folder from all backups in the directory specified by the argument by first
choosing what to purge from a list of everything that's ever been backed up. If there is no folder
specified after --purge-list, then the current directory is used. If the file exists in the user's
folder, it is not deleted. The backup location argument --backup-folder is required."""))

    only_one_action_group.add_argument("--delete-only", action="store_true", help=format_help(
"""Delete old backups according to --free-up or --delete-after without running a backup first."""))

    action_group.add_argument("--generate-config", metavar="FILE_NAME", help=format_help(
"""Generate a configuration file that matches the other arguments in the call."""))

    action_group.add_argument(
        "--generate-windows-scripts",
        metavar="DIRECTORY",
        help=format_help(
"""Generate scripts and config files for use with Windows Task Scheduler."""))

    backup_group = user_input.add_argument_group("Options for backing up")

    backup_group.add_argument("-u", "--user-folder", help=format_help(
"""The directory to be backed up. The contents of this
folder and all subfolders will be backed up recursively."""))

    backup_group.add_argument("-b", "--backup-folder", help=format_help(
"""The destination of the backed up files. This folder will
contain a set of folders labeled by year, and each year's
folder will contain all of that year's backups."""))

    backup_group.add_argument("-f", "--filter", metavar="FILTER_FILE_NAME", help=format_help(
"""Filter the set of files that will be backed up. The value of this argument should be the name of
a text file that contains lines specifying what files to include or exclude.

Each line in the file consists of a symbol followed by a path. The symbol must be a minus (-),
plus (+), or hash (#). Lines with minus signs specify files and folders to exclude. Lines with plus
signs specify files and folders to include. Lines with hash signs are ignored. Prior to reading the
first line, everything in the user's folder is included. The path that follows may contain wildcard
characters like *, **, [], and ? to allow for matching multiple path names. If you want to match a
single name that contains wildcards, put brackets around them: What Is Life[?].pdf, for example.
Since leading and trailing whitespace is normally removed, use brackets around each leading/trailing
space character: - [ ][ ]has_two_leading_and_three_trailing_spaces.txt[ ][ ][ ]

Only files will be matched against each line in this file. If you want to include or exclude an
entire directory, the line must end with a "/**" or "\\**" to match all of its contents. The paths
may be absolute or relative. If a path is relative, it is relative to the user's folder.

All paths must reside within the directory tree of the --user-folder. For example, if backing up
C:\\Users\\Alice, the following filter file:

    # Ignore AppData except Firefox
    - AppData/**
    + AppData/Roaming/Mozilla/Firefox/**

will exclude everything in C:\\Users\\Alice\\AppData\\ except the
Roaming\\Mozilla\\Firefox subfolder. The order of the lines matters. If the - and + lines above
were reversed, the Firefox folder would be included and then excluded by the following - Appdata
line.

Because each line only matches to files, some glob patterns may not do what the user expects. Here
are some examples of such patterns:

    # Assume that dir1 is a folder in the user's --user-folder and dir2 is a folder inside dir1.

    # This line does nothing.
    - dir1

    # This line will exclude all files in dir1, but not folders. dir1/dir2 is still included.
    - dir1/*

    # This line will exclude dir1 and all of its contents.
    - dir1/**"""))

    backup_group.add_argument("-w", "--whole-file", action="store_true", help=format_help(
"""Examine the entire contents of a file to determine if it has
changed and needs to be copied to the new backup. Without this
option, only the file's size, type, and modification date are
checked for differences. Using this option will make backups
take considerably longer."""))

    add_no_option(backup_group, "whole-file")

    backup_group.add_argument("--free-up", metavar="SPACE", help=format_help(
"""After a successful backup, delete old backups until the amount of free space on the
backup destination is at least SPACE.

The argument should be a bare number or a number followed by letters that
indicate a unit in bytes. The number will be interpreted as a number
of bytes. Case does not matter, so all of the following specify
15 megabytes: 15MB, 15Mb, 15mB, 15mb, 15M, and 15m. Old backups
will be deleted until at least that much space is free.

This can be used at the same time as --delete-after.

The most recent backup will not be deleted."""))

    backup_group.add_argument("--delete-after", metavar="TIME", help=format_help(
"""After a successful backup, delete backups if they are older than the time span in the argument.
The format of the argument is Nt, where N is a whole number and t is a single letter: d for days, w
for weeks, m for calendar months, or y for calendar years.

This can be used at the same time as --free-up.

The most recent backup will not be deleted."""))

    backup_group.add_argument("--max-deletions", help=format_help(
"""Specify the maximum number of deletions per program run."""))

    backup_group.add_argument("--delete-first", action="store_true", help=format_help(
"""Delete old backups (according to --free-up, --delete-after, and --max-deletions) to make room
prior to starting a new backup.

The most recent backup will never be deleted."""))

    add_no_option(backup_group, "delete-first")

    backup_group.add_argument("--force-copy", action="store_true", help=format_help(
"""Copy all files instead of linking to files previous backups. The
new backup will contain new copies of all of the user's files,
so the backup location will require much more space than a normal
backup."""))

    add_no_option(backup_group, "force-copy")

    link_copy_probability_group = backup_group.add_mutually_exclusive_group()

    link_copy_probability_group.add_argument("--hard-link-count", help=format_help(
"""Specify the average number of hard links Vintage Backup should create for an unchanged file
before copying it again. The argument HARD_LINK_COUNT should be an integer. If specified, every
unchanged file will be copied with a probability of 1/(HARD_LINK_COUNT + 1)."""))

    link_copy_probability_group.add_argument("--copy-probability", help=format_help(
"""Specify the probability that an unchanged file will be copied instead of hard-linked during a
backup. The probability can be expressed as a decimal (0.1) or as a percent (10%%). This is an
alternate to --hard-link-count and cannot be used together with it."""))

    recover_group = user_input.add_argument_group("Recover options", format_text(
"""Choose how to search for which version of a file or folder to recover from backup."""))

    recover_group.add_argument("--search", action="store_true", help=format_help(
"""Instead of choosing a backup date, recover a version of the file so the user can examine it.
Then, after the examining the file, decide whether to restore a newer or older version as
needed."""))

    move_group = user_input.add_argument_group("Move backup options", format_text(
"""Use exactly one of these options to specify which backups to move when using --move-backup."""))

    only_one_move_group = move_group.add_mutually_exclusive_group()

    only_one_move_group.add_argument("--move-count", help=format_help(
"""Specify the number of the most recent backups to move or "all" if every backup should be moved
to the new location."""))

    only_one_move_group.add_argument("--move-age", help=format_help(
"""Specify the maximum age of backups to move. See --delete-after for the time span format to use.
"""))

    only_one_move_group.add_argument("--move-since", help=format_help(
"""Move all backups made on or after the specified date (YYYY-MM-DD)."""))

    restore_group = user_input.add_argument_group("Restore Options", format_help(
"""Exactly one of each of the following option pairs(--last-backup/--choose-backup and
--delete-extra/--keep-extra) is required when restoring a backup. The --destination option is
optional."""))

    choose_restore_backup_group = restore_group.add_mutually_exclusive_group()

    choose_restore_backup_group.add_argument(
        "--last-backup",
        action="store_true",
        help=format_help("""Restore from the most recent backup."""))

    choose_restore_backup_group.add_argument(
        "--choose-backup",
        action="store_true",
        help=format_help("""Choose which backup to restore from a list."""))

    restore_preservation_group = restore_group.add_mutually_exclusive_group()

    restore_preservation_group.add_argument(
        "--delete-extra",
        action="store_true",
        help=format_help("""Delete any extra files that are not in the backup."""))

    restore_preservation_group.add_argument(
        "--keep-extra",
        action="store_true",
        help=format_help("""Preserve any extra files that are not in the backup."""))

    restore_group.add_argument("--destination", help=format_help(
"""Specify a different destination for the backup restoration."""))

    other_group = user_input.add_argument_group("Other options")

    other_group.add_argument("-c", "--config", metavar="FILE_NAME", help=format_help(
r"""Read options from a configuration file instead of command-line arguments. The format
of the file should be one option per line with a colon separating the parameter name
and value. The parameter names have the same names as the double-dashed command line options
(i.e., "user-folder", not "u"). If a parameter does not take a value, like "whole-file",
leave the value blank. Any line starting with a # will be ignored. As an example:

    # Ignored comment
    user-folder: C:\Users\Alice\
    backup-folder: E:\Backups
    delete-on-error:

The parameter names may also be spelled with spaces instead of the dashes and with mixed case:

    # Ignored comment
    User Folder: C:\Users\Alice\
    Backup Folder: E:\Backups
    Delete on error:

Values like file and folder names may contain any characters--no escaping or quoting necessary.
Whitespace at the beginning and end of the values will be trimmed off. If a file or folder name
begins or ends with spaces, surrounding the name with double quotes will preserve this space.

    User Folder: "/home/bob/folder that ends with a space "

If a file or folder name is already quoted--that is, starts and ends with double quotes--then
another pair of quotes will preserve these quotes. If the filter file is name
"the alleged file.txt" with quotes in the name, then the configuration file line should look like
this:

    filter file: ""the alleged file.txt""

If both --config and other command line options are used and they conflict, then the command
line options override the config file options.

A final note: recursive configuration files are not supported. Using the parameter "config" inside
a configuration file will cause the program to quit with an error."""))

    other_group.add_argument("--debug", action="store_true", help=format_help(
        """Log information on all actions during a program run."""))

    add_no_option(other_group, "debug")

    other_group.add_argument(
        "-l", "--log",
        default=str(default_log_file_name),
        help=format_help(
f"""Where to log the activity of this program. The default is
{default_log_file_name.name} in the user's home folder. If no
log file is desired, use the file name {os.devnull}."""))

    other_group.add_argument("--error-log", help=format_help(
"""Where to copy log lines that are warnings or errors. This file will only appear when unexpected
events occur."""))

    # The following arguments are only used for testing.

    # Bypass keyboard input when testing functions that ask for a choice from a menu.
    user_input.add_argument("--choice", help=argparse.SUPPRESS)

    # Allow for backups to be created more quickly by providing a timestamp instead of using
    # datetime.datetime.now().
    user_input.add_argument("--timestamp", help=argparse.SUPPRESS)

    # Skip confirmation prompt for backup restorations.
    user_input.add_argument("--skip-prompt", action="store_true", help=argparse.SUPPRESS)

    # Give user input that causes errors.
    user_input.add_argument("--bad-input", action="store_true", help=argparse.SUPPRESS)

    return user_input


def parse_command_line(argv: list[str]) -> argparse.Namespace:
    """Parse the command line options and incorporate configuration file options if needed."""
    if argv and argv[0] == sys.argv[0]:
        argv = argv[1:]

    command_line_options = argv or ["--help"]
    user_input = argument_parser()
    command_line_args = user_input.parse_args(command_line_options)
    if command_line_args.config:
        file_options = read_configuation_file(Path(command_line_args.config))
        return user_input.parse_args(file_options + command_line_options)
    else:
        return command_line_args


def print_usage(destination: io.TextIOBase | None = None) -> None:
    """Print short instructions for the command line options."""
    argument_parser().print_usage(destination)


def print_help(destination: io.TextIOBase | None = None) -> None:
    """Print full manual for Vintage Backup."""
    argument_parser().print_help(destination)
