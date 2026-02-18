"""Functions for reading and writing configuration files."""

import logging
import os
import argparse
from typing import Any

from lib.exceptions import CommandLineError
from lib.filesystem import Absolute_Path, unique_path_name

logger = logging.getLogger()


def generate_config(args: argparse.Namespace) -> Absolute_Path:
    """Generate a configuration file from the arguments and return the path of that file."""
    no_arguments: set[str] = set()
    no_prefix = "no_"
    arguments: list[tuple[str, Any]] = []
    for option, value in vars(args).items():
        if not value or option in {"generate_config", "generate_windows_scripts", "config"}:
            continue

        if option.startswith(no_prefix) and value:
            no_arguments.add(option.removeprefix(no_prefix))
            continue

        arguments.append((option, value))

    arguments = [(arg, val) for arg, val in arguments if arg not in no_arguments]
    config_path = unique_path_name(Absolute_Path(args.generate_config))
    with config_path.open_text("w", encoding="utf8") as config_file:
        for option, value in arguments:
            parameter = option.replace("_", " ").capitalize()
            value_string = "" if value is True else str(value)
            is_path = option in {"user_folder", "backup_folder", "filter", "destination"}
            is_non_null_log = option == "log" and value_string != os.devnull
            if is_path or is_non_null_log:
                value_string = str(Absolute_Path(value_string))
            needs_quotes = (value_string.strip() != value_string)
            parameter_value = f'"{value_string}"' if needs_quotes else value_string
            config_file.write(f"{parameter}: {parameter_value}".strip() + "\n")

    logger.info("Generated configuration file: %s", config_path)
    return config_path


def read_configuation_file(config_file: Absolute_Path) -> list[str]:
    """Parse a configuration file into command line arguments."""
    try:
        with config_file.open_text(encoding="utf8") as file:
            arguments: list[str] = []
            for line_raw in file:
                line = line_raw.strip()
                if not line or line.startswith("#"):
                    continue
                parameter_raw, value_raw = line.split(":", maxsplit=1)

                parameter = "-".join(parameter_raw.lower().split())
                if parameter == "config":
                    raise CommandLineError(
                        "The parameter `config` within a configuration file has no effect.")
                arguments.append(f"--{parameter}")

                value = remove_quotes(value_raw)
                if value:
                    arguments.append(value)
            return arguments
    except FileNotFoundError:
        raise CommandLineError(f"Configuation file does not exist: {config_file}") from None


def remove_quotes(s: str) -> str:
    """
    After stripping a string of outer whitespace, remove pairs of quotes from the start and end.

    >>> remove_quotes('  "  a b c  "   ')
    '  a b c  '

    Strings without quotes are stripped of outer whitespace.

    >>> remove_quotes(' abc  ')
    'abc'

    All other strings are unchanged.

    >>> remove_quotes('Inner "quoted strings" are not affected.')
    'Inner "quoted strings" are not affected.'

    >>> remove_quotes('"This quote will stay," he said.')
    '"This quote will stay," he said.'

    If a string (usually a file name read from a file) has leading or trailing spaces,
    then the user should surround this file name with quotations marks to make sure the
    spaces are included in the return value.

    If the file name actually does begin and end with quotation marks, then surround the
    file name with another pair of quotation marks. Only one pair will be removed.

    >>> remove_quotes('""abc.txt""')
    '"abc.txt"'
    """
    s = s.strip()
    if len(s) > 1 and (s[0] == s[-1] == '"'):
        return s[1:-1]
    return s
