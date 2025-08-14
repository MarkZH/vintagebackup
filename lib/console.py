"""Functions for displaying information in the console."""

import logging
import platform
import argparse
from io import TextIOBase

from lib.filesystem import absolute_path

logger = logging.getLogger()


def choose_from_menu(
        menu_choices: list[str],
        prompt: str,
        test_choice: int | None = None,
        output: TextIOBase | None = None) -> int:
    """
    Let user choose from options presented a numbered list in a terminal.

    :param menu_choices: List of choices
    :param prompt: Message to show user prior to the prompt for a choice.

    :returns int: The returned number is an index into the input list. The interface has the user
    choose a number from 1 to len(menu_list), but returns a number from 0 to len(menu_list) - 1.
    """
    if test_choice is None or output is not None:
        number_column_size = len(str(len(menu_choices)))
        for number, choice in enumerate(menu_choices, 1):
            print(f"{number:>{number_column_size}}: {choice}", file=output)

    console_prompt = f"{prompt} ({cancel_key()} to quit): "
    while True:
        try:
            user_choice = test_choice if test_choice is not None else int(input(console_prompt))
            if 1 <= user_choice <= len(menu_choices):
                return user_choice - 1
        except ValueError:
            pass

        print(f"Enter a number from 1 to {len(menu_choices)}", file=output)


def cancel_key() -> str:
    """Return string describing the key combination that emits a SIGINT."""
    action_key = "Cmd" if platform.system() == "Darwin" else "Ctrl"
    return f"{action_key}-C"


def plural_noun(count: int, word: str) -> str:
    """
    Convert a noun to a simple plural phrase if the count is not one.

    >>> plural_noun(5, "cow")
    '5 cows'

    >>> plural_noun(1, "cat")
    '1 cat'

    Irregular nouns that are not pluralized by appending an "s" are not supported.
    >>> plural_noun(3, "fox")
    '3 foxs'
    """
    return f"{count} {word}{'' if count == 1 else 's'}"


def print_run_title(command_line_args: argparse.Namespace, action_title: str) -> None:
    """Print the action taking place."""
    logger.info("")
    divider = "="*(len(action_title) + 2)
    logger.info(divider)
    logger.info(" %s", action_title)
    logger.info(divider)
    logger.info("")

    if command_line_args.config:
        logger.info("Reading configuration from file: %s", absolute_path(command_line_args.config))
        logger.info("")
