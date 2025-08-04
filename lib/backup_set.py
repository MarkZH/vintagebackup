"""A class for generating paths for backups with optional filtering."""

import argparse
import logging
import sys
import io
from collections.abc import Iterator
from pathlib import Path
from typing import cast

from lib.argument_parser import path_or_none
from lib.filesystem import get_existing_path, path_listing

logger = logging.getLogger(__name__)


class Backup_Set:
    """Generate the list of all paths to be backed up after filtering."""

    def __init__(self, user_folder: Path, filter_file: Path | None) -> None:
        """
        Prepare the path generator by parsing the filter file.

        :param user_folder: The folder to be backed up.
        :param filter_file: The path of the filter file that edits the paths to backup.
        """
        self.entries: list[tuple[int, str, Path]] = []
        self.lines_used: set[int] = set()
        self.user_folder = user_folder
        self.filter_file = filter_file

        if not filter_file:
            return

        with filter_file.open(encoding="utf8") as filters:
            for line_number, line_raw in enumerate(filters, 1):
                line = line_raw.strip()
                if not line:
                    continue
                sign = line[0]

                if sign not in "-+#":
                    raise ValueError(
                        f"Line #{line_number} ({line}): The first symbol "
                        "of each line in the filter file must be -, +, or #.")

                if sign == "#":
                    continue

                pattern = user_folder/line[1:].strip()
                if not pattern.is_relative_to(user_folder):
                    raise ValueError(
                        f"Line #{line_number} ({line}): Filter looks at paths outside user folder.")

                logger.debug("Filter added: %s --> %s %s", line, sign, pattern)
                self.entries.append((line_number, sign, pattern))

    def __iter__(self) -> Iterator[tuple[Path, list[str]]]:
        """Create the iterator that yields the paths to backup."""
        for current_directory, _, files in self.user_folder.walk():
            good_files = list(filter(self.passes, (current_directory/file for file in files)))
            if good_files:
                yield (current_directory, [file.name for file in good_files])

        self.log_unused_lines()

    def passes(self, path: Path) -> bool:
        """Determine if a path should be included in the backup according to the filter file."""
        is_included = not path.is_junction()
        for line_number, sign, pattern in self.entries:
            should_include = (sign == "+")
            if is_included == should_include or not path.full_match(pattern):
                continue

            self.lines_used.add(line_number)
            is_included = should_include
            logger.debug(
                "File: %s %s by line %d: %s %s",
                path,
                "included" if is_included else "excluded",
                line_number,
                sign,
                pattern)

        return is_included

    def log_unused_lines(self) -> None:
        """Warn the user if any of the lines in the filter file had no effect on the backup."""
        for line_number, sign, pattern in self.entries:
            if line_number not in self.lines_used:
                logger.info(
                    "%s: line #%d (%s %s) had no effect.",
                    self.filter_file,
                    line_number,
                    sign,
                    pattern)


def preview_filter(args: argparse.Namespace) -> None:
    """Print a list of files that will make it through the --filter file."""
    user_folder = get_existing_path(args.user_folder, "user folder")
    filter_file = path_or_none(args.filter)
    output_file = path_or_none(args.preview_filter)
    if output_file:
        with output_file.open("w", encoding="utf8") as output:
            path_listing(Backup_Set(user_folder, filter_file), output)
    else:
        stdout = cast(io.TextIOBase, sys.stdout)
        path_listing(Backup_Set(user_folder, filter_file), stdout)
