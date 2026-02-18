"""Functions for working with the storage filesystem."""

from io import BytesIO
import logging
import os
import shutil
import stat
import math
from collections.abc import Callable, Iterable, Generator
from pathlib import Path
from typing import TextIO, cast

from lib.exceptions import CommandLineError

logger = logging.getLogger()


storage_prefixes = ["", "k", "M", "G", "T", "P", "E", "Z", "Y", "R", "Q"]


def byte_units(size: float) -> str:
    """
    Display a number of bytes with four significant figures with byte units.

    >>> byte_units(12345)
    '12.35 kB'

    >>> byte_units(12)
    '12.00 B'
    """
    if size < 0.0:
        raise RuntimeError(f"Got invalid value for byte_units(): {size}")

    if size < 1.0:
        return "0.000 B"

    prefix_step = 1000
    index = int(math.log10(size)/math.log10(prefix_step))
    prefix_size: int = prefix_step**index
    size_in_units = size/prefix_size
    prefix = storage_prefixes[index]
    decimal_digits = 4 - math.floor(math.log10(size_in_units) + 1)
    return f"{size_in_units:.{decimal_digits}f} {prefix}B"


class Absolute_Path:
    """A class representing absolute paths."""

    def __init__(self, path: str | Path) -> None:
        """Initialize an absolute path."""
        self.path = Path(os.path.abspath(path))  # noqa: PTH100

    def exists(self) -> bool:
        """
        Return whether the specified path exists.

        Symlinks are not followed.
        """
        return self.path.exists(follow_symlinks=False)

    @property
    def parent(self) -> "Absolute_Path":
        """Return parent of current Absolute_Path."""
        return Absolute_Path(self.path.parent)

    def __truediv__(self, new_part: "str | Path | Absolute_Path") -> "Absolute_Path":
        """Concatenate paths."""
        return Absolute_Path(
            self.path/new_part.path if isinstance(new_part, Absolute_Path) else self.path/new_part)

    def mkdir(self, *, mode: int = 511, parents: bool = False, exist_ok: bool = False) -> None:
        """Create the current path as a directory."""
        self.path.mkdir(mode, parents, exist_ok)

    def rmdir(self) -> None:
        """Delete the empty directory at the current path."""
        self.path.rmdir()

    def iterdir(self) -> Generator["Absolute_Path"]:
        """Iterate through contents of current directory path."""
        yield from map(Absolute_Path, self.path.iterdir())

    def walk(self) -> Generator[tuple["Absolute_Path", list[str], list[str]]]:
        """Walk through directory tree as in Path.walk()."""
        yield from (
            (Absolute_Path(directory), dirs, files)
            for directory, dirs, files in self.path.walk())

    def touch(self, *, exist_ok: bool = True) -> None:
        """Create a file at the current path and/or update its mtime."""
        self.path.touch(exist_ok=exist_ok)

    def unlink(self) -> None:
        """Delete file named by path."""
        self.path.unlink()

    @property
    def name(self) -> str:
        """Return the last part of the path."""
        return str(self.path.name)

    @property
    def stem(self) -> str:
        """Returns name of current path without suffix."""
        return self.path.stem

    @property
    def suffix(self) -> str:
        """Return suffix (i.e., file name extension) or current path."""
        return self.path.suffix

    def with_suffix(self, new_suffix: str) -> "Absolute_Path":
        """Create a new absolute path with the given suffix."""
        return Absolute_Path(self.path.with_suffix(new_suffix))

    def read_text(self, encoding: str) -> str:
        """Read text information from file path."""
        return self.path.read_text(encoding=encoding)

    def write_text(self, text: str, encoding: str) -> None:
        """Write text inforamation to file path."""
        self.path.write_text(data=text, encoding=encoding)

    def open_text(self, mode: str = "r", *, encoding: str | None) -> TextIO:
        """Open file for reading and/or writing."""
        text_file = self.path.open(mode=mode, encoding=encoding)
        return cast(TextIO, text_file)

    def open_binary(self) -> BytesIO:
        """Open file in binary read mode."""
        data_file = self.path.open("rb")
        return cast(BytesIO, data_file)

    def rename(self, other: "Absolute_Path") -> None:
        """Rename the current path to the other path."""
        self.path.rename(other.path)

    def is_real_file(self) -> bool:
        """
        Whether the current path is a file.

        Does not follow symlinks.
        """
        return self.path.is_file(follow_symlinks=False)

    def is_file(self) -> bool:
        """Whether the current path is a file or a symlink to a file."""
        return self.path.is_file()

    def is_real_directory(self) -> bool:
        """Whether the current path is a directory and not a symlink."""
        return self.path.is_dir(follow_symlinks=False)

    def is_dir(self) -> bool:
        """Whether the current path is a directory or a symlink to a directory."""
        return self.path.is_dir()

    def is_junction(self) -> bool:
        """Whether the current path is a Windows junction point."""
        return self.path.is_junction()

    def is_symlink(self) -> bool:
        """Whether the current path is a symlink."""
        return self.path.is_symlink()

    def symlink_to(self, target: "Absolute_Path | Path | str") -> None:
        """Create symlink to target at current path."""
        self.path.symlink_to(target.path if isinstance(target, Absolute_Path) else target)

    def hardlink_to(self, target: "Absolute_Path | Path | str") -> None:
        """Create hardlink to target path."""
        self.path.hardlink_to(target.path if isinstance(target, Absolute_Path) else target)

    def full_match(self, pattern: "Path | Absolute_Path") -> bool:
        """Whether the current path matches the glob-style pattern."""
        return self.path.full_match(pattern if isinstance(pattern, Path) else pattern.path)

    def stat(self) -> os.stat_result:
        """
        Return stat information as from Path.stat().

        Symlink are not followed.
        """
        return self.path.stat(follow_symlinks=False)

    def chmod(self, mode: int) -> None:
        """Change permissions of current path as in Path.chmod()."""
        self.path.chmod(mode, follow_symlinks=False)

    def relative_to(self, other_path: "Path | Absolute_Path") -> Path:
        """Returns a new path relative to the current path."""
        other = other_path.path if isinstance(other_path, Absolute_Path) else other_path
        return self.path.relative_to(other)

    def is_relative_to(self, other: "Path | Absolute_Path") -> bool:
        """Whether current path is contained without other path."""
        return self.path.is_relative_to(other.path if isinstance(other, Absolute_Path) else other)

    def samefile(self, other: "str | Path | Absolute_Path") -> bool:
        """Whether the current path references the same file as the argument."""
        return self.path.samefile(other.path if isinstance(other, Absolute_Path) else other)

    def __lt__(self, other: "Absolute_Path") -> bool:
        """Whether this path sorts before another path."""
        return self.path.__lt__(other.path)

    def __eq__(self, value: object) -> bool:
        """Check paths for equality."""
        return self.path == value.path if isinstance(value, Absolute_Path) else self.path == value

    def __hash__(self) -> int:
        """Get hash value for current path."""
        return self.path.__hash__()

    def __str__(self) -> str:
        """Create string representation."""
        return self.path.__str__()

    def __repr__(self) -> str:
        """Return standard representation."""
        return self.path.__repr__()


default_log_file_name = Absolute_Path(Path.home())/"vintagebackup.log"


def unique_path_name(destination_path: Absolute_Path) -> Absolute_Path:
    """
    Create a unique name for a path if something already exists at that path.

    If there is nothing at the destination path, it is returned unchanged. Otherwise, a number will
    be inserted between the name and suffix (if any) to prevent clobbering any existing files or
    folders.

    Arguments:
        destination_path: The path that will be modified if something already exists there.
    """
    unique_path = destination_path
    unique_id = 0
    while unique_path.exists():
        unique_id += 1
        new_path_name = f"{destination_path.stem}.{unique_id}{destination_path.suffix}"
        unique_path = destination_path.parent/new_path_name
    return unique_path


def find_unique_path(path: Absolute_Path) -> Absolute_Path | None:
    """Determine whether a path or one created by unique_path_name() exists."""
    result: Absolute_Path | None = None
    if path.exists():
        result = path

    stem = path.stem
    ext = path.suffix
    number = 0
    for p in path.parent.iterdir():
        if p.stem.startswith(stem) and p.suffix == ext:
            addition = p.stem.removeprefix(path.stem)
            if addition.startswith(".") and addition[1:].isdigit():
                new_number = int(addition[1:])
                if new_number > number:
                    result = p
                    number = new_number

    return result


def path_or_none(arg: str | None) -> Absolute_Path | None:
    """Create a Path instance if the input string is valid."""
    return Absolute_Path(arg) if arg else None


def delete_directory_tree(directory: Path | Absolute_Path, *, ignore_errors: bool = False) -> None:
    """
    Delete a single directory.

    If ignore_errors is True, skip files and folders that cannot be deleted and continue deleting
    the rest of the directory's contents. Otherwise, the function will raise an exception.
    """

    def remove_readonly(func: Callable[..., object], path: str, _: object) -> None:
        """
        Clear the readonly bit and reattempt the removal.

        Copied from https://docs.python.org/3/library/shutil.html#rmtree-example
        """
        try:
            os.chmod(path, stat.S_IWRITE, follow_symlinks=False)  # noqa: PTH101
            func(path)
        except Exception as error:
            if ignore_errors:
                logger.error("Could not delete %s: %s", path, error)
            else:
                raise

    directory_path = directory.path if isinstance(directory, Absolute_Path) else directory
    shutil.rmtree(directory_path, onexc=remove_readonly)


def delete_file(file_path: Absolute_Path, *, ignore_errors: bool = False) -> None:
    """
    Delete file with option to ignore errors.

    If ignore_errors is True, then an error message is printed. Otherwise, the exception from
    Path.unlink() is raised.
    """
    try:
        file_path.unlink()
    except Exception as error:
        if ignore_errors:
            logger.error("Could not delete %s: %s", file_path, error)
        else:
            raise


def delete_path(path: Absolute_Path, *, ignore_errors: bool = False) -> None:
    """
    Delete a path whether it is a file, folder, or something else.

    If ignore_errors is True, then an error message is printed if an exception occurs. Otherwise,
    the exception from the deletion call is raised.
    """
    if path.is_real_directory():
        delete_directory_tree(path, ignore_errors=ignore_errors)
    else:
        delete_file(path, ignore_errors=ignore_errors)


def parse_storage_space(space_requirement: str) -> float:
    """
    Parse a string into a number of bytes of storage space.

    Arguments:
        space_requirement: A string indicating an amount of space as an absolute number of
            bytes. Byte units and prefixes are allowed.

    >>> parse_storage_space("100")
    100.0

    >>> parse_storage_space("152 kB")
    152000.0

    Note that the byte units are case and spacing insensitive.
    >>> parse_storage_space("123gb")
    123000000000.0
    """
    text = "".join(space_requirement.upper().split())
    text = text.replace("K", "k")
    text = text.rstrip("B")
    try:
        number, prefix = (text[:-1], text[-1]) if text[-1].isalpha() else (text, "")
        multiplier: int = 1000**storage_prefixes.index(prefix)
        return float(number)*multiplier
    except (ValueError, IndexError):
        raise CommandLineError(f"Invalid storage space value: {space_requirement}") from None


def write_directory(output: TextIO, directory: Absolute_Path, file_names: list[str]) -> None:
    """Write the full path of a directory followed by a list of files it contains."""
    if file_names:
        output.write(f"{directory}{os.sep}\n")
        output.writelines(f"    {name}\n" for name in file_names)


def get_existing_path(path: str | None, folder_type: str) -> Absolute_Path:
    """
    Return the absolute version of the given existing path.

    Raise an exception if the path does not exist.
    """
    if not path:
        raise CommandLineError(f"{folder_type.capitalize()} not specified.")

    abs_path = Absolute_Path(path)
    if not abs_path.exists():
        raise CommandLineError(f"Could not find {folder_type.lower()}: {path}")
    return abs_path


def path_listing(listing: Iterable[tuple[Absolute_Path, list[str]]], output: TextIO) -> None:
    """
    Print a list of paths with file names listed under their directories.

    Arguments:
        listing: The list of paths. Each entry should be a directory path and the files it
            contains. The first directory should be the root directory that contains all other
            paths.
        output: Destination for the printed output.
    """
    for directory, file_names in listing:
        write_directory(output, directory, file_names)


def classify_path(path: Absolute_Path) -> str:
    """Return a text description of the item at the given path (file, folder, etc.)."""
    return (
        "Symlink" if path.is_symlink()
        else "Folder" if path.is_dir()
        else "File" if path.is_real_file()
        else "Unknown")
