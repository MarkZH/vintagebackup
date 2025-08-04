
import os
import shutil
import stat
import math
import io
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any

storage_prefixes = ["", "k", "M", "G", "T", "P", "E", "Z", "Y", "R", "Q"]


def byte_units(size: float) -> str:
    """
    Display a number of bytes with four significant figures with byte units.

    >>> byte_units(12345)
    '12.35 kB'

    >>> byte_units(12)
    '12.00 B'
    """
    if size == 0.0:
        return "0.000 B"

    if size < 0.0:
        raise RuntimeError(f"Got invalid value for byte_units(): {size}")

    prefix_step = 1000
    index = int(math.log10(size)/math.log10(prefix_step))
    prefix_size: int = prefix_step**index
    size_in_units = size/prefix_size
    prefix = storage_prefixes[index]
    decimal_digits = 4 - math.floor(math.log10(size_in_units) + 1)
    return f"{size_in_units:.{decimal_digits}f} {prefix}B"


def is_real_directory(path: Path) -> bool:
    """Return True if path is a directory and not a symlink."""
    return path.is_dir(follow_symlinks=False)


def unique_path_name(destination_path: Path) -> Path:
    """
    Create a unique name for a path if something already exists at that path.

    If there is nothing at the destination path, it is returned unchanged. Otherwise, a number will
    be inserted between the name and suffix (if any) to prevent clobbering any existing files or
    folders.

    :param destination_path: The path that will be modified if something already exists there.
    """
    unique_path = destination_path
    unique_id = 0
    while unique_path.exists(follow_symlinks=False):
        unique_id += 1
        new_path_name = f"{destination_path.stem}.{unique_id}{destination_path.suffix}"
        unique_path = destination_path.parent/new_path_name
    return unique_path


def delete_directory_tree(directory: Path, *, ignore_errors: bool = False) -> None:
    """
    Delete a single backup.

    If ignore_errors is True, skip files and folders that cannot be deleted and continue deleting
    the rest of the directory's contents. Otherwise, the function will raise an exception.
    """

    def remove_readonly(func: Callable[..., Any], path: str, _: Any) -> None:
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

    shutil.rmtree(directory, onexc=remove_readonly)


def delete_file(file_path: Path, *, ignore_errors: bool = False) -> None:
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


def delete_path(path: Path, *, ignore_errors: bool = False) -> None:
    """
    Delete a path whether it is a file, folder, or something else.

    If ignore_errors is True, then an error message is printed if an exception occurs. Otherwise,
    the exception from the deletion call is raised.
    """
    if is_real_directory(path):
        delete_directory_tree(path, ignore_errors=ignore_errors)
    else:
        delete_file(path, ignore_errors=ignore_errors)


def parse_storage_space(space_requirement: str) -> float:
    """
    Parse a string into a number of bytes of storage space.

    :param space_requirement: A string indicating an amount of space as an absolute number of
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


def write_directory(output: io.TextIOBase, directory: Path, file_names: list[str]) -> None:
    """Write the full path of a directory followed by a list of files it contains."""
    if file_names:
        output.write(f"{absolute_path(directory)}{os.sep}\n")
        output.writelines(f"    {name}\n" for name in file_names)


def get_existing_path(path: str | None, folder_type: str) -> Path:
    """
    Return the absolute version of the given existing path.

    Raise an exception if the path does not exist.
    """
    if not path:
        raise CommandLineError(f"{folder_type.capitalize()} not specified.")

    try:
        return absolute_path(path, strict=True)
    except FileNotFoundError:
        raise CommandLineError(f"Could not find {folder_type.lower()}: {path}") from None


def absolute_path(path: Path | str, *, strict: bool = False) -> Path:
    """
    Return an absolute version of the given path.

    Relative path segments (..) are removed. Symlinks are not resolved.

    :param path: The path to be made absolute.
    :param stict: If True, raise a FileNotFoundError if the path does not exist. Symlinks are
    not followed, so an existing symlink to a non-existent file or folder does not raise an error.
    """
    abs_path = Path(os.path.abspath(path))  # noqa: PTH100
    if strict and not abs_path.exists(follow_symlinks=False):
        raise FileNotFoundError(f"The path {abs_path}, resolved from {path} does not exist.")
    return abs_path


def path_listing(
        listing: Iterable[tuple[Path, list[str]]],
        output: io.TextIOBase) -> None:
    """
    Print a list of paths with file names listed under their directories.

    :param listing: The list of paths. Each entry should be a directory path and the files it
    contains. The first directory should be the root directory that contains all other paths.
    :param output: An alternate destination for the printed output.
    """
    for directory, file_names in listing:
        write_directory(output, directory, file_names)
