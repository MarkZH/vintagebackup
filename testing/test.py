"""Testing code for Vintage Backup."""
import sys
import unittest
import doctest
import tempfile
import os
import filecmp
import datetime
import shutil
import logging
from pathlib import Path
import itertools
import stat
import enum
import random
import string
import platform
from typing import cast, TextIO
import re
import io
from inspect import getsourcefile
import hashlib

from lib import backup_set
from lib import main
import lib.argument_parser as argparse
import lib.backup_utilities as util
import lib.filesystem as fs
import lib.backup as bak
import lib.datetime_calculations as dates
from lib import purge
from lib import logs
from lib import recovery
from lib import restoration
import lib.backup_deletion as deletion
import lib.move_backups as moving
from lib import backup_info
import lib.verification as verify
import lib.configuration as config
import lib.backup_lock as lock
from lib import console
from lib.exceptions import CommandLineError, ConcurrencyError
from lib import find_missing


def load_tests(loader, tests, ignore):  # type: ignore[no-untyped-def] # noqa: ANN001 ANN201 ARG001
    """Load doctests for running with unittest."""  # noqa: DOC201
    for module in (fs, dates, config, console):
        tests.addTests(doctest.DocTestSuite(module))
    return tests


def main_no_log(args: list[str]) -> int:
    """
    Run the main() function without logging to a file.

    Arguments:
        args: A list of arguments similar to sys.argv

    Returns:
        exit_code: The exit code of the call to main.main()
    """
    return main.main([*args, "--log", os.devnull], testing=True)


def main_assert_no_error_log(args: list[str], testcase: unittest.TestCase) -> int:
    """
    Run the main() function to assert there are no errors logged without logging to a file.

    Arguments:
        args: A list of arguments similar to sys.argv
        testcase: The test case that is calling this function

    Returns:
        exit_code: The exit code of the call to main.main()
    """
    with testcase.assertNoLogs(level=logging.ERROR):
        return main_no_log(args)


testing_timestamp = datetime.datetime.now()


def unique_timestamp() -> datetime.datetime:
    """
    Create a unique timestamp backups in testing so that backups can be made more rapidly.

    Returns:
        timestamp: A datetime value 10 seconds after the previous call to unique_timestamp().
    """
    global testing_timestamp  # noqa:PLW0603
    testing_timestamp += datetime.timedelta(seconds=10)
    return testing_timestamp


def unique_timestamp_string() -> str:
    """Return the stringified version of the unique_timestamp() result."""
    return unique_timestamp().strftime(util.backup_date_format)


def random_string(length: int) -> str:
    """Return a string with random ASCII letters of a given length."""
    return "".join(random.choices(string.ascii_letters, k=length))


def create_user_data(base_directory: Path) -> None:
    """
    Fill the given directory with folders and files.

    This creates a set of user data to test backups.

    Arguments:
        base_directory: The directory into which all created files and folders go.
    """
    root_file = base_directory/"root_file.txt"
    root_file.write_text("File at root of user folder.\n", encoding="utf8")
    for sub_num in range(3):
        subfolder = base_directory/f"sub_directory_{sub_num}"
        subfolder.mkdir()
        subfile = subfolder/"sub_root_file.txt"
        subfile.write_text(f"File in subfolder {sub_num}.\n", encoding="utf8")
        for sub_sub_num in range(3):
            subsubfolder = subfolder/f"sub_sub_directory_{sub_sub_num}"
            subsubfolder.mkdir()
            for file_num in range(3):
                file_path = subsubfolder/f"file_{file_num}.txt"
                file_path.write_text(
                    f"File contents: {sub_num}/{sub_sub_num}/{file_num}\n", encoding="utf8")

    music_folder = base_directory/"Music"
    music_folder.mkdir(parents=True)
    for movement in (
            "01 Dvořák Piano Quintent in A (Op. 81) - I. Allegro ma non tanto.mp3",
            "02 Dvořák Piano Quintent in A (Op. 81) - II. Dumka - Andante con moto.mp3",
            "03 Dvořák Piano Quintent in A (Op. 81) - III. Scherzo (Furiant) - Molto vivace.mp3",
            "04 Dvořák Piano Quintent in A (Op. 81) - IV. Finale - Allegro.mp3"):
        movement_file = music_folder/movement
        movement_file.write_text(movement, encoding="utf8")


def default_backup(user_path: Path, backup_path: Path) -> None:
    """
    Run a backup with all default options.

    filter_file=None,
    examine_whole_file=False,
    force_copy=False,
    copy_probability=0.0,
    timestamp=unique_timestamp()
    """
    bak.create_new_backup(
        user_path,
        backup_path,
        filter_file=None,
        examine_whole_file=False,
        force_copy=False,
        copy_probability=0.0,
        timestamp=unique_timestamp())


def create_old_monthly_backups(backup_base_directory: Path, count: int) -> None:
    """
    Create a set of empty monthly backups.

    Arguments:
        backup_base_directory: The directory that will contain the backup folders.
        count: The number of backups to create. The oldest will be (count - 1) months old.
    """
    now = datetime.datetime.now()
    for months_back in range(count):
        backup_date = dates.months_ago(now, months_back)
        backup_timestamp = datetime.datetime.combine(backup_date, now.time())
        create_old_backup(backup_base_directory, backup_timestamp)


def create_old_backup(backup_base_directory: Path, backup_timestamp: datetime.datetime) -> None:
    """Create a single empty backup with the given timestamp."""
    backup_name = backup_timestamp.strftime(util.backup_date_format)
    backup_path = backup_base_directory/str(backup_timestamp.year)/backup_name
    backup_path.mkdir(parents=True)


def create_old_daily_backups(backup_base_directory: Path, count: int) -> None:
    """
    Create a set of empty daily backups.

    Arguments:
        backup_base_directory: The directory that will contain the backup folders.
        count: The number of backups to create. The oldest will be (count - 1) days old.
    """
    now = datetime.datetime.now()
    for days_back in range(count):
        backup_timestamp = now - datetime.timedelta(days=days_back)
        create_old_backup(backup_base_directory, backup_timestamp)


def directory_contents(base_directory: Path) -> set[Path]:
    """Return a set of all paths in a directory relative to that directory."""
    paths: set[Path] = set()
    for directory, directories, files in base_directory.walk():
        relative_directory = directory.relative_to(base_directory)
        paths.update(relative_directory/name for name in itertools.chain(directories, files))
    return paths


def all_files_have_same_content(standard_directory: Path, test_directory: Path) -> bool:
    """
    Test that every file in the standard directory exists also in the test directory.

    Corresponding files will also be checked for identical contents.

    Arguments:
        standard_directory: The base directory that will serve as the standard of comparison.
        test_directory: This directory must possess every file in the standard directory in the
            same location and with the same contents. Extra files in this directory will not result
            in failure.

    Returns:
        compare_result: Whether test_directory contains all of the files and folders in
            standard_directory.
    """
    for directory_1, _, file_names in standard_directory.walk():
        directory_2 = test_directory/directory_1.relative_to(standard_directory)
        _, mismatches, errors = filecmp.cmpfiles(
            directory_1,
            directory_2,
            file_names,
            shallow=False)
        if mismatches or errors:
            return False
    return True


def directories_have_identical_content(base_directory_1: Path, base_directory_2: Path) -> bool:
    """
    Check that both directories have same directory tree and file contents.

    Arguments:
        base_directory_1: Path to folder
        base_directory_2: Path to folder

    Returns:
        compare_result: whether both directories have the same files in the same folders with the
            same content.
    """
    return (all_files_have_same_content(base_directory_1, base_directory_2)
            and all_files_have_same_content(base_directory_2, base_directory_1))


def all_files_are_hardlinked(standard_directory: Path, test_directory: Path) -> bool:
    """
    Test that every file in the standard directory is hardlinked in the test_directory.

    Arguments:
        standard_directory: A directory whose files should all have links in the test_directory
        test_directory: A directory that should contain hard links to all file in standard_directory

    Returns:
        hard_link_result: Whether all files in standard_directory are hard linked to files in
            test_directory.
    """
    for directory_1, _, file_names in standard_directory.walk():
        directory_2 = test_directory/(directory_1.relative_to(standard_directory))
        for file_name in file_names:
            inode_1 = (directory_1/file_name).stat().st_ino
            inode_2 = (directory_2/file_name).stat().st_ino
            if inode_1 != inode_2:
                return False
    return True


def directories_are_completely_hardlinked(base_directory_1: Path, base_directory_2: Path) -> bool:
    """
    Check that both directories have same tree and all files are hardlinked together.

    Arguments:
        base_directory_1: Path to folders for comparison
        base_directory_2: Path to folders for comparison

    Returns:
        link_result: True if all files in both directories are hardlinked to their counterparts in
            the other folder.
    """
    return (all_files_are_hardlinked(base_directory_1, base_directory_2)
            and all_files_are_hardlinked(base_directory_2, base_directory_1))


def no_files_are_hardlinks(standard_directory: Path, test_directory: Path) -> bool:
    """
    Test files in standard directory are not hard linked to counterparts in test directory.

    Arguments:
        standard_directory: Path to folders to check for links
        test_directory: Path to folders to check for links

    Returns:
        no_links: True if there is no file in standard_directory that is hardlinked to its
            counterpart in test_directory
    """
    for directory_1, _, file_names in standard_directory.walk():
        directory_2 = test_directory/(directory_1.relative_to(standard_directory))
        for file_name in file_names:
            inode_1 = (directory_1/file_name).stat().st_ino
            inode_2 = (directory_2/file_name).stat().st_ino
            if inode_1 == inode_2:
                return False
    return True


def directories_are_completely_copied(base_directory_1: Path, base_directory_2: Path) -> bool:
    """
    Check that both directories have same tree and all files are copies.

    Arguments:
        base_directory_1: Path to folders to compare
        base_directory_2: Path to folders to compare

    Returns:
        compare_result: Whether both folders have the same directory tree, both file sets contain
            the same data, and no files are hardlinked between them



    """
    return (no_files_are_hardlinks(base_directory_1, base_directory_2)
            and no_files_are_hardlinks(base_directory_2, base_directory_1)
            and directories_have_identical_content(base_directory_1, base_directory_2))


class Invocation(enum.StrEnum):
    """Specify whether to test a direct function call or a CLI invocation."""

    function = enum.auto()
    cli = enum.auto()


def run_backup(
        run_method: Invocation,
        user_data: Path,
        backup_location: Path,
        filter_file: Path | None,
        *,
        examine_whole_file: bool,
        force_copy: bool,
        timestamp: datetime.datetime) -> int:
    """
    Create a new backup while choosing a direct function call or a CLI invocation.

    Arguments:
        run_method: How to run the function under test: direct call or command line arguments
        user_data: Path to test user data
        backup_location: Path to test backup directory
        filter_file: Path to a file with a backup set filter
        examine_whole_file: Whether to compare file contents when making a new backup
        force_copy: If True, do not hard link any files, even if unchanged
        timestamp: The timestamp to use for the backup to make sure backups created successively do
            not have the same timestamp

    Returns:
        exit_code: The exit code of the program run: zero for success and non-zero for failure.
    """
    if run_method == Invocation.function:
        bak.create_new_backup(
            user_data,
            backup_location,
            filter_file=filter_file,
            examine_whole_file=examine_whole_file,
            force_copy=force_copy,
            copy_probability=0.0,
            timestamp=timestamp)
        return 0
    elif run_method == Invocation.cli:
        argv = [
            "--user-folder", str(user_data),
            "--backup-folder", str(backup_location),
            "--timestamp", timestamp.strftime(util.backup_date_format)]
        if filter_file:
            argv.extend(["--filter", str(filter_file)])
        if examine_whole_file:
            argv.append("--compare-contents")
        if force_copy:
            argv.append("--force-copy")
        return main_no_log(argv)
    else:
        raise NotImplementedError(f"Backup test with {run_method} not implemented.")


def run_backup_assert_no_error_logs(
        testcase: unittest.TestCase,
        run_method: Invocation,
        user_data: Path,
        backup_location: Path,
        filter_file: Path | None,
        *,
        examine_whole_file: bool,
        force_copy: bool,
        timestamp: datetime.datetime) -> int:
    """
    Run backup while asserting that no errors are logged.

    Arguments:
        testcase: The current TestCase being run
        run_method: How to run the function under test: direct call or command line arguments
        user_data: Path to test user data
        backup_location: Path to test backup directory
        filter_file: Path to a file with a backup set filter
        examine_whole_file: Whether to compare file contents when making a new backup
        force_copy: If True, do not hard link any files, even if unchanged
        timestamp: The timestamp to use for the backup to make sure backups created successively do
            not have the same timestamp

    Returns:
        exit_code: The exit code of the program run: zero for success and non-zero for failure.
    """
    with testcase.assertNoLogs(level=logging.ERROR):
        return run_backup(
            run_method,
            user_data,
            backup_location,
            filter_file,
            examine_whole_file=examine_whole_file,
            force_copy=force_copy,
            timestamp=timestamp)


class TestCaseWithTemporaryFilesAndFolders(unittest.TestCase):
    """Base class that sets up temporary files and folders."""

    def setUp(self) -> None:
        """Create folders and files for backup tests."""
        self.make_new_user_folder()
        self.make_new_backup_folder()
        self.config_path = self.user_path/"config.txt"
        self.filter_path = self.user_path/"filter.txt"
        self.log_path = self.user_path/"log.txt"

    def tearDown(self) -> None:
        """Delete the temporary directories and reset the logger."""
        logs.setup_initial_null_logger()
        for directory in (self.user_path, self.backup_path):
            fs.delete_directory_tree(directory)

    def reset_backup_folder(self) -> None:
        """Delete backup directory and create a new empty one."""
        fs.delete_directory_tree(self.backup_path)
        self.make_new_backup_folder()

    def make_new_backup_folder(self) -> None:
        """Recreate backup folder after manually deleting it."""
        self.backup_path = Path(tempfile.mkdtemp())

    def reset_user_folder(self) -> None:
        """Delete backup directory and create a new empty one."""
        fs.delete_directory_tree(self.user_path)
        self.make_new_user_folder()

    def make_new_user_folder(self) -> None:
        """Recreate user folder after manually deleting it."""
        self.user_path = Path(tempfile.mkdtemp())


class BackupTests(TestCaseWithTemporaryFilesAndFolders):
    """Test the main backup procedure."""

    def test_first_backup_copies_all_user_data(self) -> None:
        """Test that the first default backup copies everything in user data."""
        create_user_data(self.user_path)
        for method in Invocation:
            exit_code = run_backup_assert_no_error_logs(
                self,
                method,
                self.user_path,
                self.backup_path,
                filter_file=None,
                examine_whole_file=False,
                force_copy=False,
                timestamp=unique_timestamp())
            self.assertEqual(exit_code, 0, method)
            backups = util.all_backups(self.backup_path)
            self.assertEqual(len(backups), 1, method)
            self.assertEqual(backups[0], util.find_previous_backup(self.backup_path), method)
            self.assertTrue(directories_are_completely_copied(self.user_path, backups[0]), method)
            self.reset_backup_folder()

    def test_second_backup_with_unchanged_data_hardlinks_everything_in_first_backup(self) -> None:
        """Test that second default backup with same data hard links everything in first backup."""
        create_user_data(self.user_path)
        for method in Invocation:
            for _ in range(2):
                exit_code = run_backup_assert_no_error_logs(
                    self,
                    method,
                    self.user_path,
                    self.backup_path,
                    filter_file=None,
                    examine_whole_file=False,
                    force_copy=False,
                    timestamp=unique_timestamp())
                self.assertEqual(exit_code, 0)
            backups = util.all_backups(self.backup_path)
            self.assertEqual(len(backups), 2, method)
            self.assertEqual(backups[1], util.find_previous_backup(self.backup_path), method)
            self.assertTrue(directories_are_completely_hardlinked(*backups), method)
            self.reset_backup_folder()

    def test_force_copy_results_in_backup_with_copied_user_data(self) -> None:
        """Test that latest backup is a copy of user data with --force-copy option."""
        create_user_data(self.user_path)
        for method in Invocation:
            for _ in range(2):
                exit_code = run_backup_assert_no_error_logs(
                    self,
                    method,
                    self.user_path,
                    self.backup_path,
                    filter_file=None,
                    examine_whole_file=False,
                    force_copy=True,
                    timestamp=unique_timestamp())
                self.assertEqual(exit_code, 0)
            backups = util.all_backups(self.backup_path)
            self.assertEqual(len(backups), 2, method)
            self.assertEqual(backups[1], util.find_previous_backup(self.backup_path), method)
            self.assertTrue(directories_are_completely_copied(self.user_path, backups[-1]), method)
            self.assertTrue(directories_are_completely_copied(*backups), method)
            self.reset_backup_folder()

    def test_examining_whole_files_still_hardlinks_identical_files(self) -> None:
        """
        Test that examining file contents results in hardlinks to identical files in new backup.

        Even if the timestamp has changed, --compare-contents will hard link files with the same
        data.
        """
        create_user_data(self.user_path)
        for method in Invocation:
            for _ in range(2):
                exit_code = run_backup_assert_no_error_logs(
                    self,
                    method,
                    self.user_path,
                    self.backup_path,
                    filter_file=None,
                    examine_whole_file=True,
                    force_copy=False,
                    timestamp=unique_timestamp())
                self.assertEqual(exit_code, 0, method)
                for current_directory, _, files in self.user_path.walk():
                    for file in files:
                        (current_directory/file).touch()  # update timestamps

            backups = util.all_backups(self.backup_path)
            self.assertEqual(len(backups), 2, method)
            self.assertEqual(backups[-1], util.find_previous_backup(self.backup_path), method)
            self.assertTrue(directories_are_completely_hardlinked(*backups), method)
            self.reset_backup_folder()

    def test_force_copy_overrides_examine_whole_file(self) -> None:
        """Test that --force-copy results in a copy backup even if --compare-contents is present."""
        create_user_data(self.user_path)
        for method in Invocation:
            for _ in range(2):
                exit_code = run_backup_assert_no_error_logs(
                    self,
                    method,
                    self.user_path,
                    self.backup_path,
                    filter_file=None,
                    examine_whole_file=True,
                    force_copy=True,
                    timestamp=unique_timestamp())
                self.assertEqual(exit_code, 0, method)
            backups = util.all_backups(self.backup_path)
            self.assertEqual(len(backups), 2, method)
            self.assertEqual(backups[-1], util.find_previous_backup(self.backup_path), method)
            self.assertTrue(directories_are_completely_copied(*backups), method)
            self.reset_backup_folder()

    def test_compare_contents_every_compares_contents_on_first_backup(self) -> None:
        """Test that --compare-contents-every causes contents to be compared on first backup."""
        create_user_data(self.user_path)
        with self.assertLogs(level=logging.INFO) as logs:
            exit_code = main.main([
                "-u", str(self.user_path),
                "-b", str(self.backup_path),
                "--compare-contents-every", "30d",
                "-l", os.devnull],
                testing=True)
            self.assertEqual(exit_code, 0)

        compare_contents_line = "INFO:root:Reading file contents = True"
        self.assertIn(compare_contents_line, logs.output)

    def test_compare_contents_every_does_not_compare_contents_on_second_backup(self) -> None:
        """Test that --compare-contents-every does not compare contents before time elapsed."""
        create_user_data(self.user_path)
        interval = datetime.timedelta(days=1)
        timestamp = datetime.datetime.now()
        with self.assertLogs(level=logging.INFO) as logs:
            for _ in range(2):
                exit_code = main.main([
                    "-u", str(self.user_path),
                    "-b", str(self.backup_path),
                    "--compare-contents-every", "30d",
                    "--timestamp", timestamp.strftime(util.backup_date_format),
                    "-l", os.devnull],
                    testing=True)
                self.assertEqual(exit_code, 0)
                timestamp += interval

        compare_contents_line = "INFO:root:Reading file contents = True"
        self.assertEqual(logs.output.count(compare_contents_line), 1)
        compare_index = logs.output.index(compare_contents_line)

        no_compare_contents_line = "INFO:root:Reading file contents = False"
        self.assertEqual(logs.output.count(no_compare_contents_line), 1)
        no_compare_index = logs.output.index(no_compare_contents_line)

        self.assertGreater(no_compare_index, compare_index)

    def test_compare_contents_every_compares_contents_on_correct_backups(self) -> None:
        """Test that --compare-contents-every compare contents at correct interval."""
        create_user_data(self.user_path)
        backup_interval = datetime.timedelta(days=1)
        timestamp = datetime.datetime.now()
        backups = 11
        compare_interval = 5
        with self.assertLogs(level=logging.INFO) as logs:
            for _ in range(backups):
                exit_code = main.main([
                    "-u", str(self.user_path),
                    "-b", str(self.backup_path),
                    "--compare-contents-every", f"{compare_interval} d",
                    "--timestamp", timestamp.strftime(util.backup_date_format),
                    "-l", os.devnull],
                    testing=True)
                self.assertEqual(exit_code, 0)
                timestamp += backup_interval

        compare_line_start = "INFO:root:Reading file contents = "
        compare_lines = filter(lambda s: s.startswith(compare_line_start), logs.output)
        actually_compared = [s.removeprefix(compare_line_start) == "True" for s in compare_lines]
        expected_compares = [i % compare_interval == 0 for i in range(backups)]
        self.assertEqual(actually_compared, expected_compares)

    def test_file_that_changed_between_backups_is_copied(self) -> None:
        """Check that a file changed between backups is copied with others are hardlinked."""
        create_user_data(self.user_path)
        default_backup(self.user_path, self.backup_path)

        changed_file_name = self.user_path/"sub_directory_2"/"sub_sub_directory_0"/"file_1.txt"
        with changed_file_name.open("a", encoding="utf8") as changed_file:
            changed_file.write("the change\n")

        default_backup(self.user_path, self.backup_path)
        backup_1, backup_2 = util.all_backups(self.backup_path)
        contents_1 = directory_contents(backup_1)
        contents_2 = directory_contents(backup_2)
        self.assertEqual(contents_1, contents_2)
        relative_changed_file = changed_file_name.relative_to(self.user_path)
        for file in filter(lambda f: (backup_1/f).is_file(), contents_1):
            self.assertEqual(
                file != relative_changed_file,
                (backup_1/file).stat().st_ino == (backup_2/file).stat().st_ino)

    @unittest.skipIf(
            platform.system() == "Windows",
            "Cannot create symlinks on Windows without elevated privileges.")
    def test_symlinks_are_always_copied_as_symlinks(self) -> None:
        """Test that symlinks in user data are symlinks in backups."""
        create_user_data(self.user_path)
        directory_symlink_name = "directory_symlink"
        (self.user_path/directory_symlink_name).symlink_to(self.user_path/"sub_directory_1")
        file_symlink_name = "file_symlink.txt"
        file_link_target = self.user_path/"sub_directory_1"/"sub_sub_directory_1"/"file_2.txt"
        (self.user_path/file_symlink_name).symlink_to(file_link_target)

        default_backup(self.user_path, self.backup_path)
        last_backup = util.find_previous_backup(self.backup_path)
        self.assertIsNotNone(last_backup)
        last_backup = cast(Path, last_backup)
        self.assertTrue((last_backup/directory_symlink_name).is_symlink())
        self.assertTrue((last_backup/file_symlink_name).is_symlink())

    @unittest.skipIf(
            platform.system() == "Windows",
            "Cannot create symlinks on Windows without elevated privileges.")
    def test_symlinks_are_never_hardlinked(self) -> None:
        """Test that multiple backups of symlinks are always copied."""
        create_user_data(self.user_path)
        directory_symlink_name = "directory_symlink"
        (self.user_path/directory_symlink_name).symlink_to(self.user_path/"sub_directory_1")
        file_symlink_name = "file_symlink.txt"
        file_link_target = self.user_path/"sub_directory_1"/"sub_sub_directory_1"/"file_2.txt"
        (self.user_path/file_symlink_name).symlink_to(file_link_target)

        for _ in range(2):
            default_backup(self.user_path, self.backup_path)
        backup_1, backup_2 = util.all_backups(self.backup_path)
        self.assertNotEqual(
            (backup_1/directory_symlink_name).stat(follow_symlinks=False).st_ino,
            (backup_2/directory_symlink_name).stat(follow_symlinks=False).st_ino)
        self.assertNotEqual(
            (backup_1/file_symlink_name).stat(follow_symlinks=False).st_ino,
            (backup_2/file_symlink_name).stat(follow_symlinks=False).st_ino)

    def test_backing_up_different_folder_to_existing_backup_set_is_an_error(self) -> None:
        """Test that backing up different folders to the same backup folder raises an exception."""
        with tempfile.TemporaryDirectory() as other_user_folder:
            other_user_path = Path(other_user_folder)
            create_user_data(other_user_path)
            default_backup(other_user_path, self.backup_path)

            create_user_data(self.user_path)
            with self.assertRaises(CommandLineError):
                default_backup(self.user_path, self.backup_path)

    def test_warn_when_backup_is_larger_than_free_up(self) -> None:
        """Test that a warning is logged when a backup is larger that the free-up argument."""
        create_large_files(self.user_path, 50_000_000)
        arguments = [
            "--user-folder", str(self.user_path),
            "--backup-folder", str(self.backup_path),
            "--free-up", "1MB"]

        with self.assertLogs(level=logging.WARNING) as log_lines:
            exit_code = main_no_log(arguments)
        self.assertEqual(exit_code, 0)

        prefix = r"WARNING:root:"
        space_warning = f"{prefix}Backup space used: 50.00 MB (5000% of --free-up)"
        self.assertEqual(len(log_lines.output), 2)
        self.assertEqual(space_warning, log_lines.output[0])
        self.assertEqual(
            log_lines.output[1],
            f"{prefix}Consider increasing the size of the --free-up parameter.")

    def test_warn_when_backup_is_nearly_as_large_as_free_up(self) -> None:
        """Test that a warning is logged when a backup is more than 90% of the free-up argument."""
        create_large_files(self.user_path, 50_000_000)
        arguments = [
            "--user-folder", str(self.user_path),
            "--backup-folder", str(self.backup_path),
            "--free-up", "51 MB"]

        with self.assertLogs(level=logging.WARNING) as log_lines:
            exit_code = main_no_log(arguments)
        self.assertEqual(exit_code, 0)

        prefix = r"WARNING:root:"
        space_warning = f"{prefix}Backup space used: 50.00 MB (99% of --free-up)"
        self.assertEqual(len(log_lines.output), 2)
        self.assertEqual(space_warning, log_lines.output[0])
        self.assertEqual(
            log_lines.output[1],
            f"{prefix}Consider increasing the size of the --free-up parameter.")

    def test_info_when_backup_is_smaller_than_free_up(self) -> None:
        """Test that a warning is not logged when a backup is smaller that the free-up argument."""
        create_large_files(self.user_path, 50_000_000)
        arguments = [
            "--user-folder", str(self.user_path),
            "--backup-folder", str(self.backup_path),
            "--free-up", "100 MB"]
        with self.assertLogs(level=logging.INFO) as logs:
            exit_code = main_no_log(arguments)
        self.assertEqual(exit_code, 0)
        expected_message = "INFO:root:Backup space used: 50.00 MB (50% of --free-up)"
        self.assertIn(expected_message, logs.output)
        self.assertFalse(any(line.startswith("WARNING:") for line in logs.output), logs.output)
        self.assertFalse(any(line.startswith("ERROR:") for line in logs.output), logs.output)

    def test_no_user_folder_specified_for_backup_is_an_error(self) -> None:
        """Test that omitting the user folder prints the correct error message."""
        with self.assertLogs(level=logging.ERROR) as log_check:
            exit_code = main_no_log(["-b", "backup_folder"])
        self.assertEqual(exit_code, 1)
        self.assertEqual(log_check.output, ["ERROR:root:User's folder not specified."])

    def test_no_backup_folder_specified_for_backup_error(self) -> None:
        """Test that omitting the backup folder prints the correct error message."""
        with self.assertLogs(level=logging.ERROR) as log_check:
            exit_code = main_no_log(["-u", str(self.user_path)])
        self.assertEqual(exit_code, 1)
        self.assertEqual(log_check.output, ["ERROR:root:Backup folder not specified."])

    def test_non_existent_user_folder_in_a_backup_is_an_error(self) -> None:
        """Test that non-existent user folder prints correct error message."""
        user_folder = random_string(50)
        with self.assertLogs(level=logging.ERROR) as log_check:
            exit_code = main_no_log(["-u", user_folder])
        self.assertEqual(exit_code, 1)
        expected_logs = [f"ERROR:root:Could not find user's folder: {user_folder}"]
        self.assertEqual(log_check.output, expected_logs)

    def test_backing_up_different_user_folders_to_same_backup_location_is_an_error(self) -> None:
        """Check that error is raised when attempted to change the source of a backup set."""
        with tempfile.TemporaryDirectory() as other_user_folder:
            other_user_path = Path(other_user_folder)
            with self.assertRaises(CommandLineError) as error:
                default_backup(self.user_path, self.backup_path)
                default_backup(other_user_path, self.backup_path)

        expected_error_message = (
            "Previous backup stored a different user folder. Previously: "
            f"{self.user_path}; Now: {other_user_path}")
        self.assertEqual(error.exception.args, (expected_error_message,))

    def test_warning_printed_if_no_user_data_is_backed_up(self) -> None:
        """Make sure a warning is printed if no files are backed up."""
        with self.assertLogs(level=logging.WARNING) as assert_log:
            default_backup(self.user_path, self.backup_path)
        self.assertIn("WARNING:root:No files were backed up!", assert_log.output)
        self.assertEqual(
            list(self.backup_path.iterdir()),
            [self.backup_path/"vintagebackup.source.txt"])

    def test_no_dated_backup_folder_created_if_no_data_backed_up(self) -> None:
        """Test that a dated backup folder is not created if there is no data to back up."""
        default_backup(self.user_path, self.backup_path)
        self.assertEqual(
            list(self.backup_path.iterdir()),
            [self.backup_path/"vintagebackup.source.txt"])

    def test_warning_printed_if_all_user_files_filtered_out(self) -> None:
        """Make sure the user is warned if a filter file removes all files from the backup set."""
        create_user_data(self.user_path)
        self.filter_path.write_text("- **/*.txt\n- **/*.mp3\n", encoding="utf8")

        with self.assertLogs(level=logging.WARNING) as assert_log:
            bak.create_new_backup(
                self.user_path,
                self.backup_path,
                filter_file=self.filter_path,
                examine_whole_file=False,
                force_copy=False,
                copy_probability=0.0,
                timestamp=unique_timestamp())
        self.assertIn("WARNING:root:No files were backed up!", assert_log.output)
        self.assertEqual(
            list(self.backup_path.iterdir()),
            [self.backup_path/"vintagebackup.source.txt"])

    def test_hard_links_in_user_data_are_not_preserved(self) -> None:
        """
        Test that files that are hard-linked in the user's folder are not linked in backups.

        This test ensures that the documentation about hard links in user data is correct, (see the
        Technical Details in the command line help and the Other Details section in the Backup page
        of the wiki). It's not really a feature, just a limitation.
        """
        create_user_data(self.user_path)
        linked_file = self.user_path/"root_file.txt"
        self.assertTrue(linked_file.is_file())
        other_linked_file = self.user_path/"linked_root_file.txt"
        self.assertFalse(other_linked_file.exists())
        other_linked_file.hardlink_to(linked_file)
        self.assertEqual(linked_file.stat().st_ino, other_linked_file.stat().st_ino)
        default_backup(self.user_path, self.backup_path)
        backup = util.find_previous_backup(self.backup_path)
        self.assertIsNotNone(backup)
        backup = cast(Path, backup)
        linked_backup_file = backup/linked_file.relative_to(self.user_path)
        self.assertTrue(linked_backup_file.is_file())
        other_linked_backup_file = backup/other_linked_file.relative_to(self.user_path)
        self.assertTrue(other_linked_backup_file.is_file())
        self.assertNotEqual(
            linked_backup_file.stat().st_ino,
            other_linked_backup_file.stat().st_ino)


class FilterTests(TestCaseWithTemporaryFilesAndFolders):
    """Test that filter files work properly."""

    def test_paths_excluded_in_filter_file_do_not_appear_in_backup(self) -> None:
        """Test that filter files with only exclusions result in the right files being excluded."""
        create_user_data(self.user_path)
        with self.filter_path.open("w", encoding="utf8") as filter_file:
            filter_file.write("- sub_directory_2/**\n    \n")
            filter_file.write(str(Path("- *")/"sub_sub_directory_0/**\n\n"))

        user_paths = directory_contents(self.user_path)
        expected_backups = user_paths.copy()
        expected_backups.difference_update(
            path for path in user_paths if "sub_directory_2" in path.parts)
        expected_backups.difference_update(
            path for path in user_paths if "sub_sub_directory_0" in path.parts)

        for method in Invocation:
            exit_code = run_backup_assert_no_error_logs(
                self,
                method,
                self.user_path,
                self.backup_path,
                filter_file=self.filter_path,
                examine_whole_file=False,
                force_copy=False,
                timestamp=unique_timestamp())
            self.assertEqual(exit_code, 0)

            last_backup = util.find_previous_backup(self.backup_path)
            self.assertIsNotNone(last_backup, method)
            last_backup = cast(Path, last_backup)

            self.assertEqual(directory_contents(last_backup), expected_backups, method)
            self.assertNotEqual(directory_contents(self.user_path), expected_backups, method)
            self.reset_backup_folder()

    def test_path_excluded_with_absolute_file_name_in_filter_file_are_not_in_backup(self) -> None:
        """Test that filter files with absolute paths excluded exclude the right paths."""
        create_user_data(self.user_path)
        with self.filter_path.open("w", encoding="utf8") as filter_file:
            filter_file.write(f"- {self.user_path/'sub_directory_2'/'**'}\n\n")
        user_paths = directory_contents(self.user_path)
        expected_backups = user_paths.copy()
        expected_backups.difference_update(
            path for path in user_paths if "sub_directory_2" in path.parts)

        for method in Invocation:
            exit_code = run_backup_assert_no_error_logs(
                self,
                method,
                self.user_path,
                self.backup_path,
                filter_file=self.filter_path,
                examine_whole_file=False,
                force_copy=False,
                timestamp=unique_timestamp())
            self.assertEqual(exit_code, 0, method)

            last_backup = util.find_previous_backup(self.backup_path)
            self.assertIsNotNone(last_backup, method)
            last_backup = cast(Path, last_backup)

            self.assertEqual(directory_contents(last_backup), expected_backups, method)
            self.assertNotEqual(directory_contents(self.user_path), expected_backups, method)
            self.reset_backup_folder()

    def test_paths_included_after_exclusions_appear_in_backup(self) -> None:
        """Test that filter files with inclusions and exclusions work properly."""
        create_user_data(self.user_path)
        with self.filter_path.open("w", encoding="utf8") as filter_file:
            filter_file.write("- sub_directory_2/**\n\n")
            filter_file.write(str(Path("- *")/"sub_sub_directory_0/**\n\n"))
            filter_file.write(str(Path("+ sub_directory_1")/"sub_sub_directory_0"/"file_1.txt\n\n"))

        user_paths = directory_contents(self.user_path)
        expected_backup_paths = user_paths.copy()
        expected_backup_paths.difference_update(
            path for path in user_paths if "sub_directory_2" in path.parts)
        expected_backup_paths.difference_update(
            path for path in user_paths if "sub_sub_directory_0" in path.parts)
        expected_backup_paths.add(Path("sub_directory_1")/"sub_sub_directory_0")
        expected_backup_paths.add(Path("sub_directory_1")/"sub_sub_directory_0"/"file_1.txt")

        bak.create_new_backup(
            self.user_path,
            self.backup_path,
            filter_file=self.filter_path,
            examine_whole_file=False,
            force_copy=False,
            copy_probability=0.0,
            timestamp=unique_timestamp())

        self.assertEqual(len(util.all_backups(self.backup_path)), 1)
        last_backup = util.find_previous_backup(self.backup_path)
        self.assertIsNotNone(last_backup)
        last_backup = cast(Path, last_backup)

        self.assertEqual(directory_contents(last_backup), expected_backup_paths)
        self.assertNotEqual(directory_contents(self.user_path), expected_backup_paths)

    def test_filter_lines_that_have_no_effect_are_logged(self) -> None:
        """Test that filter lines with no effect on the backup files are detected."""
        create_user_data(self.user_path)

        with self.filter_path.open("w", encoding="utf8") as filter_file:
            filter_file.write("- sub_directory_1/**\n")

            bad_lines = [
                ("-", "sub_directory_1/sub_sub_directory_0/**"),  # redundant exclusion
                ("+", "sub_directory_0/**"),  # redundant inclusion
                ("-", "does_not_exist.txt"),  # excluding non-existent file
                ("-", "sub_directory_0"),  # ineffective exclusion of folder
                ("-", "sub_directory_1/*")]  # ineffective exlusion of folder

            filter_file.write("# Ineffective lines:\n")
            for sign, line in bad_lines:
                filter_file.write(f"{sign} {line}\n")

        with self.assertLogs() as log_assert:
            for _ in backup_set.Backup_Set(self.user_path, self.filter_path):
                pass

        for line_number, (sign, path) in enumerate(bad_lines, 3):
            self.assertIn(
                f"INFO:root:{filter_file.name}: line #{line_number} "
                f"({sign} {self.user_path/path}) had no effect.",
                log_assert.output)

        self.assertTrue(
            all("Ineffective" not in message for message in log_assert.output),
            log_assert.output)

    def test_invalid_filter_symbol_raises_exception(self) -> None:
        """Test that a filter symbol not in "+-#" raises an exceptions."""
        self.filter_path.write_text("* invalid_sign\n", encoding="utf8")
        with self.assertRaises(ValueError) as error:
            backup_set.Backup_Set(Path(), self.filter_path)
        self.assertIn("The first symbol of each line", error.exception.args[0])

    def test_path_outside_user_folder_in_filter_file_raises_exception(self) -> None:
        """Test that adding a path outside the user folder (--user-folder) raises an exception."""
        create_user_data(self.user_path)
        self.filter_path.write_text("- /other_place/sub_directory_0", encoding="utf8")
        with self.assertRaises(ValueError) as error:
            backup_set.Backup_Set(self.user_path, self.filter_path)
        self.assertIn("outside user folder", error.exception.args[0])

    def test_filter_preview_lists_correct_files(self) -> None:
        """Test that previewing a filter matches the files that are backed up."""
        create_user_data(self.user_path)
        self.filter_path.write_text("- **/*1.txt\n", encoding="utf8")
        preview_path = self.user_path/"preview.txt"
        main_assert_no_error_log([
            "--user-folder", str(self.user_path),
            "--filter", str(self.filter_path),
            "--preview-filter", str(preview_path)],
            self)

        with preview_path.open(encoding="utf8") as preview:
            previewed_paths = read_paths_file(preview)
        previewed_paths = {path.relative_to(self.user_path) for path in previewed_paths}

        main_assert_no_error_log([
            "--user-folder", str(self.user_path),
            "--backup-folder", str(self.backup_path),
            "--filter", str(self.filter_path)],
            self)

        backup_list_path = self.user_path/"backed_up.txt"
        last_backup = cast(Path, util.find_previous_backup(self.backup_path))
        with backup_list_path.open("w", encoding="utf8") as backup_list:
            for directory, _, files in last_backup.walk():
                fs.write_directory(backup_list, directory, files)

        with backup_list_path.open(encoding="utf8") as backup_list:
            backed_up_paths = read_paths_file(backup_list)
        backed_up_paths = {path.relative_to(last_backup) for path in backed_up_paths}

        self.assertEqual(previewed_paths, backed_up_paths)


def run_recovery(
        method: Invocation,
        backup_location: Path,
        file_path: Path,
        *,
        choices: int | str,
        search: bool) -> int:
    """
    Test file recovery through a direct function call or a CLI invocation.

    Arguments:
        method: How to run the function under test: direct call or command line arguments
        backup_location: Folder containing all dated backups
        file_path: The original path to the file being recovered
        choices: Which backup to recover. If a single integer, then that choice will be made from
            the console menu. If a string, then it should be a sequence of the letters "o", "n",
            and "c" to choose a recovery target using binary choice.
        search: If True, use binary choice to pick the backup version. If False, choose the
            recovery target from a menu.

    Returns:
        exit_code: The exit code of the program run: zero for success and non-zero for failure.
    """
    if method == Invocation.function:
        recovery.recover_path(file_path, backup_location, search=search, choice=choices)
        return 0
    elif method == Invocation.cli:
        argv = [
            "--recover", str(file_path),
            "--backup-folder", str(backup_location),
            "--choice", str(choices)]
        if search:
            argv.append("--search")
        return main_no_log(argv)
    else:
        raise NotImplementedError(f"Backup test with {method} not implemented.")


class RecoveryTests(TestCaseWithTemporaryFilesAndFolders):
    """Test recovering files and folders from backups."""

    def test_file_recovered_from_backup_is_identical_to_original(self) -> None:
        """Test that recovering a single file gets back same data."""
        create_user_data(self.user_path)
        for method in Invocation:
            default_backup(self.user_path, self.backup_path)
            file = self.user_path/"sub_directory_0"/"sub_sub_directory_0"/"file_0.txt"
            moved_file_path = file.parent/(file.name + "_moved")
            file.rename(moved_file_path)
            with self.assertNoLogs(level=logging.ERROR):
                exit_code = run_recovery(method, self.backup_path, file, choices=0, search=False)
            self.assertEqual(exit_code, 0, method)
            self.assertTrue(filecmp.cmp(file, moved_file_path, shallow=False), method)

            self.reset_backup_folder()
            moved_file_path.unlink()

    def test_recovered_file_renamed_to_not_clobber_original_and_is_same_as_original(self) -> None:
        """Test that recovering a file that exists in user data does not overwrite any files."""
        create_user_data(self.user_path)
        default_backup(self.user_path, self.backup_path)
        file_path = self.user_path/"sub_directory_0"/"sub_sub_directory_0"/"file_0.txt"
        recovery.recover_path(file_path, self.backup_path, search=False, choice=0)
        recovered_file_path = file_path.parent/f"{file_path.stem}.1{file_path.suffix}"
        self.assertTrue(filecmp.cmp(file_path, recovered_file_path, shallow=False))

    def test_recovered_folder_is_renamed_to_not_clobber_original_and_has_all_data(self) -> None:
        """Test that recovering a folder retrieves all data and doesn't overwrite user data."""
        create_user_data(self.user_path)
        default_backup(self.user_path, self.backup_path)
        folder_path = self.user_path/"sub_directory_1"
        recovery.recover_path(folder_path, self.backup_path, search=False, choice=0)
        recovered_folder_path = folder_path.parent/f"{folder_path.name}.1"
        self.assertTrue(directories_are_completely_copied(folder_path, recovered_folder_path))

    def test_file_to_be_recovered_can_be_chosen_from_menu(self) -> None:
        """Test that a file can be recovered after choosing from a list."""
        create_user_data(self.user_path)
        default_backup(self.user_path, self.backup_path)
        folder_path = self.user_path/"sub_directory_1"/"sub_sub_directory_1"
        chosen_file = recovery.search_backups(
            folder_path,
            self.backup_path,
            missing_only=False,
            operation="recovery",
            choice=1)
        self.assertIsNotNone(chosen_file)
        chosen_file = cast(Path, chosen_file)
        self.assertEqual(chosen_file, folder_path/"file_1.txt")
        recovery.recover_path(chosen_file, self.backup_path, search=False, choice=0)
        recovered_file_path = chosen_file.parent/f"{chosen_file.stem}.1{chosen_file.suffix}"
        self.assertTrue(filecmp.cmp(chosen_file, recovered_file_path, shallow=False))

    def test_missing_only_has_no_results_if_no_files_are_missing(self) -> None:
        """Test that if no user files are missing, --missing-only does not show a menu."""
        create_user_data(self.user_path)
        default_backup(self.user_path, self.backup_path)
        with self.assertLogs(level=logging.INFO) as logs:
            result = recovery.search_backups(
                self.user_path,
                self.backup_path,
                missing_only=True,
                operation="Recovery")
        self.assertIsNone(result)
        self.assertEqual(
            logs.output[-1],
            f"INFO:root:No backups found for the folder {self.user_path}")

    def test_missing_only_only_shows_missing_files_in_menu(self) -> None:
        """Test that the --missing-only option only shows missing files in menu."""
        create_user_data(self.user_path)
        default_backup(self.user_path, self.backup_path)
        missing_path = self.user_path/"sub_directory_2"/"sub_sub_directory_2"/"file_2.txt"
        missing_data = missing_path.read_text()
        missing_path.unlink()
        folder_path = missing_path.parent

        # No other choices beside the missing file
        with self.assertRaises(IndexError):
            chosen_file = recovery.search_backups(
                folder_path,
                self.backup_path,
                missing_only=True,
                operation="recovery",
                choice=1)

        chosen_file = recovery.search_backups(
            folder_path,
            self.backup_path,
            missing_only=True,
            operation="recovery",
            choice=0)
        self.assertIsNotNone(chosen_file)
        chosen_file = cast(Path, chosen_file)
        self.assertEqual(chosen_file, missing_path)
        recovery.recover_path(chosen_file, self.backup_path, search=False, choice=0)
        self.assertTrue(missing_path.is_file())
        recovered_data = missing_path.read_text()
        self.assertEqual(missing_data, recovered_data)

    def test_binary_search(self) -> None:
        """Test that sequences of older/newer choices result in the right backup."""
        create_user_data(self.user_path)
        for method in Invocation:
            self.reset_backup_folder()
            for _ in range(9):
                bak.create_new_backup(
                    self.user_path,
                    self.backup_path,
                    filter_file=None,
                    examine_whole_file=False,
                    force_copy=True,
                    copy_probability=0.0,
                    timestamp=unique_timestamp())

            sought_file = self.user_path/"root_file.txt"
            with self.assertLogs(level=logging.INFO) as logs:
                exit_code = run_recovery(
                    method,
                    self.backup_path,
                    sought_file,
                    choices="on",
                    search=True)
                self.assertEqual(exit_code, 0, method)

            backups = util.all_backups(self.backup_path)
            expected_backup_sequence = [backups[i] for i in [4, 2, 3]]
            current_recovery_index = 0
            log_prefix = "INFO:root:"
            for line in logs.output:
                if line.startswith(f"{log_prefix}Copying "):
                    self.assertIn(
                        str(expected_backup_sequence[current_recovery_index]),
                        line,
                        method)
                    recovered_file = (
                        sought_file.parent/
                        f"{sought_file.stem}.{current_recovery_index + 1}{sought_file.suffix}")
                    self.assertTrue(recovered_file.is_file(), (recovered_file, method))
                    recovered_file.unlink()
                    current_recovery_index += 1
            self.assertEqual(current_recovery_index, len(expected_backup_sequence), method)
            self.assertEqual(logs.output[-1], f"{log_prefix}Only one choice for recovery.", method)

    def test_recover_path_not_in_backups_logs_and_returns_normally(self) -> None:
        """Test that trying to recover a file not in backups prints message and returns."""
        create_user_data(self.user_path)
        default_backup(self.user_path, self.backup_path)
        new_file = self.user_path/"new_file.txt"
        new_file.touch()
        with self.assertLogs(level=logging.INFO) as logs:
            recovery.recover_path(new_file, self.backup_path, search=False, choice=0)
        self.assertEqual(logs.output, [f"INFO:root:No backups found for {new_file}"])

    def test_choose_target_from_backups_finds_all_user_files_in_backup(self) -> None:
        """The function choose_target_from_backups() can find all user files after backups."""
        create_user_data(self.user_path)
        default_backup(self.user_path, self.backup_path)
        for current_directory, directories, files in self.user_path.walk():
            all_paths = sorted(itertools.chain(files, directories))
            for choice, path in enumerate(all_paths, 0):
                arguments = [
                    "--backup-folder", str(self.backup_path),
                    "--list", str(current_directory),
                    "--choice", str(choice)]
                args = argparse.parse_command_line(arguments)
                found_path = recovery.choose_target_path_from_backups(args)
                self.assertEqual(current_directory/path, found_path)

    def test_choose_target_from_backups_finds_added_user_files(self) -> None:
        """The function choose_target_from_backups() can find all user files after backups."""
        create_user_data(self.user_path)
        default_backup(self.user_path, self.backup_path)
        (self.user_path/"extra_file.txt").touch(exist_ok=False)
        default_backup(self.user_path, self.backup_path)
        for current_directory, directories, files in self.user_path.walk():
            all_paths = sorted(itertools.chain(files, directories))
            for choice, path in enumerate(all_paths, 0):
                arguments = [
                    "--backup-folder", str(self.backup_path),
                    "--list", str(current_directory),
                    "--choice", str(choice)]
                args = argparse.parse_command_line(arguments)
                found_path = recovery.choose_target_path_from_backups(args)
                self.assertEqual(current_directory/path, found_path)

    def test_choose_target_from_backups_raises_error_for_non_existant_folder(self) -> None:
        """If a search directory does not exist, the function raises a CommandLineException."""
        create_user_data(self.user_path)
        default_backup(self.user_path, self.backup_path)
        bad_directory = self.user_path/"does-not-exist"
        arguments = [
            "--backup-folder", str(self.backup_path),
            "--list", str(bad_directory)]
        args = argparse.parse_command_line(arguments)
        with self.assertRaises(CommandLineError) as error:
            recovery.choose_target_path_from_backups(args)
        self.assertEqual(
            error.exception.args[0],
            f"The given search path is not a directory: {bad_directory}")

    def test_choose_target_from_backups_raises_error_for_non_folder(self) -> None:
        """If a search directory is not a directory, the function raises a CommandLineException."""
        create_user_data(self.user_path)
        default_backup(self.user_path, self.backup_path)
        bad_directory = self.user_path/"does-not-exist"
        bad_directory.touch()
        arguments = [
            "--backup-folder", str(self.backup_path),
            "--list", str(bad_directory)]
        args = argparse.parse_command_line(arguments)
        with self.assertRaises(CommandLineError) as error:
            recovery.choose_target_path_from_backups(args)
        self.assertEqual(
            error.exception.args[0],
            f"The given search path is not a directory: {bad_directory}")

    def test_choose_target_from_backups_returns_none_and_logs_for_empty_folder(self) -> None:
        """If nothing is found for a search directory, the function returns None and logs."""
        create_user_data(self.user_path)
        bad_directory = self.user_path/"does-not-exist"
        bad_directory.mkdir()
        default_backup(self.user_path, self.backup_path)
        arguments = [
            "--backup-folder", str(self.backup_path),
            "--list", str(bad_directory)]
        args = argparse.parse_command_line(arguments)
        with self.assertLogs(level=logging.INFO) as logs:
            found_path = recovery.choose_target_path_from_backups(args)
        self.assertIsNone(found_path)
        self.assertEqual(
            logs.output[-1],
            f"INFO:root:No backups found for the folder {bad_directory}")

    def test_search_backups_finds_all_user_files_in_backup(self) -> None:
        """The function search_backups() can find all user files after backups."""
        create_user_data(self.user_path)
        default_backup(self.user_path, self.backup_path)
        for current_directory, directories, files in self.user_path.walk():
            all_paths = sorted(itertools.chain(files, directories))
            for choice, path in enumerate(all_paths, 0):
                found_path = recovery.search_backups(
                    current_directory,
                    self.backup_path,
                    missing_only=False,
                    operation="",
                    choice=choice)
                self.assertEqual(current_directory/path, found_path)

    def test_search_backups_finds_added_user_files(self) -> None:
        """The function search_backups() can find all user files after backups."""
        create_user_data(self.user_path)
        default_backup(self.user_path, self.backup_path)
        (self.user_path/"extra_file.txt").touch(exist_ok=False)
        default_backup(self.user_path, self.backup_path)
        for current_directory, directories, files in self.user_path.walk():
            all_paths = sorted(itertools.chain(files, directories))
            for choice, path in enumerate(all_paths, 0):
                found_path = recovery.search_backups(
                    current_directory,
                    self.backup_path,
                    missing_only=False,
                    operation="",
                    choice=choice)
                self.assertEqual(current_directory/path, found_path)

    def test_search_backups_raises_error_for_non_existant_folder(self) -> None:
        """If a search directory does not exist, the function raises a CommandLineException."""
        create_user_data(self.user_path)
        default_backup(self.user_path, self.backup_path)
        bad_directory = self.user_path/"does-not-exist"
        with self.assertRaises(CommandLineError) as error:
            recovery.search_backups(
                bad_directory,
                self.backup_path,
                missing_only=False,
                operation="")
        self.assertEqual(
            error.exception.args[0],
            f"The given search path is not a directory: {bad_directory}")

    def test_search_backups_raises_error_for_non_folder(self) -> None:
        """If a search directory is not a directory, the function raises a CommandLineException."""
        create_user_data(self.user_path)
        default_backup(self.user_path, self.backup_path)
        bad_directory = self.user_path/"does-not-exist"
        bad_directory.touch()
        with self.assertRaises(CommandLineError) as error:
            recovery.search_backups(
                bad_directory,
                self.backup_path,
                missing_only=False,
                operation="")
        self.assertEqual(
            error.exception.args[0],
            f"The given search path is not a directory: {bad_directory}")

    def test_search_backups_returns_none_and_logs_for_empty_folder(self) -> None:
        """If nothing is found for a search directory, the function returns None and logs."""
        create_user_data(self.user_path)
        bad_directory = self.user_path/"does-not-exist"
        bad_directory.mkdir()
        default_backup(self.user_path, self.backup_path)
        with self.assertLogs(level=logging.INFO) as logs:
            found_path = recovery.search_backups(
                bad_directory,
                self.backup_path,
                missing_only=False,
                operation="")
        self.assertIsNone(found_path)
        self.assertEqual(
            logs.output[-1],
            f"INFO:root:No backups found for the folder {bad_directory}")

    def test_choose_recovery_target_from_backups_finds_all_user_files_in_backup(self) -> None:
        """The function choose_recovery_target_from_backups() can find user files after backups."""
        create_user_data(self.user_path)
        default_backup(self.user_path, self.backup_path)
        for current_directory, directories, files in self.user_path.walk():
            all_paths = sorted(itertools.chain(files, directories))
            for choice, path in enumerate(all_paths, 0):
                arguments = [
                    "--backup-folder", str(self.backup_path),
                    "--list", str(current_directory),
                    "--choice", f"0{choice}"]
                args = argparse.parse_command_line(arguments)
                expected_path = fs.unique_path_name(current_directory/path)
                recovery.choose_recovery_target_from_backups(args)
                self.assertTrue(expected_path.exists())

    def test_choose_recovery_target_from_backups_finds_added_user_files(self) -> None:
        """The function choose_recovery_target_from_backups() can find user files after backups."""
        create_user_data(self.user_path)
        default_backup(self.user_path, self.backup_path)
        (self.user_path/"extra_file.txt").touch(exist_ok=False)
        default_backup(self.user_path, self.backup_path)
        for current_directory, directories, files in self.user_path.walk():
            all_paths = sorted(itertools.chain(files, directories))
            for choice, path in enumerate(all_paths, 0):
                arguments = [
                    "--backup-folder", str(self.backup_path),
                    "--list", str(current_directory),
                    "--choice", f"0{choice}"]
                args = argparse.parse_command_line(arguments)
                expected_path = fs.unique_path_name(current_directory/path)
                recovery.choose_recovery_target_from_backups(args)
                self.assertTrue(expected_path.exists())

    def test_choose_recovery_target_from_backups_raises_error_for_non_existant_folder(self) -> None:
        """If a search directory does not exist, the function raises a CommandLineException."""
        create_user_data(self.user_path)
        default_backup(self.user_path, self.backup_path)
        bad_directory = self.user_path/"does-not-exist"
        arguments = [
            "--backup-folder", str(self.backup_path),
            "--list", str(bad_directory)]
        args = argparse.parse_command_line(arguments)
        with self.assertRaises(CommandLineError) as error:
            recovery.choose_recovery_target_from_backups(args)
        self.assertEqual(
            error.exception.args[0],
            f"The given search path is not a directory: {bad_directory}")

    def test_choose_recovery_target_from_backups_raises_error_for_non_folder(self) -> None:
        """If a search directory is not a directory, the function raises a CommandLineException."""
        create_user_data(self.user_path)
        default_backup(self.user_path, self.backup_path)
        bad_directory = self.user_path/"does-not-exist"
        bad_directory.touch()
        arguments = [
            "--backup-folder", str(self.backup_path),
            "--list", str(bad_directory)]
        args = argparse.parse_command_line(arguments)
        with self.assertRaises(CommandLineError) as error:
            recovery.choose_recovery_target_from_backups(args)
        self.assertEqual(
            error.exception.args[0],
            f"The given search path is not a directory: {bad_directory}")

    def test_choose_recovery_target_from_backups_logs_for_empty_folder(self) -> None:
        """If nothing is found for a search directory, the function returns None and logs."""
        create_user_data(self.user_path)
        bad_directory = self.user_path/"does-not-exist"
        bad_directory.mkdir()
        default_backup(self.user_path, self.backup_path)
        arguments = [
            "--backup-folder", str(self.backup_path),
            "--list", str(bad_directory)]
        args = argparse.parse_command_line(arguments)
        with self.assertLogs(level=logging.INFO) as logs:
            recovery.choose_recovery_target_from_backups(args)
        self.assertEqual(
            logs.output[-1],
            f"INFO:root:No backups found for the folder {bad_directory}")


def create_large_files(base_folder: Path, file_size: int) -> None:
    """Create a file of a give size in every leaf subdirectory."""
    data = "A"*file_size
    for directory_name, sub_directory_names, _ in base_folder.walk():
        if not sub_directory_names:
            (directory_name/"file.txt").write_text(data, encoding="utf8")


class DeleteBackupTests(TestCaseWithTemporaryFilesAndFolders):
    """Test deleting backups."""

    def test_deleting_single_backup(self) -> None:
        """Test deleting only the most recent backup."""
        create_old_monthly_backups(self.backup_path, 10)
        backups = util.all_backups(self.backup_path)
        fs.delete_directory_tree(backups[0])
        expected_remaining_backups = backups[1:]
        all_backups_left = util.all_backups(self.backup_path)
        self.assertEqual(expected_remaining_backups, all_backups_left)

    def test_deleting_backup_with_read_only_file(self) -> None:
        """Test deleting a backup containing a readonly file."""
        create_user_data(self.user_path)
        (self.user_path/"sub_directory_1"/"sub_sub_directory_1"/"file_1.txt").chmod(stat.S_IRUSR)
        default_backup(self.user_path, self.backup_path)
        backups = util.all_backups(self.backup_path)
        self.assertEqual(len(backups), 1)

        fs.delete_directory_tree(backups[0])
        backup_count_after = len(util.all_backups(self.backup_path))
        self.assertEqual(backup_count_after, 0)

    def test_deleting_backup_with_read_only_folder(self) -> None:
        """Test deleting a backup containing a readonly file."""
        create_user_data(self.user_path)
        read_only_folder = self.user_path/"sub_directory_1"/"sub_sub_directory_1"
        read_only = stat.S_IRUSR | stat.S_IXUSR
        read_only_folder.chmod(read_only)

        default_backup(self.user_path, self.backup_path)
        backups = util.all_backups(self.backup_path)
        self.assertEqual(len(backups), 1)

        fs.delete_directory_tree(backups[0])
        backup_count_after = len(util.all_backups(self.backup_path))
        self.assertEqual(backup_count_after, 0)

        # Restore write access to folder so it can be deleted in self.tearDown()
        read_only_folder.chmod(read_only | stat.S_IWUSR)

    def test_free_up_option_with_absolute_size_deletes_backups_to_free_storage_space(self) -> None:
        """Test deleting backups until there is a given amount of free space."""
        for method in Invocation:
            backups_created = 30
            create_old_monthly_backups(self.backup_path, backups_created)
            file_size = 10_000_000
            create_large_files(self.backup_path, file_size)
            backups_after_deletion = 10
            size_of_deleted_backups = (backups_created - backups_after_deletion)*file_size
            after_backup_space = shutil.disk_usage(self.backup_path).free
            goal_space = after_backup_space + size_of_deleted_backups - file_size/2
            goal_space_str = f"{goal_space}B"
            if method == Invocation.function:
                deletion.delete_oldest_backups_for_space(self.backup_path, goal_space_str, None)
            elif method == Invocation.cli:
                create_large_files(self.user_path, file_size)
                exit_code = main_assert_no_error_log([
                    "--user-folder", str(self.user_path),
                    "--backup-folder", str(self.backup_path),
                    "--free-up", goal_space_str,
                    "--timestamp", unique_timestamp_string()],
                    self)
                self.assertEqual(exit_code, 0, method)

                # While backups are being deleted, the fake user data still exists, so one more
                # backup needs to be deleted to free up the required space.
                backups_after_deletion -= 1
            else:
                raise NotImplementedError(f"Delete backup test not implemented for {method}")
            backups_left = len(util.all_backups(self.backup_path))
            self.assertIn(backups_left - backups_after_deletion, [0, 1], method)

            self.reset_backup_folder()

    def test_max_deletions_limits_the_number_of_backup_deletions(self) -> None:
        """Test that no more than the maximum number of backups are deleted when freeing space."""
        backups_created = 30
        create_old_monthly_backups(self.backup_path, backups_created)
        file_size = 10_000_000
        create_large_files(self.backup_path, file_size)
        backups_after_deletion = 10
        size_of_deleted_backups = (backups_created - backups_after_deletion)*file_size
        after_backup_space = shutil.disk_usage(self.backup_path).free
        goal_space = after_backup_space + size_of_deleted_backups - file_size/2
        goal_space_str = f"{goal_space}B"
        maximum_deletions = 5
        expected_backups_count = backups_created - maximum_deletions
        with self.assertLogs(level=logging.INFO) as log_check:
            deletion.delete_oldest_backups_for_space(
                self.backup_path,
                goal_space_str,
                None,
                expected_backups_count)
        self.assertIn(
            "INFO:root:Stopped after reaching maximum number of deletions.",
            log_check.output)
        all_backups_after_deletion = util.all_backups(self.backup_path)
        self.assertEqual(len(all_backups_after_deletion), expected_backups_count)

    def test_delete_after_deletes_all_backups_prior_to_given_date(self) -> None:
        """Test that backups older than a given date can be deleted with --delete-after."""
        for method in Invocation:
            create_old_monthly_backups(self.backup_path, 30)
            max_age = "1y"
            now = datetime.datetime.now()
            earliest_backup = datetime.datetime.combine(
                dates.fix_end_of_month(now.year - 1, now.month, now.day),
                datetime.time(now.hour, now.minute, now.second, now.microsecond))
            if method == Invocation.function:
                deletion.delete_backups_older_than(self.backup_path, max_age, None)
            elif method == Invocation.cli:
                exit_code = main_assert_no_error_log([
                    "--user-folder", str(self.user_path),
                    "--backup-folder", str(self.backup_path),
                    "--delete-after", max_age,
                    "--delete-only",
                    "--timestamp", unique_timestamp_string()],
                    self)
                self.assertEqual(exit_code, 0, method)
            else:
                raise NotImplementedError(f"Delete backup test not implemented for {method}")
            backups = util.all_backups(self.backup_path)
            self.assertEqual(len(backups), 12, method)
            self.assertLessEqual(earliest_backup, util.backup_datetime(backups[0]), method)

            self.reset_backup_folder()

    def test_max_deletions_limits_deletions_with_delete_after(self) -> None:
        """Test that --max-deletions limits backups deletions when using --delete-after."""
        backups_created = 30
        create_old_monthly_backups(self.backup_path, backups_created)
        max_age = "1y"
        max_deletions = 10
        expected_backup_count = backups_created - max_deletions
        with self.assertLogs(level=logging.INFO) as log_check:
            deletion.delete_backups_older_than(
                self.backup_path,
                max_age,
                None,
                expected_backup_count)
        self.assertIn(
            "INFO:root:Stopped after reaching maximum number of deletions.",
            log_check.output)
        backups_left = util.all_backups(self.backup_path)
        self.assertEqual(len(backups_left), expected_backup_count)

    def test_delete_after_never_deletes_most_recent_backup(self) -> None:
        """Test that deleting all backups with --delete_after actually leaves the last one."""
        create_old_monthly_backups(self.backup_path, 30)
        most_recent_backup = moving.last_n_backups(1, self.backup_path)[0]
        last_backup = moving.last_n_backups(2, self.backup_path)[0]
        fs.delete_directory_tree(most_recent_backup)
        deletion.delete_backups_older_than(self.backup_path, "1d", None)
        self.assertEqual(util.all_backups(self.backup_path), [last_backup])

    def test_delete_after_deletes_too_old_backups_before_new_backup(self) -> None:
        """Test that backups older than a given date can be deleted with --delete-after."""
        create_old_monthly_backups(self.backup_path, 30)
        max_age = "1y"
        now = datetime.datetime.now()
        earliest_backup = datetime.datetime(
            now.year - 1, now.month, now.day,
            now.hour, now.minute, now.second, now.microsecond)
        create_user_data(self.user_path)
        most_recent_backup = moving.last_n_backups(1, self.backup_path)[0]
        fs.delete_directory_tree(most_recent_backup)
        with self.assertLogs(level=logging.INFO) as logs:
            exit_code = main.main([
                "-u", str(self.user_path),
                "-b", str(self.backup_path),
                "--delete-after", max_age,
                "--log", os.devnull],
                testing=True)
        self.assertEqual(exit_code, 0)
        backups = util.all_backups(self.backup_path)
        self.assertEqual(len(backups), 12)
        self.assertLessEqual(earliest_backup, util.backup_datetime(backups[0]))

        backups_deleted = False
        created_backup = False
        for line in logs.output:
            self.assertTrue(line.startswith("INFO:root:"))
            if line.startswith("INFO:root:Deleting oldest backup"):
                self.assertFalse(created_backup)
                backups_deleted = True

            if line == "INFO:root:Running backup ...":
                self.assertTrue(backups_deleted)
                created_backup = True

        self.assertTrue(backups_deleted)
        self.assertTrue(created_backup)

    def test_free_up_never_deletes_most_recent_backup(self) -> None:
        """Test that deleting all backups with --free-up actually leaves the last one."""
        create_old_monthly_backups(self.backup_path, 30)
        last_backup = moving.last_n_backups(1, self.backup_path)[0]
        total_space = shutil.disk_usage(self.backup_path).total
        deletion.delete_oldest_backups_for_space(self.backup_path, f"{total_space}B", None)
        self.assertEqual(util.all_backups(self.backup_path), [last_backup])

    def test_attempt_to_free_more_space_than_capacity_of_backup_location_is_an_error(self) -> None:
        """Test that error is thrown when trying to free too much space."""
        max_space = shutil.disk_usage(self.backup_path).total
        too_much_space = 2*max_space
        with self.assertRaises(CommandLineError):
            deletion.delete_oldest_backups_for_space(self.backup_path, f"{too_much_space}B", None)

    def test_deleting_last_backup_in_year_folder_deletes_year_folder(self) -> None:
        """Test that deleting a backup leaves a year folder empty, that year folder is deleted."""
        today = datetime.date.today()
        create_old_monthly_backups(self.backup_path, today.month + 1)
        oldest_backup_year_folder = self.backup_path/f"{today.year - 1}"
        self.assertTrue(oldest_backup_year_folder.is_dir())
        self.assertEqual(len(list(oldest_backup_year_folder.iterdir())), 1)
        deletion.delete_backups_older_than(self.backup_path, f"{today.month}m", None)
        self.assertFalse(oldest_backup_year_folder.is_dir())
        this_year_backup_folder = self.backup_path/f"{today.year}"
        self.assertIsNotNone(this_year_backup_folder)

    def test_delete_only_command_line_option(self) -> None:
        """Test that --delete-only deletes backups without running a backup."""
        create_old_monthly_backups(self.backup_path, 30)
        oldest_backup_age = datetime.timedelta(days=120)
        arguments = [
            "--backup-folder", str(self.backup_path),
            "--delete-after", f"{oldest_backup_age.days}d",
            "--delete-only"]
        exit_code = main_assert_no_error_log(arguments, self)
        self.assertEqual(exit_code, 0)
        backups = util.all_backups(self.backup_path)
        self.assertEqual(len(backups), 4)  # 120 days = 4 months
        now = datetime.datetime.now()
        earliest_backup_timestamp = util.backup_datetime(backups[0])
        self.assertLessEqual(now - earliest_backup_timestamp, oldest_backup_age)

    def test_keep_weekly_after_only_retains_weekly_backups_after_time_span(self) -> None:
        """After the given time span, every backup is at least a week apart."""
        create_old_daily_backups(self.backup_path, 30)
        time_span_keep_all_backups = "2w"
        backups = util.all_backups(self.backup_path)
        expected_indexes_remaining = [
            0, 7, 14, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29]
        expected_backups_remaining = [backups[i] for i in expected_indexes_remaining]
        main_assert_no_error_log([
            "--keep-weekly-after", time_span_keep_all_backups,
            "--delete-only",
            "--backup-folder", str(self.backup_path)],
            self)
        backups_remaining = util.all_backups(self.backup_path)
        self.assertEqual(backups_remaining, expected_backups_remaining)

    def test_keep_monthly_after_only_retains_monthly_backups_after_time_span(self) -> None:
        """After the given time span, every backup is at least a calendar month apart."""
        def days_in_month(year: int, month: int) -> int:
            return dates.fix_end_of_month(year, month, 31).day

        create_old_daily_backups(self.backup_path, 33)
        time_span_to_keep_all_backups = "1d"
        backups = util.all_backups(self.backup_path)
        first_backup_timestamp = util.backup_datetime(backups[0])
        first_retained_index = days_in_month(
            first_backup_timestamp.year,
            first_backup_timestamp.month)
        expected_backups_remaining = [backups[0], backups[first_retained_index], backups[-1]]

        main_assert_no_error_log([
            "--keep-monthly-after", time_span_to_keep_all_backups,
            "--delete-only",
            "--backup-folder", str(self.backup_path)],
            self)
        backups_remaining = util.all_backups(self.backup_path)
        self.assertEqual(backups_remaining, expected_backups_remaining)

    def test_keep_yearly_after_only_retains_yearly_backups_after_time_span(self) -> None:
        """After the given time span, every backup is at least a calendar year apart."""
        create_old_monthly_backups(self.backup_path, 27)
        time_span_to_keep_all_backups = "2w"
        backups = util.all_backups(self.backup_path)
        expected_backups_remaining = [backups[0], backups[12], backups[24], backups[-1]]

        main_assert_no_error_log([
            "--keep-yearly-after", time_span_to_keep_all_backups,
            "--delete-only",
            "--backup-folder", str(self.backup_path)],
            self)
        backups_remaining = util.all_backups(self.backup_path)
        self.assertEqual(backups_remaining, expected_backups_remaining)

    def test_incorrect_keep_x_after_parameters_raise_exceptions(self) -> None:
        """Test that less frequent backup retention specs having smaller time spans is an error."""
        with self.assertRaises(CommandLineError) as error:
            args = [
                "--keep-weekly-after", "2m",
                "--keep-monthly-after", "1m",
                "--delete-only",
                "--backup-folder", str(self.backup_path)]
            arguments = argparse.parse_command_line(args)
            deletion.check_time_span_parameters(arguments)
        error_message = (
                "The monthly time span (1m) is not longer than the weekly time span (2m). "
                "Less frequent backup specs must have longer time spans.")
        self.assertEqual(error.exception.args, (error_message,))

        with self.assertRaises(CommandLineError) as error:
            args = [
                "--keep-weekly-after", "100d",
                "--keep-yearly-after", "2m",
                "--delete-only",
                "--backup-folder", str(self.backup_path)]
            arguments = argparse.parse_command_line(args)
            deletion.check_time_span_parameters(arguments)
        error_message = (
                "The yearly time span (2m) is not longer than the weekly time span (100d). "
                "Less frequent backup specs must have longer time spans.")
        self.assertEqual(error.exception.args, (error_message,))

        with self.assertRaises(CommandLineError) as error:
            args = [
                "--keep-monthly-after", "60w",
                "--keep-yearly-after", "1y",
                "--delete-only",
                "--backup-folder", str(self.backup_path)]
            arguments = argparse.parse_command_line(args)
            deletion.check_time_span_parameters(arguments)
        error_message = (
                "The yearly time span (1y) is not longer than the monthly time span (60w). "
                "Less frequent backup specs must have longer time spans.")
        self.assertEqual(error.exception.args, (error_message,))

    def test_using_all_keep_x_after_options_is_not_an_error(self) -> None:
        """Test that use all --keep-x-after options with suitable time spans is not an error."""
        args = argparse.parse_command_line([
            "--keep-weekly-after", "1d",
            "--keep-monthly-after", "2d",
            "--keep-yearly-after", "3d",
            "--backup-folder", str(self.backup_path)])
        deletion.check_time_span_parameters(args)

    def test_keep_x_after_respects_maximum_deletions(self) -> None:
        """Make sure all --keep-x-after options respect --max-deletions."""
        max_deletions = 50
        for option in ("weekly", "monthly", "yearly"):
            create_old_daily_backups(self.backup_path, 100)
            starting_backup_count = len(util.all_backups(self.backup_path))
            main_assert_no_error_log([
                "--backup-folder", str(self.backup_path),
                f"--keep-{option}-after", "1d",
                "--delete-only",
                "--max-deletions", str(max_deletions)],
                self)
            retained_backup_count = len(util.all_backups(self.backup_path))
            self.assertEqual(starting_backup_count - retained_backup_count, 50)
            self.reset_backup_folder()


class MoveBackupsTests(TestCaseWithTemporaryFilesAndFolders):
    """Test moving backup sets to a different location."""

    def test_moving_all_backups_preserves_structure_and_hardlinks_of_original(self) -> None:
        """Test that moving backups preserves the names and hardlinks of the original."""
        create_user_data(self.user_path)
        backup_count = 10
        for _ in range(backup_count):
            default_backup(self.user_path, self.backup_path)

        for method in Invocation:
            with tempfile.TemporaryDirectory() as new_backup_folder:
                new_backup_location = Path(new_backup_folder)
                if method == Invocation.function:
                    backups_to_move = util.all_backups(self.backup_path)
                    self.assertEqual(len(backups_to_move), backup_count, method)
                    moving.move_backups(
                        self.backup_path,
                        new_backup_location,
                        backups_to_move)
                elif method == Invocation.cli:
                    exit_code = main_assert_no_error_log([
                        "--backup-folder", str(self.backup_path),
                        "--move-backup", new_backup_folder,
                        "--move-count", "all"],
                        self)
                    self.assertEqual(exit_code, 0, method)
                else:
                    raise NotImplementedError(f"Move backup test not implemented for {method}.")

                self.assertTrue(
                    directories_are_completely_copied(self.backup_path, new_backup_location),
                    method)
                self.assertEqual(
                    backup_info.backup_source(self.backup_path),
                    backup_info.backup_source(new_backup_location),
                    method)

                original_backups = util.all_backups(self.backup_path)
                original_names = [p.relative_to(self.backup_path) for p in original_backups]
                moved_backups = util.all_backups(new_backup_location)
                moved_names = [p.relative_to(new_backup_location) for p in moved_backups]
                self.assertEqual(original_names, moved_names, method)
                for backup_1, backup_2 in itertools.pairwise(moved_backups):
                    self.assertTrue(
                        directories_are_completely_hardlinked(backup_1, backup_2),
                        method)

    def test_move_n_backups_moves_subset_and_preserves_structure_and_hardlinks(self) -> None:
        """Test that moving N backups moves correct number of backups and correctly links files."""
        create_user_data(self.user_path)
        for _ in range(10):
            default_backup(self.user_path, self.backup_path)

        move_count = 5
        for method in Invocation:
            with tempfile.TemporaryDirectory() as new_backup_folder:
                new_backup_location = Path(new_backup_folder)
                if method == Invocation.function:
                    backups_to_move = moving.last_n_backups(move_count, self.backup_path)
                    self.assertEqual(len(backups_to_move), move_count, method)
                    moving.move_backups(
                        self.backup_path,
                        new_backup_location,
                        backups_to_move)
                elif method == Invocation.cli:
                    exit_code = main_assert_no_error_log([
                        "--backup-folder", str(self.backup_path),
                        "--move-backup", new_backup_folder,
                        "--move-count", str(move_count)],
                        self)
                    self.assertEqual(exit_code, 0, method)
                else:
                    raise NotImplementedError(f"Move backup test not implemented for {method}")

                backups_at_new_location = util.all_backups(new_backup_location)
                self.assertEqual(len(backups_at_new_location), move_count, method)
                old_backups = moving.last_n_backups(move_count, self.backup_path)
                old_backup_names = [p.relative_to(self.backup_path) for p in old_backups]
                new_backups = util.all_backups(new_backup_location)
                new_backup_names = [p.relative_to(new_backup_location) for p in new_backups]
                self.assertEqual(old_backup_names, new_backup_names, method)
                self.assertEqual(
                    backup_info.backup_source(self.backup_path),
                    backup_info.backup_source(new_backup_location),
                    method)
                for backup_1, backup_2 in itertools.pairwise(new_backups):
                    self.assertTrue(
                        directories_are_completely_hardlinked(backup_1, backup_2),
                        method)

    def test_move_age_backups_moves_only_backups_within_given_timespan(self) -> None:
        """Test that moving backups based on a time span works."""
        create_old_monthly_backups(self.backup_path, 25)
        six_months_ago = dates.parse_time_span_to_timepoint("6m")
        backups_to_move = moving.backups_since(six_months_ago, self.backup_path)
        self.assertEqual(len(backups_to_move), 6)
        self.assertEqual(moving.last_n_backups(6, self.backup_path), backups_to_move)
        oldest_backup_timestamp = util.backup_datetime(backups_to_move[0])
        self.assertLessEqual(six_months_ago, oldest_backup_timestamp)

    def test_move_without_specifying_how_many_to_move_is_an_error(self) -> None:
        """Test that missing --move-count, --move-age, and --move-since results in an error."""
        create_user_data(self.user_path)
        default_backup(self.user_path, self.backup_path)
        with (self.assertLogs(level=logging.ERROR) as no_move_choice_log,
            tempfile.TemporaryDirectory() as move_destination):

            exit_code = main_no_log([
                "--move-backup", move_destination,
                "--user-folder", str(self.user_path),
                "--backup-folder", str(self.backup_path)])
        self.assertEqual(exit_code, 1)
        expected_logs = [
            ("ERROR:root:Exactly one of the following is required: "
             "--move-count, --move-age, or --move-since")]
        self.assertEqual(expected_logs, no_move_choice_log.output)

    def test_move_age_argument_selects_correct_backups(self) -> None:
        """Test that --move-age argument selects the correct backups."""
        create_old_monthly_backups(self.backup_path, 12)
        args = argparse.parse_command_line(["--move-age", "100d"])
        backups = moving.choose_backups_to_move(args, self.backup_path)
        expected_backup_count = 4
        self.assertEqual(len(backups), expected_backup_count)
        expected_backups = util.all_backups(self.backup_path)[-expected_backup_count:]
        self.assertEqual(backups, expected_backups)

    def test_move_since_argument_selects_correct_backups(self) -> None:
        """Test that --move-age argument selects the correct backups."""
        create_user_data(self.user_path)
        for day in range(1, 32):
            backup_date = datetime.datetime(2025, 8, day, 2, 0, 0)
            bak.create_new_backup(
                self.user_path,
                self.backup_path,
                filter_file=None,
                examine_whole_file=False,
                force_copy=False,
                copy_probability=0.0,
                timestamp=backup_date)
        args = argparse.parse_command_line(["--move-since", "2025-08-15"])
        backups = moving.choose_backups_to_move(args, self.backup_path)
        expected_backup_count = 17  # Aug. 15 to Aug. 31
        self.assertEqual(len(backups), expected_backup_count)
        expected_backups = util.all_backups(self.backup_path)[-expected_backup_count:]
        self.assertEqual(backups, expected_backups)


def read_paths_file(verify_file: TextIO) -> set[Path]:
    """
    Read an opened verification file and return the path contents.

    Arguments:
        verify_file: A stream from opening a file in text mode

    Returns:
        path_set: A set of paths read from the file
    """
    files_from_verify: set[Path] = set()
    current_directory: Path | None = None
    for line in verify_file:
        if os.sep in line:
            current_directory = Path(line.removesuffix("\n"))
        else:
            if not current_directory:
                raise ValueError("File names must be preceded by a directory path.")
            file_name = line.removeprefix("    ").removesuffix("\n")
            files_from_verify.add(current_directory/file_name)
    return files_from_verify


class VerificationTests(TestCaseWithTemporaryFilesAndFolders):
    """Test backup verification."""

    def test_backup_verification_sorts_files_into_matching_mismatching_and_errors(self) -> None:
        """Test that verification sorts files into matching, mismatching, and error lists."""
        create_user_data(self.user_path)
        default_backup(self.user_path, self.backup_path)

        mismatch_file = self.user_path/"sub_directory_1"/"sub_sub_directory_2"/"file_0.txt"
        with mismatch_file.open("a", encoding="utf8") as file:
            file.write("\naddition\n")

        error_file = self.user_path/"sub_directory_2"/"sub_sub_directory_0"/"file_1.txt"
        last_backup = util.find_previous_backup(self.backup_path)
        self.assertIsNotNone(last_backup)
        last_backup = cast(Path, last_backup)
        (last_backup/error_file.relative_to(self.user_path)).unlink()

        matching_path_set: set[Path] = set()
        mismatching_path_set: set[Path] = set()
        error_path_set: set[Path] = set()
        user_paths = backup_set.Backup_Set(self.user_path, None)
        for directory, file_names in user_paths:
            for file_name in file_names:
                path = directory/file_name
                path_set = (
                    mismatching_path_set if path == mismatch_file
                    else error_path_set if path == error_file
                    else matching_path_set)
                path_set.add(path)

        for method in Invocation:
            with tempfile.TemporaryDirectory() as verification_folder:
                verification_location = Path(verification_folder)
                if method == Invocation.function:
                    verify.verify_last_backup(verification_location, self.backup_path, None)
                else:
                    exit_code = main_assert_no_error_log([
                        "--user-folder", str(self.user_path),
                        "--backup-folder", str(self.backup_path),
                        "--verify", verification_folder],
                        self)
                    self.assertEqual(exit_code, 0, method)

                verify_files = {p.name for p in verification_location.iterdir()}
                expected_files = {"matching files.txt", "mismatching files.txt", "error files.txt"}
                self.assertEqual(verify_files, expected_files)
                for file_name in verify_files:
                    path_set = (
                        matching_path_set if file_name.startswith("matching ")
                        else mismatching_path_set if file_name.startswith("mismatching ")
                        else error_path_set)

                    with (verification_location/file_name).open(encoding="utf8") as verify_file:
                        first_line = verify_file.readline()
                        first_line_format = "Comparison: (.*) <---> (.*)\n"
                        matches = cast(re.Match[str], re.match(first_line_format, first_line))
                        user_folder, backup_folder = matches.groups()
                        self.assertTrue(self.user_path.samefile(user_folder))
                        self.assertTrue(self.backup_path.samefile(backup_folder))
                        files_from_verify = read_paths_file(verify_file)
                        self.assertEqual(files_from_verify, path_set)

    def test_verification_files_do_not_overwrite_existing_files(self) -> None:
        """Make sure that the verifying function does not clobber existing files."""
        create_user_data(self.user_path)
        default_backup(self.user_path, self.backup_path)

        file_names = ("matching files.txt", "mismatching files.txt", "error files.txt")
        for file_name in file_names:
            (self.user_path/file_name).touch()

        verify.verify_last_backup(self.user_path, self.backup_path, None)

        for file_name in file_names:
            fake_verify_file = self.user_path/file_name
            self.assertEqual(fake_verify_file.read_text(), "")
            actual_verify_file = fake_verify_file.with_suffix(f".1{fake_verify_file.suffix}")
            self.assertTrue(actual_verify_file.is_file(follow_symlinks=False))

    def test_verification_with_no_backups_raises_error(self) -> None:
        """Test that verification raises an error when there are no backups."""
        with self.assertRaises(CommandLineError) as error:
            verify.verify_last_backup(self.user_path, self.backup_path, None)
        self.assertTrue(error.exception.args[0].startswith("No backups found in "))

    def test_verification_with_missing_user_folder_raises_error(self) -> None:
        """Test that verification with missing (renamed) user folder raises error."""
        create_user_data(self.user_path)
        default_backup(self.user_path, self.backup_path)

        self.reset_user_folder()
        with self.assertRaises(CommandLineError) as error:
            verify.verify_last_backup(self.user_path, self.backup_path, None)
        self.assertTrue(error.exception.args[0].startswith("Could not find user folder: "))

    def test_verification_with_empty_backups_raises_error(self) -> None:
        """Test that verification with empty backup folder raises error."""
        create_user_data(self.user_path)
        default_backup(self.user_path, self.backup_path)

        last_backup = util.find_previous_backup(self.backup_path)
        self.assertIsNotNone(last_backup)
        last_backup = cast(Path, last_backup)
        fs.delete_directory_tree(last_backup)
        with self.assertRaises(CommandLineError) as error:
            verify.verify_last_backup(self.user_path, self.backup_path, None)
        self.assertTrue(error.exception.args[0].startswith("No backups found in "))

    def test_checksum_verification(self) -> None:
        """Test that checksums are written and read consistently."""
        create_user_data(self.user_path)
        default_backup(self.user_path, self.backup_path)

        last_backup = util.find_previous_backup(self.backup_path)
        self.assertIsNotNone(last_backup)
        last_backup = cast(Path, last_backup)
        backed_up_files = directory_contents(last_backup)

        verify.create_checksum_for_last_backup(self.backup_path)
        checksum_path = last_backup/verify.checksum_file_name
        with checksum_path.open(encoding="utf8") as checksum_data:
            for line in checksum_data:
                path, digest = line.rstrip("\n").rsplit(" ", maxsplit=1)
                relative_path = Path(path)
                backup_path = last_backup/path
                with backup_path.open("rb") as backup_file_data:
                    backed_up_hash = hashlib.file_digest(
                        backup_file_data,
                        verify.hash_function).hexdigest()
                    self.assertEqual(digest, backed_up_hash, f"{path}, i.e., {backup_path}")
                    self.assertIn(relative_path, backed_up_files)
                    backed_up_files.remove(relative_path)

        for remaining in backed_up_files:
            self.assertTrue(fs.is_real_directory(last_backup/remaining), remaining)

    def test_checksums_of_data_and_backup_match(self) -> None:
        """Test that the checksums of backups match the checksums of the original data."""
        create_user_data(self.user_path)
        default_backup(self.user_path, self.backup_path)

        last_backup = util.find_previous_backup(self.backup_path)
        self.assertIsNotNone(last_backup)
        last_backup = cast(Path, last_backup)

        verify.create_checksum_for_last_backup(self.backup_path)
        backup_checksum_file = last_backup/verify.checksum_file_name
        backup_checksums = backup_checksum_file.read_text(encoding="utf8").splitlines()
        backup_checksums.sort()

        verify.create_checksum_for_folder(self.user_path)
        user_checksum_file = self.user_path/verify.checksum_file_name
        user_checksums = user_checksum_file.read_text(encoding="utf8").splitlines()
        user_checksums.sort()

        self.assertEqual(backup_checksums, user_checksums)

    def test_no_checksum_date_is_found_if_no_checksum_performed(self) -> None:
        """Test that no checksum date is found if no checksumming occurred."""
        create_user_data(self.user_path)
        default_backup(self.user_path, self.backup_path)
        self.assertIsNone(verify.last_checksum(self.backup_path))

    def test_checksum_with_no_backups_is_error(self) -> None:
        """Test that calling create_checksum_for_last_backup() with no backups is an error."""
        with self.assertRaises(CommandLineError):
            verify.create_checksum_for_last_backup(self.backup_path)

    def test_checksum_date_is_found_if_checksum_performed(self) -> None:
        """Test that a checksum date can be found if checksumming occurred."""
        create_user_data(self.user_path)
        exit_code = main.main([
            "-u", str(self.user_path),
            "-b", str(self.backup_path),
            "--checksum",
            "--log", str(self.log_path)],
            testing=True)
        self.assertEqual(exit_code, 0)
        last_checksum_date = verify.last_checksum(self.backup_path)
        self.assertIsNotNone(last_checksum_date)
        last_checksum_date = cast(datetime.datetime, last_checksum_date)
        backup_with_checksum = util.find_previous_backup(self.backup_path)
        self.assertIsNotNone(backup_with_checksum)
        backup_with_checksum = cast(Path, backup_with_checksum)
        backup_date = util.backup_datetime(backup_with_checksum)
        self.assertEqual(backup_date, last_checksum_date)

    def test_checksum_date_found_among_backups_with_no_checksums(self) -> None:
        """Test that checksum date is found."""
        create_user_data(self.user_path)
        default_backup(self.user_path, self.backup_path)
        exit_code = main.main([
            "-u", str(self.user_path),
            "-b", str(self.backup_path),
            "--checksum",
            "--log", str(self.log_path)],
            testing=True)
        self.assertEqual(exit_code, 0)
        default_backup(self.user_path, self.backup_path)
        backups = util.all_backups(self.backup_path)
        self.assertEqual(len(backups), 3)
        checksum_date = verify.last_checksum(self.backup_path)
        backup_with_checksum = backups[1]
        self.assertEqual(checksum_date, util.backup_datetime(backup_with_checksum))

    def test_last_checksum_finds_most_recent_checksum(self) -> None:
        """Test that last_checksum() finds most recent backup with checksum."""
        create_user_data(self.user_path)
        for _ in range(2):
            exit_code = main.main([
                "-u", str(self.user_path),
                "-b", str(self.backup_path),
                "--checksum",
                "--log", str(self.log_path),
                "--timestamp", unique_timestamp_string()],
                testing=True)
            self.assertEqual(exit_code, 0)

        last_checksum_date = verify.last_checksum(self.backup_path)
        last_backup = util.find_previous_backup(self.backup_path)
        self.assertIsNotNone(last_backup)
        last_backup = cast(Path, last_backup)
        self.assertEqual(last_checksum_date, util.backup_datetime(last_backup))

    def test_checksum_every_creates_checksum_when_no_prior_checksums(self) -> None:
        """Test that a checksum is performed when there are not prior checksums."""
        create_user_data(self.user_path)
        main.main([
            "-u", str(self.user_path),
            "-b", str(self.backup_path),
            "--checksum-every", "1d",
            "-l", str(self.log_path),
            "--timestamp", unique_timestamp_string()],
            testing=True)
        self.assertTrue((util.all_backups(self.backup_path)[0]/verify.checksum_file_name).is_file())

    def test_checksum_created_after_enough_time_passes_without_a_checksum(self) -> None:
        """Test that checksum is created using --checksum-every option after enough time passed."""
        create_user_data(self.user_path)
        now = datetime.datetime.now()
        for n in range(9):
            timestamp = now + datetime.timedelta(days=n)
            main.main([
                "-u", str(self.user_path),
                "-b", str(self.backup_path),
                "--checksum-every", "3d",
                "-l", str(self.log_path),
                "--timestamp", timestamp.strftime(util.backup_date_format)],
                testing=True)

        backups = util.all_backups(self.backup_path)
        checksum_exists = [(backup/verify.checksum_file_name).is_file() for backup in backups]
        self.assertEqual(
            checksum_exists,
            [True, False, False, True, False, False, True, False, False])

    def test_no_checksum_overrides_checksum_every_on_command_line(self) -> None:
        """Test that --no-checksum cancels --checksum-every in argument_parser."""
        args = argparse.parse_command_line(["--checksum-every", "1m", "--no-checksum"])
        self.assertFalse(
            util.should_do_periodic_action(
                args,
                "checksum",
                self.backup_path,
                verify.last_checksum))

    def test_no_checksum_overrides_checksum_every(self) -> None:
        """Test that --no-checksum cancels --checksum-every."""
        create_user_data(self.user_path)
        main.main([
            "-u", str(self.user_path),
            "-b", str(self.backup_path),
            "--no-checksum",
            "--checksum-every", "1m",
            "-l", str(self.log_path)],
            testing=True)
        self.assertFalse((util.all_backups(self.backup_path)[0]/verify.checksum_file_name).exists())

    def test_no_checksum_overrides_checksum(self) -> None:
        """Test that --no-checksum cancels --checksum."""
        create_user_data(self.user_path)
        main.main([
            "-u", str(self.user_path),
            "-b", str(self.backup_path),
            "--no-checksum",
            "--checksum",
            "-l", str(self.log_path)],
            testing=True)
        self.assertFalse((util.all_backups(self.backup_path)[0]/verify.checksum_file_name).exists())

    def test_verifying_checksum_with_no_changes_does_not_create_result_file(self) -> None:
        """Test that if checksum verification finds no changed files, no result file is created."""
        create_user_data(self.user_path)
        exit_code = main.main([
            "-u", str(self.user_path),
            "-b", str(self.backup_path),
            "--checksum",
            "--log", str(self.log_path)],
        testing=True)
        self.assertEqual(exit_code, 0)
        backup_folder = util.find_previous_backup(self.backup_path)
        self.assertIsNotNone(backup_folder)
        backup_folder = cast(Path, backup_folder)
        with self.assertLogs(level=logging.INFO) as logs:
            checksum_verify_file = verify.verify_backup_checksum(backup_folder, self.user_path)
        self.assertIsNone(checksum_verify_file)
        self.assertEqual(logs.output, [f"INFO:root:No changed files found in {backup_folder}"])

    def test_verify_checksum_writes_changed_file(self) -> None:
        """Test that a file is written when a changed file in a backup is detected."""
        create_user_data(self.user_path)
        exit_code = main.main([
            "-u", str(self.user_path),
            "-b", str(self.backup_path),
            "--checksum",
            "--log", str(self.log_path)],
        testing=True)
        self.assertEqual(exit_code, 0)
        backup_folder = util.find_previous_backup(self.backup_path)
        self.assertIsNotNone(backup_folder)
        backup_folder = cast(Path, backup_folder)
        changed_path = backup_folder/"sub_directory_1"/"sub_root_file.txt"
        self.assertTrue(changed_path.exists())
        changed_path.write_text("Corrupted data\n", encoding="utf8")
        with self.assertLogs(level=logging.WARNING) as checksum_verify_logs:
            checksum_verify_file = verify.verify_backup_checksum(backup_folder, self.user_path)
        self.assertEqual(
            checksum_verify_logs.output,
            [f"WARNING:root:File changed in backup: {changed_path.relative_to(backup_folder)}",
             f"WARNING:root:Writing changed files to {checksum_verify_file} ..."])
        self.assertIsNotNone(checksum_verify_file)
        checksum_verify_file = cast(Path, checksum_verify_file)
        self.assertTrue(checksum_verify_file.is_file())
        verify_data = checksum_verify_file.read_text(encoding="utf8").splitlines()
        self.assertEqual(len(verify_data), 2)
        self.assertEqual(verify_data[0], f"Verifying checksums of {backup_folder}")
        relative_path, old_checksum, new_checksum = verify_data[1].rsplit(" ", maxsplit=2)
        self.assertEqual(backup_folder/relative_path, changed_path)
        self.assertNotEqual(old_checksum, new_checksum)

    def test_verify_checksum_writes_missing_file(self) -> None:
        """Test that a file is written when a changed file in a backup is detected."""
        create_user_data(self.user_path)
        exit_code = main.main([
            "-u", str(self.user_path),
            "-b", str(self.backup_path),
            "--checksum",
            "--log", str(self.log_path)],
        testing=True)
        self.assertEqual(exit_code, 0)
        backup_folder = util.find_previous_backup(self.backup_path)
        self.assertIsNotNone(backup_folder)
        backup_folder = cast(Path, backup_folder)
        missing_path = backup_folder/"sub_directory_2"/"sub_root_file.txt"
        self.assertTrue(missing_path.exists())
        missing_path.unlink()
        with self.assertLogs(level=logging.WARNING) as checksum_verify_logs:
            checksum_verify_file = verify.verify_backup_checksum(backup_folder, self.user_path)
        self.assertEqual(
            checksum_verify_logs.output,
            [f"WARNING:root:File missing in backup: {missing_path.relative_to(backup_folder)}",
             f"WARNING:root:Writing changed files to {checksum_verify_file} ..."])
        self.assertIsNotNone(checksum_verify_file)
        checksum_verify_file = cast(Path, checksum_verify_file)
        self.assertTrue(checksum_verify_file.is_file())
        verify_data = checksum_verify_file.read_text(encoding="utf8").splitlines()
        self.assertEqual(len(verify_data), 2)
        self.assertEqual(verify_data[0], f"Verifying checksums of {backup_folder}")
        relative_path, _, new_checksum = verify_data[1].rsplit(" ", maxsplit=2)
        self.assertEqual(backup_folder/relative_path, missing_path)
        self.assertEqual("-", new_checksum)

    def test_verifying_checksum_creates_non_existent_result_directory(self) -> None:
        """Test that checksum verification creates a non-existent result folder."""
        create_user_data(self.user_path)
        exit_code = main.main([
            "-u", str(self.user_path),
            "-b", str(self.backup_path),
            "--checksum",
            "--log", str(self.log_path)],
        testing=True)
        self.assertEqual(exit_code, 0)
        backup_folder = util.find_previous_backup(self.backup_path)
        self.assertIsNotNone(backup_folder)
        backup_folder = cast(Path, backup_folder)
        changed_path = backup_folder/"sub_directory_2"/"sub_root_file.txt"
        self.assertTrue(changed_path.exists())
        changed_path.write_text("Corrupted data\n", encoding="utf8")
        verify_folder = self.user_path/"result"
        with self.assertLogs(level=logging.WARNING) as checksum_verify_logs:
            checksum_verify_file = verify.verify_backup_checksum(backup_folder, verify_folder)
        self.assertEqual(
            checksum_verify_logs.output,
            [f"WARNING:root:File changed in backup: {changed_path.relative_to(backup_folder)}",
             f"WARNING:root:Writing changed files to {checksum_verify_file} ..."])
        self.assertIsNotNone(checksum_verify_file)
        checksum_verify_file = cast(Path, checksum_verify_file)
        self.assertEqual(checksum_verify_file.parent, verify_folder)
        self.assertTrue(checksum_verify_file.is_file())
        verify_data = checksum_verify_file.read_text(encoding="utf8").splitlines()
        self.assertEqual(len(verify_data), 2)
        self.assertEqual(verify_data[0], f"Verifying checksums of {backup_folder}")
        relative_path, old_checksum, new_checksum = verify_data[1].rsplit(" ", maxsplit=2)
        self.assertEqual(backup_folder/relative_path, changed_path)
        self.assertNotEqual(old_checksum, new_checksum)

    def test_verify_checksum_raises_error_when_no_checksum_file(self) -> None:
        """Test that an error is raised when the backup being checked has no checksum file."""
        create_user_data(self.user_path)
        default_backup(self.user_path, self.backup_path)
        backup_folder = util.find_previous_backup(self.backup_path)
        self.assertIsNotNone(backup_folder)
        backup_folder = cast(Path, backup_folder)
        with self.assertRaises(FileNotFoundError):
            verify.verify_backup_checksum(backup_folder, self.user_path)

    def test_verify_checksum_with_oldest_option_verifies_oldest_backup(self) -> None:
        """Test that --oldest option causes oldest backup with checksum to be verified."""
        create_user_data(self.user_path)
        default_backup(self.user_path, self.backup_path)
        exit_code = main.main([
            "-u", str(self.user_path),
            "-b", str(self.backup_path),
            "-l", str(self.log_path),
            "--timestamp", unique_timestamp_string(),
            "--checksum"],
            testing=True)
        self.assertEqual(exit_code, 0)
        default_backup(self.user_path, self.backup_path)
        exit_code = main.main([
            "-u", str(self.user_path),
            "-b", str(self.backup_path),
            "-l", str(self.log_path),
            "--timestamp", unique_timestamp_string(),
            "--checksum"],
            testing=True)
        self.assertEqual(exit_code, 0)
        default_backup(self.user_path, self.backup_path)

        backups = util.all_backups(self.backup_path)
        self.assertEqual(len(backups), 5)
        oldest_checksummed_backup = backups[1]
        self.assertTrue((oldest_checksummed_backup/verify.checksum_file_name).is_file())

        changed_file = (
            oldest_checksummed_backup/"sub_directory_2"/"sub_sub_directory_0"/"file_1.txt")
        self.assertTrue(changed_file.is_file())
        changed_file.write_text("Corrupted data", encoding="utf8")

        exit_code = main.main([
            "-b", str(self.backup_path),
            "-l", str(self.log_path),
            "--verify-checksum", str(self.user_path),
            "--oldest"],
            testing=True)
        self.assertEqual(exit_code, 0)

        checksum_verify_path = self.user_path/verify.verify_checksum_file_name
        self.assertTrue(checksum_verify_path.is_file())
        verify_data = checksum_verify_path.read_text().splitlines()
        self.assertEqual(len(verify_data), 2)
        self.assertEqual(verify_data[0], f"Verifying checksums of {oldest_checksummed_backup}")
        relative_path, old_checksum, new_checksum = verify_data[1].rsplit(maxsplit=2)
        self.assertEqual(changed_file, oldest_checksummed_backup/relative_path)
        self.assertNotEqual(old_checksum, new_checksum)

    def test_verify_checksum_with_newest_option_verifies_oldest_backup(self) -> None:
        """Test that --newest option causes newest backup with checksum to be verified."""
        create_user_data(self.user_path)
        default_backup(self.user_path, self.backup_path)
        exit_code = main.main([
            "-u", str(self.user_path),
            "-b", str(self.backup_path),
            "-l", str(self.log_path),
            "--timestamp", unique_timestamp_string(),
            "--checksum"],
            testing=True)
        self.assertEqual(exit_code, 0)
        default_backup(self.user_path, self.backup_path)
        exit_code = main.main([
            "-u", str(self.user_path),
            "-b", str(self.backup_path),
            "-l", str(self.log_path),
            "--timestamp", unique_timestamp_string(),
            "--checksum"],
            testing=True)
        self.assertEqual(exit_code, 0)
        default_backup(self.user_path, self.backup_path)

        backups = util.all_backups(self.backup_path)
        self.assertEqual(len(backups), 5)
        newest_checksummed_backup = backups[3]
        self.assertTrue((newest_checksummed_backup/verify.checksum_file_name).is_file())

        changed_file = (
            newest_checksummed_backup/"sub_directory_0"/"sub_sub_directory_1"/"file_2.txt")
        self.assertTrue(changed_file.is_file())
        changed_file.write_text("Corrupted data", encoding="utf8")

        exit_code = main.main([
            "-b", str(self.backup_path),
            "-l", str(self.log_path),
            "--verify-checksum", str(self.user_path),
            "--newest"],
            testing=True)
        self.assertEqual(exit_code, 0)

        checksum_verify_path = self.user_path/verify.verify_checksum_file_name
        self.assertTrue(checksum_verify_path.is_file())
        verify_data = checksum_verify_path.read_text().splitlines()
        self.assertEqual(len(verify_data), 2)
        self.assertEqual(verify_data[0], f"Verifying checksums of {newest_checksummed_backup}")
        relative_path, old_checksum, new_checksum = verify_data[1].rsplit(maxsplit=2)
        self.assertEqual(changed_file, newest_checksummed_backup/relative_path)
        self.assertNotEqual(old_checksum, new_checksum)

    def test_verify_checksum_with_menu_verifies_chosen_backup(self) -> None:
        """Test that choosing backup from menuvcauses chosen backup with checksum to be verified."""
        create_user_data(self.user_path)
        default_backup(self.user_path, self.backup_path)
        exit_code = main.main([
            "-u", str(self.user_path),
            "-b", str(self.backup_path),
            "-l", str(self.log_path),
            "--timestamp", unique_timestamp_string(),
            "--checksum"],
            testing=True)
        self.assertEqual(exit_code, 0)
        default_backup(self.user_path, self.backup_path)
        exit_code = main.main([
            "-u", str(self.user_path),
            "-b", str(self.backup_path),
            "-l", str(self.log_path),
            "--timestamp", unique_timestamp_string(),
            "--checksum"],
            testing=True)
        self.assertEqual(exit_code, 0)
        default_backup(self.user_path, self.backup_path)

        backups = util.all_backups(self.backup_path)
        self.assertEqual(len(backups), 5)
        chosen_checksummed_backup = backups[1]
        self.assertTrue((chosen_checksummed_backup/verify.checksum_file_name).is_file())

        changed_file = (
            chosen_checksummed_backup/"sub_directory_2"/"sub_sub_directory_1"/"file_0.txt")
        self.assertTrue(changed_file.is_file())
        changed_file.write_text("Corrupted data", encoding="utf8")

        exit_code = main.main([
            "-b", str(self.backup_path),
            "-l", str(self.log_path),
            "--verify-checksum", str(self.user_path),
            "--choice", "1"],
            testing=True)
        self.assertEqual(exit_code, 0)

        checksum_verify_path = self.user_path/verify.verify_checksum_file_name
        self.assertTrue(checksum_verify_path.is_file())
        verify_data = checksum_verify_path.read_text().splitlines()
        self.assertEqual(len(verify_data), 2)
        self.assertEqual(verify_data[0], f"Verifying checksums of {chosen_checksummed_backup}")
        relative_path, old_checksum, new_checksum = verify_data[1].rsplit(maxsplit=2)
        self.assertEqual(changed_file, chosen_checksummed_backup/relative_path)
        self.assertNotEqual(old_checksum, new_checksum)

    def test_verify_checksum_with_no_checksummed_backups_is_error(self) -> None:
        """Test that trying to verify a checksum file with no checksummed backups raises error."""
        create_user_data(self.user_path)
        default_backup(self.user_path, self.backup_path)
        previous_backup = util.find_previous_backup(self.backup_path)
        self.assertIsNotNone(previous_backup)
        previous_backup = cast(Path, previous_backup)
        with self.assertRaises(FileNotFoundError):
            verify.verify_backup_checksum(previous_backup, self.user_path)

    def test_verify_checksum_with_no_checksummed_backups_on_command_line_is_error(self) -> None:
        """Test that verifying a checksum file with no checksummed backups prints error message."""
        create_user_data(self.user_path)
        default_backup(self.user_path, self.backup_path)
        with self.assertLogs(level=logging.ERROR) as logs:
            exit_code = main.main([
                "-u", str(self.user_path),
                "-b", str(self.backup_path),
                "-l", os.devnull,
                "--verify-checksum", str(self.user_path),
                "--oldest"],
            testing=True)

        self.assertEqual(exit_code, 1)
        self.assertEqual(
            logs.output,
            [f"ERROR:root:No backups with checksums found in {self.backup_path}"])

    def test_verify_checksum_before_deletion(self) -> None:
        """Test that a checksummed backup is verified before automatic deletion."""
        create_user_data(self.user_path)
        timestamp = datetime.datetime.now() - datetime.timedelta(days=2)
        main.main([
            "-u", str(self.user_path),
            "-b", str(self.backup_path),
            "-l", os.devnull,
            "--checksum",
            "--timestamp", timestamp.strftime(util.backup_date_format)],
            testing=True)

        backups = util.all_backups(self.backup_path)
        self.assertEqual(len(backups), 1)
        checksummed_backup = backups[0]
        self.assertTrue((checksummed_backup/verify.checksum_file_name).is_file())

        changed_file = (
            checksummed_backup/"sub_directory_2"/"sub_sub_directory_1"/"file_2.txt")
        self.assertTrue(changed_file.is_file())
        changed_file.write_text("Corrupted data", encoding="utf8")

        main.main([
            "-u", str(self.user_path),
            "-b", str(self.backup_path),
            "-l", os.devnull,
            "--delete-after", "1d",
            "--verify-checksum-before-deletion", str(self.user_path)],
        testing=False)

        result_path = self.user_path/verify.verify_checksum_file_name
        self.assertTrue(result_path.is_file())
        verify_data = result_path.read_text().splitlines()
        self.assertEqual(len(verify_data), 2)
        self.assertEqual(verify_data[0], f"Verifying checksums of {checksummed_backup}")
        relative_path, old_checksum, new_checksum = verify_data[1].rsplit(maxsplit=2)
        self.assertEqual(changed_file, checksummed_backup/relative_path)
        self.assertNotEqual(old_checksum, new_checksum)

    def test_verify_checksum_before_deletion_with_no_checksum(self) -> None:
        """Test that a checksummed backup is verified before automatic deletion."""
        create_user_data(self.user_path)
        timestamp = datetime.datetime.now() - datetime.timedelta(days=2)
        main.main([
            "-u", str(self.user_path),
            "-b", str(self.backup_path),
            "-l", os.devnull,
            "--timestamp", timestamp.strftime(util.backup_date_format)],
            testing=True)

        main.main([
            "-u", str(self.user_path),
            "-b", str(self.backup_path),
            "-l", os.devnull,
            "--delete-after", "1d",
            "--verify-checksum-before-deletion", str(self.user_path)],
        testing=False)

        result_path = self.user_path/verify.verify_checksum_file_name
        self.assertFalse(result_path.exists())

    def test_that_verifying_checksum_files_ignore_blank_lines(self) -> None:
        """Test that lines with just whitespace do not affect checksum verification."""
        create_user_data(self.user_path)
        exit_code = main.main([
            "-u", str(self.user_path),
            "-b", str(self.backup_path),
            "-l", os.devnull,
            "--checksum"],
            testing=True)
        self.assertEqual(exit_code, 0)
        backup = util.find_previous_backup(self.backup_path)
        self.assertIsNotNone(backup)
        backup = cast(Path, backup)
        checksum_file_name = backup/"checksums.sha3"
        self.assertTrue(checksum_file_name.is_file())
        new_checksum_file = fs.unique_path_name(checksum_file_name)
        with (checksum_file_name.open(encoding="utf8") as reader,
            new_checksum_file.open("w", encoding="utf8") as writer):

            for line in reader:
                writer.write(line)
                writer.write(" \n")

        checksum_file_name.unlink()

        with self.assertNoLogs(level=logging.WARNING):
            exit_code = main.main([
                "--verify-checksum", str(self.user_path),
                "--oldest",
                "-b", str(self.backup_path),
                "-l", os.devnull],
                testing=True)

        self.assertEqual(exit_code, 0)


class ConfigurationFileTests(TestCaseWithTemporaryFilesAndFolders):
    """Test configuration file functionality."""

    def test_configuration_file_reading_is_insensitive_to_variant_writings(self) -> None:
        """
        Test that configuration file reading is insensitive to variations in writing.

        These include:
        1. Upper vs. lowercase vs. mixed
        2. Spacing
        3. Parameters spelled with dashes (as on command line) or spaces
        """
        user_folder = r"C:\Files"
        backup_folder = r"D:\Backup"
        filter_file = "filter_file.txt"
        self.config_path.write_text(
rf"""
USER FOLDER:     {user_folder}
backup folder:   {backup_folder}

FiLteR    :    {filter_file}
force-copy:

compare    contents :
checkSUM       :
Checksum Every: 1m
""", encoding="utf8")
        command_line = config.read_configuation_file(Path(self.config_path))
        expected_command_line = [
            "--user-folder", user_folder,
            "--backup-folder", backup_folder,
            "--filter", filter_file,
            "--force-copy",
            "--compare-contents",
            "--checksum",
            "--checksum-every", "1m"]

        self.assertEqual(command_line, expected_command_line)
        arg_parser = argparse.argument_parser()
        args = arg_parser.parse_args(command_line)
        self.assertEqual(args.user_folder, user_folder)
        self.assertEqual(args.backup_folder, backup_folder)
        self.assertEqual(args.filter, filter_file)
        self.assertTrue(args.force_copy)
        self.assertTrue(args.checksum)
        self.assertEqual(args.checksum_every, "1m")

    def test_command_line_options_override_config_file_options(self) -> None:
        """Test that command line options override file configurations and leave others alone."""
        user_folder = r"C:\Users\Test User"
        self.config_path.write_text(
rf"""
User Folder : {user_folder}
Backup Folder: temp_back
filter: filter.txt
log: temp_log.txt
compare contents:
Debug:""", encoding="utf8")
        actual_backup_folder = "temp_back2"
        actual_log_file = "temporary_log.log"
        command_line_options = [
            "-b", actual_backup_folder,
            "-c", str(self.config_path),
            "-l", actual_log_file]
        options = argparse.parse_command_line(command_line_options)
        self.assertEqual(options.user_folder, user_folder)
        self.assertEqual(options.backup_folder, actual_backup_folder)
        self.assertEqual(options.log, actual_log_file)
        self.assertTrue(options.compare_contents)
        self.assertTrue(options.debug)

    def test_negating_command_line_parameters_override_config_file(self) -> None:
        """Test that command line options like --no-X override file configurations."""
        self.config_path.write_text(
r"""
compare contents:
Debug:
force copy:
checksum:
""", encoding="utf8")
        command_line_options = [
            "-c", str(self.config_path),
            "--no-compare-contents",
            "--no-debug",
            "--no-force-copy",
            "--no-checksum"]
        options = argparse.parse_command_line(command_line_options)
        self.assertFalse(argparse.toggle_is_set(options, "compare_contents"))
        self.assertFalse(argparse.toggle_is_set(options, "debug"))
        self.assertFalse(argparse.toggle_is_set(options, "force_copy"))
        self.assertFalse(argparse.toggle_is_set(options, "checksum"))

    def test_recursive_config_files_are_not_allowed(self) -> None:
        """Test that putting a config parameter in a configuration file raises an exception."""
        self.config_path.write_text("config: config_file_2.txt", encoding="utf8")
        with self.assertRaises(CommandLineError):
            config.read_configuation_file(Path(self.config_path))

    def test_missing_config_file_error(self) -> None:
        """Test that a missing configuration file raises a CommandLineError."""
        with self.assertRaises(CommandLineError):
            config.read_configuation_file(self.config_path)


class RestorationTests(TestCaseWithTemporaryFilesAndFolders):
    """Test that restoring backups works correctly."""

    def test_restore_last_backup_with_delete_extra_option_deletes_new_paths(self) -> None:
        """Test that restoring with --delete-extra deletes new files since last backup."""
        create_user_data(self.user_path)
        default_backup(self.user_path, self.backup_path)
        self.assertEqual(len(util.all_backups(self.backup_path)), 1)

        first_extra_file = self.user_path/"extra_file1.txt"
        first_extra_file.write_text("extra 1\n", encoding="utf8")
        first_extra_folder = self.user_path/"extra_folder_1"
        first_extra_folder.mkdir()
        first_extra_folder_file = first_extra_folder/"file_in_folder_1.txt"
        first_extra_folder_file.write_text("extra file in folder 1\n", encoding="utf8")

        default_backup(self.user_path, self.backup_path)
        self.assertEqual(len(util.all_backups(self.backup_path)), 2)

        second_extra_file = self.user_path/"extra_file2.txt"
        second_extra_file.write_text("extra 2\n", encoding="utf8")
        second_extra_folder = self.user_path/"extra_folder_2"
        second_extra_folder.mkdir()
        second_extra_folder_file = second_extra_folder/"file_in_folder_2.txt"
        second_extra_folder_file.write_text("extra file in folder 2\n", encoding="utf8")

        exit_code = main_assert_no_error_log([
            "--restore",
            "--destination", str(self.user_path),
            "--backup-folder", str(self.backup_path),
            "--last-backup", "--delete-extra",
            "--skip-prompt"],
            self)

        self.assertEqual(exit_code, 0)
        last_backup = util.find_previous_backup(self.backup_path)
        self.assertIsNotNone(last_backup)
        last_backup = cast(Path, last_backup)
        self.assertTrue(first_extra_file.is_file(follow_symlinks=False))
        self.assertTrue(first_extra_folder.is_dir(follow_symlinks=False))
        self.assertTrue(first_extra_folder_file.is_file(follow_symlinks=False))
        self.assertFalse(second_extra_file.exists(follow_symlinks=False))
        self.assertFalse(second_extra_folder.exists(follow_symlinks=False))
        self.assertFalse(second_extra_folder_file.exists(follow_symlinks=False))
        self.assertTrue(directories_have_identical_content(self.user_path, last_backup))

    def test_restore_last_backup_with_keep_extra_preserves_new_paths(self) -> None:
        """Test that restoring with --keep-extra does not delete new files since the last backup."""
        create_user_data(self.user_path)
        default_backup(self.user_path, self.backup_path)
        self.assertEqual(len(util.all_backups(self.backup_path)), 1)

        first_extra_file = self.user_path/"extra_file1.txt"
        first_extra_file.write_text("extra 1\n", encoding="utf8")
        first_extra_folder = self.user_path/"extra_folder_1"
        first_extra_folder.mkdir()
        first_extra_folder_file = first_extra_folder/"file_in_folder_1.txt"
        first_extra_folder_file.write_text("extra file in folder 1\n", encoding="utf8")

        default_backup(self.user_path, self.backup_path)
        self.assertEqual(len(util.all_backups(self.backup_path)), 2)

        second_extra_file = self.user_path/"extra_file2.txt"
        second_extra_file.write_text("extra 2\n", encoding="utf8")
        second_extra_folder = self.user_path/"extra_folder_2"
        second_extra_folder.mkdir()
        second_extra_folder_file = second_extra_folder/"file_in_folder_2.txt"
        second_extra_folder_file.write_text("extra file in folder 2\n", encoding="utf8")

        exit_code = main_assert_no_error_log([
            "--restore",
            "--destination", str(self.user_path),
            "--backup-folder", str(self.backup_path),
            "--last-backup", "--keep-extra",
            "--skip-prompt"],
            self)

        self.assertEqual(exit_code, 0)
        last_backup = util.find_previous_backup(self.backup_path)
        self.assertIsNotNone(last_backup)
        last_backup = cast(Path, last_backup)
        self.assertTrue(first_extra_file.is_file(follow_symlinks=False))
        self.assertTrue(first_extra_folder.is_dir(follow_symlinks=False))
        self.assertTrue(first_extra_folder_file.is_file(follow_symlinks=False))
        self.assertTrue(second_extra_file.is_file(follow_symlinks=False))
        self.assertTrue(second_extra_folder.is_dir(follow_symlinks=False))
        self.assertTrue(second_extra_folder_file.is_file(follow_symlinks=False))
        second_extra_file.unlink()
        fs.delete_directory_tree(second_extra_folder)
        self.assertTrue(directories_have_identical_content(self.user_path, last_backup))

    def test_restore_backup_from_menu_choice_and_delete_extra_deletes_new_files(self) -> None:
        """Test restoring a chosen backup from a menu with --delete-extra deletes new files."""
        create_user_data(self.user_path)
        default_backup(self.user_path, self.backup_path)
        self.assertEqual(len(util.all_backups(self.backup_path)), 1)

        first_extra_file = self.user_path/"extra_file1.txt"
        first_extra_file.write_text("extra 1\n", encoding="utf8")

        default_backup(self.user_path, self.backup_path)
        self.assertEqual(len(util.all_backups(self.backup_path)), 2)

        second_extra_file = self.user_path/"extra_file2.txt"
        second_extra_file.write_text("extra 2\n", encoding="utf8")

        choice = 0
        exit_code = main_assert_no_error_log([
            "--restore",
            "--destination", str(self.user_path),
            "--backup-folder", str(self.backup_path),
            "--choose-backup", "--delete-extra",
            "--choice", str(choice),
            "--skip-prompt"],
            self)

        self.assertEqual(exit_code, 0)
        restored_backup = util.all_backups(self.backup_path)[choice]
        self.assertFalse(first_extra_file.exists(follow_symlinks=False))
        self.assertFalse(second_extra_file.exists(follow_symlinks=False))
        self.assertTrue(directories_have_identical_content(self.user_path, restored_backup))

    def test_restore_backup_from_menu_choice_and_keep_extra_preserves_new_files(self) -> None:
        """Test restoring a chosen backup from a menu with --keep-extra preserves new files."""
        create_user_data(self.user_path)
        default_backup(self.user_path, self.backup_path)
        self.assertEqual(len(util.all_backups(self.backup_path)), 1)

        first_extra_file = self.user_path/"extra_file1.txt"
        first_extra_file.write_text("extra 1\n", encoding="utf8")

        default_backup(self.user_path, self.backup_path)
        self.assertEqual(len(util.all_backups(self.backup_path)), 2)

        second_extra_file = self.user_path/"extra_file2.txt"
        second_extra_file.write_text("extra 2\n", encoding="utf8")

        choice = 0
        exit_code = main_assert_no_error_log([
            "--restore",
            "--destination", str(self.user_path),
            "--backup-folder", str(self.backup_path),
            "--choose-backup", "--keep-extra",
            "--choice", str(choice),
            "--skip-prompt"],
            self)

        self.assertEqual(exit_code, 0)
        restored_backup = util.all_backups(self.backup_path)[choice]
        self.assertTrue(first_extra_file.is_file(follow_symlinks=False))
        self.assertTrue(second_extra_file.is_file(follow_symlinks=False))
        first_extra_file.unlink()
        second_extra_file.unlink()
        self.assertTrue(directories_have_identical_content(self.user_path, restored_backup))

    def test_restore_backup_with_destination_delete_extra_restores_to_new_location(self) -> None:
        """Test restoring with --destination and --delete-extra recreates backup in new location."""
        with tempfile.TemporaryDirectory() as destination_folder:
            create_user_data(self.user_path)
            default_backup(self.user_path, self.backup_path)
            exit_code = main_assert_no_error_log([
                "--restore",
                "--backup-folder", str(self.backup_path),
                "--last-backup", "--delete-extra",
                "--destination", destination_folder,
                "--skip-prompt"],
                self)

            self.assertEqual(exit_code, 0)
            destination_path = Path(destination_folder)
            last_backup = util.find_previous_backup(self.backup_path)
            self.assertIsNotNone(last_backup)
            last_backup = cast(Path, last_backup)
            self.assertTrue(directories_have_identical_content(last_backup, destination_path))
            self.assertTrue(directories_have_identical_content(self.user_path, destination_path))

    def test_restore_backup_with_destination_keep_extra_preserves_extra_files(self) -> None:
        """Test restoring with --destination and --keep-extra keeps extra files in new location."""
        with tempfile.TemporaryDirectory() as destination_folder:
            create_user_data(self.user_path)
            default_backup(self.user_path, self.backup_path)

            destination_path = Path(destination_folder)
            extra_file = destination_path/"extra_file1.txt"
            extra_file.write_text("extra 1\n", encoding="utf8")

            exit_code = main_assert_no_error_log([
                "--restore",
                "--backup-folder", str(self.backup_path),
                "--last-backup", "--keep-extra",
                "--destination", destination_folder,
                "--skip-prompt"],
                self)

            self.assertEqual(exit_code, 0)
            self.assertTrue(extra_file.is_file(follow_symlinks=False))
            last_backup = util.find_previous_backup(self.backup_path)
            self.assertIsNotNone(last_backup)
            last_backup = cast(Path, last_backup)
            extra_file.unlink()
            self.assertTrue(directories_have_identical_content(last_backup, destination_path))
            self.assertTrue(directories_have_identical_content(self.user_path, destination_path))

    def test_restore_without_delete_extra_or_keep_extra_is_an_error(self) -> None:
        """Test that missing --delete-extra and --keep-extra results in an error."""
        create_user_data(self.user_path)
        default_backup(self.user_path, self.backup_path)
        with self.assertLogs(level=logging.ERROR) as no_extra_log:
            exit_code = main_no_log([
                "--restore",
                "--destination", str(self.user_path),
                "--backup-folder", str(self.backup_path),
                "--last-backup"])
        self.assertEqual(exit_code, 1)
        expected_logs = [
            "ERROR:root:Exactly one of the following is required: --delete-extra or --keep-extra"]
        self.assertEqual(expected_logs, no_extra_log.output)

    def test_restore_without_last_backup_or_choose_backup_is_an_error(self) -> None:
        """Test that missing --last-backup and --choose-backup results in an error."""
        create_user_data(self.user_path)
        default_backup(self.user_path, self.backup_path)
        with self.assertLogs(level=logging.ERROR) as no_backup_choice_log:
            exit_code = main_no_log([
                "--restore",
                "--destination", str(self.user_path),
                "--backup-folder", str(self.backup_path),
                "--keep-extra"])
        self.assertEqual(exit_code, 1)
        expected_logs = [
            "ERROR:root:Exactly one of the following is required: --last-backup or --choose-backup"]
        self.assertEqual(expected_logs, no_backup_choice_log.output)

    def test_restore_with_bad_response_to_overwrite_confirmation_is_an_error(self) -> None:
        """Test that wrong response to overwrite confirmation ends program with error code."""
        create_user_data(self.user_path)
        default_backup(self.user_path, self.backup_path)
        with self.assertLogs(level=logging.INFO) as bad_prompt_log:
            exit_code = main_no_log([
                "--restore",
                "--destination", str(self.user_path),
                "--backup-folder", str(self.backup_path),
                "--choose-backup",
                "--delete-extra",
                "--skip-prompt",
                "--bad-input",
                "--choice", "0"])
        self.assertEqual(exit_code, 0)
        rejection_line = (
            'INFO:root:The response was "no" and not "yes", so the '
            'restoration is cancelled.')
        self.assertIn(rejection_line, bad_prompt_log.output)

    def test_attempt_to_restore_from_non_existent_backups_raises_command_line_error(self) -> None:
        """If there are no backups, then attempting to restore raises a CommandLineError."""
        args = argparse.parse_command_line([
            "--restore",
            "--backup-folder", str(self.backup_path),
            "--destination", str(self.user_path),
            "--choose-backup",
            "--delete-extra"])
        with self.assertRaises(CommandLineError):
            restoration.start_backup_restore(args)

    def test_choose_backup_with_no_previous_backups_returns_none(self) -> None:
        """Ensure that the choose_backup() function returns None when there are no backups."""
        self.assertIsNone(restoration.choose_backup(self.backup_path, choice=None))

    def test_start_backup_restore_with_no_backups_logs_and_returns_normally(self) -> None:
        """If there are no backups, then a log message is printed and no errors occur."""
        create_user_data(self.user_path)
        default_backup(self.user_path, self.backup_path)
        last_backup = util.find_previous_backup(self.backup_path)
        last_backup = cast(Path, last_backup)
        self.assertTrue(last_backup.is_dir())
        fs.delete_directory_tree(last_backup)
        with self.assertLogs(level=logging.INFO) as restore_log:
            exit_code = main_no_log([
                "--restore",
                "--backup-folder", str(self.backup_path),
                "--destination", str(self.user_path),
                "--last-backup",
                "--delete-extra"])
        self.assertEqual(exit_code, 1)
        self.assertEqual(restore_log.output, [f"ERROR:root:No backups found in {self.backup_path}"])


class BackupLockTests(TestCaseWithTemporaryFilesAndFolders):
    """Test that the lock prevents simultaneous access to a backup location."""

    def test_backup_while_lock_is_present_raises_concurrency_error(self) -> None:
        """Test that locking raises an error when the lock is present."""
        create_user_data(self.user_path)
        with lock.Backup_Lock(self.backup_path, "no wait test"):
            exit_code = run_backup(
                Invocation.cli,
                self.user_path,
                self.backup_path,
                filter_file=None,
                examine_whole_file=False,
                force_copy=False,
                timestamp=unique_timestamp())
            self.assertNotEqual(exit_code, 0)

            with self.assertRaises(ConcurrencyError):
                options = argparse.argument_parser()
                args = options.parse_args([
                    "--user-folder", str(self.user_path),
                    "--backup-folder", str(self.backup_path)])
                bak.start_backup(args)

    def test_lock_writes_process_info_to_lock_file_and_deletes_on_exit(self) -> None:
        """Test that lock file is created when entering with statement and deleted when exiting."""
        test_pid = str(os.getpid())
        test_operation = "lock data test"
        with lock.Backup_Lock(self.backup_path, test_operation):
            lock_path = self.backup_path/"vintagebackup.lock"
            pid, operation = filter(None, lock_path.read_text(encoding="utf8").split("\n"))
            self.assertEqual(pid, test_pid)
            self.assertEqual(operation, test_operation)

        self.assertFalse(lock_path.is_file(follow_symlinks=False))


class CopyProbabilityTests(TestCaseWithTemporaryFilesAndFolders):
    """Test that copy probability or hard link count causes identical files to be copied."""

    def test_max_average_hard_links_causes_some_unchanged_files_to_be_copied(self) -> None:
        """Test some files are copied instead of linked when max_average_hard_links is non-zero."""
        create_user_data(self.user_path)
        arguments = [
            "--user-folder", str(self.user_path),
            "--backup-folder", str(self.backup_path),
            "--hard-link-count", "1",
            "--timestamp", unique_timestamp_string()]
        exit_code = main_assert_no_error_log(arguments, self)
        self.assertEqual(exit_code, 0)
        arguments[-1] = unique_timestamp_string()
        exit_code = main_assert_no_error_log(arguments, self)
        self.assertEqual(exit_code, 0)

        backups = util.all_backups(self.backup_path)
        self.assertEqual(len(backups), 2)
        self.assertTrue(all_files_have_same_content(*backups))
        self.assertFalse(directories_are_completely_hardlinked(*backups))
        self.assertFalse(directories_are_completely_copied(*backups))

    def test_hard_link_count_must_be_a_positive_number(self) -> None:
        """Test that all inputs to --hard-link-count besides positive whole numbers are errors."""
        arguments = [
            "--user-folder", str(self.user_path),
            "--backup-folder", str(self.backup_path),
            "--hard-link-count", "Z"]
        with self.assertLogs(level=logging.ERROR) as error_log:
            exit_code = main_no_log(arguments)
        self.assertEqual(exit_code, 1)
        self.assertEqual(
            error_log.output, ["ERROR:root:Invalid value for hard link count: Z"])

        arguments[-1] = "0"
        with self.assertLogs(level=logging.ERROR) as error_log:
            exit_code = main_no_log(arguments)
        self.assertEqual(exit_code, 1)
        self.assertEqual(
            error_log.output,
            ["ERROR:root:Hard link count must be a positive whole number. Got: 0"])

    def test_copy_probability_decimal_must_be_between_zero_and_one(self) -> None:
        """Test that only values from 0.0 to 1.0 are valid for --copy-probability."""
        for good_value in ["0.0", "0.5", "1.0"]:
            self.assertEqual(float(good_value), bak.parse_probability(good_value))

        for bad_value in ["-1.0", "1.5"]:
            with self.assertRaises(CommandLineError):
                bak.parse_probability(bad_value)

    def test_copy_probability_percent_must_be_between_zero_and_one_hundred(self) -> None:
        """Test that only values from 0.0 to 1.0 are valid for --copy-probability."""
        for good_value in ["0.0%", "50%", "100%"]:
            decimal = float(good_value[:-1])/100
            self.assertEqual(decimal, bak.parse_probability(good_value))

        for bad_value in ["-100%", "150%"]:
            with self.assertRaises(CommandLineError):
                bak.parse_probability(bad_value)

    def test_copy_probability_zero_hard_links_all_files(self) -> None:
        """Test that a copy probability of zero links all unchanged files."""
        create_user_data(self.user_path)
        arguments = [
            "--user-folder", str(self.user_path),
            "--backup-folder", str(self.backup_path),
            "--copy-probability", "0",
            "--timestamp", unique_timestamp_string()]
        exit_code = main_assert_no_error_log(arguments, self)
        self.assertEqual(exit_code, 0)
        arguments[-1] = unique_timestamp_string()
        exit_code = main_assert_no_error_log(arguments, self)
        self.assertEqual(exit_code, 0)

        backups = util.all_backups(self.backup_path)
        self.assertEqual(len(backups), 2)
        self.assertTrue(directories_are_completely_hardlinked(*backups))

    def test_no_copy_probability_argument_hard_links_all_files(self) -> None:
        """Test that a copy probability of zero links all unchanged files."""
        create_user_data(self.user_path)
        arguments = [
            "--user-folder", str(self.user_path),
            "--backup-folder", str(self.backup_path),
            "--timestamp", unique_timestamp_string()]
        exit_code = main_assert_no_error_log(arguments, self)
        self.assertEqual(exit_code, 0)
        arguments[-1] = unique_timestamp_string()
        exit_code = main_assert_no_error_log(arguments, self)
        self.assertEqual(exit_code, 0)

        backups = util.all_backups(self.backup_path)
        self.assertEqual(len(backups), 2)
        self.assertTrue(directories_are_completely_hardlinked(*backups))

    def test_copy_probability_one_copies_all_files(self) -> None:
        """Test that a copy probability of one causes all files to be copied."""
        create_user_data(self.user_path)
        arguments = [
            "--user-folder", str(self.user_path),
            "--backup-folder", str(self.backup_path),
            "--copy-probability", "1",
            "--timestamp", unique_timestamp_string()]
        exit_code = main_assert_no_error_log(arguments, self)
        self.assertEqual(exit_code, 0)
        arguments[-1] = unique_timestamp_string()
        exit_code = main_assert_no_error_log(arguments, self)
        self.assertEqual(exit_code, 0)

        backups = util.all_backups(self.backup_path)
        self.assertEqual(len(backups), 2)
        self.assertTrue(directories_are_completely_copied(*backups))

    def test_copy_probability_half_hard_links_some_files(self) -> None:
        """Test that a middle range copy probability copies some files and hard links others."""
        create_user_data(self.user_path)
        arguments = [
            "--user-folder", str(self.user_path),
            "--backup-folder", str(self.backup_path),
            "--copy-probability", "50%",
            "--timestamp", unique_timestamp_string()]
        exit_code = main_assert_no_error_log(arguments, self)
        self.assertEqual(exit_code, 0)
        arguments[-1] = unique_timestamp_string()
        exit_code = main_assert_no_error_log(arguments, self)
        self.assertEqual(exit_code, 0)

        backups = util.all_backups(self.backup_path)
        self.assertEqual(len(backups), 2)
        self.assertTrue(all_files_have_same_content(*backups))
        self.assertFalse(directories_are_completely_hardlinked(*backups))
        self.assertFalse(directories_are_completely_copied(*backups))

    def test_copy_probability_returns_zero_if_no_hard_link_argument_present(self) -> None:
        """Test if no --hard-link-count argument is present, probability of copy is zero."""
        user_input = argparse.argument_parser()
        no_arguments = user_input.parse_args([])
        self.assertEqual(bak.copy_probability(no_arguments), 0.0)

    def test_copy_probability_with_non_positive_argument_is_an_error(self) -> None:
        """Any argument to --hard-link-count that is not a positive integer raises an exception."""
        for bad_arg in ("-1", "0", "z"):
            with self.assertRaises(CommandLineError):
                bak.copy_probability_from_hard_link_count(bad_arg)

    def test_copy_probability_returns_one_over_n_plus_one_for_n_hard_links(self) -> None:
        """Test that the probability for N hard links is 1/(N + 1)."""
        for n in range(1, 10):
            probability = bak.copy_probability_from_hard_link_count(str(n))
            self.assertAlmostEqual(1/(n + 1), probability)


class AtomicBackupTests(TestCaseWithTemporaryFilesAndFolders):
    """Test atomicity of backups."""

    def test_staging_folder_does_not_exist_after_successful_backup(self) -> None:
        """Test that the staging folder is deleted after a successful backup."""
        create_user_data(self.user_path)
        staging_path = self.backup_path/"Staging"
        default_backup(self.user_path, self.backup_path)
        self.assertFalse(staging_path.exists())

    def test_staging_folder_deleted_by_new_backup(self) -> None:
        """Test that a backup process deletes a staging folder should it already exist."""
        create_user_data(self.user_path)
        staging_path = self.backup_path/"Staging"
        staging_path.mkdir()
        (staging_path/"leftover_file.txt").write_text(
            "Leftover from last backup\n", encoding="utf8")
        with self.assertLogs(level=logging.INFO) as logs:
            default_backup(self.user_path, self.backup_path)
        self.assertFalse(staging_path.exists())
        staging_message = (
            "INFO:root:There is a staging folder "
            "leftover from previous incomplete backup.")
        self.assertIn(staging_message, logs.output)
        deletion_message = f"INFO:root:Deleting {staging_path} ..."
        self.assertIn(deletion_message, logs.output)


class PurgeTests(TestCaseWithTemporaryFilesAndFolders):
    """Tests for purging files and folders from backups."""

    def test_file_purge(self) -> None:
        """Test that a purged file is deleted from all backups."""
        create_user_data(self.user_path)
        number_of_backups = 5
        for _ in range(number_of_backups):
            default_backup(self.user_path, self.backup_path)

        purged_file = self.user_path/"sub_directory_2"/"sub_sub_directory_1"/"file_0.txt"
        self.assertTrue(purged_file.is_file())
        purge_command_line = argparse.parse_command_line(
            ["--purge", str(purged_file), "--backup-folder", str(self.backup_path)])
        purge.start_backup_purge(purge_command_line, "y")
        expected_contents = directory_contents(self.user_path)
        expected_contents.remove(purged_file.relative_to(self.user_path))
        for backup in util.all_backups(self.backup_path):
            self.assertEqual(expected_contents, directory_contents(backup))

    def test_folder_purge(self) -> None:
        """Test that a purged folder is deleted from all backups."""
        create_user_data(self.user_path)
        number_of_backups = 5
        for _ in range(number_of_backups):
            default_backup(self.user_path, self.backup_path)

        purged_folder = self.user_path/"sub_directory_2"/"sub_sub_directory_1"
        self.assertTrue(purged_folder.is_dir())
        purge_command_line = argparse.parse_command_line(
            ["--purge", str(purged_folder), "--backup-folder", str(self.backup_path)])
        purge.start_backup_purge(purge_command_line, "y")
        expected_contents = directory_contents(self.user_path)
        purged_contents = set(filter(
            lambda p: (self.user_path/p).is_relative_to(purged_folder), expected_contents))
        expected_contents.difference_update(purged_contents)
        for backup in util.all_backups(self.backup_path):
            self.assertEqual(expected_contents, directory_contents(backup))

    def test_purging_missing_file_logs_absence(self) -> None:
        """If a purge target does not exist, this is logged and the function returns normally."""
        create_user_data(self.user_path)
        default_backup(self.user_path, self.backup_path)
        non_existent_file = self.user_path/"does_not_exist.txt"
        with self.assertLogs(level=logging.INFO) as logs:
            purge.purge_path(non_existent_file, self.backup_path, None, None)
        self.assertEqual(len(logs.output), 1)
        self.assertEqual(
            logs.output[0],
            f"INFO:root:Could not find any backed up copies of {non_existent_file}")

    def test_file_purge_with_prompt_only_deletes_files(self) -> None:
        """Test that a purging a non-existent file only deletes files in backups."""
        create_user_data(self.user_path)
        number_of_backups = 5
        for _ in range(number_of_backups):
            default_backup(self.user_path, self.backup_path)

        purged_path = self.user_path/"sub_directory_2"/"sub_sub_directory_1"
        fs.delete_directory_tree(purged_path)
        purged_path.touch()

        for _ in range(number_of_backups):
            default_backup(self.user_path, self.backup_path)

        self.assertTrue(purged_path.is_file())
        purged_path.unlink()
        purge_command_line = argparse.parse_command_line([
            "--purge", str(purged_path),
            "--backup-folder", str(self.backup_path),
            "--choice", "0"])
        purge.start_backup_purge(purge_command_line, "y")
        relative_purge_file = purged_path.relative_to(self.user_path)
        for backup in util.all_backups(self.backup_path):
            backup_file_path = backup/relative_purge_file
            self.assertTrue(
                fs.is_real_directory(backup_file_path)
                or not backup_file_path.exists())

    def test_folder_purge_with_prompt_only_deletes_folders(self) -> None:
        """Test that a purging a non-existent folder only deletes folders in backups."""
        create_user_data(self.user_path)
        number_of_backups = 5
        for _ in range(number_of_backups):
            default_backup(self.user_path, self.backup_path)

        purged_path = self.user_path/"sub_directory_2"/"sub_sub_directory_1"
        fs.delete_directory_tree(purged_path)
        purged_path.touch()

        for _ in range(number_of_backups):
            default_backup(self.user_path, self.backup_path)

        self.assertTrue(purged_path.is_file())
        purged_path.unlink()
        purge_command_line = argparse.parse_command_line([
            "--purge", str(purged_path),
            "--backup-folder", str(self.backup_path),
            "--choice", "1"])
        purge.start_backup_purge(purge_command_line, "y")
        relative_purge_file = purged_path.relative_to(self.user_path)
        for backup in util.all_backups(self.backup_path):
            backup_file_path = backup/relative_purge_file
            self.assertTrue(backup_file_path.is_file() or not backup_file_path.exists())

    def test_purge_with_non_y_confirmation_response_deletes_nothing(self) -> None:
        """Test that a entering something other that 'y' at confirmation purges nothing."""
        create_user_data(self.user_path)
        number_of_backups = 5
        for _ in range(number_of_backups):
            default_backup(self.user_path, self.backup_path)

        purged_path = self.user_path/"sub_directory_2"/"sub_sub_directory_1"
        self.assertTrue(purged_path.is_dir(follow_symlinks=False))
        purge_command_line = argparse.parse_command_line([
            "--purge", str(purged_path),
            "--backup-folder", str(self.backup_path)])
        purge.start_backup_purge(purge_command_line, "thing")

        for backup in util.all_backups(self.backup_path):
            self.assertTrue(directories_have_identical_content(backup, self.user_path))

    def test_folder_purge_from_list_with_prompt_only_deletes_folders(self) -> None:
        """Test that a purging a folder from a menu only deletes folders in backups."""
        create_user_data(self.user_path)
        number_of_backups = 5
        for _ in range(number_of_backups):
            default_backup(self.user_path, self.backup_path)

        purged_path = self.user_path/"sub_directory_2"/"sub_sub_directory_0"
        fs.delete_directory_tree(purged_path)
        purged_path.touch()

        for _ in range(number_of_backups):
            default_backup(self.user_path, self.backup_path)

        self.assertTrue(purged_path.is_file())
        purged_path.unlink()
        search_directory = purged_path.parent
        purge_command_line = argparse.parse_command_line([
            "--purge-list", str(search_directory),
            "--backup-folder", str(self.backup_path),
            "--choice", "2"])
        purge.choose_purge_target_from_backups(purge_command_line, "y")
        relative_purge_file = purged_path.relative_to(self.user_path)
        for backup in util.all_backups(self.backup_path):
            backup_file_path = backup/relative_purge_file
            self.assertTrue(
                fs.is_real_directory(backup_file_path) or not backup_file_path.exists())

    def test_purge_file_suggests_filter_line(self) -> None:
        """Test that purging a file logs a filter line for the purged file."""
        create_user_data(self.user_path)
        number_of_backups = 5
        for _ in range(number_of_backups):
            default_backup(self.user_path, self.backup_path)

        purged_file = self.user_path/"sub_directory_2"/"sub_sub_directory_1"/"file_0.txt"
        self.assertTrue(purged_file.is_file())
        purge_command_line = argparse.parse_command_line([
            "--purge", str(purged_file),
            "--backup-folder", str(self.backup_path)])
        with self.assertLogs() as log_lines:
            purge.start_backup_purge(purge_command_line, "y")
        relative_purge_file = purged_file.relative_to(self.user_path)
        self.assertEqual(log_lines.output[-1], f"INFO:root:- {relative_purge_file}")

    def test_purge_folder_suggests_recursive_filter_line(self) -> None:
        """Test that purging a file logs a filter line for the purged file."""
        create_user_data(self.user_path)
        number_of_backups = 5
        for _ in range(number_of_backups):
            default_backup(self.user_path, self.backup_path)

        purged_file = self.user_path/"sub_directory_2"
        self.assertTrue(purged_file.is_dir())
        purge_command_line = argparse.parse_command_line([
            "--purge", str(purged_file),
            "--backup-folder", str(self.backup_path)])
        with self.assertLogs() as log_lines:
            purge.start_backup_purge(purge_command_line, "y")
        relative_purge_file = purged_file.relative_to(self.user_path)/"**"
        self.assertEqual(log_lines.output[-1], f"INFO:root:- {relative_purge_file}")


class EndOfMonthFixTests(unittest.TestCase):
    """Test date fixing function."""

    def test_fix_end_of_month_does_not_change_valid_dates(self) -> None:
        """Test that valid dates are returned unchanged."""
        start_date = datetime.date(2024, 1, 1)
        end_date = datetime.date(2025, 12, 31)
        date = start_date
        while date <= end_date:
            self.assertEqual(date, dates.fix_end_of_month(date.year, date.month, date.day))
            date += datetime.timedelta(days=1)

    def test_fix_end_of_month_always_returns_last_day_of_month_for_invalid_dates(self) -> None:
        """Test that an invalid date is fixed to be the end of the month."""
        january = 1
        december = 12

        for year in [2024, 2025]:
            for month in range(january, december + 1):
                bad_day = 40
                last_day_of_month = dates.fix_end_of_month(year, month, bad_day)
                day_after = last_day_of_month + datetime.timedelta(days=1)
                if last_day_of_month.month == december:
                    first_day_of_next_month = datetime.date(year + 1, january, 1)
                else:
                    first_day_of_next_month = datetime.date(year, month + 1, 1)
                self.assertEqual(day_after, first_day_of_next_month)

    def test_fix_end_of_month_rejects_inherently_bad_dates(self) -> None:
        """Test that fix_end_of_month() rejects bad values: zero, negatives, etc."""
        def assert_is_bad_date(year: int, month: int, day: int) -> None:
            with self.assertRaises(ValueError):
                dates.fix_end_of_month(year, month, day)

        # Bad years
        assert_is_bad_date(datetime.MINYEAR - 2, 1, 1)
        assert_is_bad_date(datetime.MINYEAR - 1, 1, 1)
        assert_is_bad_date(datetime.MAXYEAR + 1, 1, 1)
        assert_is_bad_date(datetime.MAXYEAR + 2, 1, 1)

        # Bad months
        assert_is_bad_date(2026, -1, 1)
        assert_is_bad_date(2026, 0, 1)
        assert_is_bad_date(2026, 13, 1)

        # Bad days
        assert_is_bad_date(2026, 1, -1)
        assert_is_bad_date(2026, 1, 0)


class PluralTests(unittest.TestCase):
    """Test pluralizing function."""

    def test_one_noun_results_in_singular_noun(self) -> None:
        """Test that exactly 1 of a noun leaves the noun unchanged."""
        self.assertEqual(console.plural_noun(1, "cat"), "1 cat")

    def test_several_nouns_results_in_simple_plural_noun(self) -> None:
        """Test that a number not equal to 1 appends s to noun."""
        for number in [0, 2, 3, 4]:
            self.assertEqual(console.plural_noun(number, "dog"), f"{number} dogs")


class AllBackupsTests(TestCaseWithTemporaryFilesAndFolders):
    """Test util.all_backups() function."""

    def test_all_backups_returns_all_backups(self) -> None:
        """Test that util.all_backups() returns all expected backups."""
        create_user_data(self.user_path)
        backups_to_create = 7
        timestamps: list[datetime.datetime] = []
        for _ in range(backups_to_create):
            timestamp = unique_timestamp()
            timestamps.append(timestamp)
            bak.create_new_backup(
                self.user_path,
                self.backup_path,
                filter_file=None,
                examine_whole_file=False,
                force_copy=False,
                copy_probability=0.0,
                timestamp=timestamp)
        backups = util.all_backups(self.backup_path)
        for timestamp, backup in zip(timestamps, backups, strict=True):
            year_path = str(timestamp.year)
            dated_folder_name = timestamp.strftime(util.backup_date_format)
            expected_folder = self.backup_path/year_path/dated_folder_name
            self.assertEqual(backup, expected_folder)

    def test_all_backups_returns_only_backups(self) -> None:
        """Test that util.all_backups() returns all expected backups."""
        create_user_data(self.user_path)
        backups_to_create = 7
        timestamps: list[datetime.datetime] = []
        for _ in range(backups_to_create):
            timestamp = unique_timestamp()
            timestamps.append(timestamp)
            bak.create_new_backup(
                self.user_path,
                self.backup_path,
                filter_file=None,
                examine_whole_file=False,
                force_copy=False,
                copy_probability=0.0,
                timestamp=timestamp)

        # Create entries that should be left out of util.all_backups() list
        timestamp = timestamps[-1]
        (self.backup_path/"extra year folder"/"extra backup folder").mkdir(parents=True)
        (self.backup_path/"extra year file").touch()
        (self.backup_path/str(timestamp.year)/"extra backup folder").mkdir()
        (self.backup_path/str(timestamp.year)/"extra backup file").touch()

        backups = util.all_backups(self.backup_path)
        for timestamp, backup in zip(timestamps, backups, strict=True):
            year_path = str(timestamp.year)
            dated_folder_name = timestamp.strftime(util.backup_date_format)
            expected_folder = self.backup_path/year_path/dated_folder_name
            self.assertEqual(backup, expected_folder)


class BackupNameTests(unittest.TestCase):
    """Test backup_name() and util.backup_datetime() functions."""

    def test_backup_name_and_backup_datetime_are_inverse_functions(self) -> None:
        """Test that a timestamp is preserved in a backup name."""
        now = datetime.datetime.now()
        timestamp = datetime.datetime(
            now.year, now.month, now.day, now.hour, now.minute, now.second)
        backup = bak.backup_name(timestamp)
        backup_timestamp = util.backup_datetime(backup)
        self.assertEqual(timestamp, backup_timestamp)

    def test_backup_name_puts_backup_folder_in_correct_year_folder(self) -> None:
        """Test that backups with the same year are grouped together."""
        timestamp = datetime.datetime.now()
        backup_folder = bak.backup_name(timestamp)
        backup_timestamp = util.backup_datetime(backup_folder)
        self.assertEqual(int(backup_folder.parent.name), backup_timestamp.year)


def is_even(n: int) -> bool:
    """Return whether an integer is even."""
    return n % 2 == 0


class SeparateTests(unittest.TestCase):
    """Tests for the separate() function."""

    def setUp(self) -> None:
        """Set up lists for testing separate()."""
        super().setUp()
        self.numbers = list(itertools.chain(range(100), range(50, 200)))
        random.shuffle(self.numbers)
        self.evens, self.odds = bak.separate(self.numbers, is_even)

    def test_separate_results_are_disjoint(self) -> None:
        """Test that separate() result lists have no items in common."""
        self.assertTrue(set(self.evens).isdisjoint(self.odds))

    def test_separate_results_union_equals_the_original_list(self) -> None:
        """Test that the combined separate() results contain every item in the original list."""
        self.assertEqual(sorted(self.evens + self.odds), sorted(self.numbers))

    def test_separate_first_results_always_satisfy_predicate(self) -> None:
        """Test that every member of the first separate() list satisfies predicate."""
        self.assertTrue(all(map(is_even, self.evens)))

    def test_separate_second_results_always_fail_predicate(self) -> None:
        """Test that every member of the first separate() list satisfies predicate."""
        self.assertFalse(any(map(is_even, self.odds)))

    def test_separate_lists_retain_order_of_original_list(self) -> None:
        """Test that each element of each list keeps original elements in same order."""
        self.assertEqual(self.evens, list(filter(is_even, self.numbers)))
        self.assertEqual(self.odds, list(itertools.filterfalse(is_even, self.numbers)))

    def test_separating_empty_list_results_in_empty_lists(self) -> None:
        """Test that an empty list separates into two empty lists."""
        a: list[object]
        b: list[object]
        a, b = bak.separate([], lambda _: True)
        self.assertFalse(a)
        self.assertFalse(b)


class ParseStorageTests(unittest.TestCase):
    """Test parse_storage_space() function."""

    def test_parse_storage_space_return_bare_numbers_unchanged(self) -> None:
        """Test that sending a string version of a number returns that number unchanged."""
        for number in range(10000):
            self.assertEqual(number, fs.parse_storage_space(str(number)))

    def test_parse_storage_space_is_unaffected_by_presense_or_absence_of_b(self) -> None:
        """Test that adding or removing 'B' from byte unit does not affect returned value."""
        for unit in fs.storage_prefixes:
            self.assertEqual(
                fs.parse_storage_space(f"3{unit}"),
                fs.parse_storage_space(f"3{unit}b"))

    def test_parse_storage_space_is_unaffected_by_space_between_number_and_unit(self) -> None:
        """Test that spaces don't matter when parsing storage space."""
        self.assertEqual(
            fs.parse_storage_space("4 GB"),
            fs.parse_storage_space("4GB"))

    def test_each_storage_prefix_is_a_thousand_times_larger_than_the_last(self) -> None:
        """Test that storage prefixes are interpretted correctly."""
        base_size = 5
        self.assertEqual(base_size, fs.parse_storage_space(str(base_size)))
        for prefix_1, prefix_2 in itertools.pairwise(fs.storage_prefixes):
            size_1 = fs.parse_storage_space(f"{base_size}{prefix_1}B")
            size_2 = fs.parse_storage_space(f"{base_size}{prefix_2}B")
            self.assertEqual(round(size_2/size_1), 1000)

    def test_parse_storage_space_argument_with_no_numbers_is_an_error(self) -> None:
        """Test that sending non-numeric text to parse_strorage_space() is an error."""
        with self.assertRaises(CommandLineError):
            fs.parse_storage_space("abcdefg")

    def test_parse_storage_space_argument_with_invalid_unit_is_an_error(self) -> None:
        """Test that an invalid unit raises an exception in parse_storage_space."""
        with self.assertRaises(CommandLineError):
            fs.parse_storage_space("123 AB")

    def test_parse_storage_space_and_byte_units_are_inverses(self) -> None:
        """Test that parse_storage_space(byte_units(x)) == x."""
        for unit in fs.storage_prefixes:
            text = f"1.000 {unit}B"
            size = fs.parse_storage_space(text)
            self.assertEqual(fs.byte_units(size), text)

    def test_parse_storage_space_with_empty_string_is_an_error(self) -> None:
        """Test that an empty string causes a CommandLineError in parse_storage_space."""
        with self.assertRaises(CommandLineError):
            fs.parse_storage_space("")

    def test_number_part_of_byte_units_result_is_less_than_one_thousand(self) -> None:
        """Test that the numeric part of they byte_units() result is less than 1000."""
        for digit_count in range(1, 20):
            number = int("1"*digit_count)
            text = fs.byte_units(number)
            number_part = float(text.split()[0])
            self.assertLess(number_part, 1000)

    def test_zero_bytes_returns_zero_from_byte_units(self) -> None:
        """Make sure byte_units can handle inputs of 0."""
        self.assertEqual("0.000 B", fs.byte_units(0))

    def test_negative_bytes_is_an_error(self) -> None:
        """Assert negative storages sizes are invalid."""
        with self.assertRaises(RuntimeError):
            fs.byte_units(-1)

    def test_one_byte_results_in_one_byte(self) -> None:
        """Assert input of 1 results in 1.000."""
        self.assertEqual(fs.byte_units(1), "1.000 B")

    def test_arbitrary_byte_size(self) -> None:
        """Test a random number."""
        self.assertEqual(fs.byte_units(123456789), "123.5 MB")


class HelpFormatterTests(unittest.TestCase):
    """Tests for functions that format --help paragraphs."""

    def test_format_paragraph_for_short_line_returned_as_is(self) -> None:
        """Test that text that is shorter than the line length is returned unchanged."""
        text = "A short line."
        wrapped_text = argparse.format_paragraphs(text, 100)
        self.assertEqual(text, wrapped_text)

    def test_format_paragraph_for_indented_text_returned_as_is(self) -> None:
        """Test that indented text is not changed no matter how long the line is."""
        text = "        This is a very very long line indeed."
        wrapped_text = argparse.format_paragraphs(text, 10)
        self.assertEqual(text, wrapped_text)

    def test_format_paragraph_separates_paragraphs_by_exactly_two_newlines(self) -> None:
        """Test that formatted paragraphs are separated by single blank lines."""
        text = """
The is the first paragraph.

This is the second paragraph.


This is the third paragraph.



This is the fourth paragraph."""

        expected_wrapped_text = (
"""The is the first paragraph.

This is the second paragraph.

This is the third paragraph.

This is the fourth paragraph.""")

        wrapped_text = argparse.format_paragraphs(text, 100)
        self.assertEqual(wrapped_text, expected_wrapped_text)

    def test_format_paragraphs_wraps_long_lines(self) -> None:
        """Test that format_paragraphs correctly wraps long lines."""
        text = "This is a very long line of text that needs to be wrapped to multiple lines."
        max_line_length = 20
        wrapped_text = argparse.format_paragraphs(text, max_line_length)
        first_word_length = 0
        for line in wrapped_text.split("\n"):
            line_length = len(line)

            # Lines are not too long
            self.assertLessEqual(line_length, max_line_length)

            # Line not broken too early
            self.assertTrue(
                line_length + first_word_length > max_line_length or first_word_length == 0)

            first_word_length = len(line.split()[0])

        # Text is not changed except for line breaks.
        self.assertEqual(text, " ".join(wrapped_text.split()))


class ClassifyPathsTests(unittest.TestCase):
    """Tests for classify_paths() function."""

    def test_classify_paths_classifies_files_as_files(self) -> None:
        """Test that classify_paths() correctly identifies files."""
        with tempfile.NamedTemporaryFile() as test_file:
            self.assertEqual(fs.classify_path(Path(test_file.name)), "File")

    def test_classify_paths_classifies_folders_as_folders(self) -> None:
        """Test that classify_paths() correctly identifies folders."""
        with tempfile.TemporaryDirectory() as test_directory:
            self.assertEqual(fs.classify_path(Path(test_directory)), "Folder")

    @unittest.skipIf(
            platform.system() == "Windows",
            "Cannot create symlinks on Windows without elevated privileges.")
    def test_classify_paths_classifies_symlinks_as_symlinks(self) -> None:
        """Test that classify_paths() correctly identifies symlinks."""
        with tempfile.TemporaryDirectory() as test_directory:
            symlink = Path(test_directory)/"symlink"
            symlink.symlink_to(".")
            self.assertEqual(fs.classify_path(symlink), "Symlink")

    def test_classify_paths_classifies_non_existent_files_as_unknown(self) -> None:
        """Test that classify_paths() returns 'Unknown' for non-existent files."""
        self.assertEqual(fs.classify_path(Path(random_string(50))), "Unknown")


class ParseTimeSpanTests(unittest.TestCase):
    """Tests for parse_time_span_to_time_point() function."""

    def test_parse_timespan_with_no_numeric_part_is_an_error(self) -> None:
        """Test that the lack of a number in the argument is an error."""
        with self.assertRaises(CommandLineError):
            dates.parse_time_span_to_timepoint("y")

    def test_parse_timespan_with_no_time_unit_part_is_an_error(self) -> None:
        """Test that the lack of a unit in the argument is an error."""
        with self.assertRaises(CommandLineError):
            dates.parse_time_span_to_timepoint("100")

    def test_parse_timespan_with_small_or_negative_number_is_an_error(self) -> None:
        """Test that the lack of a unit in the argument is an error."""
        with self.assertRaises(CommandLineError):
            dates.parse_time_span_to_timepoint("0.5d")

        with self.assertRaises(CommandLineError):
            dates.parse_time_span_to_timepoint("-2y")

    def test_parse_timespan_with_invalid_time_unit_is_an_error(self) -> None:
        """Test that an unknown time unit raise an exception."""
        with self.assertRaises(CommandLineError):
            dates.parse_time_span_to_timepoint("3u")

    def test_parse_timespan_correctly_calculates_days_ago(self) -> None:
        """Test that arguments of the form "Nd" for some number N gives the correct results."""
        for days in range(1, 10):
            now = datetime.datetime.now()
            then = dates.parse_time_span_to_timepoint(f"{days}d", now)
            self.assertEqual(now - then, datetime.timedelta(days=days))

    def test_parse_timespan_correctly_calculates_weeks_ago(self) -> None:
        """Test that arguments of the form "Nw" for some number N gives the correct results."""
        for weeks in range(1, 10):
            now = datetime.datetime.now()
            then = dates.parse_time_span_to_timepoint(f"{weeks}w", now)
            self.assertEqual(now - then, datetime.timedelta(weeks=weeks))

    def test_parse_timespan_correctly_calculates_months_ago(self) -> None:
        """Test that arguments of the form "Nm" for some number N gives the correct results."""
        now = datetime.datetime(2024, 3, 31, 12, 0, 0)
        expected_then_1 = datetime.datetime(2024, 2, 29, 12, 0, 0)
        then_1 = dates.parse_time_span_to_timepoint("1m", now)
        self.assertEqual(then_1, expected_then_1)

        expected_then_2 = datetime.datetime(2024, 1, 31, 12, 0, 0)
        then_2 = dates.parse_time_span_to_timepoint("2m", now)
        self.assertEqual(then_2, expected_then_2)

    def test_parse_timespan_correctly_calculates_years_ago(self) -> None:
        """Test that arguments of the form "Ny" for some number N gives the correct results."""
        now_1 = datetime.datetime(2024, 2, 29, 12, 0, 0)
        expected_then_1 = datetime.datetime(2023, 2, 28, 12, 0, 0)
        then_1 = dates.parse_time_span_to_timepoint("1y", now_1)
        self.assertEqual(then_1, expected_then_1)

        now_2 = datetime.datetime(2025, 1, 31, 12, 0, 0)
        expected_then_2 = datetime.datetime(2023, 1, 31, 12, 0, 0)
        then_2 = dates.parse_time_span_to_timepoint("2y", now_2)
        self.assertEqual(then_2, expected_then_2)


class RemoveQuotesTests(unittest.TestCase):
    """Tests for remove_quotes() function."""

    def test_remove_quotes_on_string_with_no_quotes_or_spaces_changes_nothing(self) -> None:
        """Test that a string with no quotation marks or spaces is returned unchanged."""
        s = "abc"
        self.assertEqual(s, config.remove_quotes(s))

    def test_remove_quotes_on_string_with_no_quotes_and_end_whitespace_is_stripped(self) -> None:
        """Test that a string with no quotation marks is stripped of leading/trailing whitespace."""
        s = "   abc  "
        self.assertEqual(s.strip(), config.remove_quotes(s))

    def test_remove_quotes_on_string_with_quotes_strips_whitespace_outside_quotes(self) -> None:
        """Test that quotations marks prevent whitespace inside from being stripped."""
        s = '     "  abc  " '
        s_after = "  abc  "
        self.assertEqual(s_after, config.remove_quotes(s))

    def test_remove_quotes_on_string_with_doubled_quotes_preserves_single_quote_pair(self) -> None:
        """Test that a string with doubled quotation marks is returned with outer quotes removed."""
        s = '""   abc  ""'
        s_after = '"   abc  "'
        self.assertEqual(s_after, config.remove_quotes(s))

    def test_remove_quotes_on_string_with_only_starting_quote_is_unchanged(self) -> None:
        """Test that a string with only an initial quotation mark is unchanged."""
        s = '"according to".txt'
        self.assertEqual(s, config.remove_quotes(s))

    def test_remove_quotes_on_string_with_internal_quotes_is_unchanged(self) -> None:
        """Test that quotation marks inside a string have no effect."""
        s = 'this is a "text" file.png'
        self.assertEqual(s, config.remove_quotes(s))

    def test_remove_quotes_on_single_quotation_mark_does_nothing(self) -> None:
        """Test that a string consisting of a single quotation mark is not changed."""
        s = '"'
        self.assertEqual(s, config.remove_quotes(s))


class BackupSpaceWarningTests(unittest.TestCase):
    """Test warning messages when backup size exceeds --free-up parameter."""

    def test_backup_space_logged_when_no_free_up_parameter(self) -> None:
        """Test backup space taken reported if no --free-up parameter."""
        with self.assertLogs(level=logging.INFO) as logs:
            bak.log_backup_size(None, 1)
        self.assertEqual(logs.output, ["INFO:root:Backup space used: 1.000 B"])

    def test_backup_space_logged_when_backup_smaller_than_free_up_parameter(self) -> None:
        """Test space taken reported if backup's size is smaller than --free-up parameter."""
        with self.assertLogs(level=logging.INFO) as logs:
            bak.log_backup_size("10", 2)
        space_message = "INFO:root:Backup space used: 2.000 B (20% of --free-up)"
        self.assertEqual(logs.output, [space_message])

    def test_warning_if_backup_space_close_to_free_up_parameter(self) -> None:
        """Test warning logged if space taken by backup is close to --free-up parameter."""
        with self.assertLogs(level=logging.WARNING) as logs:
            bak.log_backup_size("100", 91)
        prefix = "WARNING:root:"
        space_message = f"{prefix}Backup space used: 91.00 B (91% of --free-up)"
        consider_warning = f"{prefix}Consider increasing the size of the --free-up parameter."
        self.assertEqual(logs.output, [space_message, consider_warning])

    def test_warning_if_backup_space_bigger_than_free_up_parameter(self) -> None:
        """Test warning logged if space taken by backup is larger than --free-up parameter."""
        with self.assertLogs(level=logging.WARNING) as logs:
            bak.log_backup_size("100", 101)
        prefix = "WARNING:root:"
        space_message = f"{prefix}Backup space used: 101.0 B (101% of --free-up)"
        consider_warning = f"{prefix}Consider increasing the size of the --free-up parameter."
        self.assertEqual(logs.output, [space_message, consider_warning])


class LastNBackupTests(TestCaseWithTemporaryFilesAndFolders):
    """Test calls to last_n_backups()."""

    def setUp(self) -> None:
        """Set up old backups for retrieval."""
        super().setUp()
        self.backup_count = 10
        create_old_monthly_backups(self.backup_path, self.backup_count)

    def test_last_n_backups_with_number_argument_returns_correct_number_of_backups(self) -> None:
        """Test that last_n_backups() returns correct number of backups."""
        for n in range(1, self.backup_count + 1):
            self.assertEqual(n, len(moving.last_n_backups(n, self.backup_path)))

    def test_last_n_backups_with_string_argument_returns_correct_number_of_backups(self) -> None:
        """Test that last_n_backups() returns correct number of backups if argument is a string."""
        for n in range(1, self.backup_count + 1):
            self.assertEqual(n, len(moving.last_n_backups(str(n), self.backup_path)))

    def test_last_n_backups_with_all_argument_returns_all_backups(self) -> None:
        """Test that the argument 'all' returns all backups."""
        backups = util.all_backups(self.backup_path)
        all_n_backups = moving.last_n_backups("all", self.backup_path)
        self.assertEqual(backups, all_n_backups)

    def test_all_argument_is_case_insensitive(self) -> None:
        """Test that capitalization does not matter for value 'all'."""
        backups = util.all_backups(self.backup_path)
        all_n_backups = moving.last_n_backups("All", self.backup_path)
        self.assertEqual(backups, all_n_backups)

    def test_non_positive_argument_is_an_error(self) -> None:
        """Test that negative or zero arguments raise an exception."""
        with self.assertRaises(ValueError):
            moving.last_n_backups(0, self.backup_path)

        with self.assertRaises(ValueError):
            moving.last_n_backups("-1", self.backup_path)

    def test_non_numeric_argument_besides_all_is_error(self) -> None:
        """Test that any other string argument besides 'all' is an error."""
        with self.assertRaises(ValueError):
            moving.last_n_backups("most", self.backup_path)

    def test_non_whole_number_arguments_are_errors(self) -> None:
        """Test that decimal number result in errors."""
        with self.assertRaises(ValueError):
            moving.last_n_backups("3.14", self.backup_path)


class ConfirmChoiceMadeTests(unittest.TestCase):
    """Test that confirm_choice_made() limits how options are used."""

    def setUp(self) -> None:
        """Set up test command line arguments."""
        super().setUp()
        self.args = argparse.parse_command_line([
            "--user-folder", "a",
            "--backup-folder", "b"])

    def test_zero_choices_is_an_error(self) -> None:
        """Test that choosing none of a required set is an error."""
        with self.assertRaises(CommandLineError):
            argparse.confirm_choice_made(self.args, "no_argument_1", "no_argument_2")

    def test_one_choice_is_not_an_error(self) -> None:
        """Test that choosing one option has no error."""
        argparse.confirm_choice_made(self.args, "user_folder", "filter")

    def test_two_choices_is_an_error(self) -> None:
        """Test that picking two choices when one is required is an error."""
        with self.assertRaises(CommandLineError):
            argparse.confirm_choice_made(self.args, "user_folder", "backup_folder")


class ConfirmUserLocationIsUnchangedTests(TestCaseWithTemporaryFilesAndFolders):
    """Tests for the funciton confirm_user_location_is_unchanged."""

    def test_no_backups_is_not_an_error_if_missing_ok(self) -> None:
        """Calling the function before any backups is not an error."""
        backup_info.confirm_user_location_is_unchanged(self.user_path, self.backup_path)

    def test_unchanged_user_folder_is_not_an_error(self) -> None:
        """Pass test if the backup location has not changed after a backup."""
        bak.create_new_backup(
            self.user_path,
            self.backup_path,
            filter_file=None,
            examine_whole_file=False,
            force_copy=False,
            copy_probability=0.0,
            timestamp=None)

        backup_info.confirm_user_location_is_unchanged(self.user_path, self.backup_path)

    def test_changed_user_folder_is_an_error(self) -> None:
        """Raise exception if the backup location has changed after a backup."""
        bak.create_new_backup(
            self.user_path,
            self.backup_path,
            filter_file=None,
            examine_whole_file=False,
            force_copy=False,
            copy_probability=0.0,
            timestamp=None)

        with self.assertRaises(CommandLineError) as error:
            backup_info.confirm_user_location_is_unchanged(self.backup_path, self.backup_path)

        self.assertIn("different user folder", error.exception.args[0])


class GenerateConfigTests(TestCaseWithTemporaryFilesAndFolders):
    """Test the generation of configuration files."""

    def assert_config_file_creation(self, command_line: list[str]) -> None:
        """Assert that the config file was created with no errors and with the correct path."""
        with self.assertLogs(level=logging.INFO) as logs:
            main_no_log(command_line)

        gen_config_index = command_line.index("--generate-config")
        config_file_name = command_line[gen_config_index + 1]
        self.assertEqual(
            logs.output,
            [f"INFO:root:Generated configuration file: {config_file_name}"])

    @unittest.skipIf(platform.system() != "Windows", "This test assumes Windows-style paths.")
    def test_generation_of_config_files_with_windows_path_parameters(self) -> None:
        """"Test that command line options with Windows path arguements are correctly recorded."""
        command_line = [
            "--user-folder", r"C:\Users\Alice",
            "--backup-folder", r"D:\Backups",
            "--generate-config", str(self.config_path)]

        self.assert_config_file_creation(command_line)

        expected_config_data = (
fr"""User folder: C:\Users\Alice
Backup folder: D:\Backups
Log: {os.devnull}
""")
        config_data = self.config_path.read_text(encoding="utf8")
        self.assertEqual(expected_config_data, config_data)

    @unittest.skipIf(platform.system() == "Windows", "This test assumes Unix-style paths.")
    def test_generation_of_config_files_with_unix_like_path_parameters(self) -> None:
        """"Test that command line options with Unix-like path arguements are correctly recorded."""
        command_line = [
            "--user-folder", "/home/bob",
            "--backup-folder", r"/mnt/backups/",
            "--generate-config", str(self.config_path)]

        self.assert_config_file_creation(command_line)

        # The
        expected_config_data = (
f"""User folder: /home/bob
Backup folder: /mnt/backups
Log: {os.devnull}
""")
        config_data = self.config_path.read_text(encoding="utf8")
        self.assertEqual(expected_config_data, config_data)

    @unittest.skipIf(platform.system() != "Windows", "This test assumes Windows-style paths.")
    def test_generation_of_config_files_with_short_windows_path_parameters(self) -> None:
        """"Test that short options (-u, -b, etc.) arguments are correctly recorded."""
        command_line = [
            "-u", r"C:\Users\Alice",
            "-b", r"D:\Backups",
            "-f", r"C:\Users\Alice\AppData\vintage_backup_config.txt",
            "--generate-config", str(self.config_path)]

        self.assert_config_file_creation(command_line)

        expected_config_data = (
fr"""User folder: C:\Users\Alice
Backup folder: D:\Backups
Filter: C:\Users\Alice\AppData\vintage_backup_config.txt
Log: {os.devnull}
""")
        config_data = self.config_path.read_text(encoding="utf8")
        self.assertEqual(expected_config_data, config_data)

    @unittest.skipIf(platform.system() == "Windows", "This test assumes Unix-style paths.")
    def test_generation_of_config_files_with_short_unix_path_parameters(self) -> None:
        """"Test that short options (-u, -b, etc.) arguments are correctly recorded."""
        command_line = [
            "-u", r"/home/bob",
            "-b", r"/mnt/backups",
            "-f", r"/home/bob/.config/vintage_backup_config.txt",
            "--generate-config", str(self.config_path)]

        self.assert_config_file_creation(command_line)

        expected_config_data = (
fr"""User folder: /home/bob
Backup folder: /mnt/backups
Filter: /home/bob/.config/vintage_backup_config.txt
Log: {os.devnull}
""")
        config_data = self.config_path.read_text(encoding="utf8")
        self.assertEqual(expected_config_data, config_data)

    def test_generation_of_config_files_with_toggle_parameters(self) -> None:
        """Test that command line options with toggle parameters (no arguments) are recorded."""
        command_line = [
            "--compare-contents",
            "--generate-config", str(self.config_path)]

        self.assert_config_file_creation(command_line)

        expected_config_data = (
f"""Compare contents:
Log: {os.devnull}
""")
        config_data = self.config_path.read_text(encoding="utf8")
        self.assertEqual(expected_config_data, config_data)

    def test_generation_of_config_files_with_negated_toggle_parameters(self) -> None:
        """Test that command line options with negated toggle parameters are not recorded."""
        command_line = [
            "--compare-contents",
            "--no-compare-contents",
            "--generate-config", str(self.config_path)]

        self.assert_config_file_creation(command_line)

        expected_config_data = f"Log: {os.devnull}\n"
        config_data = self.config_path.read_text(encoding="utf8")
        self.assertEqual(expected_config_data, config_data)

    def test_generation_of_config_files_from_another_config_file(self) -> None:
        """Test that the parameters in a --config file get included into the new config file."""
        self.config_path.write_text(
fr"""User folder: {self.user_path}
Backup folder: {self.backup_path}
Filter: {self.filter_path}
Log: {os.devnull}
""",
encoding="utf8")

        generated_config_path = self.user_path/"gen_config.txt"
        self.assert_config_file_creation([
            "--config", str(self.config_path),
            "--generate-config", str(generated_config_path)])
        self.assertEqual(
            self.config_path.read_text(encoding="utf8"),
            generated_config_path.read_text(encoding="utf8"))

    def test_generation_of_config_files_when_name_already_exists(self) -> None:
        """Test that a generated config file does not clobber an existing file."""
        command_line = [
            "--compare-contents",
            "--generate-config", str(self.config_path)]

        self.config_path.touch()
        with self.assertLogs(level=logging.INFO) as logs:
            main_no_log(command_line)
        actual_config_path = self.config_path.with_suffix(f".1{self.config_path.suffix}")
        self.assertEqual(
            logs.output,
            [f"INFO:root:Generated configuration file: {actual_config_path}"])

        expected_config_data = (
f"""Compare contents:
Log: {os.devnull}
""")
        config_data = actual_config_path.read_text(encoding="utf8")
        self.assertEqual(expected_config_data, config_data)

    def test_generate_config_for_verify_action(self) -> None:
        """Test that config files for non-backup actions can be scripted."""
        command_line = [
            "--generate-config", str(self.config_path),
            "--verify", str(self.user_path),
            "--user-folder", str(self.user_path),
            "--backup-folder", str(self.backup_path)]

        self.assert_config_file_creation(command_line)

        expected_config_data = (
f"""Verify: {self.user_path}
User folder: {self.user_path}
Backup folder: {self.backup_path}
Log: {os.devnull}
""")
        actual_config_data = self.config_path.read_text(encoding="utf8")
        self.assertEqual(actual_config_data, expected_config_data)


class GenerateWindowsScriptFilesTests(TestCaseWithTemporaryFilesAndFolders):
    """Make sure that script files for Windows Scheduler are generated correctly."""

    @unittest.skipIf(platform.system() != "Windows", "Only applicable to Windows systems.")
    def test_that_scripts_are_generated_correctly(self) -> None:
        """Make sure that the config file, batch script, and VB script are generated correctly."""
        # Generate all scripts
        args = [
            "-u", str(self.user_path),
            "-b", str(self.backup_path),
            "--compare-contents",
            "-f", str(self.user_path/"filter.txt"),
            "--generate-windows-scripts", str(self.user_path)]

        with self.assertLogs(level=logging.INFO) as logs:
            main_no_log(args)

        prefix = "INFO:root:"
        self.assertEqual(
            logs.output, [
                f"{prefix}Generated configuration file: {self.user_path/'config.txt'}",
                f"{prefix}Generated batch script: {self.user_path/'batch_script.bat'}",
                f"{prefix}Generated VB script: {self.user_path/'vb_script.vbs'}"])

        # Check contents of configuration file
        expected_config_contents = (
f"""User folder: {self.user_path}
Backup folder: {self.backup_path}
Filter: {self.user_path/'filter.txt'}
Compare contents:
Log: nul
""")
        config_path = self.user_path/"config.txt"
        actual_config_contents = config_path.read_text()
        self.assertEqual(expected_config_contents, actual_config_contents)

        # Check contents of batch script file
        main_path = fs.absolute_path(cast(str, getsourcefile(main)))
        vintage_backup_file = main_path.parent/"vintagebackup.py"
        python_version = ".".join(map(str, sys.version_info[:2]))
        expected_batch_script = (
            f'py -{python_version} "{vintage_backup_file}" --config "{config_path}"\n')
        batch_script_path = self.user_path/"batch_script.bat"
        actual_batch_script = batch_script_path.read_text()
        self.assertEqual(expected_batch_script, actual_batch_script)

        # Check contents of VB script file
        expected_vb_script_contents = (
f'''Dim Shell
Set Shell = CreateObject("WScript.Shell")
Shell.Run """{batch_script_path}""", 0, true
Set Shell = Nothing
''')
        vb_script_path = self.user_path/"vb_script.vbs"
        actual_vb_script_contents = vb_script_path.read_text()
        self.assertEqual(expected_vb_script_contents, actual_vb_script_contents)

    @unittest.skipIf(platform.system() != "Windows", "Only applicable to Windows systems.")
    def test_that_generated_scripts_do_not_clobber_existing_files(self) -> None:
        """Make sure that no existing files are clobbered when generating files."""
        # Create all files before generating
        for file_name in ("config.txt", "batch_script.bat", "vb_script.vbs"):
            (self.user_path/file_name).touch()

        # Generate all scripts
        args = [
            "-u", str(self.user_path),
            "-b", str(self.backup_path),
            "--compare-contents",
            "-f", str(self.user_path/"filter.txt"),
            "--generate-windows-scripts", str(self.user_path)]

        with self.assertLogs(level=logging.INFO) as logs:
            main_no_log(args)

        actual_config_path = self.user_path/"config.1.txt"
        actual_batch_path = self.user_path/"batch_script.1.bat"
        actual_vb_path = self.user_path/"vb_script.1.vbs"
        prefix = "INFO:root:"
        self.assertEqual(
            logs.output, [
                f"{prefix}Generated configuration file: {actual_config_path}",
                f"{prefix}Generated batch script: {actual_batch_path}",
                f"{prefix}Generated VB script: {actual_vb_path}"])

        # Check contents of configuration file
        expected_config_contents = (
f"""User folder: {self.user_path}
Backup folder: {self.backup_path}
Filter: {self.user_path/'filter.txt'}
Compare contents:
Log: nul
""")
        actual_config_contents = actual_config_path.read_text()
        self.assertEqual(expected_config_contents, actual_config_contents)

        # Check contents of batch script file
        main_path = fs.absolute_path(cast(str, getsourcefile(main)))
        vintage_backup_file = main_path.parent/"vintagebackup.py"
        python_version = f"{sys.version_info[0]}.{sys.version_info[1]}"
        expected_batch_script = (
            f'py -{python_version} "{vintage_backup_file}" --config "{actual_config_path}"\n')
        actual_batch_script = actual_batch_path.read_text()
        self.assertEqual(expected_batch_script, actual_batch_script)

        # Check contents of VB script file
        expected_vb_script_contents = (
f'''Dim Shell
Set Shell = CreateObject("WScript.Shell")
Shell.Run """{actual_batch_path}""", 0, true
Set Shell = Nothing
''')
        actual_vb_script_contents = actual_vb_path.read_text()
        self.assertEqual(expected_vb_script_contents, actual_vb_script_contents)


class LogTests(TestCaseWithTemporaryFilesAndFolders):
    """Tests for log files."""

    def test_log_option_specifies_log_file(self) -> None:
        """Pick log file from --log option."""
        selected_log_path = backup_info.primary_log_path(str(self.log_path), None)
        self.assertEqual(self.log_path, selected_log_path)

    def test_backup_folder_determines_log_without_log_path(self) -> None:
        """Select log file from previous backup."""
        create_user_data(self.user_path)
        exit_code = main.main([
            "--user-folder", str(self.user_path),
            "--backup-folder", str(self.backup_path),
            "--log", str(self.log_path)],
            testing=True)
        self.assertEqual(exit_code, 0)
        selected_log_path = backup_info.primary_log_path(None, str(self.backup_path))
        self.assertEqual(selected_log_path, self.log_path)
        selected_log_path = backup_info.primary_log_path("", str(self.backup_path))
        self.assertEqual(selected_log_path, self.log_path)

    def test_log_option_overrides_backup_folder_log_record(self) -> None:
        """Use chosen log file if specified despite a recorded log file from previous backup."""
        create_user_data(self.user_path)
        exit_code = main.main([
            "--user-folder", str(self.user_path),
            "--backup-folder", str(self.backup_path),
            "--log", str(self.log_path)],
            testing=True)
        self.assertEqual(exit_code, 0)
        log_path_2 = self.user_path/"log2.txt"
        selected_log_file = backup_info.primary_log_path(str(log_path_2), str(self.backup_path))
        self.assertEqual(selected_log_file, log_path_2)

    def test_return_default_log_if_no_log_and_backup_folder_specified(self) -> None:
        """Return default log file if no log and no previous backup but backup folder specified."""
        selected_log_file = backup_info.primary_log_path(None, str(self.backup_path))
        self.assertEqual(selected_log_file, fs.default_log_file_name)

    def test_return_none_if_os_devnull_is_specified(self) -> None:
        """Return nul or /dev/null if the user selects it."""
        null_file = "nul" if platform.system() == "Windows" else "/dev/null"
        selected_log_file = backup_info.primary_log_path(null_file, str(self.backup_path))
        self.assertIsNone(selected_log_file)
        selected_log_file = backup_info.primary_log_path(null_file, None)
        self.assertIsNone(selected_log_file)

    def test_return_none_if_no_log_and_no_backup_folder(self) -> None:
        """Return None if no log and no backup folder are specified (nothing worth logging)."""
        selected_log_file = backup_info.primary_log_path(None, None)
        self.assertIsNone(selected_log_file)

    def test_logs_written_to_log_file(self) -> None:
        """Check that log file contents match the log output."""
        create_user_data(self.user_path)
        with self.assertLogs(level=logging.INFO) as log_record:
            logs.setup_log_file(str(self.log_path), None, str(self.backup_path), debug=False)
            default_backup(self.user_path, self.backup_path)
            close_all_file_logs()

        with self.log_path.open(encoding="utf8") as log_file:
            for log_line, file_log_line in itertools.zip_longest(
                    log_record.output,
                    log_file,
                    fillvalue=""):
                log_message = log_line.split(":", maxsplit=2)[2].strip()
                file_message = "".join(file_log_line.split(maxsplit=3)[3:]).strip()
                self.assertEqual(log_message, file_message)

    def test_recorded_log_file_used_when_next_backup_does_not_specify_log(self) -> None:
        """Test that subsequent backups use the same log file even if --log is not specified."""
        self.assertNotEqual(fs.default_log_file_name.absolute(), self.log_path.absolute())
        self.assertFalse(self.log_path.is_file())

        create_user_data(self.user_path)
        exit_code = main.main([
            "-u", str(self.user_path),
            "-b", str(self.backup_path),
            "-l", str(self.log_path),
            "--timestamp", unique_timestamp_string()],
            testing=True)
        self.assertEqual(exit_code, 0)

        if fs.default_log_file_name.is_file():
            with fs.default_log_file_name.open(encoding="utf8") as default_log:
                for line in default_log:
                    self.assertNotIn(str(self.user_path), line)

        self.assertTrue(self.log_path.is_file())
        log_size = self.log_path.stat().st_size
        self.assertGreater(log_size, 0)

        exit_code = main.main([
            "-u", str(self.user_path),
            "-b", str(self.backup_path),
            "--timestamp", unique_timestamp_string()],
            testing=True)
        self.assertEqual(exit_code, 0)

        self.assertTrue(self.log_path.is_file())
        self.assertGreater(self.log_path.stat().st_size, log_size)

        if fs.default_log_file_name.is_file():
            with fs.default_log_file_name.open(encoding="utf8") as default_log:
                for line in default_log:
                    self.assertNotIn(str(self.user_path), line)


class UniquePathNameTests(TestCaseWithTemporaryFilesAndFolders):
    """Tests that unique_path_name() prevents file overwriting."""

    def test_non_existing_file_name_returns_same_name(self) -> None:
        """Test that if a path name does not exist, the same name is returned."""
        path = self.user_path/"non-existent.txt"
        self.assertEqual(path, fs.unique_path_name(path))

    def test_existing_file_name_result_in_1_appended_to_name(self) -> None:
        """Test that an existing path name is replaced with a 1 just before the suffix."""
        path = self.user_path/"existing.txt"
        path.touch()
        self.assertEqual(self.user_path/"existing.1.txt", fs.unique_path_name(path))

    def test_multiple_existing_file_names_result_in_increasing_appended_numbers(self) -> None:
        """Test that the appended number increases until a non-existent name is found."""
        path = self.user_path/"existing.txt"
        path.touch()
        for number in range(1, 10):
            new_path_name = self.user_path/f"existing.{number}.txt"
            self.assertEqual(new_path_name, fs.unique_path_name(path))
            new_path_name.touch()

    def test_unique_path_original_name_exists(self) -> None:
        """Test that the unchanged path name exists after creation via unique_path_name()."""
        path = self.user_path/"unique.txt"
        unique_path = fs.unique_path_name(path)
        self.assertEqual(path, unique_path)
        unique_path.touch()
        self.assertTrue(unique_path.exists())
        self.assertEqual(fs.find_unique_path(path), path)

    def test_unique_path_new_name_exists(self) -> None:
        """Test that a unique path name exists after creation via unique_path_name()."""
        path = self.user_path/"unique.txt"
        path.touch()
        unique_path = fs.unique_path_name(path)
        self.assertNotEqual(path, unique_path)
        unique_path.touch()
        path.unlink()
        self.assertEqual(fs.find_unique_path(path), unique_path)

    def test_unique_path_new_name_with_gap_exists(self) -> None:
        """Test that a unique path name exists after creation via many unique_path_name() calls."""
        path = self.user_path/"unique.txt"
        path.touch()
        count = 3
        unique_files = [path]
        for _ in range(count):
            unique_path = fs.unique_path_name(path)
            self.assertNotIn(unique_path, unique_files)
            unique_path.touch()
            unique_files.append(unique_path)

        gapped_unique_file = unique_files[-1]
        for p in unique_files[:-1]:
            p.unlink()
        self.assertEqual(fs.find_unique_path(path), gapped_unique_file)

    def test_find_unique_path_returns_numbered_file_with_highest_number(self) -> None:
        """Test that only the unique file name with the highest number is returned."""
        path = self.user_path/"unique.txt"
        path.touch()
        unique_file_count = 50
        last_unique_path = path
        for _ in range(unique_file_count):
            new_path = fs.unique_path_name(path)
            new_path.touch()
            last_unique_path = new_path
        self.assertNotEqual(path, last_unique_path)
        found_unique_path = fs.find_unique_path(path)
        self.assertEqual(found_unique_path, last_unique_path)

    def test_find_unique_path_returns_none_if_no_version_of_path_exists(self) -> None:
        """Test that find_unique_path() returns None of no version of the file exists."""
        self.assertIsNone(fs.find_unique_path(self.user_path/"does_not_exists.txt"))


def close_all_file_logs() -> None:
    """Close error file to prevent errors when leaving assertLogs contexts."""
    logger = logging.getLogger()
    file_handlers = list(filter(lambda h: isinstance(h, logging.FileHandler), logger.handlers))
    for handler in file_handlers:
        handler.close()
        logger.removeHandler(handler)


class ErrorLogTests(TestCaseWithTemporaryFilesAndFolders):
    """Tests for generating a second log file with only errors."""

    def setUp(self) -> None:
        """Create path for error log file."""
        super().setUp()
        self.error_log = self.user_path/"errors.log"

    def test_no_errors_results_in_no_error_file(self) -> None:
        """If no warnings or errors are logged, then the error file is not created."""
        create_user_data(self.user_path)
        main_assert_no_error_log([
            "-u", str(self.user_path),
            "-b", str(self.backup_path),
            "--error-log", str(self.error_log)],
            self)

        self.assertFalse(self.error_log.exists())

    def test_errors_result_in_error_file(self) -> None:
        """If warnings/errors are logged, the error file is created with the warnings/errors."""
        non_existent_folder = self.user_path.parent/"non-existent"
        with self.assertLogs(level=logging.WARNING) as error_logs:
            main_no_log([
                "-u", str(non_existent_folder),
                "-b", str(self.backup_path),
                "--error-log", str(self.error_log)])
            close_all_file_logs()

        self.assertTrue(self.error_log.is_file())

        def error_file_line_message(line: str) -> str:
            return line.split(maxsplit=3)[-1].removesuffix("\n")

        with self.error_log.open(encoding="utf8") as error_file:
            error_file_lines = list(map(error_file_line_message, error_file))

        def error_log_line_message(line: str) -> str:
            return line.split(":", maxsplit=2)[-1]

        error_log_lines = list(map(error_log_line_message, error_logs.output))
        self.assertEqual(error_log_lines, error_file_lines)


class HelpTests(unittest.TestCase):
    """Make sure argument parser help commands run without error."""

    def test_full_help_text_prints_without_error(self) -> None:
        """Test help text has no errors."""
        with self.assertNoLogs():
            ignore = io.StringIO()
            argparse.print_help(ignore)

    def test_usage_text_prints_without_error(self) -> None:
        """Test help text has no errors."""
        with self.assertNoLogs():
            ignore = io.StringIO()
            argparse.print_usage(ignore)


class MenuTests(unittest.TestCase):
    """Test that console menus work correctly."""

    def test_menu_selection(self) -> None:
        """Check that user selections correspond to choices."""
        choices = list("abc")
        for selection, letter in enumerate(choices, 1):
            index = console.choose_from_menu(choices, "", selection)
            self.assertEqual(choices[index], letter)


class RunTitleTests(TestCaseWithTemporaryFilesAndFolders):
    """Test the printing of run titles."""

    def test_odd_length_titles_are_printed_centered_between_equal_sign_borders(self) -> None:
        """Test that a title with an odd number of characters is centered between the borders."""
        title = "An odd-length title"
        expected_log_text = [
            "",
            "=====================",
            " " + title,
            "=====================",
            ""]
        log_prefix = "INFO:root:"
        expected_logs = [f"{log_prefix}{line}" for line in expected_log_text]

        args = argparse.parse_command_line([])
        with self.assertLogs(level=logging.INFO) as logs:
            console.print_run_title(args, title)

        self.assertEqual(expected_logs, logs.output)

    def test_even_length_titles_are_printed_centered_between_equal_sign_borders(self) -> None:
        """Test that a title with an even number of characters is centered between the borders."""
        title = "An even-length title"
        expected_log_text = [
            "",
            "======================",
            " " + title,
            "======================",
            ""]
        log_prefix = "INFO:root:"
        expected_logs = [f"{log_prefix}{line}" for line in expected_log_text]

        args = argparse.parse_command_line([])
        with self.assertLogs(level=logging.INFO) as logs:
            console.print_run_title(args, title)

        self.assertEqual(expected_logs, logs.output)

    def test_config_file_path_is_printed_below_title(self) -> None:
        """Test that a title with an even number of characters is centered between the borders."""
        title = "An odd-length title"
        self.config_path.touch()
        expected_log_text = [
            "",
            "=====================",
            " " + title,
            "=====================",
            "",
            f"Reading configuration from file: {self.config_path}",
            ""]
        log_prefix = "INFO:root:"
        expected_logs = [f"{log_prefix}{line}" for line in expected_log_text]

        args = argparse.parse_command_line(["--config", str(self.config_path)])
        with self.assertLogs(level=logging.INFO) as logs:
            console.print_run_title(args, title)

        self.assertEqual(expected_logs, logs.output)


class CancelKeyTests(unittest.TestCase):
    """Test that key combination for canceling a program run is correct on different OSes."""

    @unittest.skipIf(platform.system() == "Darwin", "This test is for Windows and Linux.")
    def test_non_mac_cancel_key(self) -> None:
        """Test that cancel_key() returns 'Ctrl-C'."""
        self.assertEqual(console.cancel_key(), "Ctrl-C")

    @unittest.skipUnless(platform.system() == "Darwin", "This test is for MacOS.")
    def test_mac_cancel_key(self) -> None:
        """Test that cancel_key() returns 'Cmd-C'."""
        self.assertEqual(console.cancel_key(), "Cmd-C")


class ValidPathsTests(TestCaseWithTemporaryFilesAndFolders):
    """Test of valid backup paths testing."""

    def test_existing_user_folder_and_backup_folder_results_in_no_exceptions(self) -> None:
        """An existing user folder and backup folder raise no exceptions."""
        bak.check_paths_for_validity(self.user_path, self.backup_path, None)

    def test_existing_user_folder_and_non_existent_backup_folder_raises_no_exceptions(self) -> None:
        """An existing user folder and non-existent backup folder raises no exceptions."""
        fs.delete_directory_tree(self.backup_path)
        bak.check_paths_for_validity(self.user_path, self.backup_path, None)
        self.make_new_backup_folder()

    def test_all_paths_exist_raises_no_exceptions(self) -> None:
        """User folder, backup folder, and filter file existing raises no exceptions."""
        self.filter_path.touch()
        bak.check_paths_for_validity(self.user_path, self.backup_path, self.filter_path)

    def test_non_existent_user_folder_raises_exception(self) -> None:
        """A missing user folder raises a CommandLineError."""
        with self.assertRaises(CommandLineError) as error:
            bak.check_paths_for_validity(
                self.user_path/"non-existent",
                self.backup_path,
                None)
        self.assertIn("The user folder path is not a folder", error.exception.args[0])

    def test_user_folder_is_file_raises_exception(self) -> None:
        """A user folder that's actually a file raises a CommandLineError."""
        with self.assertRaises(CommandLineError) as error:
            user_file = self.user_path/"a_file.txt"
            user_file.touch()
            bak.check_paths_for_validity(user_file, self.backup_path, None)
        self.assertIn("The user folder path is not a folder", error.exception.args[0])

    def test_backup_folder_is_file_raises_exception(self) -> None:
        """A backup folder that's actually a file raises a CommandLineError."""
        with self.assertRaises(CommandLineError) as error:
            backup_file = self.backup_path/"a_file.txt"
            backup_file.touch()
            bak.check_paths_for_validity(self.user_path, backup_file, None)
        self.assertIn("Backup location exists but is not a folder", error.exception.args[0])

    def test_backup_folder_inside_user_folder_raises_exception(self) -> None:
        """Backing up a user folder to a location inside that folder raises a CommandLineError."""
        with self.assertRaises(CommandLineError) as error:
            bak.check_paths_for_validity(self.user_path, self.user_path/"backup", None)
        self.assertIn("Backup destination cannot be inside user's folder:", error.exception.args[0])

    def test_non_existent_filter_path_raises_exception(self) -> None:
        """Specifying a filter path that doesn't exist raises a CommandLineError."""
        with self.assertRaises(CommandLineError) as error:
            bak.check_paths_for_validity(self.user_path, self.backup_path, self.filter_path)
        self.assertIn("Filter file not found", error.exception.args[0])


class MonthsAgoTests(unittest.TestCase):
    """Check calculations of calendar months ago."""

    def test_middle_of_month_dates_in_same_year_result_in_only_month_changing(self) -> None:
        """Dates in the middle of the month only change month within the same eyar."""
        date = datetime.date(2025, 12, 13)
        for previous_month in range(12):
            calculated_date = dates.months_ago(date, previous_month)
            new_month = date.month - previous_month
            expected_date = datetime.date(date.year, new_month, date.day)
            self.assertEqual(expected_date, calculated_date)

    def test_end_of_month_date_results_in_end_of_month_date(self) -> None:
        """An end-of-month date result in an end-of-month date if the result is a shorter month."""
        date = datetime.date(2023, 3, 31)
        calculated_date = dates.months_ago(date, 1)
        expected_date = datetime.date(2023, 2, 28)
        self.assertEqual(expected_date, calculated_date)

        date = datetime.date(2024, 3, 31)
        calculated_date = dates.months_ago(date, 1)
        expected_date = datetime.date(2024, 2, 29)
        self.assertEqual(expected_date, calculated_date)

    def test_months_ago_correctly_handles_crossing_year_boundries(self) -> None:
        """Ensure calculated date is correct when result is in previous year."""
        date = datetime.date(2021, 5, 31)
        calculated_date = dates.months_ago(date, 7)
        expected_date = datetime.date(2020, 10, 31)
        self.assertEqual(expected_date, calculated_date)


class ConsoleMenuTests(unittest.TestCase):
    """Tests for the console menu function."""

    def test_menu_is_printed_correctly(self) -> None:
        """Test that the menu entries display items in correct order and spaced correctly."""
        menu_output = io.StringIO()
        choices = ["a", "b", "c"]
        choice = "b"
        index = choices.index(choice)
        menu_choice = index + 1
        result = console.choose_from_menu(
            choices,
            "Choose",
            test_choice=menu_choice,
            output=menu_output)
        self.assertEqual(result, index)
        self.assertEqual(choices[index], choice)

        expected_output = """
1: a
2: b
3: c
""".removeprefix("\n")
        self.assertEqual(expected_output, menu_output.getvalue())

    def test_long_menu_is_printed_correctly(self) -> None:
        """Ensure entries are aligned even with multi-digit numbering."""
        length = 100
        choices = list(map(str, range(1, length + 1)))
        choice = 1
        menu_text = io.StringIO()
        result = console.choose_from_menu(choices, "Choose", test_choice=choice, output=menu_text)
        self.assertEqual(choice - 1, result)
        expected_text = "".join(f"{i:>3}: {i}\n" for i in range(1, length + 1))
        self.assertEqual(expected_text, menu_text.getvalue())

    def test_menu_prints_instructions_for_wrong_input(self) -> None:
        """Ensure entries are aligned even with multi-digit numbering."""
        choices = list(map(str, range(1, 4)))
        user_choices = [0, 4, 2]
        menu_text = io.StringIO()
        result = console.choose_from_menu(
            choices,
            "Choose",
            test_choice=user_choices,
            output=menu_text)
        self.assertEqual(user_choices[-1] - 1, result)
        expected_text = """
1: 1
2: 2
3: 3
Enter a number from 1 to 3
Enter a number from 1 to 3
""".removeprefix("\n")
        self.assertEqual(expected_text, menu_text.getvalue())


class ArgumentParserTests(unittest.TestCase):
    """Tests of custom argument parser behavior."""

    def test_inclusion_of_program_name_in_arguments_does_not_change_parse_output(self) -> None:
        """Parsing arguments is not affected by the program name in the first argument."""
        args = ["--user-folder", "/user/things", "--backup-folder", "/backup/things"]
        program_args = [sys.argv[0], *args]
        self.assertNotEqual(program_args[0], "")
        self.assertEqual(
            argparse.parse_command_line(args),
            argparse.parse_command_line(program_args))


class FolderNavigationTests(TestCaseWithTemporaryFilesAndFolders):
    """Tests of functions for navigating within backups."""

    def test_path_relative_to_backups_fails_when_no_backups(self) -> None:
        """If there are no backups, then path_relative_to_backups() fails."""
        with self.assertRaises(CommandLineError) as error:
            recovery.path_relative_to_backups(self.user_path/"test.txt", self.backup_path)
        self.assertTrue(error.exception.args[0].startswith("No backups found at "))

    def test_path_relative_to_backups_fails_for_paths_outside_of_user_folder(self) -> None:
        """If the user path is outside the user folder, path_relative_to_backups() fails."""
        create_user_data(self.user_path)
        default_backup(self.user_path, self.backup_path)

        with self.assertRaises(CommandLineError) as error:
            recovery.path_relative_to_backups(self.user_path.parent/"file.txt", self.backup_path)
        self.assertIn(" is not contained in the backup set ", error.exception.args[0])

    def test_path_relative_to_backups_returns_path_relative_to_user_folder(self) -> None:
        """Function path_relative_to_backups() returns path relative to backed up user folder."""
        create_user_data(self.user_path)
        default_backup(self.user_path, self.backup_path)
        file = self.user_path/"random_file.txt"
        relative_file = recovery.path_relative_to_backups(file, self.backup_path)
        self.assertEqual(self.user_path/relative_file, file)
        folder = self.user_path/"folder"/"folder"/"folder"
        relative_folder = recovery.path_relative_to_backups(folder, self.backup_path)
        self.assertEqual(self.user_path/relative_folder, folder)

    def test_directory_relative_to_backup_fails_for_non_directory_path(self) -> None:
        """Function directory_relative_to_backup() fails if argument is not a directory."""
        create_user_data(self.user_path)
        default_backup(self.user_path, self.backup_path)

        with self.assertRaises(CommandLineError) as error:
            recovery.directory_relative_to_backup(self.user_path/"root_file.txt", self.backup_path)
        self.assertTrue(
            error.exception.args[0].startswith("The given search path is not a directory: "))

    def test_directory_relative_to_backup_returns_directory_relative_to_user_folder(self) -> None:
        """Test directory_relative_to_backup() returns paths relative to backed up user folder."""
        create_user_data(self.user_path)
        default_backup(self.user_path, self.backup_path)
        folder = self.user_path/"sub_directory_1"/"sub_sub_directory_2"
        relative_folder = recovery.directory_relative_to_backup(folder, self.backup_path)
        self.assertEqual(self.user_path/relative_folder, folder)


class BackupInfoTests(TestCaseWithTemporaryFilesAndFolders):
    """Tests for reading and writing backup info files."""

    def test_backup_source_returns_none_if_there_is_no_backup_info_file(self) -> None:
        """Test that it is an error when backup source queried and there is no info file."""
        self.assertIsNone(backup_info.backup_source(self.backup_path))

    def test_backup_log_file_returns_none_if_there_is_no_backup_info_file(self) -> None:
        """Test that None is returned for log file if there is no backup info file."""
        self.assertIsNone(backup_info.backup_log_file(self.backup_path))

    def test_backup_source_is_written_after_backup(self) -> None:
        """Test that the backed up folder is written to the backup info file."""
        backup_info_file = backup_info.get_backup_info_file(self.backup_path)
        self.assertFalse(backup_info_file.exists())
        default_backup(self.user_path, self.backup_path)
        self.assertTrue(backup_info_file.exists())
        backup_source = backup_info.backup_source(self.backup_path)
        self.assertEqual(backup_source, self.user_path)

    def test_backup_log_is_written_after_backup(self) -> None:
        """Test that the log for the backup is written to the backup info_file."""
        backup_info_file = backup_info.get_backup_info_file(self.backup_path)
        self.assertFalse(backup_info_file.exists())
        self.assertFalse(self.log_path.exists())
        create_user_data(self.user_path)
        exit_code = main.main([
            "-u", str(self.user_path),
            "-b", str(self.backup_path),
            "--log", str(self.log_path)],
            testing=True)
        self.assertEqual(exit_code, 0)
        self.assertTrue(backup_info_file.exists())
        self.assertTrue(self.log_path.exists())
        actual_log_file = backup_info.backup_log_file(self.backup_path)
        self.assertEqual(self.log_path, actual_log_file)

    def test_blank_lines_are_ignored_in_backup_source_files(self) -> None:
        """Test that blank lines in a backup source file do not change behavior."""
        backup_info_file = backup_info.get_backup_info_file(self.backup_path)
        self.assertFalse(backup_info_file.exists())
        create_user_data(self.user_path)
        exit_code = main.main([
            "-u", str(self.user_path),
            "-b", str(self.backup_path),
            "--log", str(self.log_path)],
            testing=True)
        self.assertEqual(exit_code, 0)
        self.assertTrue(backup_info_file.exists())

        original_backup_info = backup_info.read_backup_information(self.backup_path)
        temp_backup_info_file = fs.unique_path_name(backup_info_file)
        with (backup_info_file.open(encoding="utf8") as reader,
            temp_backup_info_file.open("w", encoding="utf8") as writer):

            for line in reader:
                writer.write(line)
                writer.write("  \n")

        backup_info_file.rename(fs.unique_path_name(backup_info_file))
        temp_backup_info_file.rename(backup_info_file)
        new_backup_info = backup_info.read_backup_information(self.backup_path)

        self.assertEqual(original_backup_info, new_backup_info)


def run_find_missing_files(
        method: Invocation,
        backup_path: Path,
        result_directory: Path,
        *,
        debug: bool) -> int:
    """
    Run the missing files function either from a function call or from command line arguments.

    Arguments:
        method: How to run the function under test: direct call or command line arguments
        backup_path: The path to the base backup directory that holds all backups
        result_directory: The directory where the missing_files.txt file will be written
        debug: Whether to turn on debug logging

    Returns:
        exit_code: The exit code of the program run: zero for success and non-zero for failure
    """
    if method == Invocation.function:
        find_missing.find_missing_files(backup_path, None, result_directory)
        return 0
    else:
        args = [
            "--find-missing", str(result_directory),
            "-b", str(backup_path),
            "-l", os.devnull]

        if debug:
            args.append("--debug")

        return main.main(args, testing=True)


class FindMissingFilesTests(TestCaseWithTemporaryFilesAndFolders):
    """Test the --find-missing functions."""

    def test_no_missing_files_results_in_no_warning_logs_and_no_list_files(self) -> None:
        """Test that no WARNING log messages are printed if no missing files are found."""
        create_user_data(self.user_path)
        default_backup(self.user_path, self.backup_path)
        backup = util.find_previous_backup(self.backup_path)
        self.assertIsNotNone(backup)
        backup = cast(Path, backup)

        for method in Invocation:
            with self.assertNoLogs(level=logging.WARNING):
                exit_code = run_find_missing_files(
                    method,
                    self.backup_path,
                    self.user_path,
                    debug=False)
                self.assertEqual(exit_code, 0, method)

            list_file = self.user_path/"missing_files.txt"
            self.assertFalse(list_file.exists(), method)

    def test_missing_file_results_in_warning_and_debug_output_and_list_file(self) -> None:
        """
        Test that WARNING and DEBUG messages are printed if missing files are found.

        Process messages are printed as warnings.
        Files are printed as DEBUG.
        """
        create_user_data(self.user_path)
        default_backup(self.user_path, self.backup_path)
        backup = util.find_previous_backup(self.backup_path)
        self.assertIsNotNone(backup)
        backup = cast(Path, backup)

        missing_file = self.user_path/"sub_directory_1"/"sub_root_file.txt"
        missing_file.unlink()

        for method in Invocation:
            with self.assertLogs(level=logging.DEBUG) as logs:
                exit_code = run_find_missing_files(
                    method,
                    self.backup_path,
                    self.user_path,
                    debug=True)
                self.assertEqual(exit_code, 0, method)

            list_file = self.user_path/"missing_files.txt"
            warning_log_messages = [
                f"Files missing from user folder {self.user_path} found in {self.backup_path}",
                f"Copying list to {list_file}"]
            debug_log_messages = [
                f"{missing_file.relative_to(self.user_path).parent}",
                f"    {missing_file.name}    last seen: {backup.name}"]

            warning_log_output = [f"WARNING:root:{message}" for message in warning_log_messages]
            debug_log_output = [f"DEBUG:root:{message}" for message in debug_log_messages]

            log_output: list[str] = []
            for line in logs.output:
                if line.startswith("INFO:"):
                    continue

                if line.startswith("DEBUG:root:Namespace"):
                    continue

                log_output.append(line)

            self.assertEqual(log_output, warning_log_output + debug_log_output, method)

            file_lines = [f"Missing user files found in {self.backup_path}:", *debug_log_messages]
            file_contents = "\n".join(file_lines) + "\n"
            self.assertTrue(list_file.is_file(), method)
            self.assertEqual(list_file.read_text(encoding="utf8"), file_contents, method)
            list_file.unlink()

    def test_missing_files_in_backups_outputs_warning_and_debug_and_list_file(self) -> None:
        """
        Test that WARNING and DEBUG messages are printed if multiple missing files are found.

        Process messages are printed as warnings.
        Files are printed as DEBUG in sorted order.
        """
        create_user_data(self.user_path)
        default_backup(self.user_path, self.backup_path)

        missing_file_1 = self.user_path/"sub_directory_2"/"sub_sub_directory_0"/"file_1.txt"
        missing_file_1.unlink()

        default_backup(self.user_path, self.backup_path)
        backup_1, backup_2 = util.all_backups(self.backup_path)

        missing_file_2 = (
            self.user_path/"Music"/
            "02 Dvořák Piano Quintent in A (Op. 81) - II. Dumka - Andante con moto.mp3")
        missing_file_2.unlink()

        self.assertGreater(missing_file_1, missing_file_2)

        for method in Invocation:
            with self.assertLogs(level=logging.DEBUG) as logs:
                exit_code = run_find_missing_files(
                    method,
                    self.backup_path,
                    self.user_path,
                    debug=True)
                self.assertEqual(exit_code, 0, method)

            list_file = self.user_path/"missing_files.txt"
            warning_log_messages = [
                f"Files missing from user folder {self.user_path} found in {self.backup_path}",
                f"Copying list to {list_file}"]
            debug_log_messages = [
                f"{missing_file_2.relative_to(self.user_path).parent}",
                f"    {missing_file_2.name}    last seen: {backup_2.name}",
                f"{missing_file_1.relative_to(self.user_path).parent}",
                f"    {missing_file_1.name}    last seen: {backup_1.name}"]

            log_lines: list[str] = []
            for line in logs.output:
                if line.startswith("INFO:"):
                    continue

                if line.startswith("DEBUG:root:Namespace"):
                    continue

                log_lines.append(line)

            warning_log_lines = [f"WARNING:root:{message}" for message in warning_log_messages]
            debug_log_lines = [f"DEBUG:root:{message}" for message in debug_log_messages]
            self.assertEqual(log_lines, warning_log_lines + debug_log_lines, method)

            file_lines = [f"Missing user files found in {self.backup_path}:", *debug_log_messages]
            file_contents = "\n".join(file_lines) + "\n"
            self.assertTrue(list_file.is_file(), method)
            self.assertEqual(list_file.read_text(encoding="utf8"), file_contents, method)
            list_file.unlink()

    def test_missing_files_printed_in_correct_order(self) -> None:
        """
        Test that missing files are printed in correct order.

        With the built-in sorting for paths, the following files would be printed in this order:
          ./broker.txt
          ./code/script.py
          ./new_file.txt

        But, the correct order is
          ./broker.txt
          ./new_file.txt
          ./code/script.py
        because broker.txt and new_file.txt are in the same directory.
        """
        file_1 = self.user_path/"broker.txt"
        file_1.touch()
        folder = self.user_path/"code"
        folder.mkdir()
        file_2 = folder/"script.py"
        file_2.touch()
        file_3 = self.user_path/"new_file.txt"
        file_3.touch()

        default_backup(self.user_path, self.backup_path)
        backup = util.find_previous_backup(self.backup_path)
        self.assertIsNotNone(backup)
        backup = cast(Path, backup)
        old_user_path = self.user_path
        self.reset_user_folder()

        expected_missing_file_contents = (
f"""Missing user files found in {self.backup_path}:
{file_1.parent.relative_to(old_user_path)}
    {file_1.name}    last seen: {backup.name}
    {file_3.name}    last seen: {backup.name}
{file_2.parent.relative_to(old_user_path)}
    {file_2.name}    last seen: {backup.name}
""")

        for method in Invocation:
            run_find_missing_files(
                method,
                self.backup_path,
                self.user_path,
                debug=False)

            missing_file = self.user_path/"missing_files.txt"
            missing_file_data = missing_file.read_text(encoding="utf8")
            self.assertEqual(expected_missing_file_contents, missing_file_data)
