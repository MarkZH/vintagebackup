"""Testing code for Vintage Backup."""
import sys
import unittest
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
from typing import cast
import re
import io
from inspect import getsourcefile

from lib import backup_set
from lib import main
import lib.argument_parser as argparse
import lib.filesystem as fs
import lib.backup as lib_backup
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


def main_no_log(args: list[str]) -> int:
    """Run the main() function without logging to a file."""
    return main.main([*args, "--log", os.devnull])


def main_assert_no_error_log(args: list[str], testcase: unittest.TestCase) -> int:
    """Run the main() function to assert there are no errors logged without logging to a file."""
    with testcase.assertNoLogs(level=logging.ERROR):
        return main_no_log(args)


testing_timestamp = datetime.datetime.now()


def unique_timestamp() -> datetime.datetime:
    """Create a unique timestamp backups in testing so that backups can be made more rapidly."""
    global testing_timestamp  # noqa:PLW0603
    testing_timestamp += datetime.timedelta(seconds=10)
    return testing_timestamp


def unique_timestamp_string() -> str:
    """Return the stringified version of the unique_timestamp() result."""
    return unique_timestamp().strftime(lib_backup.backup_date_format)


def random_string(length: int) -> str:
    """Return a string with random ASCII letters of a given length."""
    return "".join(random.choices(string.ascii_letters, k=length))


def create_user_data(base_directory: Path) -> None:
    """
    Fill the given directory with folders and files.

    This creates a set of user data to test backups.

    :param base_directory: The directory into which all created files and folders go.
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


def create_old_backups(backup_base_directory: Path, count: int) -> None:
    """
    Create a set of empty monthly backups.

    :param backup_base_directory: The directory that will contain the backup folders.
    :param count: The number of backups to create. The oldest will be (count - 1) months old.
    """
    now = datetime.datetime.now()
    for months_back in range(count):
        new_month = now.month - months_back
        new_year = now.year
        while new_month < 1:
            new_month += 12
            new_year -= 1
        backup_date = dates.fix_end_of_month(new_year, new_month, now.day)
        backup_timestamp = datetime.datetime.combine(backup_date, now.time())
        backup_name = backup_timestamp.strftime(lib_backup.backup_date_format)
        (backup_base_directory/str(new_year)/backup_name).mkdir(parents=True)


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

    :param standard_directory: The base directory that will serve as the standard of comparison.
    :param test_directory: This directory must possess every file in the standard directory in the
    same location and with the same contents. Extra files in this directory will not result in
    failure.
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
    """Check that both directories have same directory tree and file contents."""
    return (all_files_have_same_content(base_directory_1, base_directory_2)
            and all_files_have_same_content(base_directory_2, base_directory_1))


def all_files_are_hardlinked(standard_directory: Path, test_directory: Path) -> bool:
    """Test that every file in the standard directory is hardlinked in the test_directory."""
    for directory_1, _, file_names in standard_directory.walk():
        directory_2 = test_directory/(directory_1.relative_to(standard_directory))
        for file_name in file_names:
            inode_1 = (directory_1/file_name).stat().st_ino
            inode_2 = (directory_2/file_name).stat().st_ino
            if inode_1 != inode_2:
                return False
    return True


def directories_are_completely_hardlinked(base_directory_1: Path, base_directory_2: Path) -> bool:
    """Check that both directories have same tree and all files are hardlinked together."""
    return (all_files_are_hardlinked(base_directory_1, base_directory_2)
            and all_files_are_hardlinked(base_directory_2, base_directory_1))


def no_files_are_hardlinks(standard_directory: Path, test_directory: Path) -> bool:
    """Test files in standard directory are not hard linked to counterparts in test directory."""
    for directory_1, _, file_names in standard_directory.walk():
        directory_2 = test_directory/(directory_1.relative_to(standard_directory))
        for file_name in file_names:
            inode_1 = (directory_1/file_name).stat().st_ino
            inode_2 = (directory_2/file_name).stat().st_ino
            if inode_1 == inode_2:
                return False
    return True


def directories_are_completely_copied(base_directory_1: Path, base_directory_2: Path) -> bool:
    """Check that both directories have same tree and all files are copies."""
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
    """Create a new backup while choosing a direct function call or a CLI invocation."""
    if run_method == Invocation.function:
        lib_backup.create_new_backup(
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
            "--timestamp", timestamp.strftime(lib_backup.backup_date_format)]
        if filter_file:
            argv.extend(["--filter", str(filter_file)])
        if examine_whole_file:
            argv.append("--whole-file")
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
    """Run backup while asserting that no errors are logged."""
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
        self.user_path = Path(tempfile.mkdtemp())
        self.backup_path = Path(tempfile.mkdtemp())
        self.config_path = self.user_path/"config.txt"
        self.filter_path = self.user_path/"filter.txt"

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
            self.assertEqual(exit_code, 0)
            backups = lib_backup.all_backups(self.backup_path)
            self.assertEqual(len(backups), 1)
            self.assertEqual(backups[0], lib_backup.find_previous_backup(self.backup_path))
            self.assertTrue(directories_are_completely_copied(self.user_path, backups[0]))
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
            backups = lib_backup.all_backups(self.backup_path)
            self.assertEqual(len(backups), 2)
            self.assertEqual(backups[1], lib_backup.find_previous_backup(self.backup_path))
            self.assertTrue(directories_are_completely_hardlinked(*backups))
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
            backups = lib_backup.all_backups(self.backup_path)
            self.assertEqual(len(backups), 2)
            self.assertEqual(backups[1], lib_backup.find_previous_backup(self.backup_path))
            self.assertTrue(directories_are_completely_copied(self.user_path, backups[-1]))
            self.assertTrue(directories_are_completely_copied(*backups))
            self.reset_backup_folder()

    def test_examining_whole_files_still_hardlinks_identical_files(self) -> None:
        """
        Test that examining whole files results in hardlinks to identical files in new backup.

        Even if the timestamp has changed, --whole-file will hard link files with the same data.
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
                self.assertEqual(exit_code, 0)
                for current_directory, _, files in self.user_path.walk():
                    for file in files:
                        (current_directory/file).touch()  # update timestamps

            backups = lib_backup.all_backups(self.backup_path)
            self.assertEqual(len(backups), 2)
            self.assertEqual(backups[-1], lib_backup.find_previous_backup(self.backup_path))
            self.assertTrue(directories_are_completely_hardlinked(*backups))
            self.reset_backup_folder()

    def test_force_copy_overrides_examine_whole_file(self) -> None:
        """Test that --force-copy results in a copy backup even if --whole-file is present."""
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
                self.assertEqual(exit_code, 0)
            backups = lib_backup.all_backups(self.backup_path)
            self.assertEqual(len(backups), 2)
            self.assertEqual(backups[-1], lib_backup.find_previous_backup(self.backup_path))
            self.assertTrue(directories_are_completely_copied(*backups))
            self.reset_backup_folder()

    def test_file_that_changed_between_backups_is_copied(self) -> None:
        """Check that a file changed between backups is copied with others are hardlinked."""
        create_user_data(self.user_path)
        lib_backup.create_new_backup(
            self.user_path,
            self.backup_path,
            filter_file=None,
            examine_whole_file=False,
            force_copy=False,
            copy_probability=0.0,
            timestamp=unique_timestamp())

        changed_file_name = self.user_path/"sub_directory_2"/"sub_sub_directory_0"/"file_1.txt"
        with changed_file_name.open("a", encoding="utf8") as changed_file:
            changed_file.write("the change\n")

        lib_backup.create_new_backup(
            self.user_path,
            self.backup_path,
            filter_file=None,
            examine_whole_file=False,
            force_copy=False,
            copy_probability=0.0,
            timestamp=unique_timestamp())
        backup_1, backup_2 = lib_backup.all_backups(self.backup_path)
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

        lib_backup.create_new_backup(
            self.user_path,
            self.backup_path,
            filter_file=None,
            examine_whole_file=False,
            force_copy=False,
            copy_probability=0.0,
            timestamp=unique_timestamp())
        last_backup = lib_backup.find_previous_backup(self.backup_path)
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
            lib_backup.create_new_backup(
                self.user_path,
                self.backup_path,
                filter_file=None,
                examine_whole_file=False,
                force_copy=False,
                copy_probability=0.0,
                timestamp=unique_timestamp())
        backup_1, backup_2 = lib_backup.all_backups(self.backup_path)
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
            lib_backup.create_new_backup(
                other_user_path,
                self.backup_path,
                filter_file=None,
                examine_whole_file=False,
                force_copy=False,
                copy_probability=0.0,
                timestamp=unique_timestamp())

            create_user_data(self.user_path)
            with self.assertRaises(CommandLineError):
                lib_backup.create_new_backup(
                    self.user_path,
                    self.backup_path,
                    filter_file=None,
                    examine_whole_file=False,
                    force_copy=False,
                    copy_probability=0.0,
                    timestamp=unique_timestamp())

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
        space_warning = re.compile(
            rf"{prefix}Backup space used: 50\.0. MB \(500.% of --free-up\)")
        self.assertEqual(len(log_lines.output), 2)
        self.assertTrue(space_warning.fullmatch(log_lines.output[0]), log_lines.output[0])
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
        space_warning = re.compile(
            rf"{prefix}Backup space used: 50\.0. MB \(99% of --free-up\)")
        self.assertEqual(len(log_lines.output), 2)
        self.assertTrue(space_warning.fullmatch(log_lines.output[0]), log_lines.output[0])
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
        expected_message = re.compile(
            r"INFO:root:Backup space used: 50\.\d\d MB \(51% of --free-up\)")
        self.assertTrue(any(expected_message.fullmatch(line) for line in logs.output), logs.output)
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
                lib_backup.create_new_backup(
                    self.user_path,
                    self.backup_path,
                    filter_file=None,
                    examine_whole_file=False,
                    force_copy=False,
                    copy_probability=0.0,
                    timestamp=unique_timestamp())

                lib_backup.create_new_backup(
                    other_user_path,
                    self.backup_path,
                    filter_file=None,
                    examine_whole_file=False,
                    force_copy=False,
                    copy_probability=0.0,
                    timestamp=unique_timestamp())

        expected_error_message = (
            "Previous backup stored a different user folder. Previously: "
            f"{self.user_path}; Now: {other_user_path}")
        self.assertEqual(error.exception.args, (expected_error_message,))

    def test_warning_printed_if_no_user_data_is_backed_up(self) -> None:
        """Make sure a warning is printed if no files are backed up."""
        with self.assertLogs(level=logging.WARNING) as assert_log:
            lib_backup.create_new_backup(
                self.user_path,
                self.backup_path,
                filter_file=None,
                examine_whole_file=False,
                force_copy=False,
                copy_probability=0.0,
                timestamp=unique_timestamp())
        self.assertIn("WARNING:root:No files were backed up!", assert_log.output)
        self.assertEqual(
            list(self.backup_path.iterdir()),
            [self.backup_path/"vintagebackup.source.txt"])

    def test_no_dated_backup_folder_created_if_no_data_backed_up(self) -> None:
        """Test that a dated backup folder is not created if there is no data to back up."""
        lib_backup.create_new_backup(
            self.user_path,
            self.backup_path,
            filter_file=None,
            examine_whole_file=False,
            force_copy=False,
            copy_probability=0.0,
            timestamp=unique_timestamp())
        self.assertEqual(
            list(self.backup_path.iterdir()),
            [self.backup_path/"vintagebackup.source.txt"])

    def test_warning_printed_if_all_user_files_filtered_out(self) -> None:
        """Make sure the user is warned if a filter file removes all files from the backup set."""
        create_user_data(self.user_path)
        self.filter_path.write_text("- **/*.txt\n", encoding="utf8")

        with self.assertLogs(level=logging.WARNING) as assert_log:
            lib_backup.create_new_backup(
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


class FilterTests(TestCaseWithTemporaryFilesAndFolders):
    """Test that filter files work properly."""

    def test_paths_excluded_in_filter_file_do_not_appear_in_backup(self) -> None:
        """Test that filter files with only exclusions result in the right files being excluded."""
        create_user_data(self.user_path)
        with self.filter_path.open("w", encoding="utf8") as filter_file:
            filter_file.write("- sub_directory_2/**\n\n")
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

            last_backup = lib_backup.find_previous_backup(self.backup_path)
            self.assertIsNotNone(last_backup)
            last_backup = cast(Path, last_backup)

            self.assertEqual(directory_contents(last_backup), expected_backups)
            self.assertNotEqual(directory_contents(self.user_path), expected_backups)
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
            self.assertEqual(exit_code, 0)

            last_backup = lib_backup.find_previous_backup(self.backup_path)
            self.assertIsNotNone(last_backup)
            last_backup = cast(Path, last_backup)

            self.assertEqual(directory_contents(last_backup), expected_backups)
            self.assertNotEqual(directory_contents(self.user_path), expected_backups)
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

        lib_backup.create_new_backup(
            self.user_path,
            self.backup_path,
            filter_file=self.filter_path,
            examine_whole_file=False,
            force_copy=False,
            copy_probability=0.0,
            timestamp=unique_timestamp())

        self.assertEqual(len(lib_backup.all_backups(self.backup_path)), 1)
        last_backup = lib_backup.find_previous_backup(self.backup_path)
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
        self.filter_path.write_text("- **/*1.txt\n")
        preview_path = self.user_path/"preview.txt"
        main_assert_no_error_log([
            "--user-folder", str(self.user_path),
            "--filter", str(self.filter_path),
            "--preview-filter", str(preview_path)],
            self)

        with preview_path.open() as preview:
            previewed_paths = read_paths_file(preview)
        previewed_paths = {path.relative_to(self.user_path) for path in previewed_paths}

        main_assert_no_error_log([
            "--user-folder", str(self.user_path),
            "--backup-folder", str(self.backup_path),
            "--filter", str(self.filter_path)],
            self)

        backup_list_path = self.user_path/"backed_up.txt"
        last_backup = cast(Path, lib_backup.find_previous_backup(self.backup_path))
        with backup_list_path.open("w") as backup_list:
            for directory, _, files in last_backup.walk():
                fs.write_directory(backup_list, directory, files)

        with backup_list_path.open() as backup_list:
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
    """Test file recovery through a direct function call or a CLI invocation."""
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
            lib_backup.create_new_backup(
                self.user_path,
                self.backup_path,
                filter_file=None,
                examine_whole_file=False,
                force_copy=False,
                copy_probability=0.0,
                timestamp=unique_timestamp())
            file = self.user_path/"sub_directory_0"/"sub_sub_directory_0"/"file_0.txt"
            moved_file_path = file.parent/(file.name + "_moved")
            file.rename(moved_file_path)
            with self.assertNoLogs(level=logging.ERROR):
                exit_code = run_recovery(method, self.backup_path, file, choices=0, search=False)
            self.assertEqual(exit_code, 0)
            self.assertTrue(filecmp.cmp(file, moved_file_path, shallow=False))

            self.reset_backup_folder()
            moved_file_path.unlink()

    def test_recovered_file_renamed_to_not_clobber_original_and_is_same_as_original(self) -> None:
        """Test that recovering a file that exists in user data does not overwrite any files."""
        create_user_data(self.user_path)
        lib_backup.create_new_backup(
            self.user_path,
            self.backup_path,
            filter_file=None,
            examine_whole_file=False,
            force_copy=False,
            copy_probability=0.0,
            timestamp=unique_timestamp())
        file_path = self.user_path/"sub_directory_0"/"sub_sub_directory_0"/"file_0.txt"
        recovery.recover_path(file_path, self.backup_path, search=False, choice=0)
        recovered_file_path = file_path.parent/f"{file_path.stem}.1{file_path.suffix}"
        self.assertTrue(filecmp.cmp(file_path, recovered_file_path, shallow=False))

    def test_recovered_folder_is_renamed_to_not_clobber_original_and_has_all_data(self) -> None:
        """Test that recovering a folder retrieves all data and doesn't overwrite user data."""
        create_user_data(self.user_path)
        lib_backup.create_new_backup(
            self.user_path,
            self.backup_path,
            filter_file=None,
            examine_whole_file=False,
            force_copy=False,
            copy_probability=0.0,
            timestamp=unique_timestamp())
        folder_path = self.user_path/"sub_directory_1"
        recovery.recover_path(folder_path, self.backup_path, search=False, choice=0)
        recovered_folder_path = folder_path.parent/f"{folder_path.name}.1"
        self.assertTrue(directories_are_completely_copied(folder_path, recovered_folder_path))

    def test_file_to_be_recovered_can_be_chosen_from_menu(self) -> None:
        """Test that a file can be recovered after choosing from a list ."""
        create_user_data(self.user_path)
        lib_backup.create_new_backup(
            self.user_path,
            self.backup_path,
            filter_file=None,
            examine_whole_file=False,
            force_copy=False,
            copy_probability=0.0,
            timestamp=unique_timestamp())
        folder_path = self.user_path/"sub_directory_1"/"sub_sub_directory_1"
        chosen_file = recovery.search_backups(folder_path, self.backup_path, "recovery", 1)
        self.assertIsNotNone(chosen_file)
        chosen_file = cast(Path, chosen_file)
        self.assertEqual(chosen_file, folder_path/"file_1.txt")
        recovery.recover_path(chosen_file, self.backup_path, search=False, choice=0)
        recovered_file_path = chosen_file.parent/f"{chosen_file.stem}.1{chosen_file.suffix}"
        self.assertTrue(filecmp.cmp(chosen_file, recovered_file_path, shallow=False))

    def test_binary_search(self) -> None:
        """Test that sequences of older/newer choices result in the right backup."""
        create_user_data(self.user_path)
        for method in Invocation:
            self.reset_backup_folder()
            for _ in range(9):
                lib_backup.create_new_backup(
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
                self.assertEqual(exit_code, 0)

            backups = lib_backup.all_backups(self.backup_path)
            expected_backup_sequence = [backups[i] for i in [4, 2, 3]]
            current_recovery_index = 0
            log_prefix = "INFO:root:"
            for line in logs.output:
                if line.startswith(f"{log_prefix}Copying "):
                    self.assertIn(str(expected_backup_sequence[current_recovery_index]), line)
                    recovered_file = (
                        sought_file.parent/
                        f"{sought_file.stem}.{current_recovery_index + 1}{sought_file.suffix}")
                    self.assertTrue(recovered_file.is_file(), recovered_file)
                    recovered_file.unlink()
                    current_recovery_index += 1
            self.assertEqual(current_recovery_index, len(expected_backup_sequence))
            self.assertEqual(logs.output[-1], f"{log_prefix}Only one choice for recovery.")


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
        create_old_backups(self.backup_path, 10)
        all_backups = lib_backup.all_backups(self.backup_path)
        fs.delete_directory_tree(all_backups[0])
        expected_remaining_backups = all_backups[1:]
        all_backups_left = lib_backup.all_backups(self.backup_path)
        self.assertEqual(expected_remaining_backups, all_backups_left)

    def test_deleting_backup_with_read_only_file(self) -> None:
        """Test deleting a backup containing a readonly file."""
        create_user_data(self.user_path)
        (self.user_path/"sub_directory_1"/"sub_sub_directory_1"/"file_1.txt").chmod(stat.S_IRUSR)

        lib_backup.create_new_backup(
            self.user_path,
            self.backup_path,
            filter_file=None,
            examine_whole_file=False,
            force_copy=False,
            copy_probability=0.0,
            timestamp=unique_timestamp())

        backups = lib_backup.all_backups(self.backup_path)
        self.assertEqual(len(backups), 1)

        fs.delete_directory_tree(backups[0])
        backup_count_after = len(lib_backup.all_backups(self.backup_path))
        self.assertEqual(backup_count_after, 0)

    def test_deleting_backup_with_read_only_folder(self) -> None:
        """Test deleting a backup containing a readonly file."""
        create_user_data(self.user_path)
        read_only_folder = self.user_path/"sub_directory_1"/"sub_sub_directory_1"
        read_only = stat.S_IRUSR | stat.S_IXUSR
        read_only_folder.chmod(read_only)

        lib_backup.create_new_backup(
            self.user_path,
            self.backup_path,
            filter_file=None,
            examine_whole_file=False,
            force_copy=False,
            copy_probability=0.0,
            timestamp=unique_timestamp())

        backups = lib_backup.all_backups(self.backup_path)
        self.assertEqual(len(backups), 1)

        fs.delete_directory_tree(backups[0])
        backup_count_after = len(lib_backup.all_backups(self.backup_path))
        self.assertEqual(backup_count_after, 0)

        # Restore write access to folder so it can be deleted in self.tearDown()
        read_only_folder.chmod(read_only | stat.S_IWUSR)

    def test_free_up_option_with_absolute_size_deletes_backups_to_free_storage_space(self) -> None:
        """Test deleting backups until there is a given amount of free space."""
        for method in Invocation:
            backups_created = 30
            create_old_backups(self.backup_path, backups_created)
            file_size = 10_000_000
            create_large_files(self.backup_path, file_size)
            backups_after_deletion = 10
            size_of_deleted_backups = (backups_created - backups_after_deletion)*file_size
            after_backup_space = shutil.disk_usage(self.backup_path).free
            goal_space = after_backup_space + size_of_deleted_backups - file_size/2
            goal_space_str = f"{goal_space}B"
            if method == Invocation.function:
                deletion.delete_oldest_backups_for_space(self.backup_path, goal_space_str)
            elif method == Invocation.cli:
                create_large_files(self.user_path, file_size)
                exit_code = main_assert_no_error_log([
                    "--user-folder", str(self.user_path),
                    "--backup-folder", str(self.backup_path),
                    "--free-up", goal_space_str,
                    "--timestamp", unique_timestamp_string()],
                    self)
                self.assertEqual(exit_code, 0)

                # While backups are being deleted, the fake user data still exists, so one more
                # backup needs to be deleted to free up the required space.
                backups_after_deletion -= 1
            else:
                raise NotImplementedError(f"Delete backup test not implemented for {method}")
            backups_left = len(lib_backup.all_backups(self.backup_path))
            self.assertIn(backups_left - backups_after_deletion, [0, 1])

            self.reset_backup_folder()

    def test_max_deletions_limits_the_number_of_backup_deletions(self) -> None:
        """Test that no more than the maximum number of backups are deleted when freeing space."""
        backups_created = 30
        create_old_backups(self.backup_path, backups_created)
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
                expected_backups_count)
        self.assertIn(
            "INFO:root:Stopped after reaching maximum number of deletions.",
            log_check.output)
        all_backups_after_deletion = lib_backup.all_backups(self.backup_path)
        self.assertEqual(len(all_backups_after_deletion), expected_backups_count)

    def test_delete_after_deletes_all_backups_prior_to_given_date(self) -> None:
        """Test that backups older than a given date can be deleted with --delete-after."""
        for method in Invocation:
            create_old_backups(self.backup_path, 30)
            max_age = "1y"
            now = datetime.datetime.now()
            earliest_backup = datetime.datetime(
                now.year - 1, now.month, now.day,
                now.hour, now.minute, now.second, now.microsecond)
            if method == Invocation.function:
                deletion.delete_backups_older_than(self.backup_path, max_age)
            elif method == Invocation.cli:
                create_user_data(self.user_path)
                most_recent_backup = moving.last_n_backups(1, self.backup_path)[0]
                fs.delete_directory_tree(most_recent_backup)
                exit_code = main_assert_no_error_log([
                    "--user-folder", str(self.user_path),
                    "--backup-folder", str(self.backup_path),
                    "--delete-after", max_age,
                    "--timestamp", unique_timestamp_string()],
                    self)
                self.assertEqual(exit_code, 0)
            else:
                raise NotImplementedError(f"Delete backup test not implemented for {method}")
            backups = lib_backup.all_backups(self.backup_path)
            self.assertEqual(len(backups), 12)
            self.assertLessEqual(earliest_backup, lib_backup.backup_datetime(backups[0]))

            self.reset_backup_folder()

    def test_max_deletions_limits_deletions_with_delete_after(self) -> None:
        """Test that --max-deletions limits backups deletions when using --delete-after."""
        backups_created = 30
        create_old_backups(self.backup_path, backups_created)
        max_age = "1y"
        max_deletions = 10
        expected_backup_count = backups_created - max_deletions
        with self.assertLogs(level=logging.INFO) as log_check:
            deletion.delete_backups_older_than(
                self.backup_path,
                max_age,
                expected_backup_count)
        self.assertIn(
            "INFO:root:Stopped after reaching maximum number of deletions.",
            log_check.output)
        backups_left = lib_backup.all_backups(self.backup_path)
        self.assertEqual(len(backups_left), expected_backup_count)

    def test_delete_after_never_deletes_most_recent_backup(self) -> None:
        """Test that deleting all backups with --delete_after actually leaves the last one."""
        create_old_backups(self.backup_path, 30)
        most_recent_backup = moving.last_n_backups(1, self.backup_path)[0]
        last_backup = moving.last_n_backups(2, self.backup_path)[0]
        fs.delete_directory_tree(most_recent_backup)
        deletion.delete_backups_older_than(self.backup_path, "1d")
        self.assertEqual(lib_backup.all_backups(self.backup_path), [last_backup])

    def test_free_up_never_deletes_most_recent_backup(self) -> None:
        """Test that deleting all backups with --free-up actually leaves the last one."""
        create_old_backups(self.backup_path, 30)
        last_backup = moving.last_n_backups(1, self.backup_path)[0]
        total_space = shutil.disk_usage(self.backup_path).total
        deletion.delete_oldest_backups_for_space(self.backup_path, f"{total_space}B")
        self.assertEqual(lib_backup.all_backups(self.backup_path), [last_backup])

    def test_attempt_to_free_more_space_than_capacity_of_backup_location_is_an_error(self) -> None:
        """Test that error is thrown when trying to free too much space."""
        max_space = shutil.disk_usage(self.backup_path).total
        too_much_space = 2*max_space
        with self.assertRaises(CommandLineError):
            deletion.delete_oldest_backups_for_space(self.backup_path, f"{too_much_space}B")

    def test_deleting_last_backup_in_year_folder_deletes_year_folder(self) -> None:
        """Test that deleting a backup leaves a year folder empty, that year folder is deleted."""
        today = datetime.date.today()
        create_old_backups(self.backup_path, today.month + 1)
        oldest_backup_year_folder = self.backup_path/f"{today.year - 1}"
        self.assertTrue(oldest_backup_year_folder.is_dir())
        self.assertEqual(len(list(oldest_backup_year_folder.iterdir())), 1)
        deletion.delete_backups_older_than(self.backup_path, f"{today.month}m")
        self.assertFalse(oldest_backup_year_folder.is_dir())
        this_year_backup_folder = self.backup_path/f"{today.year}"
        self.assertIsNotNone(this_year_backup_folder)

    def test_delete_only_command_line_option(self) -> None:
        """Test that --delete-only deletes backups without running a backup."""
        create_old_backups(self.backup_path, 30)
        oldest_backup_age = datetime.timedelta(days=120)
        arguments = [
            "--backup-folder", str(self.backup_path),
            "--delete-after", f"{oldest_backup_age.days}d",
            "--delete-only"]
        exit_code = main_assert_no_error_log(arguments, self)
        self.assertEqual(exit_code, 0)
        backups = lib_backup.all_backups(self.backup_path)
        self.assertEqual(len(backups), 4)  # 120 days = 4 months
        now = datetime.datetime.now()
        earliest_backup_timestamp = lib_backup.backup_datetime(backups[0])
        self.assertLessEqual(now - earliest_backup_timestamp, oldest_backup_age)

    def test_delete_first_deletes_backups_before_backing_up(self) -> None:
        """Test that --delete-first deletes backups before creating a new backup."""
        initial_backups = 20
        create_old_backups(self.backup_path, initial_backups)
        create_user_data(self.user_path)
        arguments = [
            "--user-folder", str(self.user_path),
            "--backup-folder", str(self.backup_path),
            "--delete-after", "1y",
            "--delete-first",
            "--timestamp", unique_timestamp_string()]
        backups_in_year = 12
        expected_deletions_before_backup = initial_backups - backups_in_year
        expected_backup_count_before_backup = initial_backups - expected_deletions_before_backup
        with self.assertLogs(level=logging.INFO) as logs:
            exit_code = main_no_log(arguments)
        self.assertEqual(exit_code, 0)
        backups_remaining = lib_backup.all_backups(self.backup_path)
        expected_backups_left = expected_backup_count_before_backup + 1
        self.assertEqual(len(backups_remaining), expected_backups_left)
        backup_log_line = "INFO:root: Starting new backup"
        self.assertIn(backup_log_line, logs.output)
        backup_start_index = logs.output.index(backup_log_line)
        deletion_log_prefix = "INFO:root:Deleting oldest backup:"

        deletions_before_backup = 0
        for log_line in logs.output[:backup_start_index]:
            if log_line.startswith(deletion_log_prefix):
                deletions_before_backup += 1
        self.assertEqual(deletions_before_backup, expected_deletions_before_backup)

        deletions_after_backup = 0
        for log_line in logs.output[backup_start_index:]:
            if log_line.startswith(deletion_log_prefix):
                deletions_after_backup += 1
        self.assertEqual(deletions_after_backup, 0)

        for log_line in logs.output:
            self.assertFalse(log_line.startswith("WARNING:"), log_line)
            self.assertFalse(log_line.startswith("ERROR:"), log_line)


class MoveBackupsTests(TestCaseWithTemporaryFilesAndFolders):
    """Test moving backup sets to a different location."""

    def test_moving_all_backups_preserves_structure_and_hardlinks_of_original(self) -> None:
        """Test that moving backups preserves the names and hardlinks of the original."""
        create_user_data(self.user_path)
        backup_count = 10
        for _ in range(backup_count):
            lib_backup.create_new_backup(
                self.user_path,
                self.backup_path,
                filter_file=None,
                examine_whole_file=False,
                force_copy=False,
                copy_probability=0.0,
                timestamp=unique_timestamp())

        for method in Invocation:
            with tempfile.TemporaryDirectory() as new_backup_folder:
                new_backup_location = Path(new_backup_folder)
                if method == Invocation.function:
                    backups_to_move = lib_backup.all_backups(self.backup_path)
                    self.assertEqual(len(backups_to_move), backup_count)
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
                    self.assertEqual(exit_code, 0)
                else:
                    raise NotImplementedError(f"Move backup test not implemented for {method}.")

                self.assertTrue(
                    directories_are_completely_copied(self.backup_path, new_backup_location))
                self.assertEqual(
                    backup_info.backup_source(self.backup_path),
                    backup_info.backup_source(new_backup_location))

                original_backups = lib_backup.all_backups(self.backup_path)
                original_names = [p.relative_to(self.backup_path) for p in original_backups]
                moved_backups = lib_backup.all_backups(new_backup_location)
                moved_names = [p.relative_to(new_backup_location) for p in moved_backups]
                self.assertEqual(original_names, moved_names)
                for backup_1, backup_2 in itertools.pairwise(moved_backups):
                    self.assertTrue(directories_are_completely_hardlinked(backup_1, backup_2))

    def test_move_n_backups_moves_subset_and_preserves_structure_and_hardlinks(self) -> None:
        """Test that moving N backups moves correct number of backups and correctly links files."""
        create_user_data(self.user_path)
        for _ in range(10):
            lib_backup.create_new_backup(
                self.user_path,
                self.backup_path,
                filter_file=None,
                examine_whole_file=False,
                force_copy=False,
                copy_probability=0.0,
                timestamp=unique_timestamp())

        move_count = 5
        for method in Invocation:
            with tempfile.TemporaryDirectory() as new_backup_folder:
                new_backup_location = Path(new_backup_folder)
                if method == Invocation.function:
                    backups_to_move = moving.last_n_backups(move_count, self.backup_path)
                    self.assertEqual(len(backups_to_move), move_count)
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
                    self.assertEqual(exit_code, 0)
                else:
                    raise NotImplementedError(f"Move backup test not implemented for {method}")

                backups_at_new_location = lib_backup.all_backups(new_backup_location)
                self.assertEqual(len(backups_at_new_location), move_count)
                old_backups = moving.last_n_backups(move_count, self.backup_path)
                old_backup_names = [p.relative_to(self.backup_path) for p in old_backups]
                new_backups = lib_backup.all_backups(new_backup_location)
                new_backup_names = [p.relative_to(new_backup_location) for p in new_backups]
                self.assertEqual(old_backup_names, new_backup_names)
                self.assertEqual(
                    backup_info.backup_source(self.backup_path),
                    backup_info.backup_source(new_backup_location))
                for backup_1, backup_2 in itertools.pairwise(new_backups):
                    self.assertTrue(directories_are_completely_hardlinked(backup_1, backup_2))

    def test_move_age_backups_moves_only_backups_within_given_timespan(self) -> None:
        """Test that moving backups based on a time span works."""
        create_old_backups(self.backup_path, 25)
        six_months_ago = dates.parse_time_span_to_timepoint("6m")
        backups_to_move = moving.backups_since(six_months_ago, self.backup_path)
        self.assertEqual(len(backups_to_move), 6)
        self.assertEqual(moving.last_n_backups(6, self.backup_path), backups_to_move)
        oldest_backup_timestamp = lib_backup.backup_datetime(backups_to_move[0])
        self.assertLessEqual(six_months_ago, oldest_backup_timestamp)

    def test_move_without_specifying_how_many_to_move_is_an_error(self) -> None:
        """Test that missing --move-count, --move-age, and --move-since results in an error."""
        create_user_data(self.user_path)
        lib_backup.create_new_backup(
            self.user_path,
            self.backup_path,
            filter_file=None,
            examine_whole_file=False,
            force_copy=False,
            copy_probability=0.0,
            timestamp=unique_timestamp())
        with (self.assertLogs(level=logging.ERROR) as no_move_choice_log,
            tempfile.TemporaryDirectory() as move_destination):

            exit_code = main_no_log([
                "--move-backup", move_destination,
                "--user-folder", str(self.user_path),
                "--backup-folder", str(self.backup_path)])
        self.assertEqual(exit_code, 1)
        expected_logs = [
            "ERROR:root:Exactly one of the following is required: "
            "--move-count, --move-age, or --move-since"]
        self.assertEqual(expected_logs, no_move_choice_log.output)

    def test_move_age_argument_selects_correct_backups(self) -> None:
        """Test that --move-age argument selects the correct backups."""
        create_old_backups(self.backup_path, 12)
        args = argparse.parse_command_line(["--move-age", "100d"])
        backups = moving.choose_backups_to_move(args, self.backup_path)
        expected_backup_count = 4
        self.assertEqual(len(backups), expected_backup_count)
        expected_backups = lib_backup.all_backups(self.backup_path)[-expected_backup_count:]
        self.assertEqual(backups, expected_backups)

    def test_move_since_argument_selects_correct_backups(self) -> None:
        """Test that --move-age argument selects the correct backups."""
        create_user_data(self.user_path)
        for day in range(1, 32):
            backup_date = datetime.datetime(2025, 8, day, 2, 0, 0)
            lib_backup.create_new_backup(
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
        expected_backups = lib_backup.all_backups(self.backup_path)[-expected_backup_count:]
        self.assertEqual(backups, expected_backups)


def read_paths_file(verify_file: io.TextIOBase) -> set[Path]:
    """Read an opened verification file and return the path contents."""
    files_from_verify: set[Path] = set()
    current_directory: Path | None = None
    for line in verify_file:
        if os.sep in line:
            current_directory = Path(line.removesuffix("\n"))
        else:
            if not current_directory:
                raise ValueError("File names must be preceded by a directory path.")
            files_from_verify.add(current_directory/line.removeprefix("    ").removesuffix("\n"))
    return files_from_verify


class VerificationTests(TestCaseWithTemporaryFilesAndFolders):
    """Test backup verification."""

    def test_backup_verification_sorts_files_into_matching_mismatching_and_errors(self) -> None:
        """Test that verification sorts files into matching, mismatching, and error lists."""
        create_user_data(self.user_path)
        lib_backup.create_new_backup(
            self.user_path,
            self.backup_path,
            filter_file=None,
            examine_whole_file=False,
            force_copy=False,
            copy_probability=0.0,
            timestamp=unique_timestamp())

        mismatch_file = self.user_path/"sub_directory_1"/"sub_sub_directory_2"/"file_0.txt"
        with mismatch_file.open("a", encoding="utf8") as file:
            file.write("\naddition\n")

        error_file = self.user_path/"sub_directory_2"/"sub_sub_directory_0"/"file_1.txt"
        last_backup = lib_backup.find_previous_backup(self.backup_path)
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
                    self.assertEqual(exit_code, 0)

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
        lib_backup.create_new_backup(
            self.user_path,
            self.backup_path,
            filter_file=None,
            examine_whole_file=False,
            force_copy=False,
            copy_probability=0.0,
            timestamp=unique_timestamp())

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
        """Test that verification does something when there are no backups."""
        with self.assertRaises(CommandLineError) as error:
            verify.verify_last_backup(self.user_path, self.backup_path, None)
        self.assertTrue(error.exception.args[0].startswith("No backups found in "))


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
whole    file :
""", encoding="utf8")
        command_line = config.read_configuation_file(Path(self.config_path))
        expected_command_line = [
            "--user-folder", user_folder,
            "--backup-folder", backup_folder,
            "--filter", filter_file,
            "--force-copy",
            "--whole-file"]

        self.assertEqual(command_line, expected_command_line)
        arg_parser = argparse.argument_parser()
        args = arg_parser.parse_args(command_line)
        self.assertEqual(args.user_folder, user_folder)
        self.assertEqual(args.backup_folder, backup_folder)
        self.assertEqual(args.filter, filter_file)
        self.assertTrue(args.force_copy)

    def test_command_line_options_override_config_file_options(self) -> None:
        """Test that command line options override file configurations and leave others alone."""
        user_folder = r"C:\Users\Test User"
        self.config_path.write_text(
rf"""
User Folder : {user_folder}
Backup Folder: temp_back
filter: filter.txt
log: temp_log.txt
whole file:
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
        self.assertTrue(options.whole_file)
        self.assertTrue(options.debug)

    def test_negating_command_line_parameters_override_config_file(self) -> None:
        """Test that command line options like --no-X override file configurations."""
        self.config_path.write_text(
r"""
whole file:
Debug:
delete first:
force copy:
""", encoding="utf8")
        command_line_options = [
            "-c", str(self.config_path),
            "--no-whole-file",
            "--no-debug",
            "--no-delete-first",
            "--no-force-copy"]
        options = argparse.parse_command_line(command_line_options)
        self.assertFalse(argparse.toggle_is_set(options, "whole_file"))
        self.assertFalse(argparse.toggle_is_set(options, "debug"))
        self.assertFalse(argparse.toggle_is_set(options, "delete_first"))
        self.assertFalse(argparse.toggle_is_set(options, "force_copy"))

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
        lib_backup.create_new_backup(
            self.user_path,
            self.backup_path,
            filter_file=None,
            examine_whole_file=False,
            force_copy=False,
            copy_probability=0.0,
            timestamp=unique_timestamp())

        self.assertEqual(len(lib_backup.all_backups(self.backup_path)), 1)

        first_extra_file = self.user_path/"extra_file1.txt"
        first_extra_file.write_text("extra 1\n", encoding="utf8")
        first_extra_folder = self.user_path/"extra_folder_1"
        first_extra_folder.mkdir()
        first_extra_folder_file = first_extra_folder/"file_in_folder_1.txt"
        first_extra_folder_file.write_text("extra file in folder 1\n", encoding="utf8")

        lib_backup.create_new_backup(
            self.user_path,
            self.backup_path,
            filter_file=None,
            examine_whole_file=False,
            force_copy=False,
            copy_probability=0.0,
            timestamp=unique_timestamp())
        self.assertEqual(len(lib_backup.all_backups(self.backup_path)), 2)

        second_extra_file = self.user_path/"extra_file2.txt"
        second_extra_file.write_text("extra 2\n", encoding="utf8")
        second_extra_folder = self.user_path/"extra_folder_2"
        second_extra_folder.mkdir()
        second_extra_folder_file = second_extra_folder/"file_in_folder_2.txt"
        second_extra_folder_file.write_text("extra file in folder 2\n")

        exit_code = main_assert_no_error_log([
            "--restore",
            "--user-folder", str(self.user_path),
            "--backup-folder", str(self.backup_path),
            "--last-backup", "--delete-extra",
            "--skip-prompt"],
            self)

        self.assertEqual(exit_code, 0)
        last_backup = lib_backup.find_previous_backup(self.backup_path)
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
        lib_backup.create_new_backup(
            self.user_path,
            self.backup_path,
            filter_file=None,
            examine_whole_file=False,
            force_copy=False,
            copy_probability=0.0,
            timestamp=unique_timestamp())

        self.assertEqual(len(lib_backup.all_backups(self.backup_path)), 1)

        first_extra_file = self.user_path/"extra_file1.txt"
        first_extra_file.write_text("extra 1\n", encoding="utf8")
        first_extra_folder = self.user_path/"extra_folder_1"
        first_extra_folder.mkdir()
        first_extra_folder_file = first_extra_folder/"file_in_folder_1.txt"
        first_extra_folder_file.write_text("extra file in folder 1\n", encoding="utf8")

        lib_backup.create_new_backup(
            self.user_path,
            self.backup_path,
            filter_file=None,
            examine_whole_file=False,
            force_copy=False,
            copy_probability=0.0,
            timestamp=unique_timestamp())
        self.assertEqual(len(lib_backup.all_backups(self.backup_path)), 2)

        second_extra_file = self.user_path/"extra_file2.txt"
        second_extra_file.write_text("extra 2\n", encoding="utf8")
        second_extra_folder = self.user_path/"extra_folder_2"
        second_extra_folder.mkdir()
        second_extra_folder_file = second_extra_folder/"file_in_folder_2.txt"
        second_extra_folder_file.write_text("extra file in folder 2\n")

        exit_code = main_assert_no_error_log([
            "--restore",
            "--user-folder", str(self.user_path),
            "--backup-folder", str(self.backup_path),
            "--last-backup", "--keep-extra",
            "--skip-prompt"],
            self)

        self.assertEqual(exit_code, 0)
        last_backup = lib_backup.find_previous_backup(self.backup_path)
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
        lib_backup.create_new_backup(
            self.user_path,
            self.backup_path,
            filter_file=None,
            examine_whole_file=False,
            force_copy=False,
            copy_probability=0.0,
            timestamp=unique_timestamp())

        self.assertEqual(len(lib_backup.all_backups(self.backup_path)), 1)

        first_extra_file = self.user_path/"extra_file1.txt"
        first_extra_file.write_text("extra 1\n", encoding="utf8")

        lib_backup.create_new_backup(
            self.user_path,
            self.backup_path,
            filter_file=None,
            examine_whole_file=False,
            force_copy=False,
            copy_probability=0.0,
            timestamp=unique_timestamp())
        self.assertEqual(len(lib_backup.all_backups(self.backup_path)), 2)

        second_extra_file = self.user_path/"extra_file2.txt"
        second_extra_file.write_text("extra 2\n", encoding="utf8")

        choice = 0
        exit_code = main_assert_no_error_log([
            "--restore",
            "--user-folder", str(self.user_path),
            "--backup-folder", str(self.backup_path),
            "--choose-backup", "--delete-extra",
            "--choice", str(choice),
            "--skip-prompt"],
            self)

        self.assertEqual(exit_code, 0)
        restored_backup = lib_backup.all_backups(self.backup_path)[choice]
        self.assertFalse(first_extra_file.exists(follow_symlinks=False))
        self.assertFalse(second_extra_file.exists(follow_symlinks=False))
        self.assertTrue(directories_have_identical_content(self.user_path, restored_backup))

    def test_restore_backup_from_menu_choice_and_keep_extra_preserves_new_files(self) -> None:
        """Test restoring a chosen backup from a menu with --keep-extra preserves new files."""
        create_user_data(self.user_path)
        lib_backup.create_new_backup(
            self.user_path,
            self.backup_path,
            filter_file=None,
            examine_whole_file=False,
            force_copy=False,
            copy_probability=0.0,
            timestamp=unique_timestamp())

        self.assertEqual(len(lib_backup.all_backups(self.backup_path)), 1)

        first_extra_file = self.user_path/"extra_file1.txt"
        first_extra_file.write_text("extra 1\n", encoding="utf8")

        lib_backup.create_new_backup(
            self.user_path,
            self.backup_path,
            filter_file=None,
            examine_whole_file=False,
            force_copy=False,
            copy_probability=0.0,
            timestamp=unique_timestamp())
        self.assertEqual(len(lib_backup.all_backups(self.backup_path)), 2)

        second_extra_file = self.user_path/"extra_file2.txt"
        second_extra_file.write_text("extra 2\n", encoding="utf8")

        choice = 0
        exit_code = main_assert_no_error_log([
            "--restore",
            "--user-folder", str(self.user_path),
            "--backup-folder", str(self.backup_path),
            "--choose-backup", "--keep-extra",
            "--choice", str(choice),
            "--skip-prompt"],
            self)

        self.assertEqual(exit_code, 0)
        restored_backup = lib_backup.all_backups(self.backup_path)[choice]
        self.assertTrue(first_extra_file.is_file(follow_symlinks=False))
        self.assertTrue(second_extra_file.is_file(follow_symlinks=False))
        first_extra_file.unlink()
        second_extra_file.unlink()
        self.assertTrue(directories_have_identical_content(self.user_path, restored_backup))

    def test_restore_backup_with_destination_delete_extra_restores_to_new_location(self) -> None:
        """Test restoring with --destination and --delete-extra recreates backup in new location."""
        with tempfile.TemporaryDirectory() as destination_folder:
            create_user_data(self.user_path)
            lib_backup.create_new_backup(
                self.user_path,
                self.backup_path,
                filter_file=None,
                examine_whole_file=False,
                force_copy=False,
                copy_probability=0.0,
                timestamp=unique_timestamp())

            exit_code = main_assert_no_error_log([
                "--restore",
                "--backup-folder", str(self.backup_path),
                "--last-backup", "--delete-extra",
                "--destination", destination_folder,
                "--skip-prompt"],
                self)

            self.assertEqual(exit_code, 0)
            destination_path = Path(destination_folder)
            last_backup = lib_backup.find_previous_backup(self.backup_path)
            self.assertIsNotNone(last_backup)
            last_backup = cast(Path, last_backup)
            self.assertTrue(directories_have_identical_content(last_backup, destination_path))
            self.assertTrue(directories_have_identical_content(self.user_path, destination_path))

    def test_restore_backup_with_destination_keep_extra_preserves_extra_files(self) -> None:
        """Test restoring with --destination and --keep-extra keeps extra files in new location."""
        with tempfile.TemporaryDirectory() as destination_folder:
            create_user_data(self.user_path)
            lib_backup.create_new_backup(
                self.user_path,
                self.backup_path,
                filter_file=None,
                examine_whole_file=False,
                force_copy=False,
                copy_probability=0.0,
                timestamp=unique_timestamp())

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
            last_backup = lib_backup.find_previous_backup(self.backup_path)
            self.assertIsNotNone(last_backup)
            last_backup = cast(Path, last_backup)
            extra_file.unlink()
            self.assertTrue(directories_have_identical_content(last_backup, destination_path))
            self.assertTrue(directories_have_identical_content(self.user_path, destination_path))

    def test_restore_without_delete_extra_or_keep_extra_is_an_error(self) -> None:
        """Test that missing --delete-extra and --keep-extra results in an error."""
        create_user_data(self.user_path)
        lib_backup.create_new_backup(
            self.user_path,
            self.backup_path,
            filter_file=None,
            examine_whole_file=False,
            force_copy=False,
            copy_probability=0.0,
            timestamp=unique_timestamp())

        with self.assertLogs(level=logging.ERROR) as no_extra_log:
            exit_code = main_no_log([
                "--restore",
                "--user-folder", str(self.user_path),
                "--backup-folder", str(self.backup_path),
                "--last-backup"])
        self.assertEqual(exit_code, 1)
        expected_logs = [
            "ERROR:root:Exactly one of the following is required: "
            "--delete-extra or --keep-extra"]
        self.assertEqual(expected_logs, no_extra_log.output)

    def test_restore_without_last_backup_or_choose_backup_is_an_error(self) -> None:
        """Test that missing --last-backup and --choose-backup results in an error."""
        create_user_data(self.user_path)
        lib_backup.create_new_backup(
            self.user_path,
            self.backup_path,
            filter_file=None,
            examine_whole_file=False,
            force_copy=False,
            copy_probability=0.0,
            timestamp=unique_timestamp())
        with self.assertLogs(level=logging.ERROR) as no_backup_choice_log:
            exit_code = main_no_log([
                "--restore",
                "--user-folder", str(self.user_path),
                "--backup-folder", str(self.backup_path),
                "--keep-extra"])
        self.assertEqual(exit_code, 1)
        expected_logs = [
            "ERROR:root:Exactly one of the following is required: "
            "--last-backup or --choose-backup"]
        self.assertEqual(expected_logs, no_backup_choice_log.output)

    def test_restore_with_bad_response_to_overwrite_confirmation_is_an_error(self) -> None:
        """Test that wrong response to overwrite confirmation ends program with error code."""
        create_user_data(self.user_path)
        lib_backup.create_new_backup(
            self.user_path,
            self.backup_path,
            filter_file=None,
            examine_whole_file=False,
            force_copy=False,
            copy_probability=0.0,
            timestamp=unique_timestamp())
        with self.assertLogs(level=logging.INFO) as bad_prompt_log:
            exit_code = main_no_log([
                "--restore",
                "--user-folder", str(self.user_path),
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
            "--user-folder", str(self.user_path),
            "--choose-backup",
            "--delete-extra"])
        with self.assertRaises(CommandLineError):
            restoration.start_backup_restore(args)

    def test_choose_backup_with_no_previous_backups_returns_none(self) -> None:
        """Ensure that the choose_backup() function returns None when there are no backups."""
        self.assertIsNone(restoration.choose_backup(self.backup_path, choice=None))


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
                lib_backup.start_backup(args)

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

        all_backups = lib_backup.all_backups(self.backup_path)
        self.assertEqual(len(all_backups), 2)
        self.assertTrue(all_files_have_same_content(*all_backups))
        self.assertFalse(directories_are_completely_hardlinked(*all_backups))
        self.assertFalse(directories_are_completely_copied(*all_backups))

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
            self.assertEqual(float(good_value), lib_backup.parse_probability(good_value))

        for bad_value in ["-1.0", "1.5"]:
            with self.assertRaises(CommandLineError):
                lib_backup.parse_probability(bad_value)

    def test_copy_probability_percent_must_be_between_zero_and_one_hundred(self) -> None:
        """Test that only values from 0.0 to 1.0 are valid for --copy-probability."""
        for good_value in ["0.0%", "50%", "100%"]:
            decimal = float(good_value[:-1])/100
            self.assertEqual(decimal, lib_backup.parse_probability(good_value))

        for bad_value in ["-100%", "150%"]:
            with self.assertRaises(CommandLineError):
                lib_backup.parse_probability(bad_value)

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

        all_backups = lib_backup.all_backups(self.backup_path)
        self.assertEqual(len(all_backups), 2)
        self.assertTrue(directories_are_completely_hardlinked(*all_backups))

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

        all_backups = lib_backup.all_backups(self.backup_path)
        self.assertEqual(len(all_backups), 2)
        self.assertTrue(directories_are_completely_hardlinked(*all_backups))

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

        all_backups = lib_backup.all_backups(self.backup_path)
        self.assertEqual(len(all_backups), 2)
        self.assertTrue(directories_are_completely_copied(*all_backups))

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

        all_backups = lib_backup.all_backups(self.backup_path)
        self.assertEqual(len(all_backups), 2)
        self.assertTrue(all_files_have_same_content(*all_backups))
        self.assertFalse(directories_are_completely_hardlinked(*all_backups))
        self.assertFalse(directories_are_completely_copied(*all_backups))

    def test_copy_probability_returns_zero_if_no_hard_link_argument_present(self) -> None:
        """Test if no --hard-link-count argument is present, probability of copy is zero."""
        user_input = argparse.argument_parser()
        no_arguments = user_input.parse_args([])
        self.assertEqual(lib_backup.copy_probability(no_arguments), 0.0)

    def test_copy_probability_with_non_positive_argument_is_an_error(self) -> None:
        """Any argument to --hard-link-count that is not a positive integer raises an exception."""
        for bad_arg in ("-1", "0", "z"):
            with self.assertRaises(CommandLineError):
                lib_backup.copy_probability_from_hard_link_count(bad_arg)

    def test_copy_probability_returns_one_over_n_plus_one_for_n_hard_links(self) -> None:
        """Test that the probability for N hard links is 1/(N + 1)."""
        for n in range(1, 10):
            probability = lib_backup.copy_probability_from_hard_link_count(str(n))
            self.assertAlmostEqual(1/(n + 1), probability)


class AtomicBackupTests(TestCaseWithTemporaryFilesAndFolders):
    """Test atomicity of backups."""

    def test_staging_folder_does_not_exist_after_successful_backup(self) -> None:
        """Test that the staging folder is deleted after a successful backup."""
        create_user_data(self.user_path)
        staging_path = self.backup_path/"Staging"
        lib_backup.create_new_backup(
            self.user_path,
            self.backup_path,
            filter_file=None,
            examine_whole_file=False,
            force_copy=False,
            copy_probability=0.0,
            timestamp=unique_timestamp())
        self.assertFalse(staging_path.exists())

    def test_staging_folder_deleted_by_new_backup(self) -> None:
        """Test that a backup process deletes a staging folder should it already exist."""
        create_user_data(self.user_path)
        staging_path = self.backup_path/"Staging"
        staging_path.mkdir()
        (staging_path/"leftover_file.txt").write_text(
            "Leftover from last backup\n", encoding="utf8")
        with self.assertLogs(level=logging.INFO) as logs:
            lib_backup.create_new_backup(
                self.user_path,
                self.backup_path,
                filter_file=None,
                examine_whole_file=False,
                force_copy=False,
                copy_probability=0.0,
                timestamp=unique_timestamp())
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
            lib_backup.create_new_backup(
                self.user_path,
                self.backup_path,
                filter_file=None,
                examine_whole_file=False,
                force_copy=False,
                copy_probability=0.0,
                timestamp=unique_timestamp())

        purged_file = self.user_path/"sub_directory_2"/"sub_sub_directory_1"/"file_0.txt"
        self.assertTrue(purged_file.is_file())
        purge_command_line = argparse.parse_command_line(
            ["--purge", str(purged_file), "--backup-folder", str(self.backup_path)])
        purge.start_backup_purge(purge_command_line, "y")
        expected_contents = directory_contents(self.user_path)
        expected_contents.remove(purged_file.relative_to(self.user_path))
        for backup in lib_backup.all_backups(self.backup_path):
            self.assertEqual(expected_contents, directory_contents(backup))

    def test_folder_purge(self) -> None:
        """Test that a purged folder is deleted from all backups."""
        create_user_data(self.user_path)
        number_of_backups = 5
        for _ in range(number_of_backups):
            lib_backup.create_new_backup(
                self.user_path,
                self.backup_path,
                filter_file=None,
                examine_whole_file=False,
                force_copy=False,
                copy_probability=0.0,
                timestamp=unique_timestamp())

        purged_folder = self.user_path/"sub_directory_2"/"sub_sub_directory_1"
        self.assertTrue(purged_folder.is_dir())
        purge_command_line = argparse.parse_command_line(
            ["--purge", str(purged_folder), "--backup-folder", str(self.backup_path)])
        purge.start_backup_purge(purge_command_line, "y")
        expected_contents = directory_contents(self.user_path)
        purged_contents = set(filter(
            lambda p: (self.user_path/p).is_relative_to(purged_folder), expected_contents))
        expected_contents.difference_update(purged_contents)
        for backup in lib_backup.all_backups(self.backup_path):
            self.assertEqual(expected_contents, directory_contents(backup))

    def test_file_purge_with_prompt_only_deletes_files(self) -> None:
        """Test that a purging a non-existent file only deletes files in backups."""
        create_user_data(self.user_path)
        number_of_backups = 5
        for _ in range(number_of_backups):
            lib_backup.create_new_backup(
                self.user_path,
                self.backup_path,
                filter_file=None,
                examine_whole_file=False,
                force_copy=False,
                copy_probability=0.0,
                timestamp=unique_timestamp())

        purged_path = self.user_path/"sub_directory_2"/"sub_sub_directory_1"
        fs.delete_directory_tree(purged_path)
        purged_path.touch()

        for _ in range(number_of_backups):
            lib_backup.create_new_backup(
                self.user_path,
                self.backup_path,
                filter_file=None,
                examine_whole_file=False,
                force_copy=False,
                copy_probability=0.0,
                timestamp=unique_timestamp())

        self.assertTrue(purged_path.is_file())
        purged_path.unlink()
        purge_command_line = argparse.parse_command_line([
            "--purge", str(purged_path),
            "--backup-folder", str(self.backup_path),
            "--choice", "0"])
        purge.start_backup_purge(purge_command_line, "y")
        relative_purge_file = purged_path.relative_to(self.user_path)
        for backup in lib_backup.all_backups(self.backup_path):
            backup_file_path = backup/relative_purge_file
            self.assertTrue(
                fs.is_real_directory(backup_file_path)
                or not backup_file_path.exists())

    def test_folder_purge_with_prompt_only_deletes_folders(self) -> None:
        """Test that a purging a non-existent folder only deletes folders in backups."""
        create_user_data(self.user_path)
        number_of_backups = 5
        for _ in range(number_of_backups):
            lib_backup.create_new_backup(
                self.user_path,
                self.backup_path,
                filter_file=None,
                examine_whole_file=False,
                force_copy=False,
                copy_probability=0.0,
                timestamp=unique_timestamp())

        purged_path = self.user_path/"sub_directory_2"/"sub_sub_directory_1"
        fs.delete_directory_tree(purged_path)
        purged_path.touch()

        for _ in range(number_of_backups):
            lib_backup.create_new_backup(
                self.user_path,
                self.backup_path,
                filter_file=None,
                examine_whole_file=False,
                force_copy=False,
                copy_probability=0.0,
                timestamp=unique_timestamp())

        self.assertTrue(purged_path.is_file())
        purged_path.unlink()
        purge_command_line = argparse.parse_command_line([
            "--purge", str(purged_path),
            "--backup-folder", str(self.backup_path),
            "--choice", "1"])
        purge.start_backup_purge(purge_command_line, "y")
        relative_purge_file = purged_path.relative_to(self.user_path)
        for backup in lib_backup.all_backups(self.backup_path):
            backup_file_path = backup/relative_purge_file
            self.assertTrue(backup_file_path.is_file() or not backup_file_path.exists())

    def test_purge_with_non_y_confirmation_response_deletes_nothing(self) -> None:
        """Test that a entering something other that 'y' at confirmation purges nothing."""
        create_user_data(self.user_path)
        number_of_backups = 5
        for _ in range(number_of_backups):
            lib_backup.create_new_backup(
                self.user_path,
                self.backup_path,
                filter_file=None,
                examine_whole_file=False,
                force_copy=False,
                copy_probability=0.0,
                timestamp=unique_timestamp())

        purged_path = self.user_path/"sub_directory_2"/"sub_sub_directory_1"
        self.assertTrue(purged_path.is_dir(follow_symlinks=False))
        purge_command_line = argparse.parse_command_line([
            "--purge", str(purged_path),
            "--backup-folder", str(self.backup_path)])
        purge.start_backup_purge(purge_command_line, "thing")

        for backup in lib_backup.all_backups(self.backup_path):
            self.assertTrue(directories_have_identical_content(backup, self.user_path))

    def test_folder_purge_from_list_with_prompt_only_deletes_folders(self) -> None:
        """Test that a purging a folder from a menu only deletes folders in backups."""
        create_user_data(self.user_path)
        number_of_backups = 5
        for _ in range(number_of_backups):
            lib_backup.create_new_backup(
                self.user_path,
                self.backup_path,
                filter_file=None,
                examine_whole_file=False,
                force_copy=False,
                copy_probability=0.0,
                timestamp=unique_timestamp())

        purged_path = self.user_path/"sub_directory_2"/"sub_sub_directory_0"
        fs.delete_directory_tree(purged_path)
        purged_path.touch()

        for _ in range(number_of_backups):
            lib_backup.create_new_backup(
                self.user_path,
                self.backup_path,
                filter_file=None,
                examine_whole_file=False,
                force_copy=False,
                copy_probability=0.0,
                timestamp=unique_timestamp())

        self.assertTrue(purged_path.is_file())
        purged_path.unlink()
        search_directory = purged_path.parent
        purge_command_line = argparse.parse_command_line([
            "--purge-list", str(search_directory),
            "--backup-folder", str(self.backup_path),
            "--choice", "2"])
        purge.choose_purge_target_from_backups(purge_command_line, "y")
        relative_purge_file = purged_path.relative_to(self.user_path)
        for backup in lib_backup.all_backups(self.backup_path):
            backup_file_path = backup/relative_purge_file
            self.assertTrue(
                fs.is_real_directory(backup_file_path) or not backup_file_path.exists())

    def test_purge_file_suggests_filter_line(self) -> None:
        """Test that purging a file logs a filter line for the purged file."""
        create_user_data(self.user_path)
        number_of_backups = 5
        for _ in range(number_of_backups):
            lib_backup.create_new_backup(
                self.user_path,
                self.backup_path,
                filter_file=None,
                examine_whole_file=False,
                force_copy=False,
                copy_probability=0.0,
                timestamp=unique_timestamp())

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
            lib_backup.create_new_backup(
                self.user_path,
                self.backup_path,
                filter_file=None,
                examine_whole_file=False,
                force_copy=False,
                copy_probability=0.0,
                timestamp=unique_timestamp())

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
    """Test all_backups() function."""

    def test_all_backups_returns_all_backups(self) -> None:
        """Test that all_backups() returns all expected backups."""
        create_user_data(self.user_path)
        backups_to_create = 7
        timestamps: list[datetime.datetime] = []
        for _ in range(backups_to_create):
            timestamp = unique_timestamp()
            timestamps.append(timestamp)
            lib_backup.create_new_backup(
                self.user_path,
                self.backup_path,
                filter_file=None,
                examine_whole_file=False,
                force_copy=False,
                copy_probability=0.0,
                timestamp=timestamp)
        backups = lib_backup.all_backups(self.backup_path)
        for timestamp, backup in zip(timestamps, backups, strict=True):
            year_path = str(timestamp.year)
            dated_folder_name = timestamp.strftime(lib_backup.backup_date_format)
            expected_folder = self.backup_path/year_path/dated_folder_name
            self.assertEqual(backup, expected_folder)

    def test_all_backups_returns_only_backups(self) -> None:
        """Test that all_backups() returns all expected backups."""
        create_user_data(self.user_path)
        backups_to_create = 7
        timestamps: list[datetime.datetime] = []
        for _ in range(backups_to_create):
            timestamp = unique_timestamp()
            timestamps.append(timestamp)
            lib_backup.create_new_backup(
                self.user_path,
                self.backup_path,
                filter_file=None,
                examine_whole_file=False,
                force_copy=False,
                copy_probability=0.0,
                timestamp=timestamp)

        # Create entries that should be left out of all_backups() list
        timestamp = timestamps[-1]
        (self.backup_path/"extra year folder"/"extra backup folder").mkdir(parents=True)
        (self.backup_path/"extra year file").touch()
        (self.backup_path/str(timestamp.year)/"extra backup folder").mkdir()
        (self.backup_path/str(timestamp.year)/"extra backup file").touch()

        backups = lib_backup.all_backups(self.backup_path)
        for timestamp, backup in zip(timestamps, backups, strict=True):
            year_path = str(timestamp.year)
            dated_folder_name = timestamp.strftime(lib_backup.backup_date_format)
            expected_folder = self.backup_path/year_path/dated_folder_name
            self.assertEqual(backup, expected_folder)


class BackupNameTests(unittest.TestCase):
    """Test backup_name() and backup_datetime() functions."""

    def test_backup_name_and_backup_datetime_are_inverse_functions(self) -> None:
        """Test that a timestamp is preserved in a backup name."""
        now = datetime.datetime.now()
        timestamp = datetime.datetime(
            now.year, now.month, now.day, now.hour, now.minute, now.second)
        backup = lib_backup.backup_name(timestamp)
        backup_timestamp = lib_backup.backup_datetime(backup)
        self.assertEqual(timestamp, backup_timestamp)

    def test_backup_name_puts_backup_folder_in_correct_year_folder(self) -> None:
        """Test that backups with the same year are grouped together."""
        timestamp = datetime.datetime.now()
        backup_folder = lib_backup.backup_name(timestamp)
        backup_timestamp = lib_backup.backup_datetime(backup_folder)
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
        self.evens, self.odds = lib_backup.separate(self.numbers, is_even)

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
        a, b = lib_backup.separate([], lambda _: True)
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
            lib_backup.log_backup_size(None, 1)
        self.assertEqual(logs.output, ["INFO:root:Backup space used: 1.000 B"])

    def test_backup_space_logged_when_backup_smaller_than_free_up_parameter(self) -> None:
        """Test space taken reported if backup's size is smaller than --free-up parameter."""
        with self.assertLogs(level=logging.INFO) as logs:
            lib_backup.log_backup_size("10", 2)
        space_message = "INFO:root:Backup space used: 2.000 B (20% of --free-up)"
        self.assertEqual(logs.output, [space_message])

    def test_warning_if_backup_space_close_to_free_up_parameter(self) -> None:
        """Test warning logged if space taken by backup is close to --free-up parameter."""
        with self.assertLogs(level=logging.WARNING) as logs:
            lib_backup.log_backup_size("100", 91)
        prefix = "WARNING:root:"
        space_message = f"{prefix}Backup space used: 91.00 B (91% of --free-up)"
        consider_warning = f"{prefix}Consider increasing the size of the --free-up parameter."
        self.assertEqual(logs.output, [space_message, consider_warning])

    def test_warning_if_backup_space_bigger_than_free_up_parameter(self) -> None:
        """Test warning logged if space taken by backup is larger than --free-up parameter."""
        with self.assertLogs(level=logging.WARNING) as logs:
            lib_backup.log_backup_size("100", 101)
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
        create_old_backups(self.backup_path, self.backup_count)

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
        all_backups = lib_backup.all_backups(self.backup_path)
        all_n_backups = moving.last_n_backups("all", self.backup_path)
        self.assertEqual(all_backups, all_n_backups)

    def test_all_argument_is_case_insensitive(self) -> None:
        """Test that capitalization does not matter for value 'all'."""
        all_backups = lib_backup.all_backups(self.backup_path)
        all_n_backups = moving.last_n_backups("All", self.backup_path)
        self.assertEqual(all_backups, all_n_backups)

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

    def test_no_backups_is_not_an_error(self) -> None:
        """Calling the function before any backups is not an error."""
        backup_info.confirm_user_location_is_unchanged(self.user_path, self.backup_path)

    def test_unchanged_user_folder_is_not_an_error(self) -> None:
        """Pass test if the backup location has not changed after a backup."""
        lib_backup.create_new_backup(
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
        lib_backup.create_new_backup(
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
            "--whole-file",
            "--generate-config", str(self.config_path)]

        self.assert_config_file_creation(command_line)

        expected_config_data = (
f"""Whole file:
Log: {os.devnull}
""")
        config_data = self.config_path.read_text(encoding="utf8")
        self.assertEqual(expected_config_data, config_data)

    def test_generation_of_config_files_with_negated_toggle_parameters(self) -> None:
        """Test that command line options with negated toggle parameters are not recorded."""
        command_line = [
            "--whole-file",
            "--no-whole-file",
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
            "--whole-file",
            "--generate-config", str(self.config_path)]

        self.config_path.touch()
        with self.assertLogs(level=logging.INFO) as logs:
            main_no_log(command_line)
        actual_config_path = self.config_path.with_suffix(f".1{self.config_path.suffix}")
        self.assertEqual(
            logs.output,
            [f"INFO:root:Generated configuration file: {actual_config_path}"])

        expected_config_data = (
f"""Whole file:
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
            "-w",
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
Whole file:
Log: nul
""")
        config_path = self.user_path/"config.txt"
        actual_config_contents = config_path.read_text()
        self.assertEqual(expected_config_contents, actual_config_contents)

        # Check contents of batch script file
        main_path = fs.absolute_path(cast(str, getsourcefile(main)))
        vintage_backup_file = main_path.parent/"vintagebackup.py"
        expected_batch_script = (f'py -3.13 "{vintage_backup_file}" --config "{config_path}"\n')
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
            "-w",
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
Whole file:
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
        log_path = self.user_path/"log.txt"
        selected_log_path = backup_info.primary_log_path(str(log_path), None)
        self.assertEqual(log_path, selected_log_path)

    def test_backup_folder_determines_log_without_log_path(self) -> None:
        """Select log file from previous backup."""
        create_user_data(self.user_path)
        log_path = self.user_path/"log.txt"
        exit_code = main.main([
            "--user-folder", str(self.user_path),
            "--backup-folder", str(self.backup_path),
            "--log", str(log_path)])
        self.assertEqual(exit_code, 0)
        selected_log_path = backup_info.primary_log_path(None, str(self.backup_path))
        self.assertEqual(selected_log_path, log_path)
        selected_log_path = backup_info.primary_log_path("", str(self.backup_path))
        self.assertEqual(selected_log_path, log_path)

    def test_log_option_overrides_backup_folder_log_record(self) -> None:
        """Use chosen log file if specified despite a recorded log file from previous backup."""
        create_user_data(self.user_path)
        log_path = self.user_path/"log.txt"
        exit_code = main.main([
            "--user-folder", str(self.user_path),
            "--backup-folder", str(self.backup_path),
            "--log", str(log_path)])
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

    def test_old_style_backup_info_file_is_read_correctly(self) -> None:
        """Confirm old info files--only backup source with no keys--can be read."""
        create_user_data(self.user_path)
        log_path = self.user_path/"log.txt"
        exit_code = main.main([
            "--user-folder", str(self.user_path),
            "--backup-folder", str(self.backup_path),
            "--log", str(log_path),
            "--timestamp", unique_timestamp_string()])
        self.assertEqual(exit_code, 0)

        new_style_info = backup_info.read_backup_information(self.backup_path)
        self.assertEqual(new_style_info["Log"], log_path)
        self.assertEqual(new_style_info["Source"], self.user_path)

        info_path = backup_info.get_backup_info_file(self.backup_path)
        info_path.write_text(f"{self.user_path}\n")
        old_style_info = backup_info.read_backup_information(self.backup_path)
        self.assertIsNone(old_style_info["Log"])
        self.assertEqual(old_style_info["Source"], self.user_path)

        exit_code = main.main([
            "--user-folder", str(self.user_path),
            "--backup-folder", str(self.backup_path),
            "--log", str(log_path),
            "--timestamp", unique_timestamp_string()])
        self.assertEqual(exit_code, 0)

        last_info = backup_info.read_backup_information(self.backup_path)
        self.assertEqual(last_info["Log"], log_path)
        self.assertEqual(last_info["Source"], self.user_path)

    def test_logs_written_to_log_file(self) -> None:
        """Check that log file contents match the log output."""
        create_user_data(self.user_path)
        log_path = self.user_path/"log.txt"
        with self.assertLogs(level=logging.INFO) as log_record:
            logs.setup_log_file(str(log_path), None, str(self.backup_path), debug=False)
            lib_backup.create_new_backup(
                self.user_path,
                self.backup_path,
                filter_file=None,
                examine_whole_file=False,
                force_copy=False,
                copy_probability=0.0,
                timestamp=unique_timestamp())

            close_all_file_logs()

        with log_path.open(encoding="utf8") as log_file:
            for log_line, file_log_line in itertools.zip_longest(
                    log_record.output,
                    log_file,
                    fillvalue=""):
                log_message = log_line.split(":", maxsplit=2)[2].strip()
                file_message = "".join(file_log_line.split(maxsplit=3)[3:]).strip()
                self.assertEqual(log_message, file_message)


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


def close_all_file_logs() -> None:
    """Close error file to prevent errors when leaving assertLogs contexts."""
    logger = logging.getLogger()
    file_handlers = filter(lambda h: isinstance(h, logging.FileHandler), logger.handlers)
    for handler in file_handlers:
        if isinstance(handler, logging.FileHandler):
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
        """Test that cancel_key() returns 'Ctrl-C'."""
        self.assertEqual(console.cancel_key(), "Cmd-C")


class ValidPathsTests(TestCaseWithTemporaryFilesAndFolders):
    """Test of valid backup paths testing."""

    def test_existing_user_folder_and_backup_folder_results_in_no_exceptions(self) -> None:
        """An existing user folder and backup folder raise no exceptions."""
        lib_backup.check_paths_for_validity(self.user_path, self.backup_path, None)

    def test_existing_user_folder_and_non_existent_backup_folder_raises_no_exceptions(self) -> None:
        """An existing user folder and non-existent backup folder raises no exceptions."""
        fs.delete_directory_tree(self.backup_path)
        lib_backup.check_paths_for_validity(self.user_path, self.backup_path, None)
        self.make_new_backup_folder()

    def test_all_paths_exist_raises_no_exceptions(self) -> None:
        """User folder, backup folder, and filter file existing raises no exceptions."""
        self.filter_path.touch()
        lib_backup.check_paths_for_validity(self.user_path, self.backup_path, self.filter_path)

    def test_non_existent_user_folder_raises_exception(self) -> None:
        """A missing user folder raises a CommandLineError."""
        with self.assertRaises(CommandLineError) as error:
            lib_backup.check_paths_for_validity(
                self.user_path/"non-existent",
                self.backup_path,
                None)
        self.assertIn("The user folder path is not a folder", error.exception.args[0])

    def test_user_folder_is_file_raises_exception(self) -> None:
        """A user folder that's actually a file raises a CommandLineError."""
        with self.assertRaises(CommandLineError) as error:
            user_file = self.user_path/"a_file.txt"
            user_file.touch()
            lib_backup.check_paths_for_validity(user_file, self.backup_path, None)
        self.assertIn("The user folder path is not a folder", error.exception.args[0])

    def test_backup_folder_is_file_raises_exception(self) -> None:
        """A backup folder that's actually a file raises a CommandLineError."""
        with self.assertRaises(CommandLineError) as error:
            backup_file = self.backup_path/"a_file.txt"
            backup_file.touch()
            lib_backup.check_paths_for_validity(self.user_path, backup_file, None)
        self.assertIn("Backup location exists but is not a folder", error.exception.args[0])

    def test_backup_folder_inside_user_folder_raises_exception(self) -> None:
        """Backing up a user folder to a location inside that folder raises a CommandLineError."""
        with self.assertRaises(CommandLineError) as error:
            lib_backup.check_paths_for_validity(self.user_path, self.user_path/"backup", None)
        self.assertIn("Backup destination cannot be inside user's folder:", error.exception.args[0])

    def test_non_existent_filter_path_raises_exception(self) -> None:
        """Specifying a filter path that doesn't exist raises a CommandLineError."""
        with self.assertRaises(CommandLineError) as error:
            lib_backup.check_paths_for_validity(self.user_path, self.backup_path, self.filter_path)
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
        lib_backup.create_new_backup(
            self.user_path,
            self.backup_path,
            filter_file=None,
            examine_whole_file=False,
            force_copy=False,
            copy_probability=0.0,
            timestamp=unique_timestamp())

        with self.assertRaises(CommandLineError) as error:
            recovery.path_relative_to_backups(self.user_path.parent/"file.txt", self.backup_path)
        self.assertIn(" is not contained in the backup set ", error.exception.args[0])

    def test_path_relative_to_backups_returns_path_relative_to_user_folder(self) -> None:
        """Function path_relative_to_backups() returns path relative to backed up user folder."""
        create_user_data(self.user_path)
        lib_backup.create_new_backup(
            self.user_path,
            self.backup_path,
            filter_file=None,
            examine_whole_file=False,
            force_copy=False,
            copy_probability=0.0,
            timestamp=unique_timestamp())
        file = self.user_path/"random_file.txt"
        relative_file = recovery.path_relative_to_backups(file, self.backup_path)
        self.assertEqual(self.user_path/relative_file, file)
        folder = self.user_path/"folder"/"folder"/"folder"
        relative_folder = recovery.path_relative_to_backups(folder, self.backup_path)
        self.assertEqual(self.user_path/relative_folder, folder)

    def test_directory_relative_to_backup_fails_for_non_directory_path(self) -> None:
        """Function directory_relative_to_backup() fails if argument is not a directory."""
        create_user_data(self.user_path)
        lib_backup.create_new_backup(
            self.user_path,
            self.backup_path,
            filter_file=None,
            examine_whole_file=False,
            force_copy=False,
            copy_probability=0.0,
            timestamp=unique_timestamp())

        with self.assertRaises(CommandLineError) as error:
            recovery.directory_relative_to_backup(self.user_path/"root_file.txt", self.backup_path)
        self.assertTrue(
            error.exception.args[0].startswith("The given search path is not a directory: "))

    def test_directory_relative_to_backup_returns_directory_relative_to_user_folder(self) -> None:
        """Test directory_relative_to_backup() returns paths relative to backed up user folder."""
        create_user_data(self.user_path)
        lib_backup.create_new_backup(
            self.user_path,
            self.backup_path,
            filter_file=None,
            examine_whole_file=False,
            force_copy=False,
            copy_probability=0.0,
            timestamp=unique_timestamp())

        folder = self.user_path/"sub_directory_1"/"sub_sub_directory_2"
        relative_folder = recovery.directory_relative_to_backup(folder, self.backup_path)
        self.assertEqual(self.user_path/relative_folder, folder)


if __name__ == "__main__":
    unittest.main()
