"""Testing code for Vintage Backup."""
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
import vintagebackup
import enum
import random
import string
import platform
from typing import cast

testing_timestamp = datetime.datetime.now()


def unique_timestamp() -> datetime.datetime:
    """Create a unique timestamp backups in testing so that backups can be made more rapidly."""
    global testing_timestamp  # noqa:PLW0603
    testing_timestamp += datetime.timedelta(seconds=10)
    return testing_timestamp


def create_user_data(base_directory: Path) -> None:
    """
    Fill the given directory with folders and files.

    This creates a set of user data to test backups.

    :param base_directory: The directory into which all created files and folders go.
    """
    for sub_num in range(3):
        subfolder = base_directory/f"sub_directory_{sub_num}"
        subfolder.mkdir()
        for sub_sub_num in range(3):
            subsubfolder = subfolder/f"sub_sub_directory_{sub_sub_num}"
            subsubfolder.mkdir()
            for file_num in range(3):
                file_path = subsubfolder/f"file_{file_num}.txt"
                file_path.write_text(f"File contents: {sub_num}/{sub_sub_num}/{file_num}\n")


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
        backup_date = vintagebackup.fix_end_of_month(new_year, new_month, now.day)
        backup_timestamp = datetime.datetime.combine(backup_date, now.time())
        backup_name = backup_timestamp.strftime(vintagebackup.backup_date_format)
        (backup_base_directory/str(new_year)/backup_name).mkdir(parents=True)


def directory_contents(base_directory: Path) -> set[Path]:
    """Return a set of all paths in a directory relative to that directory."""
    paths: set[Path] = set()
    for directory, directories, files in base_directory.walk():
        relative_directory = directory.relative_to(base_directory)
        paths.update(relative_directory/name for name in itertools.chain(directories, files))
    return paths


def all_files_have_same_content(standard_directory: Path,
                                test_directory: Path) -> bool:
    """
    Test that every file in the standard directory exists also in the test directory.

    Corresponding files will also be checked for identical contents.

    :param standard_directory: The base directory that will serve as the standard of comparison.
    :param test_directory: This directory must possess every file in the standard directory in the
    same location and with the same contents. Extra files in this directory will not result in
    failure.
    """
    for directory_name_1, _, file_names in standard_directory.walk():
        directory_1 = Path(directory_name_1)
        directory_2 = test_directory/(directory_1.relative_to(standard_directory))
        _, mismatches, errors = filecmp.cmpfiles(directory_1,
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
    for directory_name_1, _, file_names in standard_directory.walk():
        directory_1 = Path(directory_name_1)
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
    for directory_name_1, _, file_names in standard_directory.walk():
        directory_1 = Path(directory_name_1)
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


def run_backup(run_method: Invocation,
               user_data: Path,
               backup_location: Path,
               filter_file: Path | None,
               examine_whole_file: bool,
               force_copy: bool,
               timestamp: datetime.datetime) -> int:
    """Create a new backup while choosing a direct function call or a CLI invocation."""
    if run_method == Invocation.function:
        vintagebackup.create_new_backup(user_data,
                                        backup_location,
                                        filter_file=filter_file,
                                        examine_whole_file=examine_whole_file,
                                        force_copy=force_copy,
                                        max_average_hard_links=None,
                                        timestamp=timestamp)
        return 0
    elif run_method == Invocation.cli:
        argv = ["--user-folder", str(user_data),
                "--backup-folder", str(backup_location),
                "--log", os.devnull,
                "--timestamp", timestamp.strftime(vintagebackup.backup_date_format)]
        if filter_file:
            argv.extend(["--filter", str(filter_file)])
        if examine_whole_file:
            argv.append("--whole-file")
        if force_copy:
            argv.append("--force-copy")
        return vintagebackup.main(argv)
    else:
        raise NotImplementedError(f"Backup test with {run_method} not implemented.")


class BackupTest(unittest.TestCase):
    """Test the main backup procedure."""

    def test_first_backup_copies_all_user_data(self) -> None:
        """Test that the first default backup copies everything in user data."""
        for method in Invocation:
            with (tempfile.TemporaryDirectory() as user_data_folder,
                  tempfile.TemporaryDirectory() as backup_location_folder):
                user_data = Path(user_data_folder)
                backup_location = Path(backup_location_folder)
                create_user_data(user_data)
                exit_code = run_backup(method,
                                       user_data,
                                       backup_location,
                                       filter_file=None,
                                       examine_whole_file=False,
                                       force_copy=False,
                                       timestamp=unique_timestamp())
                self.assertEqual(exit_code, 0)
                backups = vintagebackup.all_backups(backup_location)
                self.assertEqual(len(backups), 1)
                self.assertEqual(backups[0], vintagebackup.find_previous_backup(backup_location))
                self.assertTrue(directories_are_completely_copied(user_data, backups[0]))

    def test_second_backup_with_unchanged_data_hardlinks_everything_in_first_backup(self) -> None:
        """Test that second default backup with same data hard links everything in first backup."""
        for method in Invocation:
            with (tempfile.TemporaryDirectory() as user_data_folder,
                  tempfile.TemporaryDirectory() as backup_location_folder):
                user_data = Path(user_data_folder)
                backup_location = Path(backup_location_folder)
                create_user_data(user_data)
                for _ in range(2):
                    exit_code = run_backup(method,
                                           user_data,
                                           backup_location,
                                           filter_file=None,
                                           examine_whole_file=False,
                                           force_copy=False,
                                           timestamp=unique_timestamp())
                    self.assertEqual(exit_code, 0)
                backups = vintagebackup.all_backups(backup_location)
                self.assertEqual(len(backups), 2)
                self.assertEqual(backups[1], vintagebackup.find_previous_backup(backup_location))
                self.assertTrue(directories_are_completely_hardlinked(*backups))

    def test_force_copy_results_in_backup_with_copied_user_data(self) -> None:
        """Test that latest backup is a copy of user data with --force-copy option."""
        for method in Invocation:
            with (tempfile.TemporaryDirectory() as user_data_folder,
                  tempfile.TemporaryDirectory() as backup_location_folder):
                user_data = Path(user_data_folder)
                backup_location = Path(backup_location_folder)
                create_user_data(user_data)
                for _ in range(2):
                    exit_code = run_backup(method,
                                           user_data,
                                           backup_location,
                                           filter_file=None,
                                           examine_whole_file=False,
                                           force_copy=True,
                                           timestamp=unique_timestamp())
                    self.assertEqual(exit_code, 0)
                backups = vintagebackup.all_backups(backup_location)
                self.assertEqual(len(backups), 2)
                self.assertEqual(backups[1], vintagebackup.find_previous_backup(backup_location))
                self.assertTrue(directories_are_completely_copied(user_data, backups[-1]))
                self.assertTrue(directories_are_completely_copied(*backups))

    def test_examining_whole_files_still_hardlinks_identical_files(self) -> None:
        """
        Test that examining whole files results in hardlinks to identical files in new backup.

        Even if the timestamp has changed, --whole-file will hard link files with the same data.
        """
        for method in Invocation:
            with (tempfile.TemporaryDirectory() as user_data_folder,
                  tempfile.TemporaryDirectory() as backup_location_folder):
                user_data = Path(user_data_folder)
                backup_location = Path(backup_location_folder)
                create_user_data(user_data)
                for _ in range(2):
                    exit_code = run_backup(method,
                                           user_data,
                                           backup_location,
                                           filter_file=None,
                                           examine_whole_file=True,
                                           force_copy=False,
                                           timestamp=unique_timestamp())
                    self.assertEqual(exit_code, 0)
                    for current_directory, _, files in user_data.walk():
                        for file in files:
                            (current_directory/file).touch()  # update timestamps

                backups = vintagebackup.all_backups(backup_location)
                self.assertEqual(len(backups), 2)
                self.assertEqual(backups[-1], vintagebackup.find_previous_backup(backup_location))
                self.assertTrue(directories_are_completely_hardlinked(*backups))

    def test_force_copy_overrides_examine_whole_file(self) -> None:
        """Test that --force-copy results in a copy backup even if --whole-file is present."""
        for method in Invocation:
            with (tempfile.TemporaryDirectory() as user_data_folder,
                  tempfile.TemporaryDirectory() as backup_location_folder):
                user_data = Path(user_data_folder)
                backup_location = Path(backup_location_folder)
                create_user_data(user_data)
                for _ in range(2):
                    exit_code = run_backup(method,
                                           user_data,
                                           backup_location,
                                           filter_file=None,
                                           examine_whole_file=True,
                                           force_copy=True,
                                           timestamp=unique_timestamp())
                    self.assertEqual(exit_code, 0)
                backups = vintagebackup.all_backups(backup_location)
                self.assertEqual(len(backups), 2)
                self.assertEqual(backups[-1], vintagebackup.find_previous_backup(backup_location))
                self.assertTrue(directories_are_completely_copied(*backups))

    def test_file_that_changed_between_backups_is_copied(self) -> None:
        """Check that a file changed between backups is copied with others are hardlinked."""
        with (tempfile.TemporaryDirectory() as user_data_folder,
              tempfile.TemporaryDirectory() as backup_location_folder):
            user_data = Path(user_data_folder)
            backup_location = Path(backup_location_folder)
            create_user_data(user_data)
            vintagebackup.create_new_backup(user_data,
                                            backup_location,
                                            filter_file=None,
                                            examine_whole_file=False,
                                            force_copy=False,
                                            max_average_hard_links=None,
                                            timestamp=unique_timestamp())

            changed_file_name = user_data/"sub_directory_2"/"sub_sub_directory_0"/"file_1.txt"
            with open(changed_file_name, "a") as changed_file:
                changed_file.write("the change\n")

            vintagebackup.create_new_backup(user_data,
                                            backup_location,
                                            filter_file=None,
                                            examine_whole_file=False,
                                            force_copy=False,
                                            max_average_hard_links=None,
                                            timestamp=unique_timestamp())
            backup_1, backup_2 = vintagebackup.all_backups(backup_location)
            contents_1 = directory_contents(backup_1)
            contents_2 = directory_contents(backup_2)
            self.assertEqual(contents_1, contents_2)
            relative_changed_file = changed_file_name.relative_to(user_data)
            for file in (f for f in contents_1 if f.is_file()):
                self.assertEqual((file != relative_changed_file),
                                 ((backup_1/file).stat().st_ino == (backup_2/file).stat().st_ino))

    def test_symlinks_are_always_copied_as_symlinks(self) -> None:
        """Test that backups correctly handle symbolic links in user data."""
        if platform.system() == "Windows":
            self.skipTest("Cannot create symlinks on Windows without elevated privileges.")

        with (tempfile.TemporaryDirectory() as user_data_folder,
              tempfile.TemporaryDirectory() as backup_location_folder):
            user_data_path = Path(user_data_folder)
            create_user_data(user_data_path)
            directory_symlink_name = "directory_symlink"
            (user_data_path/directory_symlink_name).symlink_to(user_data_path/"sub_directory_1")
            file_symlink_name = "file_symlink.txt"
            file_link_target = user_data_path/"sub_directory_1"/"sub_sub_directory_1"/"file_2.txt"
            (user_data_path/file_symlink_name).symlink_to(file_link_target)

            backup_path = Path(backup_location_folder)
            vintagebackup.create_new_backup(user_data_path,
                                            backup_path,
                                            filter_file=None,
                                            examine_whole_file=False,
                                            force_copy=False,
                                            max_average_hard_links=None,
                                            timestamp=unique_timestamp())
            last_backup = vintagebackup.find_previous_backup(backup_path)
            self.assertTrue(last_backup)
            last_backup = cast(Path, last_backup)
            self.assertTrue((last_backup/directory_symlink_name).is_symlink())
            self.assertTrue((last_backup/file_symlink_name).is_symlink())


class FilterTest(unittest.TestCase):
    """Test that filter files work properly."""

    def test_paths_excluded_in_filter_file_do_not_appear_in_backup(self) -> None:
        """Test that filter files with only exclusions result in the right files being excluded."""
        for method in Invocation:
            with (tempfile.TemporaryDirectory() as user_data_location,
                  tempfile.TemporaryDirectory() as backup_folder,
                  tempfile.NamedTemporaryFile("w+", delete_on_close=False) as filter_file):

                user_data = Path(user_data_location)
                create_user_data(user_data)
                user_paths = directory_contents(user_data)

                expected_backups = user_paths.copy()
                filter_file.write("- sub_directory_2/**\n\n")
                expected_backups.difference_update(path for path in user_paths
                                                   if "sub_directory_2" in path.parts)

                filter_file.write(str(Path("- *")/"sub_sub_directory_0/**\n\n"))
                expected_backups.difference_update(path for path in user_paths
                                                   if "sub_sub_directory_0" in path.parts)

                filter_file.close()

                backup_location = Path(backup_folder)
                exit_code = run_backup(method,
                                       user_data,
                                       backup_location,
                                       filter_file=Path(filter_file.name),
                                       examine_whole_file=False,
                                       force_copy=False,
                                       timestamp=unique_timestamp())
                self.assertEqual(exit_code, 0)

                last_backup = vintagebackup.find_previous_backup(backup_location)
                self.assertTrue(last_backup)
                last_backup = cast(Path, last_backup)

                self.assertEqual(directory_contents(last_backup), expected_backups)
                self.assertNotEqual(directory_contents(user_data), expected_backups)

    def test_paths_included_after_exclusions_appear_in_backup(self) -> None:
        """Test that filter files with inclusions and exclusions work properly."""
        with (tempfile.TemporaryDirectory() as user_data_location,
              tempfile.TemporaryDirectory() as backup_folder,
              tempfile.NamedTemporaryFile("w+", delete_on_close=False) as filter_file):

            user_data = Path(user_data_location)
            create_user_data(user_data)
            user_paths = directory_contents(user_data)

            expected_backup_paths = user_paths.copy()
            filter_file.write("- sub_directory_2/**\n\n")
            expected_backup_paths.difference_update(path for path in user_paths
                                                    if "sub_directory_2" in path.parts)

            filter_file.write(str(Path("- *")/"sub_sub_directory_0/**\n\n"))
            expected_backup_paths.difference_update(path for path in user_paths
                                                    if "sub_sub_directory_0" in path.parts)

            filter_file.write(str(Path("+ sub_directory_1")/"sub_sub_directory_0"/"file_1.txt\n\n"))
            expected_backup_paths.add(Path("sub_directory_1")/"sub_sub_directory_0")
            expected_backup_paths.add(Path("sub_directory_1")/"sub_sub_directory_0"/"file_1.txt")

            filter_file.close()

            backup_location = Path(backup_folder)
            vintagebackup.create_new_backup(user_data,
                                            backup_location,
                                            filter_file=Path(filter_file.name),
                                            examine_whole_file=False,
                                            force_copy=False,
                                            max_average_hard_links=None,
                                            timestamp=unique_timestamp())

            self.assertEqual(len(vintagebackup.all_backups(backup_location)), 1)
            last_backup = vintagebackup.find_previous_backup(backup_location)
            self.assertTrue(last_backup)
            last_backup = cast(Path, last_backup)

            self.assertEqual(directory_contents(last_backup), expected_backup_paths)
            self.assertNotEqual(directory_contents(user_data), expected_backup_paths)

    def test_filter_lines_that_have_no_effect_are_logged(self) -> None:
        """Test that filter lines with no effect on the backup files are detected."""
        with (tempfile.TemporaryDirectory() as user_data_location,
              tempfile.NamedTemporaryFile("w+", delete_on_close=False) as filter_file):
            user_path = Path(user_data_location)
            create_user_data(user_path)

            filter_file.write("- sub_directory_1/**\n")

            bad_lines = [("-", "sub_directory_1/sub_sub_directory_0/**"),  # redundant exclusion
                         ("+", "sub_directory_0/**"),  # redundant inclusion
                         ("-", "does_not_exist.txt"),  # excluding non-existent file
                         ("-", "sub_directory_0"),  # ineffective exclusion of folder
                         ("-", "sub_directory_1/*")]  # ineffective exlusion of folder

            filter_file.write("# Ineffective lines:\n")
            for sign, line in bad_lines:
                filter_file.write(f"{sign} {line}\n")
            filter_file.close()

            with self.assertLogs() as log_assert:
                for _ in vintagebackup.Backup_Set(user_path, Path(filter_file.name)):
                    pass

            for line_number, (sign, path) in enumerate(bad_lines, 3):
                self.assertIn(f"INFO:vintagebackup:{filter_file.name}: line #{line_number} "
                              f"({sign} {user_path/path}) had no effect.",
                              log_assert.output)

            self.assertTrue(all("Ineffective" not in message for message in log_assert.output))

    def test_invalid_filter_symbol_raises_exception(self) -> None:
        """Test that a filter symbol not in "+-#" raises an exceptions."""
        with tempfile.NamedTemporaryFile("w", delete_on_close=False) as filter_file:
            filter_file.write("* invalid_sign\n")
            filter_file.close()
            with self.assertRaises(ValueError) as error:
                vintagebackup.Backup_Set(Path(), Path(filter_file.name))
            self.assertIn("The first symbol of each line", error.exception.args[0])

    def test_path_outside_user_folder_in_filter_file_raises_exception(self) -> None:
        """Test that adding a path outside the user folder (--user-folder) raises an exception."""
        with tempfile.TemporaryDirectory() as user_folder:
            user_path = Path(user_folder)
            create_user_data(user_path)

            with tempfile.NamedTemporaryFile("w", delete_on_close=False) as filter_file:
                filter_file.write("- /other_place/sub_directory_0")
                filter_file.close()
                with self.assertRaises(ValueError) as error:
                    vintagebackup.Backup_Set(user_path, Path(filter_file.name))
                self.assertIn("outside user folder", error.exception.args[0])


def run_recovery(method: Invocation, backup_location: Path, file_path: Path) -> int:
    """Test file recovery through a direct function call or a CLI invocation."""
    if method == Invocation.function:
        vintagebackup.recover_path(file_path, backup_location, 0)
        return 0
    elif method == Invocation.cli:
        argv = ["--recover", str(file_path),
                "--backup-folder", str(backup_location),
                "--choice", "0",
                "--log", os.devnull]
        return vintagebackup.main(argv)
    else:
        raise NotImplementedError(f"Backup test with {method} not implemented.")


class RecoveryTest(unittest.TestCase):
    """Test recovering files and folders from backups."""

    def test_file_recovered_from_backup_is_identical_to_original(self) -> None:
        """Test that recovering a single file gets back same data."""
        for method in Invocation:
            with (tempfile.TemporaryDirectory() as user_data_location,
                  tempfile.TemporaryDirectory() as backup_folder):
                user_data = Path(user_data_location)
                create_user_data(user_data)
                backup_location = Path(backup_folder)
                vintagebackup.create_new_backup(user_data,
                                                backup_location,
                                                filter_file=None,
                                                examine_whole_file=False,
                                                force_copy=False,
                                                max_average_hard_links=None,
                                                timestamp=unique_timestamp())
                file = (user_data/"sub_directory_0"/"sub_sub_directory_0"/"file_0.txt").resolve()
                moved_file_path = file.parent/(file.name + "_moved")
                file.rename(moved_file_path)
                exit_code = run_recovery(method, backup_location, file)
                self.assertEqual(exit_code, 0)
                self.assertTrue(filecmp.cmp(file, moved_file_path, shallow=False))

    def test_recovered_file_renamed_to_not_clobber_original_and_is_same_as_original(self) -> None:
        """Test that recovering a file that exists in user data does not overwrite any files."""
        with (tempfile.TemporaryDirectory() as user_data_location,
              tempfile.TemporaryDirectory() as backup_folder):
            user_data = Path(user_data_location)
            create_user_data(user_data)
            backup_location = Path(backup_folder)
            vintagebackup.create_new_backup(user_data,
                                            backup_location,
                                            filter_file=None,
                                            examine_whole_file=False,
                                            force_copy=False,
                                            max_average_hard_links=None,
                                            timestamp=unique_timestamp())
            file_path = (user_data/"sub_directory_0"/"sub_sub_directory_0"/"file_0.txt").resolve()
            vintagebackup.recover_path(file_path, backup_location, 0)
            recovered_file_path = file_path.parent/f"{file_path.stem}.1{file_path.suffix}"
            self.assertTrue(filecmp.cmp(file_path, recovered_file_path, shallow=False))

    def test_recovered_folder_is_renamed_to_not_clobber_original_and_has_all_data(self) -> None:
        """Test that recovering a folder retrieves all data and doesn't overwrite user data."""
        with (tempfile.TemporaryDirectory() as user_data_location,
              tempfile.TemporaryDirectory() as backup_folder):
            user_data = Path(user_data_location)
            create_user_data(user_data)
            backup_location = Path(backup_folder)
            vintagebackup.create_new_backup(user_data,
                                            backup_location,
                                            filter_file=None,
                                            examine_whole_file=False,
                                            force_copy=False,
                                            max_average_hard_links=None,
                                            timestamp=unique_timestamp())
            folder_path = (user_data/"sub_directory_1").resolve()
            vintagebackup.recover_path(folder_path, backup_location, 0)
            recovered_folder_path = folder_path.parent/f"{folder_path.name}.1"
            self.assertTrue(directories_are_completely_copied(folder_path, recovered_folder_path))

    def test_file_to_be_recovered_can_be_chosen_from_menu(self) -> None:
        """Test that a file can be recovered after choosing from a list ."""
        with (tempfile.TemporaryDirectory() as user_data_location,
              tempfile.TemporaryDirectory() as backup_folder):
            user_data = Path(user_data_location)
            create_user_data(user_data)
            backup_location = Path(backup_folder)
            vintagebackup.create_new_backup(user_data,
                                            backup_location,
                                            filter_file=None,
                                            examine_whole_file=False,
                                            force_copy=False,
                                            max_average_hard_links=None,
                                            timestamp=unique_timestamp())
            folder_path = (user_data/"sub_directory_1"/"sub_sub_directory_1").resolve()
            chosen_file = vintagebackup.search_backups(folder_path, backup_location, "recovery", 1)
            self.assertTrue(chosen_file)
            chosen_file = cast(Path, chosen_file)
            self.assertEqual(chosen_file, folder_path/"file_1.txt")
            vintagebackup.recover_path(chosen_file, backup_location, 0)
            recovered_file_path = chosen_file.parent/f"{chosen_file.stem}.1{chosen_file.suffix}"
            self.assertTrue(filecmp.cmp(chosen_file, recovered_file_path, shallow=False))


def create_large_files(backup_location: Path, file_size: int) -> None:
    """Create a file of a give size in every leaf subdirectory."""
    data = "A"*file_size
    for directory_name, sub_directory_names, _ in backup_location.walk():
        if not sub_directory_names:
            (Path(directory_name)/"file.txt").write_text(data)


class DeleteBackupTest(unittest.TestCase):
    """Test deleting backups."""

    def test_deleting_single_backup(self) -> None:
        """Test deleting only the most recent backup."""
        with tempfile.TemporaryDirectory() as backup_folder:
            backup_location = Path(backup_folder)
            create_old_backups(backup_location, 10)
            all_backups = vintagebackup.all_backups(backup_location)
            vintagebackup.delete_directory_tree(all_backups[0])
            expected_remaining_backups = all_backups[1:]
            all_backups_left = vintagebackup.all_backups(backup_location)
            self.assertEqual(expected_remaining_backups, all_backups_left)

    def test_deleting_backup_with_read_only_file(self) -> None:
        """Test deleting a backup containing a readonly file."""
        with (tempfile.TemporaryDirectory() as user_folder,
              tempfile.TemporaryDirectory() as backup_folder):
            user_data = Path(user_folder)
            create_user_data(user_data)
            (user_data/"sub_directory_1"/"sub_sub_directory_1"/"file_1.txt").chmod(stat.S_IRUSR)

            backup_location = Path(backup_folder)
            vintagebackup.create_new_backup(user_data,
                                            backup_location,
                                            filter_file=None,
                                            examine_whole_file=False,
                                            force_copy=False,
                                            max_average_hard_links=None,
                                            timestamp=unique_timestamp())

            backups = vintagebackup.all_backups(backup_location)
            self.assertEqual(len(backups), 1)

            vintagebackup.delete_directory_tree(backups[0])
            backup_count_after = len(vintagebackup.all_backups(backup_location))
            self.assertEqual(backup_count_after, 0)

    def test_deleting_backup_with_read_only_folder(self) -> None:
        """Test deleting a backup containing a readonly file."""
        with (tempfile.TemporaryDirectory() as user_folder,
              tempfile.TemporaryDirectory() as backup_folder):
            user_data = Path(user_folder)
            create_user_data(user_data)
            (user_data/"sub_directory_1"/"sub_sub_directory_1").chmod(stat.S_IRUSR | stat.S_IXUSR)

            backup_location = Path(backup_folder)
            vintagebackup.create_new_backup(user_data,
                                            backup_location,
                                            filter_file=None,
                                            examine_whole_file=False,
                                            force_copy=False,
                                            max_average_hard_links=None,
                                            timestamp=unique_timestamp())

            backups = vintagebackup.all_backups(backup_location)
            self.assertEqual(len(backups), 1)

            vintagebackup.delete_directory_tree(backups[0])
            backup_count_after = len(vintagebackup.all_backups(backup_location))
            self.assertEqual(backup_count_after, 0)

    def test_free_up_option_with_absolute_size_deletes_backups_to_free_storage_space(self) -> None:
        """Test deleting backups until there is a given amount of free space."""
        for method in Invocation:
            with tempfile.TemporaryDirectory() as backup_folder:
                backup_location = Path(backup_folder)
                backups_created = 30
                create_old_backups(backup_location, backups_created)
                file_size = 10_000_000
                create_large_files(backup_location, file_size)
                backups_after_deletion = 10
                size_of_deleted_backups = (backups_created - backups_after_deletion)*file_size
                after_backup_space = shutil.disk_usage(backup_location).free
                goal_space = after_backup_space + size_of_deleted_backups - file_size/2
                goal_space_str = f"{goal_space}B"
                if method == Invocation.function:
                    vintagebackup.delete_oldest_backups_for_space(backup_location, goal_space_str)
                elif method == Invocation.cli:
                    with tempfile.TemporaryDirectory() as user_folder:
                        user_data = Path(user_folder)
                        create_large_files(user_data, file_size)
                        exit_code = vintagebackup.main(["--user-folder", user_folder,
                                                        "--backup-folder", backup_folder,
                                                        "--log", os.devnull,
                                                        "--free-up", goal_space_str,
                                                        "--timestamp",
                                                        unique_timestamp().strftime(
                                                            vintagebackup.backup_date_format)])
                        self.assertEqual(exit_code, 0)

                    # While backups are being deleted, the fake user data still exists, so one more
                    # backup needs to be deleted to free up the required space.
                    backups_after_deletion -= 1
                else:
                    raise NotImplementedError(f"Delete backup test not implemented for {method}")
                backups_left = len(vintagebackup.all_backups(backup_location))
                self.assertEqual(backups_left, backups_after_deletion)

    def test_max_deletions_limits_the_number_of_backup_deletions(self) -> None:
        """Test that no more than the maximum number of backups are deleted when freeing space."""
        with tempfile.TemporaryDirectory() as backup_folder:
            backup_location = Path(backup_folder)
            backups_created = 30
            create_old_backups(backup_location, backups_created)
            file_size = 10_000_000
            create_large_files(backup_location, file_size)
            backups_after_deletion = 10
            size_of_deleted_backups = (backups_created - backups_after_deletion)*file_size
            after_backup_space = shutil.disk_usage(backup_location).free
            goal_space = after_backup_space + size_of_deleted_backups - file_size/2
            goal_space_str = f"{goal_space}B"
            maximum_deletions = 5
            expected_backups_count = backups_created - maximum_deletions
            with self.assertLogs(level=logging.INFO) as log_check:
                vintagebackup.delete_oldest_backups_for_space(backup_location,
                                                            goal_space_str,
                                                            expected_backups_count)
            self.assertIn("INFO:vintagebackup:Stopped after reaching maximum number of deletions.",
                          log_check.output)
            all_backups_after_deletion = vintagebackup.all_backups(backup_location)
            self.assertEqual(len(all_backups_after_deletion), expected_backups_count)

    def test_delete_after_deletes_all_backups_prior_to_given_date(self) -> None:
        """Test that backups older than a given date can be deleted with --delete-after."""
        for method in Invocation:
            with tempfile.TemporaryDirectory() as backup_folder:
                backup_location = Path(backup_folder)
                create_old_backups(backup_location, 30)
                max_age = "1y"
                now = datetime.datetime.now()
                earliest_backup = datetime.datetime(now.year - 1, now.month, now.day,
                                                    now.hour, now.minute, now.second,
                                                    now.microsecond)
                if method == Invocation.function:
                    vintagebackup.delete_backups_older_than(backup_location, max_age)
                elif method == Invocation.cli:
                    with tempfile.TemporaryDirectory() as user_folder:
                        user_data = Path(user_folder)
                        create_user_data(user_data)
                        most_recent_backup = vintagebackup.last_n_backups(backup_location, 1)[0]
                        vintagebackup.delete_directory_tree(most_recent_backup)
                        exit_code = vintagebackup.main(["--user-folder", user_folder,
                                                        "--backup-folder", backup_folder,
                                                        "--log", os.devnull,
                                                        "--delete-after", max_age,
                                                        "--timestamp",
                                                        unique_timestamp().strftime(
                                                            vintagebackup.backup_date_format)])
                        self.assertEqual(exit_code, 0)
                else:
                    raise NotImplementedError(f"Delete backup test not implemented for {method}")
                backups = vintagebackup.all_backups(backup_location)
                self.assertEqual(len(backups), 12)
                self.assertLessEqual(earliest_backup, vintagebackup.backup_datetime(backups[0]))

    def test_max_deletions_limits_deletions_with_delete_after(self) -> None:
        """Test that --max-deletions limits backups deletions when using --delete-after."""
        with tempfile.TemporaryDirectory() as backup_folder:
            backup_location = Path(backup_folder)
            backups_created = 30
            create_old_backups(backup_location, backups_created)
            max_age = "1y"
            max_deletions = 10
            expected_backup_count = backups_created - max_deletions
            with self.assertLogs(level=logging.INFO) as log_check:
                vintagebackup.delete_backups_older_than(backup_location,
                                                        max_age,
                                                        expected_backup_count)
            self.assertIn("INFO:vintagebackup:Stopped after reaching maximum number of deletions.",
                          log_check.output)
            backups_left = vintagebackup.all_backups(backup_location)
            self.assertEqual(len(backups_left), expected_backup_count)

    def test_delete_after_never_deletes_most_recent_backup(self) -> None:
        """Test that deleting all backups with --delete_after actually leaves the last one."""
        with tempfile.TemporaryDirectory() as backup_folder:
            backup_location = Path(backup_folder)
            create_old_backups(backup_location, 30)
            most_recent_backup = vintagebackup.last_n_backups(backup_location, 1)[0]
            last_backup = vintagebackup.last_n_backups(backup_location, 2)[0]
            vintagebackup.delete_directory_tree(most_recent_backup)
            vintagebackup.delete_backups_older_than(backup_location, "1d")
            self.assertEqual(vintagebackup.all_backups(backup_location), [last_backup])

    def test_free_up_never_deletes_most_recent_backup(self) -> None:
        """Test that deleting all backups with --free-up actually leaves the last one."""
        with tempfile.TemporaryDirectory() as backup_folder:
            backup_location = Path(backup_folder)
            create_old_backups(backup_location, 30)
            last_backup = vintagebackup.last_n_backups(backup_location, 1)[0]
            total_space = shutil.disk_usage(backup_location).total
            vintagebackup.delete_oldest_backups_for_space(backup_location, f"{total_space}B")
            self.assertEqual(vintagebackup.all_backups(backup_location), [last_backup])

    def test_attempt_to_free_more_space_than_capacity_of_backup_location_is_an_error(self) -> None:
        """Test that error is thrown when trying to free too much space."""
        with tempfile.TemporaryDirectory() as backup_folder:
            backup_location = Path(backup_folder)
            max_space = shutil.disk_usage(backup_location).total
            too_much_space = 2*max_space
            with self.assertRaises(vintagebackup.CommandLineError):
                vintagebackup.delete_oldest_backups_for_space(backup_location, f"{too_much_space}B")

    def test_deleting_last_backup_in_year_folder_deletes_year_folder(self) -> None:
        """Test that deleting a backup leaves a year folder empty, that year folder is deleted."""
        with tempfile.TemporaryDirectory() as backup_folder:
            backup_location = Path(backup_folder)
            today = datetime.date.today()
            create_old_backups(backup_location, today.month + 1)
            oldest_backup_year_folder = backup_location/f"{today.year - 1}"
            self.assertTrue(oldest_backup_year_folder.is_dir())
            self.assertEqual(len(os.listdir(oldest_backup_year_folder)), 1)
            vintagebackup.delete_backups_older_than(backup_location, f"{today.month}m")
            self.assertFalse(oldest_backup_year_folder.is_dir())
            this_year_backup_folder = backup_location/f"{today.year}"
            self.assertTrue(this_year_backup_folder)


class MoveBackupsTest(unittest.TestCase):
    """Test moving backup sets to a different location."""

    def test_moving_all_backups_preserves_structure_and_hardlinks_of_original(self) -> None:
        """Test that moving backups preserves the names and hardlinks of the original."""
        with (tempfile.TemporaryDirectory() as user_data_folder,
              tempfile.TemporaryDirectory() as backup_folder):
            user_data = Path(user_data_folder)
            create_user_data(user_data)
            backup_location = Path(backup_folder)
            backup_count = 10
            for _ in range(backup_count):
                vintagebackup.create_new_backup(user_data,
                                                backup_location,
                                                filter_file=None,
                                                examine_whole_file=False,
                                                force_copy=False,
                                                max_average_hard_links=None,
                                                timestamp=unique_timestamp())

            for method in Invocation:
                with tempfile.TemporaryDirectory() as new_backup_folder:
                    new_backup_location = Path(new_backup_folder)
                    if method == Invocation.function:
                        backups_to_move = vintagebackup.all_backups(backup_location)
                        self.assertEqual(len(backups_to_move), backup_count)
                        vintagebackup.move_backups(backup_location,
                                                   new_backup_location,
                                                   backups_to_move)
                    elif method == Invocation.cli:
                        exit_code = vintagebackup.main(["--backup-folder", backup_folder,
                                                        "--log", os.devnull,
                                                        "--move-backup", new_backup_folder,
                                                        "--move-count", "all"])
                        self.assertEqual(exit_code, 0)
                    else:
                        raise NotImplementedError(f"Move backup test not implemented for {method}.")

                    self.assertTrue(directories_are_completely_copied(backup_location,
                                                                      new_backup_location))
                    self.assertEqual(vintagebackup.backup_source(backup_location),
                                     vintagebackup.backup_source(new_backup_location))

                    original_backups = vintagebackup.all_backups(backup_location)
                    original_names = [p.relative_to(backup_location) for p in original_backups]
                    moved_backups = vintagebackup.all_backups(new_backup_location)
                    moved_names = [p.relative_to(new_backup_location) for p in moved_backups]
                    self.assertEqual(original_names, moved_names)
                    for backup_1, backup_2 in itertools.pairwise(moved_backups):
                        self.assertTrue(directories_are_completely_hardlinked(backup_1, backup_2))

    def test_move_n_backups_moves_subset_and_preserves_structure_and_hardlinks(self) -> None:
        """Test that moving N backups moves correct number of backups and correctly links files."""
        with (tempfile.TemporaryDirectory() as user_data_folder,
              tempfile.TemporaryDirectory() as backup_folder):
            user_data = Path(user_data_folder)
            create_user_data(user_data)
            backup_location = Path(backup_folder)
            for _ in range(10):
                vintagebackup.create_new_backup(user_data,
                                                backup_location,
                                                filter_file=None,
                                                examine_whole_file=False,
                                                force_copy=False,
                                                max_average_hard_links=None,
                                                timestamp=unique_timestamp())

            move_count = 5
            for method in Invocation:
                with tempfile.TemporaryDirectory() as new_backup_folder:
                    new_backup_location = Path(new_backup_folder)
                    if method == Invocation.function:
                        backups_to_move = vintagebackup.last_n_backups(backup_location, move_count)
                        self.assertEqual(len(backups_to_move), move_count)
                        vintagebackup.move_backups(backup_location,
                                                   new_backup_location,
                                                   backups_to_move)
                    elif method == Invocation.cli:
                        exit_code = vintagebackup.main(["--backup-folder", backup_folder,
                                                        "--log", os.devnull,
                                                        "--move-backup", new_backup_folder,
                                                        "--move-count", str(move_count)])
                        self.assertEqual(exit_code, 0)
                    else:
                        raise NotImplementedError(f"Move backup test not implemented for {method}")

                    backups_at_new_location = vintagebackup.all_backups(new_backup_location)
                    self.assertEqual(len(backups_at_new_location), move_count)
                    old_backups = vintagebackup.last_n_backups(backup_location, move_count)
                    old_backup_names = [p.relative_to(backup_location) for p in old_backups]
                    new_backups = vintagebackup.all_backups(new_backup_location)
                    new_backup_names = [p.relative_to(new_backup_location) for p in new_backups]
                    self.assertEqual(old_backup_names, new_backup_names)
                    self.assertEqual(vintagebackup.backup_source(backup_location),
                                     vintagebackup.backup_source(new_backup_location))
                    for backup_1, backup_2 in itertools.pairwise(new_backups):
                        self.assertTrue(directories_are_completely_hardlinked(backup_1, backup_2))

    def test_move_age_backups_moves_only_backups_within_given_timespan(self) -> None:
        """Test that moving backups based on a time span works."""
        with tempfile.TemporaryDirectory() as backup_folder:
            backup_location = Path(backup_folder)
            create_old_backups(backup_location, 25)
            six_months_ago = vintagebackup.parse_time_span_to_timepoint("6m")
            backups_to_move = vintagebackup.backups_since(six_months_ago, backup_location)
            self.assertEqual(len(backups_to_move), 6)
            self.assertEqual(vintagebackup.last_n_backups(backup_location, 6), backups_to_move)
            oldest_backup_timestamp = vintagebackup.backup_datetime(backups_to_move[0])
            self.assertLessEqual(six_months_ago, oldest_backup_timestamp)

    def test_move_without_specifying_how_many_to_move_is_an_error(self) -> None:
        """Test that missing --move-count, --move-age, and --move-since results in an error."""
        with (tempfile.TemporaryDirectory() as user_folder,
              tempfile.TemporaryDirectory() as backup_folder):
            user_path = Path(user_folder)
            create_user_data(user_path)
            backup_path = Path(backup_folder)
            vintagebackup.create_new_backup(user_path,
                                            backup_path,
                                            filter_file=None,
                                            examine_whole_file=False,
                                            force_copy=False,
                                            max_average_hard_links=None,
                                            timestamp=unique_timestamp())
            with (self.assertLogs(level=logging.ERROR) as no_move_choice_log,
                  tempfile.TemporaryDirectory() as move_destination):
                exit_code = vintagebackup.main(["--move-backup", move_destination,
                                                "--user-folder", user_folder,
                                                "--backup-folder", backup_folder,
                                                "--log", os.devnull])
            self.assertEqual(exit_code, 1)
            expected_logs = ["ERROR:vintagebackup:One of the following are required: "
                             "--move-count, --move-age, or --move-since"]
            self.assertEqual(expected_logs, no_move_choice_log.output)


class VerificationTest(unittest.TestCase):
    """Test backup verification."""

    def test_backup_verification_sorts_files_into_matching_mismatching_and_errors(self) -> None:
        """Test that verification sorts files into matching, mismatching, and error lists."""
        with (tempfile.TemporaryDirectory() as user_folder,
              tempfile.TemporaryDirectory() as backup_folder):
            user_location = Path(user_folder)
            backup_location = Path(backup_folder)
            create_user_data(user_location)
            vintagebackup.create_new_backup(user_location,
                                            backup_location,
                                            filter_file=None,
                                            examine_whole_file=False,
                                            force_copy=False,
                                            max_average_hard_links=None,
                                            timestamp=unique_timestamp())

            mismatch_file = Path("sub_directory_1")/"sub_sub_directory_2"/"file_0.txt"
            with open(user_location/mismatch_file, "a") as file:
                file.write("\naddition\n")

            error_file = Path("sub_directory_2")/"sub_sub_directory_0"/"file_1.txt"
            last_backup = vintagebackup.find_previous_backup(backup_location)
            self.assertTrue(last_backup)
            last_backup = cast(Path, last_backup)
            (last_backup/error_file).unlink()

            matching_path_set: set[Path] = set()
            mismatching_path_set: set[Path] = set()
            error_path_set: set[Path] = set()
            user_paths = vintagebackup.Backup_Set(user_location, None)
            for directory, file_names in user_paths:
                for file_name in file_names:
                    path = (directory/file_name).relative_to(user_location)
                    path_set = (mismatching_path_set if path == mismatch_file
                                else error_path_set if path == error_file
                                else matching_path_set)
                    path_set.add(path)

            for method in Invocation:
                with tempfile.TemporaryDirectory() as verification_folder:
                    verification_location = Path(verification_folder)
                    if method == Invocation.function:
                        vintagebackup.verify_last_backup(user_location,
                                                         backup_location,
                                                         None,
                                                         verification_location)
                    else:
                        exit_code = vintagebackup.main(["--user-folder", user_folder,
                                                        "--backup-folder", backup_folder,
                                                        "--verify", verification_folder,
                                                        "--log", os.devnull])
                        self.assertEqual(exit_code, 0)

                    for file_name in os.listdir(verification_location):
                        if " matching " in file_name:
                            path_set = matching_path_set
                        elif " mismatching " in file_name:
                            path_set = mismatching_path_set
                        elif " error " in file_name:
                            path_set = error_path_set
                        else:
                            # Should be unreachable
                            raise AssertionError

                        verify_file_path = verification_location/file_name
                        with open(verify_file_path) as verify_file:
                            verify_file.readline()
                            files_from_verify = {Path(line.strip("\n")) for line in verify_file}

                        self.assertEqual(files_from_verify, path_set)


class ConfigurationFileTest(unittest.TestCase):
    """Test configuration file functionality."""

    def test_configuration_file_reading_is_insensitive_to_variant_writings(self) -> None:
        """
        Test that configuration file reading is insensitive to variations in writing.

        These include:
        1. Upper vs. lowercase vs. mixed
        2. Spacing
        3. Parameters spelled with dashes (as on command line) or spaces
        """
        with tempfile.NamedTemporaryFile("w+", delete_on_close=False) as config_file:
            user_folder = r"C:\Files"
            backup_folder = r"D:\Backup"
            filter_file = "filter_file.txt"
            config_file.write(rf"""
USER FOLDER:     {user_folder}
backup folder:   {backup_folder}
  FiLteR    :    {filter_file}
  force-copy:
  whole    file :
""")
            config_file.close()
            command_line = vintagebackup.read_configuation_file(config_file.name)
            self.assertEqual(command_line,
                             ["--user-folder", user_folder,
                             "--backup-folder", backup_folder,
                             "--filter", filter_file,
                             "--force-copy",
                             "--whole-file"])
            arg_parser = vintagebackup.argument_parser()
            args = arg_parser.parse_args(command_line)
            self.assertEqual(args.user_folder, user_folder)
            self.assertEqual(args.backup_folder, backup_folder)
            self.assertEqual(args.filter, filter_file)
            self.assertTrue(args.force_copy)

    def test_command_line_options_override_config_file_options(self) -> None:
        """Test that command line options override file configurations and leave others alone."""
        with tempfile.NamedTemporaryFile("w+", delete_on_close=False) as config_file:
            user_folder = r"C:\Users\Test User"
            config_file.write(rf"""
User Folder : {user_folder}
Backup Folder: temp_back
filter: filter.txt
log: temp_log.txt
whole file:
Debug:""")
            config_file.close()
            actual_backup_folder = "temp_back2"
            actual_log_file = "temporary_log.log"
            command_line_options = ["-b", actual_backup_folder,
                                    "-c", config_file.name,
                                    "-l", actual_log_file]
            options = vintagebackup.parse_command_line(command_line_options)
            self.assertEqual(options.user_folder, user_folder)
            self.assertEqual(options.backup_folder, actual_backup_folder)
            self.assertEqual(options.log, actual_log_file)
            self.assertTrue(options.whole_file)
            self.assertTrue(options.debug)

    def test_negating_command_line_parameters_override_config_file(self) -> None:
        """Test that command line options like --no-X override file configurations."""
        with tempfile.NamedTemporaryFile("w+", delete_on_close=False) as config_file:
            config_file.write(r"""
whole file:
Debug:""")
            config_file.close()
            command_line_options = ["-c", config_file.name,
                                    "--no-whole-file",
                                    "--no-debug"]
            options = vintagebackup.parse_command_line(command_line_options)
            self.assertFalse(vintagebackup.toggle_is_set(options, "whole_file"))
            self.assertFalse(vintagebackup.toggle_is_set(options, "debug"))

    def test_recursive_config_files_are_not_allowed(self) -> None:
        """Test that putting a config parameter in a configuration file raises an exception."""
        with tempfile.NamedTemporaryFile("w+", delete_on_close=False) as config_file:
            config_file.write("config: config_file_2.txt")
            config_file.close()
            with self.assertRaises(vintagebackup.CommandLineError):
                vintagebackup.read_configuation_file(config_file.name)


class ErrorTest(unittest.TestCase):
    """Test that bad user inputs raise correct exceptions."""

    def test_no_user_folder_specified_for_backup_is_an_error(self) -> None:
        """Test that omitting the user folder prints the correct error message."""
        with self.assertLogs(level=logging.ERROR) as log_check:
            exit_code = vintagebackup.main(["-b", "backup_folder", "-l", os.devnull])
        self.assertEqual(exit_code, 1)
        self.assertEqual(log_check.output, ["ERROR:vintagebackup:User's folder not specified."])

    def test_no_backup_folder_specified_for_backup_error(self) -> None:
        """Test that omitting the backup folder prints the correct error message."""
        with (tempfile.TemporaryDirectory() as user_folder,
              self.assertLogs(level=logging.ERROR) as log_check):
            exit_code = vintagebackup.main(["-u", user_folder, "-l", os.devnull])
        self.assertEqual(exit_code, 1)
        self.assertEqual(log_check.output, ["ERROR:vintagebackup:Backup folder not specified."])

    def test_non_existent_user_folder_in_a_backup_is_an_error(self) -> None:
        """Test that non-existent user folder prints correct error message."""
        user_folder = "".join(random.choices(string.ascii_letters, k=50))
        with self.assertLogs(level=logging.ERROR) as log_check:
            exit_code = vintagebackup.main(["-u", user_folder, "-l", os.devnull])
        self.assertEqual(exit_code, 1)
        expected_logs = [f"ERROR:vintagebackup:Could not find user's folder: {user_folder}"]
        self.assertEqual(log_check.output, expected_logs)

    def test_backing_up_different_user_folders_to_same_backup_location_is_an_error(self) -> None:
        """Check that error is raised when attempted to change the source of a backup set."""
        with (tempfile.TemporaryDirectory() as user_folder,
              tempfile.TemporaryDirectory() as other_user_folder,
              tempfile.TemporaryDirectory() as backup_folder,
              self.assertRaises(RuntimeError) as error):
            user_path = Path(user_folder)
            backup_path = Path(backup_folder)
            vintagebackup.create_new_backup(user_path,
                                            backup_path,
                                            filter_file=None,
                                            examine_whole_file=False,
                                            force_copy=False,
                                            max_average_hard_links=None,
                                            timestamp=unique_timestamp())

            other_user_path = Path(other_user_folder)
            vintagebackup.create_new_backup(other_user_path,
                                            backup_path,
                                            filter_file=None,
                                            examine_whole_file=False,
                                            force_copy=False,
                                            max_average_hard_links=None,
                                            timestamp=unique_timestamp())

        expected_error_message = ("Previous backup stored a different user folder. Previously: "
                                  f"{user_path.resolve()}; Now: {other_user_path.resolve()}")
        self.assertEqual(error.exception.args, (expected_error_message,))

    def test_warning_printed_if_no_user_data_is_backed_up(self) -> None:
        """Make sure a warning is printed if no files are backed up."""
        with (tempfile.TemporaryDirectory() as user_folder,
              tempfile.TemporaryDirectory() as backup_folder):
            user_path = Path(user_folder)
            backup_path = Path(backup_folder)
            with self.assertLogs(level=logging.WARNING) as assert_log:
                vintagebackup.create_new_backup(user_path,
                                                backup_path,
                                                filter_file=None,
                                                examine_whole_file=False,
                                                force_copy=False,
                                                max_average_hard_links=None,
                                                timestamp=unique_timestamp())
            self.assertIn("WARNING:vintagebackup:No files were backed up!", assert_log.output)
            self.assertEqual(os.listdir(backup_path), ["vintagebackup.source.txt"])

    def test_no_dated_backup_folder_created_if_no_data_backed_up(self) -> None:
        """Test that a dated backup folder is not created if there is no data to back up."""
        with (tempfile.TemporaryDirectory() as user_folder,
              tempfile.TemporaryDirectory() as backup_folder):
            user_path = Path(user_folder)
            backup_path = Path(backup_folder)
            vintagebackup.create_new_backup(user_path,
                                            backup_path,
                                            filter_file=None,
                                            examine_whole_file=False,
                                            force_copy=False,
                                            max_average_hard_links=None,
                                            timestamp=unique_timestamp())
            self.assertEqual(os.listdir(backup_path), ["vintagebackup.source.txt"])

    def test_warning_printed_if_all_user_files_filtered_out(self) -> None:
        """Make sure the user is warned if a filter file removes all files from the backup set."""
        with (tempfile.TemporaryDirectory() as user_folder,
              tempfile.TemporaryDirectory() as backup_folder,
              tempfile.NamedTemporaryFile(delete_on_close=False) as filter_file_name):
            user_path = Path(user_folder)
            create_user_data(user_path)
            filter_path = Path(filter_file_name.name)
            with filter_path.open("w") as filter_file:
                filter_file.write("- **/*.txt\n")
            filter_file_name.close()
            backup_path = Path(backup_folder)

            with self.assertLogs(level=logging.WARNING) as assert_log:
                vintagebackup.create_new_backup(user_path,
                                                backup_path,
                                                filter_file=filter_path,
                                                examine_whole_file=False,
                                                force_copy=False,
                                                max_average_hard_links=None,
                                                timestamp=unique_timestamp())
            self.assertIn("WARNING:vintagebackup:No files were backed up!", assert_log.output)
            self.assertEqual(os.listdir(backup_path), ["vintagebackup.source.txt"])


class RestorationTest(unittest.TestCase):
    """Test that restoring backups works correctly."""

    def test_restore_last_backup_with_delete_extra_option_deletes_new_files(self) -> None:
        """Test that restoring with --delete-extra deletes new files since last backup."""
        with (tempfile.TemporaryDirectory() as user_folder,
              tempfile.TemporaryDirectory() as backup_folder):
            user_path = Path(user_folder)
            create_user_data(user_path)
            backup_path = Path(backup_folder)
            vintagebackup.create_new_backup(user_path,
                                            backup_path,
                                            filter_file=None,
                                            examine_whole_file=False,
                                            force_copy=False,
                                            max_average_hard_links=None,
                                            timestamp=unique_timestamp())

            self.assertEqual(len(vintagebackup.all_backups(backup_path)), 1)

            first_extra_file = user_path/"extra_file1.txt"
            first_extra_file.write_text("extra 1\n")

            vintagebackup.create_new_backup(user_path,
                                            backup_path,
                                            filter_file=None,
                                            examine_whole_file=False,
                                            force_copy=False,
                                            max_average_hard_links=None,
                                            timestamp=unique_timestamp())
            self.assertEqual(len(vintagebackup.all_backups(backup_path)), 2)

            second_extra_file = user_path/"extra_file2.txt"
            second_extra_file.write_text("extra 2\n")

            exit_code = vintagebackup.main(["--restore",
                                            "--user-folder", user_folder,
                                            "--backup-folder", backup_folder,
                                            "--last-backup", "--delete-extra",
                                            "--log", os.devnull,
                                            "--skip-prompt"])

            self.assertEqual(exit_code, 0)
            last_backup = vintagebackup.find_previous_backup(backup_path)
            self.assertTrue(last_backup)
            last_backup = cast(Path, last_backup)
            self.assertTrue(first_extra_file.exists(follow_symlinks=False))
            self.assertFalse(second_extra_file.exists(follow_symlinks=False))
            self.assertTrue(directories_have_identical_content(user_path, last_backup))

    def test_restore_last_backup_with_keep_extra_preserves_new_files(self) -> None:
        """Test that restoring with --keep-extra does not delete new files since the last backup."""
        with (tempfile.TemporaryDirectory() as user_folder,
              tempfile.TemporaryDirectory() as backup_folder):
            user_path = Path(user_folder)
            create_user_data(user_path)
            backup_path = Path(backup_folder)
            vintagebackup.create_new_backup(user_path,
                                            backup_path,
                                            filter_file=None,
                                            examine_whole_file=False,
                                            force_copy=False,
                                            max_average_hard_links=None,
                                            timestamp=unique_timestamp())

            self.assertEqual(len(vintagebackup.all_backups(backup_path)), 1)

            first_extra_file = user_path/"extra_file1.txt"
            first_extra_file.write_text("extra 1\n")

            vintagebackup.create_new_backup(user_path,
                                            backup_path,
                                            filter_file=None,
                                            examine_whole_file=False,
                                            force_copy=False,
                                            max_average_hard_links=None,
                                            timestamp=unique_timestamp())
            self.assertEqual(len(vintagebackup.all_backups(backup_path)), 2)

            second_extra_file = user_path/"extra_file2.txt"
            second_extra_file.write_text("extra 2\n")

            exit_code = vintagebackup.main(["--restore",
                                            "--user-folder", user_folder,
                                            "--backup-folder", backup_folder,
                                            "--last-backup", "--keep-extra",
                                            "--log", os.devnull,
                                            "--skip-prompt"])

            self.assertEqual(exit_code, 0)
            last_backup = vintagebackup.find_previous_backup(backup_path)
            self.assertTrue(last_backup)
            last_backup = cast(Path, last_backup)
            self.assertTrue(first_extra_file.exists(follow_symlinks=False))
            self.assertTrue(second_extra_file.exists(follow_symlinks=False))
            second_extra_file.unlink()
            self.assertTrue(directories_have_identical_content(user_path, last_backup))

    def test_restore_backup_from_menu_choice_and_delete_extra_deletes_new_files(self) -> None:
        """Test restoring a chosen backup from a menu with --delete-extra deletes new files."""
        with (tempfile.TemporaryDirectory() as user_folder,
              tempfile.TemporaryDirectory() as backup_folder):
            user_path = Path(user_folder)
            create_user_data(user_path)
            backup_path = Path(backup_folder)
            vintagebackup.create_new_backup(user_path,
                                            backup_path,
                                            filter_file=None,
                                            examine_whole_file=False,
                                            force_copy=False,
                                            max_average_hard_links=None,
                                            timestamp=unique_timestamp())

            self.assertEqual(len(vintagebackup.all_backups(backup_path)), 1)

            first_extra_file = user_path/"extra_file1.txt"
            first_extra_file.write_text("extra 1\n")

            vintagebackup.create_new_backup(user_path,
                                            backup_path,
                                            filter_file=None,
                                            examine_whole_file=False,
                                            force_copy=False,
                                            max_average_hard_links=None,
                                            timestamp=unique_timestamp())
            self.assertEqual(len(vintagebackup.all_backups(backup_path)), 2)

            second_extra_file = user_path/"extra_file2.txt"
            second_extra_file.write_text("extra 2\n")

            choice = 0
            exit_code = vintagebackup.main(["--restore",
                                            "--user-folder", user_folder,
                                            "--backup-folder", backup_folder,
                                            "--choose-backup", "--delete-extra",
                                            "--log", os.devnull,
                                            "--choice", str(choice),
                                            "--skip-prompt"])

            self.assertEqual(exit_code, 0)
            restored_backup = vintagebackup.all_backups(backup_path)[choice]
            self.assertFalse(first_extra_file.exists(follow_symlinks=False))
            self.assertFalse(second_extra_file.exists(follow_symlinks=False))
            self.assertTrue(directories_have_identical_content(user_path, restored_backup))

    def test_restore_backup_from_menu_choice_and_keep_extra_preserves_new_files(self) -> None:
        """Test restoring a chosen backup from a menu with --keep-extra preserves new files."""
        with (tempfile.TemporaryDirectory() as user_folder,
              tempfile.TemporaryDirectory() as backup_folder):
            user_path = Path(user_folder)
            create_user_data(user_path)
            backup_path = Path(backup_folder)
            vintagebackup.create_new_backup(user_path,
                                            backup_path,
                                            filter_file=None,
                                            examine_whole_file=False,
                                            force_copy=False,
                                            max_average_hard_links=None,
                                            timestamp=unique_timestamp())

            self.assertEqual(len(vintagebackup.all_backups(backup_path)), 1)

            first_extra_file = user_path/"extra_file1.txt"
            first_extra_file.write_text("extra 1\n")

            vintagebackup.create_new_backup(user_path,
                                            backup_path,
                                            filter_file=None,
                                            examine_whole_file=False,
                                            force_copy=False,
                                            max_average_hard_links=None,
                                            timestamp=unique_timestamp())
            self.assertEqual(len(vintagebackup.all_backups(backup_path)), 2)

            second_extra_file = user_path/"extra_file2.txt"
            second_extra_file.write_text("extra 2\n")

            choice = 0
            exit_code = vintagebackup.main(["--restore",
                                            "--user-folder", user_folder,
                                            "--backup-folder", backup_folder,
                                            "--choose-backup", "--keep-extra",
                                            "--log", os.devnull,
                                            "--choice", str(choice),
                                            "--skip-prompt"])

            self.assertEqual(exit_code, 0)
            restored_backup = vintagebackup.all_backups(backup_path)[choice]
            self.assertTrue(first_extra_file.exists(follow_symlinks=False))
            self.assertTrue(second_extra_file.exists(follow_symlinks=False))
            first_extra_file.unlink()
            second_extra_file.unlink()
            self.assertTrue(directories_have_identical_content(user_path, restored_backup))

    def test_restore_backup_with_destination_delete_extra_restores_to_new_location(self) -> None:
        """Test restoring with --destination and --delete-extra recreates backup in new location."""
        with (tempfile.TemporaryDirectory() as user_folder,
              tempfile.TemporaryDirectory() as backup_folder,
              tempfile.TemporaryDirectory() as destination_folder):
            user_path = Path(user_folder)
            create_user_data(user_path)
            backup_path = Path(backup_folder)
            vintagebackup.create_new_backup(user_path,
                                            backup_path,
                                            filter_file=None,
                                            examine_whole_file=False,
                                            force_copy=False,
                                            max_average_hard_links=None,
                                            timestamp=unique_timestamp())

            exit_code = vintagebackup.main(["--restore",
                                            "--user-folder", user_folder,
                                            "--backup-folder", backup_folder,
                                            "--last-backup", "--delete-extra",
                                            "--log", os.devnull,
                                            "--destination", destination_folder,
                                            "--skip-prompt"])

            self.assertEqual(exit_code, 0)
            destination_path = Path(destination_folder)
            last_backup = vintagebackup.find_previous_backup(backup_path)
            self.assertTrue(last_backup)
            last_backup = cast(Path, last_backup)
            self.assertTrue(directories_have_identical_content(last_backup, destination_path))
            self.assertTrue(directories_have_identical_content(user_path, destination_path))

    def test_restore_backup_with_destination_keep_extra_preserves_extra_files(self) -> None:
        """Test restoring with --destination and --keep-extra keeps extra files in new location."""
        with (tempfile.TemporaryDirectory() as user_folder,
              tempfile.TemporaryDirectory() as backup_folder,
              tempfile.TemporaryDirectory() as destination_folder):
            user_path = Path(user_folder)
            create_user_data(user_path)
            backup_path = Path(backup_folder)
            vintagebackup.create_new_backup(user_path,
                                            backup_path,
                                            filter_file=None,
                                            examine_whole_file=False,
                                            force_copy=False,
                                            max_average_hard_links=None,
                                            timestamp=unique_timestamp())

            destination_path = Path(destination_folder)
            extra_file = destination_path/"extra_file1.txt"
            with extra_file.open("w") as file1:
                file1.write("extra 1\n")

            exit_code = vintagebackup.main(["--restore",
                                            "--user-folder", user_folder,
                                            "--backup-folder", backup_folder,
                                            "--last-backup", "--keep-extra",
                                            "--log", os.devnull,
                                            "--destination", destination_folder,
                                            "--skip-prompt"])

            self.assertEqual(exit_code, 0)
            self.assertTrue(extra_file.is_file(follow_symlinks=False))
            last_backup = vintagebackup.find_previous_backup(backup_path)
            self.assertTrue(last_backup)
            last_backup = cast(Path, last_backup)
            extra_file.unlink()
            self.assertTrue(directories_have_identical_content(last_backup, destination_path))
            self.assertTrue(directories_have_identical_content(user_path, destination_path))

    def test_restore_without_delete_extra_or_keep_extra_is_an_error(self) -> None:
        """Test that missing --delete-extra and --keep-extra results in an error."""
        with (tempfile.TemporaryDirectory() as user_folder,
              tempfile.TemporaryDirectory() as backup_folder):
            user_path = Path(user_folder)
            create_user_data(user_path)
            backup_path = Path(backup_folder)
            vintagebackup.create_new_backup(user_path,
                                            backup_path,
                                            filter_file=None,
                                            examine_whole_file=False,
                                            force_copy=False,
                                            max_average_hard_links=None,
                                            timestamp=unique_timestamp())

            with self.assertLogs(level=logging.ERROR) as no_extra_log:
                exit_code = vintagebackup.main(["--restore",
                                                "--user-folder", user_folder,
                                                "--backup-folder", backup_folder,
                                                "--last-backup",
                                                "--log", os.devnull])
            self.assertEqual(exit_code, 1)
            expected_logs = ["ERROR:vintagebackup:One of the following are required: "
                             "--delete-extra or --keep-extra"]
            self.assertEqual(expected_logs, no_extra_log.output)

    def test_restore_without_last_backup_or_choose_backup_is_an_error(self) -> None:
        """Test that missing --last-backup and --choose-backup results in an error."""
        with (tempfile.TemporaryDirectory() as user_folder,
              tempfile.TemporaryDirectory() as backup_folder):
            user_path = Path(user_folder)
            create_user_data(user_path)
            backup_path = Path(backup_folder)
            vintagebackup.create_new_backup(user_path,
                                            backup_path,
                                            filter_file=None,
                                            examine_whole_file=False,
                                            force_copy=False,
                                            max_average_hard_links=None,
                                            timestamp=unique_timestamp())
            with self.assertLogs(level=logging.ERROR) as no_backup_choice_log:
                exit_code = vintagebackup.main(["--restore",
                                                "--user-folder", user_folder,
                                                "--backup-folder", backup_folder,
                                                "--keep-extra",
                                                "--log", os.devnull])
            self.assertEqual(exit_code, 1)
            expected_logs = ["ERROR:vintagebackup:One of the following are required: "
                             "--last-backup or --choose-backup"]
            self.assertEqual(expected_logs, no_backup_choice_log.output)

    def test_restore_with_bad_response_to_overwrite_confirmation_is_an_error(self) -> None:
        """Test that wrong response to overwrite confirmation ends program with error code."""
        with (tempfile.TemporaryDirectory() as user_folder,
              tempfile.TemporaryDirectory() as backup_folder):
            user_path = Path(user_folder)
            create_user_data(user_path)
            backup_path = Path(backup_folder)
            vintagebackup.create_new_backup(user_path,
                                            backup_path,
                                            filter_file=None,
                                            examine_whole_file=False,
                                            force_copy=False,
                                            max_average_hard_links=None,
                                            timestamp=unique_timestamp())
            with self.assertLogs(level=logging.INFO) as bad_prompt_log:
                vintagebackup.main(["--restore",
                                    "--user-folder", user_folder,
                                    "--backup-folder", backup_folder,
                                    "--choose-backup",
                                    "--delete-extra",
                                    "--log", os.devnull,
                                    "--skip-prompt",
                                    "--bad-input",
                                    "--choice", "0"])
            rejection_line = ('INFO:vintagebackup:The response was "no" and not "yes", so the '
                              'restoration is cancelled.')
            self.assertIn(rejection_line, bad_prompt_log.output)


class BackupLockTest(unittest.TestCase):
    """Test that the lock prevents simultaneous access to a backup location."""

    def test_backup_while_lock_is_present_raises_concurrency_error(self) -> None:
        """Test that locking raises an error when the lock is present."""
        with (tempfile.TemporaryDirectory() as user_folder,
              tempfile.TemporaryDirectory() as backup_folder):
            user_path = Path(user_folder)
            create_user_data(user_path)
            backup_path = Path(backup_folder)
            with vintagebackup.Backup_Lock(backup_path, "no wait test"):
                exit_code = run_backup(Invocation.cli,
                                       user_path,
                                       backup_path,
                                       filter_file=None,
                                       examine_whole_file=False,
                                       force_copy=False,
                                       timestamp=unique_timestamp())
                self.assertNotEqual(exit_code, 0)

                with self.assertRaises(vintagebackup.ConcurrencyError):
                    options = vintagebackup.argument_parser()
                    args = options.parse_args(["--user-folder", user_folder,
                                               "--backup-folder", backup_folder])
                    vintagebackup.start_backup(args)

    def test_lock_writes_process_info_to_lock_file_and_deletes_on_exit(self) -> None:
        """Test that lock file is created when entering with statement and deleted when exiting."""
        with tempfile.TemporaryDirectory() as backup_folder:
            backup_path = Path(backup_folder)
            test_pid = str(os.getpid())
            test_operation = "lock data test"
            with vintagebackup.Backup_Lock(backup_path, test_operation):
                lock_path = backup_path/"vintagebackup.lock"
                with lock_path.open() as lock_file:
                    lock_pid, lock_operation = (s.strip() for s in lock_file)

                self.assertEqual(lock_pid, test_pid)
                self.assertEqual(lock_operation, test_operation)

            self.assertFalse(lock_path.is_file(follow_symlinks=False))


class MaxAverageHardLinksTest(unittest.TestCase):
    """Test that specifying an average hard link count results in identical files being copied."""

    def test_max_average_hard_links_causes_some_unchanged_files_to_be_copied(self) -> None:
        """Test some files are copied instead of linked when max_average_hard_links is non-zero."""
        with (tempfile.TemporaryDirectory() as user_folder,
              tempfile.TemporaryDirectory() as backup_folder):
            user_path = Path(user_folder)
            create_user_data(user_path)
            backup_path = Path(backup_folder)

            vintagebackup.create_new_backup(user_path,
                                            backup_path,
                                            filter_file=None,
                                            examine_whole_file=False,
                                            force_copy=False,
                                            max_average_hard_links="1",
                                            timestamp=unique_timestamp())

            vintagebackup.create_new_backup(user_path,
                                            backup_path,
                                            filter_file=None,
                                            examine_whole_file=False,
                                            force_copy=False,
                                            max_average_hard_links="1",
                                            timestamp=unique_timestamp())

            all_backups = vintagebackup.all_backups(backup_path)
            self.assertEqual(len(all_backups), 2)
            self.assertTrue(all_files_have_same_content(*all_backups))
            self.assertFalse(directories_are_completely_hardlinked(*all_backups))
            self.assertFalse(directories_are_completely_copied(*all_backups))

    def test_hard_link_count_must_be_a_positive_number(self) -> None:
        """Test that all inputs to --hard-link-count besides positive whole numbers are errors."""
        with (tempfile.TemporaryDirectory() as user_folder,
              tempfile.TemporaryDirectory() as backup_folder):
            user_path = Path(user_folder)
            backup_path = Path(backup_folder)
            with self.assertRaises(vintagebackup.CommandLineError) as error:
                vintagebackup.create_new_backup(user_path,
                                                backup_path,
                                                filter_file=None,
                                                examine_whole_file=False,
                                                force_copy=False,
                                                max_average_hard_links="Z",
                                                timestamp=unique_timestamp())
            self.assertEqual(error.exception.args[0], "Invalid value for hard link count: Z")

            with self.assertRaises(vintagebackup.CommandLineError) as error:
                vintagebackup.create_new_backup(user_path,
                                                backup_path,
                                                filter_file=None,
                                                examine_whole_file=False,
                                                force_copy=False,
                                                max_average_hard_links="0",
                                                timestamp=unique_timestamp())
            self.assertEqual(error.exception.args[0],
                             "Hard link count must be a positive whole number. Got: 0")


class AtomicBackupTests(unittest.TestCase):
    """Test atomicity of backups."""

    def test_existence_of_staging_folder_before_backup_is_an_error(self) -> None:
        """Test that the existence of the staging folder prevents other backups from running."""
        with (tempfile.TemporaryDirectory() as user_folder,
              tempfile.TemporaryDirectory() as backup_folder):
            user_path = Path(user_folder)
            backup_path = Path(backup_folder)
            (backup_path/"Staging").mkdir()
            with self.assertRaises(RuntimeError) as error:
                vintagebackup.create_new_backup(user_path,
                                                backup_path,
                                                filter_file=None,
                                                examine_whole_file=False,
                                                force_copy=False,
                                                max_average_hard_links=None,
                                                timestamp=unique_timestamp())
            error_message, = error.exception.args
            self.assertIn("Staging", error_message)

    def test_staging_folder_does_not_exist_after_successful_backup(self) -> None:
        """Test that the staging folder is deleted after a successful backup."""
        with (tempfile.TemporaryDirectory() as user_folder,
              tempfile.TemporaryDirectory() as backup_folder):
            user_path = Path(user_folder)
            create_user_data(user_path)
            backup_path = Path(backup_folder)
            staging_path = backup_path/"Staging"
            vintagebackup.create_new_backup(user_path,
                                            backup_path,
                                            filter_file=None,
                                            examine_whole_file=False,
                                            force_copy=False,
                                            max_average_hard_links=None,
                                            timestamp=unique_timestamp())
            self.assertFalse(staging_path.exists())


class PurgeTests(unittest.TestCase):
    """Tests for purging files and folders from backups."""

    def test_file_purge(self) -> None:
        """Test that a purged file is deleted from all backups."""
        with (tempfile.TemporaryDirectory() as user_folder,
              tempfile.TemporaryDirectory() as backup_folder):
            user_path = Path(user_folder)
            create_user_data(user_path)
            number_of_backups = 5
            backup_path = Path(backup_folder)
            for _ in range(number_of_backups):
                vintagebackup.create_new_backup(user_path,
                                                backup_path,
                                                filter_file=None,
                                                examine_whole_file=False,
                                                force_copy=False,
                                                max_average_hard_links=None,
                                                timestamp=unique_timestamp())

            purged_file = user_path/"sub_directory_2"/"sub_sub_directory_1"/"file_0.txt"
            self.assertTrue(purged_file.is_file())
            purge_command_line = vintagebackup.parse_command_line(["--purge",
                                                                   str(purged_file),
                                                                   "--backup-folder",
                                                                   str(backup_path)])
            vintagebackup.start_backup_purge(purge_command_line, "y")
            relative_purge_file = purged_file.relative_to(user_path)
            for backup in vintagebackup.all_backups(backup_path):
                self.assertFalse((backup/relative_purge_file).exists())

    def test_folder_purge(self) -> None:
        """Test that a purged folder is deleted from all backups."""
        with (tempfile.TemporaryDirectory() as user_folder,
              tempfile.TemporaryDirectory() as backup_folder):
            user_path = Path(user_folder)
            create_user_data(user_path)
            number_of_backups = 5
            backup_path = Path(backup_folder)
            for _ in range(number_of_backups):
                vintagebackup.create_new_backup(user_path,
                                                backup_path,
                                                filter_file=None,
                                                examine_whole_file=False,
                                                force_copy=False,
                                                max_average_hard_links=None,
                                                timestamp=unique_timestamp())

            purged_folder = user_path/"sub_directory_2"/"sub_sub_directory_1"
            self.assertTrue(purged_folder.is_dir())
            purge_command_line = vintagebackup.parse_command_line(["--purge",
                                                                   str(purged_folder),
                                                                   "--backup-folder",
                                                                   str(backup_path)])
            vintagebackup.start_backup_purge(purge_command_line, "y")
            relative_purge_folder = purged_folder.relative_to(user_path)
            for backup in vintagebackup.all_backups(backup_path):
                self.assertFalse((backup/relative_purge_folder).exists())

    def test_file_purge_with_prompt_only_deletes_files(self) -> None:
        """Test that a purging a non-existent file only deletes files in backups."""
        with (tempfile.TemporaryDirectory() as user_folder,
              tempfile.TemporaryDirectory() as backup_folder):
            user_path = Path(user_folder)
            create_user_data(user_path)
            number_of_backups = 5
            backup_path = Path(backup_folder)
            for _ in range(number_of_backups):
                vintagebackup.create_new_backup(user_path,
                                                backup_path,
                                                filter_file=None,
                                                examine_whole_file=False,
                                                force_copy=False,
                                                max_average_hard_links=None,
                                                timestamp=unique_timestamp())

            purged_path = user_path/"sub_directory_2"/"sub_sub_directory_1"
            vintagebackup.delete_directory_tree(purged_path)
            purged_path.touch()

            for _ in range(number_of_backups):
                vintagebackup.create_new_backup(user_path,
                                                backup_path,
                                                filter_file=None,
                                                examine_whole_file=False,
                                                force_copy=False,
                                                max_average_hard_links=None,
                                                timestamp=unique_timestamp())

            self.assertTrue(purged_path.is_file())
            purged_path.unlink()
            purge_command_line = vintagebackup.parse_command_line(["--purge",
                                                                   str(purged_path),
                                                                   "--backup-folder",
                                                                   str(backup_path),
                                                                   "--choice", "0"])
            vintagebackup.start_backup_purge(purge_command_line, "y")
            relative_purge_file = purged_path.relative_to(user_path)
            for backup in vintagebackup.all_backups(backup_path):
                backup_file_path = backup/relative_purge_file
                self.assertTrue(vintagebackup.is_real_directory(backup_file_path)
                                or not backup_file_path.exists())

    def test_folder_purge_with_prompt_only_deletes_folders(self) -> None:
        """Test that a purging a non-existent file only deletes files in backups."""
        with (tempfile.TemporaryDirectory() as user_folder,
              tempfile.TemporaryDirectory() as backup_folder):
            user_path = Path(user_folder)
            create_user_data(user_path)
            number_of_backups = 5
            backup_path = Path(backup_folder)
            for _ in range(number_of_backups):
                vintagebackup.create_new_backup(user_path,
                                                backup_path,
                                                filter_file=None,
                                                examine_whole_file=False,
                                                force_copy=False,
                                                max_average_hard_links=None,
                                                timestamp=unique_timestamp())

            purged_path = user_path/"sub_directory_2"/"sub_sub_directory_1"
            vintagebackup.delete_directory_tree(purged_path)
            purged_path.touch()

            for _ in range(number_of_backups):
                vintagebackup.create_new_backup(user_path,
                                                backup_path,
                                                filter_file=None,
                                                examine_whole_file=False,
                                                force_copy=False,
                                                max_average_hard_links=None,
                                                timestamp=unique_timestamp())

            self.assertTrue(purged_path.is_file())
            purged_path.unlink()
            purge_command_line = vintagebackup.parse_command_line(["--purge",
                                                                   str(purged_path),
                                                                   "--backup-folder",
                                                                   str(backup_path),
                                                                   "--choice", "1"])
            vintagebackup.start_backup_purge(purge_command_line, "y")
            relative_purge_file = purged_path.relative_to(user_path)
            for backup in vintagebackup.all_backups(backup_path):
                backup_file_path = backup/relative_purge_file
                self.assertTrue(backup_file_path.is_file() or not backup_file_path.exists())

    def test_purge_with_non_y_confirmation_response_deletes_nothing(self) -> None:
        """Test that a purging a non-existent file only deletes files in backups."""
        with (tempfile.TemporaryDirectory() as user_folder,
              tempfile.TemporaryDirectory() as backup_folder):
            user_path = Path(user_folder)
            create_user_data(user_path)
            number_of_backups = 5
            backup_path = Path(backup_folder)
            for _ in range(number_of_backups):
                vintagebackup.create_new_backup(user_path,
                                                backup_path,
                                                filter_file=None,
                                                examine_whole_file=False,
                                                force_copy=False,
                                                max_average_hard_links=None,
                                                timestamp=unique_timestamp())

            purged_path = user_path/"sub_directory_2"/"sub_sub_directory_1"
            self.assertTrue(purged_path.is_dir(follow_symlinks=False))
            purge_command_line = vintagebackup.parse_command_line(["--purge",
                                                                   str(purged_path),
                                                                   "--backup-folder",
                                                                   str(backup_path)])
            vintagebackup.start_backup_purge(purge_command_line, "thing")

            for backup in vintagebackup.all_backups(backup_path):
                self.assertTrue(directories_have_identical_content(backup, user_path))

    def test_file_purge_from_list_with_prompt_only_deletes_folders(self) -> None:
        """Test that a purging a non-existent file only deletes files in backups."""
        with (tempfile.TemporaryDirectory() as user_folder,
              tempfile.TemporaryDirectory() as backup_folder):
            user_path = Path(user_folder)
            create_user_data(user_path)
            number_of_backups = 5
            backup_path = Path(backup_folder)
            for _ in range(number_of_backups):
                vintagebackup.create_new_backup(user_path,
                                                backup_path,
                                                filter_file=None,
                                                examine_whole_file=False,
                                                force_copy=False,
                                                max_average_hard_links=None,
                                                timestamp=unique_timestamp())

            purged_path = user_path/"sub_directory_2"/"sub_sub_directory_0"
            vintagebackup.delete_directory_tree(purged_path)
            purged_path.touch()

            for _ in range(number_of_backups):
                vintagebackup.create_new_backup(user_path,
                                                backup_path,
                                                filter_file=None,
                                                examine_whole_file=False,
                                                force_copy=False,
                                                max_average_hard_links=None,
                                                timestamp=unique_timestamp())

            self.assertTrue(purged_path.is_file())
            purged_path.unlink()
            search_directory = purged_path.parent
            purge_command_line = vintagebackup.parse_command_line(["--purge-list",
                                                                   str(search_directory),
                                                                   "--backup-folder",
                                                                   str(backup_path),
                                                                   "--choice", "0"])
            vintagebackup.choose_purge_target_from_backups(purge_command_line, "y")
            relative_purge_file = purged_path.relative_to(user_path)
            for backup in vintagebackup.all_backups(backup_path):
                backup_file_path = backup/relative_purge_file
                self.assertTrue(vintagebackup.is_real_directory(backup_file_path)
                                or not backup_file_path.exists())


def is_even(n: int) -> bool:
    """Return whether an integer is even."""
    return n % 2 == 0


class UtilityTest(unittest.TestCase):
    """Test stand-alone functions."""

    def test_fix_end_of_month_does_not_change_valid_dates(self) -> None:
        """Test that valid dates are returned unchanged."""
        start_date = datetime.date(2024, 1, 1)
        end_date = datetime.date(2025, 12, 31)
        date = start_date
        while date <= end_date:
            self.assertEqual(date, vintagebackup.fix_end_of_month(date.year, date.month, date.day))
            date += datetime.timedelta(days=1)

    def test_fix_end_of_month_always_returns_last_day_of_month_for_invalid_dates(self) -> None:
        """Test that an invalid date is fixed to be the end of the month."""
        january = 1
        december = 12

        for year in [2024, 2025]:
            for month in range(january, december + 1):
                bad_day = 40
                last_day_of_month = vintagebackup.fix_end_of_month(year, month, bad_day)
                day_after = last_day_of_month + datetime.timedelta(days=1)
                if last_day_of_month.month == december:
                    first_day_of_next_month = datetime.date(year + 1, january, 1)
                else:
                    first_day_of_next_month = datetime.date(year, month + 1, 1)
                self.assertEqual(day_after, first_day_of_next_month)

    def test_one_noun_results_in_singular_noun(self) -> None:
        """Test that exactly 1 of a noun leaves the noun unchanged."""
        self.assertEqual(vintagebackup.plural_noun(1, "cat"), "1 cat")

    def test_several_nouns_results_in_simple_plural_noun(self) -> None:
        """Test that a number not equal to 1 appends s to noun."""
        for number in [0, 2, 3, 4]:
            self.assertEqual(vintagebackup.plural_noun(number, "dog"), f"{number} dogs")

    def test_all_backups_returns_all_backups(self) -> None:
        """Test that all_backups() returns all expected backups."""
        with (tempfile.TemporaryDirectory() as user_folder,
              tempfile.TemporaryDirectory() as backup_folder):
            user_path = Path(user_folder)
            create_user_data(user_path)
            backup_path = Path(backup_folder)
            backups_to_create = 7
            timestamps: list[datetime.datetime] = []
            for _ in range(backups_to_create):
                timestamp = unique_timestamp()
                timestamps.append(timestamp)
                vintagebackup.create_new_backup(user_path,
                                                backup_path,
                                                filter_file=None,
                                                examine_whole_file=False,
                                                force_copy=False,
                                                max_average_hard_links=None,
                                                timestamp=timestamp)
            backups = vintagebackup.all_backups(backup_path)
            for timestamp, backup in zip(timestamps, backups, strict=True):
                year_path = str(timestamp.year)
                dated_folder_name = timestamp.strftime(vintagebackup.backup_date_format)
                expected_folder = backup_path/year_path/dated_folder_name
                self.assertEqual(backup, expected_folder)

    def test_all_backups_returns_only_backups(self) -> None:
        """Test that all_backups() returns all expected backups."""
        with (tempfile.TemporaryDirectory() as user_folder,
              tempfile.TemporaryDirectory() as backup_folder):
            user_path = Path(user_folder)
            create_user_data(user_path)
            backup_path = Path(backup_folder)
            backups_to_create = 7
            timestamps: list[datetime.datetime] = []
            for _ in range(backups_to_create):
                timestamp = unique_timestamp()
                timestamps.append(timestamp)
                vintagebackup.create_new_backup(user_path,
                                                backup_path,
                                                filter_file=None,
                                                examine_whole_file=False,
                                                force_copy=False,
                                                max_average_hard_links=None,
                                                timestamp=timestamp)

            # Create entries that should be left out of all_backups() list
            (backup_path/"extra year folder"/"extra backup folder").mkdir(parents=True)
            (backup_path/"extra year file").touch()
            (backup_path/str(timestamp.year)/"extra backup folder").mkdir()
            (backup_path/str(timestamp.year)/"extra backup file").touch()

            backups = vintagebackup.all_backups(backup_path)
            for timestamp, backup in zip(timestamps, backups, strict=True):
                year_path = str(timestamp.year)
                dated_folder_name = timestamp.strftime(vintagebackup.backup_date_format)
                expected_folder = backup_path/year_path/dated_folder_name
                self.assertEqual(backup, expected_folder)

    def test_backup_name_and_backup_datetime_are_inverse_functions(self) -> None:
        """Test that a timestamp is preserved in a backup name."""
        now = datetime.datetime.now()
        timestamp = datetime.datetime(now.year, now.month, now.day,
                                      now.hour, now.minute, now.second)
        backup = vintagebackup.backup_name(timestamp)
        backup_timestamp = vintagebackup.backup_datetime(backup)
        self.assertEqual(timestamp, backup_timestamp)

    def test_backup_name_puts_backup_folder_in_correct_year_folder(self) -> None:
        """Test that backups with the same year are grouped together."""
        timestamp = datetime.datetime.now()
        backup_folder = vintagebackup.backup_name(timestamp)
        backup_timestamp = vintagebackup.backup_datetime(backup_folder)
        self.assertEqual(int(backup_folder.parent.name), backup_timestamp.year)

    def test_separate_results_are_disjoint(self) -> None:
        """Test that separate() result lists have no items in common."""
        numbers = list(range(100))
        evens, odds = vintagebackup.separate(numbers, is_even)
        self.assertTrue(set(evens).isdisjoint(odds))

    def test_separate_results_union_equals_the_original_list(self) -> None:
        """Test that the combined separate() results contain every item in the original list."""
        numbers = list(range(100))
        evens, odds = vintagebackup.separate(numbers, is_even)
        self.assertEqual(sorted(evens + odds), numbers)

    def test_separate_first_results_always_satisfy_predicate(self) -> None:
        """Test that every member of the first separate() list satisfies predicate."""
        numbers = list(range(100))
        evens, _ = vintagebackup.separate(numbers, is_even)
        self.assertTrue(all(map(is_even, evens)))

    def test_separate_second_results_always_fail_predicate(self) -> None:
        """Test that every member of the first separate() list satisfies predicate."""
        numbers = list(range(100))
        _, odds = vintagebackup.separate(numbers, is_even)
        self.assertTrue(not any(map(is_even, odds)))


if __name__ == "__main__":
    unittest.main()
