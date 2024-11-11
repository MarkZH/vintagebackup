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
from typing import cast
import enum
import random
import string
import platform

testing_timestamp = datetime.datetime.now()


def unique_timestamp() -> datetime.datetime:
    """Create a unique timestamp backups in testing so that backups can be made more rapidly."""
    global testing_timestamp
    testing_timestamp += datetime.timedelta(seconds=10)
    return testing_timestamp


def delete_last_backup(backup_location: Path) -> None:
    """Delete the most recent backup."""
    last_backup_directory = vintagebackup.find_previous_backup(backup_location)
    if last_backup_directory:
        vintagebackup.delete_directory_tree(last_backup_directory)


def create_user_data(base_directory: Path) -> None:
    """
    Fill the given directory with folders and files.

    This creates a set of user data to test backups.

    Parameters:
    base_directory: The directory into which all created files and folders go.
    """
    for sub_num in range(3):
        subfolder = base_directory/f"sub_directory_{sub_num}"
        subfolder.mkdir()
        for sub_sub_num in range(3):
            subsubfolder = subfolder/f"sub_sub_directory_{sub_sub_num}"
            subsubfolder.mkdir()
            for file_num in range(3):
                file_path = subsubfolder/f"file_{file_num}.txt"
                with open(file_path, "w") as file:
                    file.write(f"File contents: {sub_num}/{sub_sub_num}/{file_num}\n")


def create_old_backups(backup_base_directory: Path, count: int) -> None:
    """
    Create a set of empty monthly backups.

    Parameters:
    backup_base_directory: The directory that will contain the backup folders.
    count: The number of backups to create. The oldest will be (count - 1) months old.
    """
    now = datetime.datetime.now()
    for months_back in range(count):
        new_month = now.month - months_back
        new_year = now.year
        while new_month < 1:
            new_month += 12
            new_year -= 1
        backup_timestamp = vintagebackup.fix_end_of_month(new_year, new_month, now.day,
                                                          now.hour, now.minute, now.second,
                                                          now.microsecond)
        backup_name = f"{backup_timestamp.strftime(vintagebackup.backup_date_format)} (Testing)"
        (backup_base_directory/str(new_year)/backup_name).mkdir(parents=True)


def directory_contents(base_directory: Path) -> set[Path]:
    """Return a set of all paths in a directory relative to that directory."""
    paths: set[Path] = set()
    for directory, directories, files in base_directory.walk():
        for path in itertools.chain(directories, files):
            paths.add(Path(directory).relative_to(base_directory)/path)
    return paths


def all_files_have_same_content(standard_directory: Path,
                                test_directory: Path) -> bool:
    """
    Test that every file in the standard directory exists also in the test directory.

    Corresponding files will also be checked for identical contents.

    Parameters:
    standard_directory: The base directory that will serve as the standard of comparison.
    test_directory: This directory must possess every file in the standard directory in the same
    location and with the same contents. Extra files in this directory will not result in failure.
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


def all_files_are_copies(base_directory_1: Path, base_directory_2: Path) -> bool:
    """Test that every file in the standard directory is copied in the test directory."""
    for directory_name_1, _, file_names in base_directory_1.walk():
        directory_1 = Path(directory_name_1)
        directory_2 = base_directory_2/(directory_1.relative_to(base_directory_1))
        for file_name in file_names:
            inode_1 = (directory_1/file_name).stat().st_ino
            inode_2 = (directory_2/file_name).stat().st_ino
            if inode_1 == inode_2:
                return False
    return True


def directories_are_completely_copied(base_directory_1: Path, base_directory_2: Path) -> bool:
    """Check that both directories have same tree and all files are copies."""
    return (all_files_are_copies(base_directory_1, base_directory_2)
            and all_files_are_copies(base_directory_2, base_directory_1))


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

    def test_backups(self) -> None:
        """Test basic backups with no include/exclude files."""
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
                first_backups = vintagebackup.last_n_backups(backup_location, "all")
                self.assertEqual(len(first_backups), 1)
                first_backup = first_backups[0]
                self.assertEqual(first_backup, vintagebackup.find_previous_backup(backup_location))
                self.assertTrue(directories_have_identical_content(user_data, first_backup))
                self.assertTrue(all_files_are_copies(user_data, first_backup))

                exit_code = run_backup(method,
                                       user_data,
                                       backup_location,
                                       filter_file=None,
                                       examine_whole_file=False,
                                       force_copy=False,
                                       timestamp=unique_timestamp())
                self.assertEqual(exit_code, 0)
                second_backups = vintagebackup.last_n_backups(backup_location, "all")
                self.assertEqual(len(second_backups), 2)
                self.assertEqual(second_backups[0], first_backup)
                second_backup = second_backups[1]
                self.assertEqual(second_backup, vintagebackup.find_previous_backup(backup_location))
                self.assertTrue(directories_are_completely_hardlinked(first_backup, second_backup))

                exit_code = run_backup(method,
                                       user_data,
                                       backup_location,
                                       filter_file=None,
                                       examine_whole_file=False,
                                       force_copy=True,
                                       timestamp=unique_timestamp())
                self.assertEqual(exit_code, 0)
                third_backups = vintagebackup.last_n_backups(backup_location, "all")
                self.assertEqual(len(third_backups), 3)
                self.assertEqual(third_backups[0], first_backup)
                self.assertEqual(third_backups[1], second_backup)
                third_backup = third_backups[2]
                self.assertEqual(third_backup, vintagebackup.find_previous_backup(backup_location))
                self.assertTrue(directories_are_completely_copied(second_backup, third_backup))

                exit_code = run_backup(method,
                                       user_data,
                                       backup_location,
                                       filter_file=None,
                                       examine_whole_file=True,
                                       force_copy=False,
                                       timestamp=unique_timestamp())
                self.assertEqual(exit_code, 0)
                fourth_backups = vintagebackup.last_n_backups(backup_location, "all")
                self.assertEqual(len(fourth_backups), 4)
                self.assertEqual(fourth_backups[0], first_backup)
                self.assertEqual(fourth_backups[1], second_backup)
                self.assertEqual(fourth_backups[2], third_backup)
                fourth_backup = fourth_backups[3]
                self.assertEqual(fourth_backup, vintagebackup.find_previous_backup(backup_location))
                self.assertTrue(directories_are_completely_hardlinked(third_backup, fourth_backup))

                exit_code = run_backup(method,
                                       user_data,
                                       backup_location,
                                       filter_file=None,
                                       examine_whole_file=True,
                                       force_copy=True,
                                       timestamp=unique_timestamp())
                self.assertEqual(exit_code, 0)
                fifth_backups = vintagebackup.last_n_backups(backup_location, "all")
                self.assertEqual(len(fifth_backups), 5)
                self.assertEqual(fifth_backups[0], first_backup)
                self.assertEqual(fifth_backups[1], second_backup)
                self.assertEqual(fifth_backups[2], third_backup)
                self.assertEqual(fifth_backups[3], fourth_backup)
                fifth_backup = fifth_backups[4]
                self.assertEqual(fifth_backup, vintagebackup.find_previous_backup(backup_location))
                self.assertTrue(directories_are_completely_copied(fourth_backup, fifth_backup))

    def test_file_changing_between_backup(self) -> None:
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
            backup_1, backup_2 = vintagebackup.last_n_backups(backup_location, "all")
            contents_1 = directory_contents(backup_1)
            contents_2 = directory_contents(backup_2)
            self.assertEqual(contents_1, contents_2)
            relative_changed_file = changed_file_name.relative_to(user_data)
            for file in (f for f in contents_1 if f.is_file()):
                self.assertEqual(file != relative_changed_file,
                                 (backup_1/file).stat().st_ino == (backup_2/file).stat().st_ino)

    def test_backup_with_symlinks(self) -> None:
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
            assert last_backup is not None
            self.assertTrue((last_backup/directory_symlink_name).is_symlink())
            self.assertTrue((last_backup/file_symlink_name).is_symlink())


class FilterTest(unittest.TestCase):
    """Test that filter files work properly."""

    def test_exclusions(self) -> None:
        """Test that filter files with only exclusions result in the right files being excluded."""
        for method in Invocation:
            with (tempfile.TemporaryDirectory() as user_data_location,
                  tempfile.TemporaryDirectory() as backup_folder,
                  tempfile.NamedTemporaryFile("w+", delete_on_close=False) as filter_file):

                user_data = Path(user_data_location)
                create_user_data(user_data)
                user_paths = directory_contents(user_data)

                expected_backups = user_paths.copy()
                filter_file.write("- sub_directory_2\n\n")
                expected_backups.difference_update(path for path in user_paths
                                                   if "sub_directory_2" in path.parts)

                filter_file.write(str(Path("- *")/"sub_sub_directory_0\n\n"))
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
                assert last_backup

                self.assertEqual(directory_contents(last_backup), expected_backups)
                self.assertNotEqual(directory_contents(user_data), expected_backups)

    def test_inclusions(self) -> None:
        """Test that filter files with inclusions and exclusions work properly."""
        with (tempfile.TemporaryDirectory() as user_data_location,
              tempfile.TemporaryDirectory() as backup_folder,
              tempfile.NamedTemporaryFile("w+", delete_on_close=False) as filter_file):

            user_data = Path(user_data_location)
            create_user_data(user_data)
            user_paths = directory_contents(user_data)

            expected_backup_paths = user_paths.copy()
            filter_file.write("- sub_directory_2\n\n")
            expected_backup_paths.difference_update(path for path in user_paths
                                                    if "sub_directory_2" in path.parts)

            filter_file.write(str(Path("- *")/"sub_sub_directory_0\n\n"))
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

            self.assertEqual(len(vintagebackup.last_n_backups(backup_location, "all")), 1)
            last_backup = vintagebackup.find_previous_backup(backup_location)
            assert last_backup

            self.assertEqual(directory_contents(last_backup), expected_backup_paths)
            self.assertNotEqual(directory_contents(user_data), expected_backup_paths)

    def test_ineffective_filter_line_detection(self) -> None:
        """Test that filter lines with no effect on the backup files are detected."""
        with (tempfile.TemporaryDirectory() as user_data_location,
              tempfile.NamedTemporaryFile("w+", delete_on_close=False) as filter_file):
            user_path = Path(user_data_location)
            create_user_data(user_path)

            ineffective_sub_directory = Path("sub_directory_1/sub_sub_directory_0")
            ineffective_directory = Path("sub_directory_0")
            filter_file.write("- sub_directory_1\n")
            filter_file.write("# Ineffective line:\n")
            filter_file.write(f"- {ineffective_sub_directory}\n")
            filter_file.write(f"+ {ineffective_directory}\n")
            filter_file.close()

            with self.assertLogs() as log_assert:
                for _ in vintagebackup.Backup_Set(user_path, Path(filter_file.name)):
                    pass

            self.assertIn(f"INFO:vintagebackup:{filter_file.name}: line #3 "
                          f"(- {user_data_location/ineffective_sub_directory/"**"}) "
                          "had no effect.",
                          log_assert.output)
            self.assertIn(f"INFO:vintagebackup:{filter_file.name}: line #4 "
                          f"(+ {user_data_location/ineffective_directory/"**"}) had no effect.",
                          log_assert.output)
            self.assertFalse(any("Ineffective" in message for message in log_assert.output))


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


class RecoveryTest(unittest.TestCase):
    """Test recovering files and folders from backups."""

    def test_single_file_recovery(self) -> None:
        """Test that recovering a single file works properly."""
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

    def test_single_file_recovery_with_renaming(self) -> None:
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

    def test_single_folder_recovery(self) -> None:
        """Test that recovering a folder works properly."""
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
            self.assertTrue(directories_have_identical_content(folder_path, recovered_folder_path))

    def test_list_file_recovery(self) -> None:
        """Test that choosing a file to recover from a list works properly."""
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
            chosen_file = vintagebackup.search_backups(folder_path, backup_location, 1)
            self.assertIsNotNone(chosen_file)
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
            with open(Path(directory_name)/"file.txt", "w") as file:
                file.write(data)


class DeleteBackupTest(unittest.TestCase):
    """Test deleting backups."""

    def test_deleting_last_backup(self) -> None:
        """Test deleting only the most recent backup."""
        with tempfile.TemporaryDirectory() as backup_folder:
            backup_location = Path(backup_folder)
            create_old_backups(backup_location, 10)
            all_backups = vintagebackup.last_n_backups(backup_location, "all")
            delete_last_backup(backup_location)
            expected_remaining_backups = all_backups[:-1]
            all_backups_left = vintagebackup.last_n_backups(backup_location, "all")
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

            backup_count_before = len(vintagebackup.last_n_backups(backup_location, "all"))
            self.assertEqual(backup_count_before, 1)

            delete_last_backup(backup_location)
            backup_count_after = len(vintagebackup.last_n_backups(backup_location, "all"))
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

            backup_count_before = len(vintagebackup.last_n_backups(backup_location, "all"))
            self.assertEqual(backup_count_before, 1)

            delete_last_backup(backup_location)
            backup_count_after = len(vintagebackup.last_n_backups(backup_location, "all"))
            self.assertEqual(backup_count_after, 0)

    def test_space_deletion(self) -> None:
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
                                                        "--free-up", goal_space_str])
                        self.assertEqual(exit_code, 0)

                    # While backups are being deleted, the fake user data still exists, so one more
                    # backup needs to be deleted to free up the required space.
                    backups_after_deletion -= 1
                else:
                    raise NotImplementedError(f"Delete backup test not implemented for {method}")
                backups_left = len(vintagebackup.last_n_backups(backup_location, "all"))
                self.assertEqual(backups_left, backups_after_deletion)

    def test_space_percent_deletion(self) -> None:
        """Test deleting backups until there is a given percent of free space."""
        for method in Invocation:
            with tempfile.TemporaryDirectory() as backup_folder:
                backup_location = Path(backup_folder)
                backups_created = 30
                create_old_backups(backup_location, backups_created)
                file_size = 10_000_000
                create_large_files(backup_location, file_size)
                backups_after_deletion = 10
                size_of_deleted_backups = file_size*(backups_created - backups_after_deletion)
                after_backup_space = shutil.disk_usage(backup_location).free
                goal_space = after_backup_space + size_of_deleted_backups - file_size/2
                goal_space_percent = 100*goal_space/shutil.disk_usage(backup_location).total
                goal_space_percent_str = f"{goal_space_percent}%"
                if method == Invocation.function:
                    vintagebackup.delete_oldest_backups_for_space(backup_location,
                                                                  goal_space_percent_str)
                elif method == Invocation.cli:
                    with tempfile.TemporaryDirectory() as user_folder:
                        user_data = Path(user_folder)
                        create_large_files(user_data, file_size)
                        exit_code = vintagebackup.main(["--user-folder", user_folder,
                                                        "--backup-folder", backup_folder,
                                                        "--log", os.devnull,
                                                        "--free-up", goal_space_percent_str])
                        self.assertEqual(exit_code, 0)

                    # While backups are being deleted, the fake user data still exists, so one more
                    # backup needs to be deleted to free up the required space.
                    backups_after_deletion -= 1
                else:
                    raise NotImplementedError("Delete backup percent test "
                                              f"not implemented for {method}")

                backups_left = len(vintagebackup.last_n_backups(backup_location, "all"))
                self.assertEqual(backups_left, backups_after_deletion)

    def test_date_deletion(self) -> None:
        """Test that backups older than a given date can be deleted."""
        for method in Invocation:
            with tempfile.TemporaryDirectory() as backup_folder:
                backup_location = Path(backup_folder)
                create_old_backups(backup_location, 30)
                max_age = "1y"
                if method == Invocation.function:
                    vintagebackup.delete_backups_older_than(backup_location, max_age)
                elif method == Invocation.cli:
                    with tempfile.TemporaryDirectory() as user_folder:
                        user_data = Path(user_folder)
                        create_user_data(user_data)
                        delete_last_backup(backup_location)
                        exit_code = vintagebackup.main(["--user-folder", user_folder,
                                                        "--backup-folder", backup_folder,
                                                        "--log", os.devnull,
                                                        "--delete-after", max_age])
                        self.assertEqual(exit_code, 0)
                else:
                    raise NotImplementedError("Delete old backup test not implemented for {method}")
                self.assertEqual(len(vintagebackup.last_n_backups(backup_location, "all")), 12)

    def test_deleting_all_backups_leaves_one(self) -> None:
        """Test that trying to delete all backups actually leaves the last one."""
        with tempfile.TemporaryDirectory() as backup_folder:
            backup_location = Path(backup_folder)
            create_old_backups(backup_location, 30)
            delete_last_backup(backup_location)
            vintagebackup.delete_backups_older_than(backup_location, "1d")
            self.assertEqual(len(vintagebackup.last_n_backups(backup_location, "all")), 1)

        with tempfile.TemporaryDirectory() as backup_folder:
            backup_location = Path(backup_folder)
            create_old_backups(backup_location, 30)
            total_space = shutil.disk_usage(backup_location).total
            vintagebackup.delete_oldest_backups_for_space(backup_location, f"{total_space}B")
            self.assertEqual(len(vintagebackup.last_n_backups(backup_location, "all")), 1)

    def test_deleting_backups_for_too_much_space(self) -> None:
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


class MoveBackupsTest(unittest.TestCase):
    """Test moving backup sets to a different location."""

    def test_move_all_backups(self) -> None:
        """Test that moving all backups works."""
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
                        backups_to_move = vintagebackup.last_n_backups(backup_location, "all")
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

    def test_move_n_backups(self) -> None:
        """Test that moving N backups works."""
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

                    backups_at_new_location = vintagebackup.last_n_backups(new_backup_location,
                                                                           "all")
                    self.assertEqual(len(backups_at_new_location), move_count)
                    old_backups = [p.relative_to(backup_location)
                                   for p in vintagebackup.last_n_backups(backup_location,
                                                                         move_count)]
                    new_backups = [p.relative_to(new_backup_location)
                                   for p in vintagebackup.last_n_backups(new_backup_location,
                                                                         "all")]
                    self.assertEqual(old_backups, new_backups)
                    self.assertEqual(vintagebackup.backup_source(backup_location),
                                     vintagebackup.backup_source(new_backup_location))

    def test_move_age_backups(self) -> None:
        """Test that moving backups based on a time span works."""
        with tempfile.TemporaryDirectory() as backup_folder:
            backup_location = Path(backup_folder)
            create_old_backups(backup_location, 25)
            six_months_ago = vintagebackup.parse_time_span_to_timepoint("6m")
            backups_to_move = vintagebackup.backups_since(six_months_ago, backup_location)
            self.assertEqual(len(backups_to_move), 6)
            self.assertEqual(vintagebackup.last_n_backups(backup_location, 6), backups_to_move)


class VerificationTest(unittest.TestCase):
    """Test backup verification."""

    def test_backup_verification(self) -> None:
        """Test that backups correctly verify."""
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
            self.assertIsNotNone(last_backup)
            assert last_backup is not None
            (last_backup/error_file).unlink()

            matching_path_set: set[Path] = set()
            mismatching_path_set: set[Path] = set()
            error_path_set: set[Path] = set()
            user_paths = vintagebackup.Backup_Set(user_location, None)
            for directory, file_names in user_paths:
                for file_name in file_names:
                    path = (directory/file_name).relative_to(user_location)
                    if path == mismatch_file:
                        mismatching_path_set.add(path)
                    elif path == error_file:
                        error_path_set.add(path)
                    else:
                        matching_path_set.add(path)

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
                            self.assertEqual(file_name, "")

                        verify_file_path = verification_location/file_name
                        with open(verify_file_path) as verify_file:
                            verify_file.readline()
                            files_from_verify = set(Path(line.strip("\n")) for line in verify_file)

                        self.assertEqual(files_from_verify, path_set)


class ConfigurationFileTest(unittest.TestCase):
    """Test configuration file functionality."""

    def test_configuration_file(self) -> None:
        """Test that a properly formatted configuration file is accepted."""
        with tempfile.NamedTemporaryFile("w+", delete_on_close=False) as config_file:
            user_folder = r"C:\Files"
            backup_folder = r"D:\Backup"
            filter_file = "filter_file.txt"
            config_file.write(rf"""
User Folder:     {user_folder}
Backup Folder:   {backup_folder}

# Extra options
FiLteR:           {filter_file}
force-copy:
""")
            config_file.close()
            command_line = vintagebackup.read_configuation_file(config_file.name)
            self.assertEqual(command_line,
                             ["--user-folder", user_folder,
                              "--backup-folder", backup_folder,
                              "--filter", filter_file,
                              "--force-copy"])
            arg_parser = vintagebackup.argument_parser()
            args = arg_parser.parse_args(command_line)
            self.assertEqual(args.user_folder, user_folder)
            self.assertEqual(args.backup_folder, backup_folder)
            self.assertEqual(args.filter, filter_file)
            self.assertTrue(args.force_copy)

    def test_override_config_file_with_command_line(self) -> None:
        """Test that command line options override file configurations."""
        with tempfile.NamedTemporaryFile("w+", delete_on_close=False) as config_file:
            config_file.write(r"""
# Test configuration file
User Folder : C:\Users\Test User\
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
            arg_parser = vintagebackup.argument_parser()
            options = vintagebackup.parse_command_line(command_line_options, arg_parser)
            self.assertEqual(options.backup_folder, actual_backup_folder)
            self.assertEqual(options.log, actual_log_file)

    def test_negating_config_file_with_command_line(self) -> None:
        """Test that command line options override file configurations."""
        with tempfile.NamedTemporaryFile("w+", delete_on_close=False) as config_file:
            config_file.write(r"""
# Test configuration file
User Folder : C:\Users\Test User\
Backup Folder: temp_back
filter: filter.txt
log: temp_log.txt
whole file:
Debug:""")
            config_file.close()
            command_line_options = ["-c", config_file.name,
                                    "--no-whole-file",
                                    "--no-debug"]
            arg_parser = vintagebackup.argument_parser()
            options = vintagebackup.parse_command_line(command_line_options, arg_parser)
            self.assertFalse(vintagebackup.toggle_is_set(options, "whole_file"))
            self.assertFalse(vintagebackup.toggle_is_set(options, "debug"))

    def test_error_on_recursive_config_file(self) -> None:
        """Test that putting a config parameter in a configuration file raises an exception."""
        with tempfile.NamedTemporaryFile("w+", delete_on_close=False) as config_file:
            user_folder = r"C:\Files"
            backup_folder = r"D:\Backup"
            filter_file = "filter_file.txt"
            config_file.write(rf"""
User Folder:     {user_folder}
Backup Folder:   {backup_folder}

# Extra options
FiLteR:           {filter_file}
force-copy:
config: config_file_2.txt
""")
            config_file.close()
            with self.assertRaises(vintagebackup.CommandLineError):
                vintagebackup.read_configuation_file(config_file.name)


class ErrorTest(unittest.TestCase):
    """Test that bad user inputs raise correct exceptions."""

    def test_no_user_folder_error(self) -> None:
        """Test that omitting the user folder prints the correct error message."""
        with self.assertLogs(level=logging.ERROR) as log_check:
            exit_code = vintagebackup.main(["-l", os.devnull])
            self.assertEqual(exit_code, 1)
            self.assertEqual(log_check.output, ["ERROR:vintagebackup:User's folder not specified."])

    def test_no_backup_folder_error(self) -> None:
        """Test that omitting the backup folder prints the correct error message."""
        with (tempfile.TemporaryDirectory() as user_folder,
              self.assertLogs(level=logging.ERROR) as log_check):
            exit_code = vintagebackup.main(["-u", user_folder, "-l", os.devnull])
            self.assertEqual(exit_code, 1)
            self.assertEqual(log_check.output, ["ERROR:vintagebackup:Backup folder not specified."])

    def test_non_existent_user_folder(self) -> None:
        """Test that non-existent user folder prints correct error message."""
        user_folder = "".join(random.choices(string.ascii_letters, k=50))
        with self.assertLogs(level=logging.ERROR) as log_check:
            exit_code = vintagebackup.main(["-u", user_folder, "-l", os.devnull])
            self.assertEqual(exit_code, 1)
            self.assertEqual(log_check.output,
                             [f"ERROR:vintagebackup:Could not find user's folder: {user_folder}"])


class RestorationTest(unittest.TestCase):
    """Test that restoring backups works correctly."""

    def test_restore_last_backup_delete_new_files(self) -> None:
        """Test restoring the last backup while deleting new files."""
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
            with open(first_extra_file, "w") as file1:
                file1.write("extra 1\n")

            vintagebackup.create_new_backup(user_path,
                                            backup_path,
                                            filter_file=None,
                                            examine_whole_file=False,
                                            force_copy=False,
                                            max_average_hard_links=None,
                                            timestamp=unique_timestamp())
            self.assertEqual(len(vintagebackup.all_backups(backup_path)), 2)

            second_extra_file = user_path/"extra_file2.txt"
            with open(second_extra_file, "w") as file2:
                file2.write("extra 2\n")

            exit_code = vintagebackup.main(["--restore",
                                            "--user-folder", user_folder,
                                            "--backup-folder", backup_folder,
                                            "--last-backup", "--delete-extra",
                                            "--log", os.devnull])

            self.assertEqual(exit_code, 0)
            last_backup = vintagebackup.find_previous_backup(backup_path)
            assert last_backup
            self.assertTrue(first_extra_file.exists(follow_symlinks=False))
            self.assertFalse(second_extra_file.exists(follow_symlinks=False))
            self.assertTrue(directories_have_identical_content(user_path, last_backup))

    def test_restore_last_backup_keep_new_files(self) -> None:
        """Test restoring the last backup while keeping new files."""
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
            with open(first_extra_file, "w") as file1:
                file1.write("extra 1\n")

            vintagebackup.create_new_backup(user_path,
                                            backup_path,
                                            filter_file=None,
                                            examine_whole_file=False,
                                            force_copy=False,
                                            max_average_hard_links=None,
                                            timestamp=unique_timestamp())
            self.assertEqual(len(vintagebackup.all_backups(backup_path)), 2)

            second_extra_file = user_path/"extra_file2.txt"
            with open(second_extra_file, "w") as file2:
                file2.write("extra 2\n")

            exit_code = vintagebackup.main(["--restore",
                                            "--user-folder", user_folder,
                                            "--backup-folder", backup_folder,
                                            "--last-backup", "--keep-extra",
                                            "--log", os.devnull])

            self.assertEqual(exit_code, 0)
            last_backup = vintagebackup.find_previous_backup(backup_path)
            assert last_backup
            self.assertTrue(first_extra_file.exists(follow_symlinks=False))
            self.assertTrue(second_extra_file.exists(follow_symlinks=False))
            second_extra_file.unlink()
            self.assertTrue(directories_have_identical_content(user_path, last_backup))

    def test_restore_choose_backup_delete_new_files(self) -> None:
        """Test restoring a chosen backup while deleting new files."""
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
            with open(first_extra_file, "w") as file1:
                file1.write("extra 1\n")

            vintagebackup.create_new_backup(user_path,
                                            backup_path,
                                            filter_file=None,
                                            examine_whole_file=False,
                                            force_copy=False,
                                            max_average_hard_links=None,
                                            timestamp=unique_timestamp())
            self.assertEqual(len(vintagebackup.all_backups(backup_path)), 2)

            second_extra_file = user_path/"extra_file2.txt"
            with open(second_extra_file, "w") as file2:
                file2.write("extra 2\n")

            choice = 0
            exit_code = vintagebackup.main(["--restore",
                                            "--user-folder", user_folder,
                                            "--backup-folder", backup_folder,
                                            "--choose-backup", "--delete-extra",
                                            "--log", os.devnull,
                                            "--choice", str(choice)])

            self.assertEqual(exit_code, 0)
            restored_backup = vintagebackup.all_backups(backup_path)[choice]
            assert restored_backup
            self.assertFalse(first_extra_file.exists(follow_symlinks=False))
            self.assertFalse(second_extra_file.exists(follow_symlinks=False))
            self.assertTrue(directories_have_identical_content(user_path, restored_backup))

    def test_restore_choose_backup_keep_new_files(self) -> None:
        """Test restoring a chosen backup while keeping new files."""
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
            with open(first_extra_file, "w") as file1:
                file1.write("extra 1\n")

            vintagebackup.create_new_backup(user_path,
                                            backup_path,
                                            filter_file=None,
                                            examine_whole_file=False,
                                            force_copy=False,
                                            max_average_hard_links=None,
                                            timestamp=unique_timestamp())
            self.assertEqual(len(vintagebackup.all_backups(backup_path)), 2)

            second_extra_file = user_path/"extra_file2.txt"
            with open(second_extra_file, "w") as file2:
                file2.write("extra 2\n")

            choice = 0
            exit_code = vintagebackup.main(["--restore",
                                            "--user-folder", user_folder,
                                            "--backup-folder", backup_folder,
                                            "--choose-backup", "--keep-extra",
                                            "--log", os.devnull,
                                            "--choice", str(choice)])

            self.assertEqual(exit_code, 0)
            restored_backup = vintagebackup.all_backups(backup_path)[choice]
            assert restored_backup
            self.assertTrue(first_extra_file.exists(follow_symlinks=False))
            self.assertTrue(second_extra_file.exists(follow_symlinks=False))
            first_extra_file.unlink()
            second_extra_file.unlink()
            self.assertTrue(directories_have_identical_content(user_path, restored_backup))

    def test_restore_backup_to_alternate_location(self) -> None:
        """Test restoring to a destination different from the user folder."""
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
                                            "--destination", destination_folder])

            self.assertEqual(exit_code, 0)
            destination_path = Path(destination_folder)
            last_backup = vintagebackup.find_previous_backup(backup_path)
            assert last_backup
            self.assertTrue(directories_have_identical_content(last_backup, destination_path))


class LockFileTest(unittest.TestCase):
    """Test that the lock file prevents simultaneous access to a backup location."""
    def test_lock_file(self) -> None:
        """Test basic locking with no waiting."""
        with (tempfile.TemporaryDirectory() as user_folder,
              tempfile.TemporaryDirectory() as backup_folder):
            user_path = Path(user_folder)
            create_user_data(user_path)
            backup_path = Path(backup_folder)
            with vintagebackup.Lock_File(backup_path, False):
                exit_code = run_backup(Invocation.cli,
                                       user_path,
                                       backup_path,
                                       filter_file=None,
                                       examine_whole_file=False,
                                       force_copy=False,
                                       timestamp=unique_timestamp())
                self.assertNotEqual(exit_code, 0)


class RandomCopyTest(unittest.TestCase):
    """Test that specifying an average hard link count results in identical files being copied."""
    def test_random_copy(self) -> None:
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
            self.assertFalse(all_files_are_hardlinked(*all_backups))
            self.assertFalse(all_files_are_copies(*all_backups))


if __name__ == "__main__":
    unittest.main()
