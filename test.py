import unittest
import tempfile
import os
import time
import filecmp
from pathlib import Path
import vintagebackup


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
    for directory_name_1, _, file_names in os.walk(standard_directory):
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
    for directory_name_1, _, file_names in os.walk(standard_directory):
        directory_1 = Path(directory_name_1)
        directory_2 = test_directory/(directory_1.relative_to(standard_directory))
        for file_name in file_names:
            inode_1 = os.stat(directory_1/file_name).st_ino
            inode_2 = os.stat(directory_2/file_name).st_ino
            if inode_1 != inode_2:
                return False
    return True


def directories_are_completely_hardlinked(base_directory_1: Path, base_directory_2: Path) -> bool:
    """Check that both directories have same tree and all files are hardlinked together."""
    return (all_files_are_hardlinked(base_directory_1, base_directory_2)
            and all_files_are_hardlinked(base_directory_2, base_directory_1))


def all_files_are_copies(base_directory_1: Path, base_directory_2: Path) -> bool:
    """Test that every file in the standard directory is copied in the test directory."""
    for directory_name_1, _, file_names in os.walk(base_directory_1):
        directory_1 = Path(directory_name_1)
        directory_2 = base_directory_2/(directory_1.relative_to(base_directory_1))
        for file_name in file_names:
            inode_1 = os.stat(directory_1/file_name).st_ino
            inode_2 = os.stat(directory_2/file_name).st_ino
            if inode_1 == inode_2:
                return False
    return True


def directories_are_completely_copied(base_directory_1: Path, base_directory_2: Path) -> bool:
    """Check that both directories have same tree and all files are copies."""
    return (all_files_are_copies(base_directory_1, base_directory_2)
            and all_files_are_copies(base_directory_2, base_directory_1))


class BackupTest(unittest.TestCase):
    """Test the main backup procedure."""

    def test_backups(self) -> None:
        """Test basic backups with no include/exclude files."""
        with (tempfile.TemporaryDirectory() as user_data_folder,
              tempfile.TemporaryDirectory() as backup_location_folder):
            user_data = Path(user_data_folder)
            backup_location = Path(backup_location_folder)
            create_user_data(user_data)
            vintagebackup.create_new_backup(user_data,
                                            backup_location,
                                            exclude_file=None,
                                            include_file=None,
                                            examine_whole_file=False,
                                            force_copy=False)
            first_backups = vintagebackup.last_n_backups(backup_location, "all")
            self.assertEqual(len(first_backups), 1)
            first_backup = first_backups[0]
            self.assertTrue(directories_have_identical_content(user_data, first_backup))
            self.assertTrue(all_files_are_copies(user_data, first_backup))

            time.sleep(1)  # Make sure backups have unique names
            vintagebackup.create_new_backup(user_data,
                                            backup_location,
                                            exclude_file=None,
                                            include_file=None,
                                            examine_whole_file=False,
                                            force_copy=False)
            second_backups = vintagebackup.last_n_backups(backup_location, "all")
            self.assertEqual(len(second_backups), 2)
            self.assertEqual(second_backups[0], first_backup)
            second_backup = second_backups[1]
            self.assertTrue(directories_are_completely_hardlinked(first_backup, second_backup))

            time.sleep(1)  # Make sure backups have unique names
            vintagebackup.create_new_backup(user_data,
                                            backup_location,
                                            exclude_file=None,
                                            include_file=None,
                                            examine_whole_file=False,
                                            force_copy=True)
            third_backups = vintagebackup.last_n_backups(backup_location, "all")
            self.assertEqual(len(third_backups), 3)
            self.assertEqual(third_backups[0], first_backup)
            self.assertEqual(third_backups[1], second_backup)
            third_backup = third_backups[2]
            self.assertTrue(directories_are_completely_copied(second_backup, third_backup))


if __name__ == "__main__":
    unittest.main()
