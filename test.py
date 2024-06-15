"""Testing code for Vintage Backup."""
import unittest
import tempfile
import os
import time
import filecmp
import datetime
import shutil
import logging
from pathlib import Path
import itertools
import stat
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
    for directory, directories, files in os.walk(base_directory):
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
                                            alter_file=None,
                                            examine_whole_file=False,
                                            force_copy=False)
            first_backups = vintagebackup.last_n_backups(backup_location, "all")
            self.assertEqual(len(first_backups), 1)
            first_backup = first_backups[0]
            self.assertEqual(first_backup, vintagebackup.find_previous_backup(backup_location))
            self.assertTrue(directories_have_identical_content(user_data, first_backup))
            self.assertTrue(all_files_are_copies(user_data, first_backup))

            time.sleep(1)  # Make sure backups have unique names
            vintagebackup.create_new_backup(user_data,
                                            backup_location,
                                            alter_file=None,
                                            examine_whole_file=False,
                                            force_copy=False)
            second_backups = vintagebackup.last_n_backups(backup_location, "all")
            self.assertEqual(len(second_backups), 2)
            self.assertEqual(second_backups[0], first_backup)
            second_backup = second_backups[1]
            self.assertEqual(second_backup, vintagebackup.find_previous_backup(backup_location))
            self.assertTrue(directories_are_completely_hardlinked(first_backup, second_backup))

            time.sleep(1)  # Make sure backups have unique names
            vintagebackup.create_new_backup(user_data,
                                            backup_location,
                                            alter_file=None,
                                            examine_whole_file=False,
                                            force_copy=True)
            third_backups = vintagebackup.last_n_backups(backup_location, "all")
            self.assertEqual(len(third_backups), 3)
            self.assertEqual(third_backups[0], first_backup)
            self.assertEqual(third_backups[1], second_backup)
            third_backup = third_backups[2]
            self.assertEqual(third_backup, vintagebackup.find_previous_backup(backup_location))
            self.assertTrue(directories_are_completely_copied(second_backup, third_backup))

            time.sleep(1)  # Make sure backups have unique names
            vintagebackup.create_new_backup(user_data,
                                            backup_location,
                                            alter_file=None,
                                            examine_whole_file=True,
                                            force_copy=False)
            fourth_backups = vintagebackup.last_n_backups(backup_location, "all")
            self.assertEqual(len(fourth_backups), 4)
            self.assertEqual(fourth_backups[0], first_backup)
            self.assertEqual(fourth_backups[1], second_backup)
            self.assertEqual(fourth_backups[2], third_backup)
            fourth_backup = fourth_backups[3]
            self.assertEqual(fourth_backup, vintagebackup.find_previous_backup(backup_location))
            self.assertTrue(directories_are_completely_hardlinked(third_backup, fourth_backup))

            time.sleep(1)  # Make sure backups have unique names
            vintagebackup.create_new_backup(user_data,
                                            backup_location,
                                            alter_file=None,
                                            examine_whole_file=True,
                                            force_copy=True)
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
                                            alter_file=None,
                                            examine_whole_file=False,
                                            force_copy=False)

            changed_file_name = user_data/"sub_directory_2"/"sub_sub_directory_0"/"file_1.txt"
            with open(changed_file_name, "a") as changed_file:
                changed_file.write("the change\n")

            time.sleep(1)
            vintagebackup.create_new_backup(user_data,
                                            backup_location,
                                            alter_file=None,
                                            examine_whole_file=False,
                                            force_copy=False)
            backup_1, backup_2 = vintagebackup.last_n_backups(backup_location, "all")
            contents_1 = directory_contents(backup_1)
            contents_2 = directory_contents(backup_2)
            self.assertEqual(contents_1, contents_2)
            relative_changed_file = changed_file_name.relative_to(user_data)
            for file in (f for f in contents_1 if f.is_file()):
                self.assertEqual(file != relative_changed_file,
                                 (backup_1/file).stat().st_ino == (backup_2/file).stat().st_ino)


class IncludeExcludeBackupTest(unittest.TestCase):
    """Test that exclude and include files work properly."""

    def test_exclusions(self) -> None:
        """Test that alter files with only exclusions result in the right files being excluded."""
        with (tempfile.TemporaryDirectory() as user_data_location,
              tempfile.TemporaryDirectory() as backup_folder,
              tempfile.NamedTemporaryFile("w+", delete_on_close=False) as alter_file):

            user_data = Path(user_data_location)
            create_user_data(user_data)
            user_paths = directory_contents(user_data)

            expected_backups = user_paths.copy()
            alter_file.write("- sub_directory_2\n\n")
            expected_backups.difference_update(path for path in user_paths
                                               if "sub_directory_2" in path.parts)

            alter_file.write(os.path.join("- *", "sub_sub_directory_0\n\n"))
            expected_backups.difference_update(path for path in user_paths
                                               if "sub_sub_directory_0" in path.parts)

            alter_file.close()

            backup_location = Path(backup_folder)
            vintagebackup.create_new_backup(user_data,
                                            backup_location,
                                            alter_file=Path(alter_file.name),
                                            examine_whole_file=False,
                                            force_copy=False)

            last_backup = vintagebackup.find_previous_backup(backup_location)
            assert last_backup

            self.assertEqual(directory_contents(last_backup), expected_backups)
            self.assertNotEqual(directory_contents(user_data), expected_backups)

    def test_inclusions(self) -> None:
        """Test that alter files with inclusions and exclusions work properly."""
        with (tempfile.TemporaryDirectory() as user_data_location,
              tempfile.TemporaryDirectory() as backup_folder,
              tempfile.NamedTemporaryFile("w+", delete_on_close=False) as alter_file):

            user_data = Path(user_data_location)
            create_user_data(user_data)
            user_paths = directory_contents(user_data)

            expected_backup_paths = user_paths.copy()
            alter_file.write("- sub_directory_2\n\n")
            expected_backup_paths.difference_update(path for path in user_paths
                                                    if "sub_directory_2" in path.parts)

            alter_file.write(os.path.join("- *", "sub_sub_directory_0\n\n"))
            expected_backup_paths.difference_update(path for path in user_paths
                                                    if "sub_sub_directory_0" in path.parts)

            alter_file.write(os.path.join("+ sub_directory_1",
                                          "sub_sub_directory_0",
                                          "file_1.txt\n\n"))
            expected_backup_paths.add(Path("sub_directory_1")/"sub_sub_directory_0")
            expected_backup_paths.add(Path("sub_directory_1")/"sub_sub_directory_0"/"file_1.txt")

            alter_file.close()

            backup_location = Path(backup_folder)
            vintagebackup.create_new_backup(user_data,
                                            backup_location,
                                            alter_file=Path(alter_file.name),
                                            examine_whole_file=False,
                                            force_copy=False)

            self.assertEqual(len(vintagebackup.last_n_backups(backup_location, "all")), 1)
            last_backup = vintagebackup.find_previous_backup(backup_location)
            assert last_backup

            self.assertEqual(directory_contents(last_backup), expected_backup_paths)
            self.assertNotEqual(directory_contents(user_data), expected_backup_paths)


class RecoveryTest(unittest.TestCase):
    """Test recovering files and folders from backups."""

    def test_single_file_recovery(self) -> None:
        """Test that recovering a single file works properly."""
        with (tempfile.TemporaryDirectory() as user_data_location,
              tempfile.TemporaryDirectory() as backup_folder):
            user_data = Path(user_data_location)
            create_user_data(user_data)
            backup_location = Path(backup_folder)
            vintagebackup.create_new_backup(user_data,
                                            backup_location,
                                            alter_file=None,
                                            examine_whole_file=False,
                                            force_copy=False)
            file_path = (user_data/"sub_directory_0"/"sub_sub_directory_0"/"file_0.txt").resolve()
            moved_file_path = file_path.parent/(file_path.name + "_moved")
            file_path.rename(moved_file_path)
            vintagebackup.recover_path(file_path, backup_location, 0)
            self.assertTrue(filecmp.cmp(file_path, moved_file_path, shallow=False))

    def test_single_file_recovery_with_renaming(self) -> None:
        """Test that recovering a file that exists in user data does not overwrite any files."""
        with (tempfile.TemporaryDirectory() as user_data_location,
              tempfile.TemporaryDirectory() as backup_folder):
            user_data = Path(user_data_location)
            create_user_data(user_data)
            backup_location = Path(backup_folder)
            vintagebackup.create_new_backup(user_data,
                                            backup_location,
                                            alter_file=None,
                                            examine_whole_file=False,
                                            force_copy=False)
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
                                            alter_file=None,
                                            examine_whole_file=False,
                                            force_copy=False)
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
                                            alter_file=None,
                                            examine_whole_file=False,
                                            force_copy=False)
            folder_path = (user_data/"sub_directory_1"/"sub_sub_directory_1").resolve()
            chosen_file = vintagebackup.search_backups(folder_path, backup_location, 1)
            self.assertEqual(chosen_file, folder_path/"file_1.txt")
            vintagebackup.recover_path(chosen_file, backup_location, 0)
            recovered_file_path = chosen_file.parent/f"{chosen_file.stem}.1{chosen_file.suffix}"
            self.assertTrue(filecmp.cmp(chosen_file, recovered_file_path, shallow=False))


class BackupDeletionTest(unittest.TestCase):
    """Test deleting backups."""

    def test_deleting_last_backup(self) -> None:
        """Test deleting only the most recent backup."""
        with tempfile.TemporaryDirectory() as backup_folder:
            backup_location = Path(backup_folder)
            create_old_backups(backup_location, 10)
            all_backups = vintagebackup.last_n_backups(backup_location, "all")
            vintagebackup.delete_last_backup(backup_location)
            expected_remaining_backups = all_backups[:-1]
            all_backups_left = vintagebackup.last_n_backups(backup_location, "all")
            self.assertEqual(expected_remaining_backups, all_backups_left)

    def test_deleting_backup_with_read_only_file(self) -> None:
        """Test deleting a backup containing a readonly file."""
        with (tempfile.TemporaryDirectory() as user_folder,
              tempfile.TemporaryDirectory() as backup_folder):
            user_data = Path(user_folder)
            create_user_data(user_data)
            os.chmod(user_data/"sub_directory_1"/"sub_sub_directory_1"/"file_1.txt", stat.S_IRUSR)

            backup_location = Path(backup_folder)
            vintagebackup.create_new_backup(user_data,
                                            backup_location,
                                            alter_file=None,
                                            examine_whole_file=False,
                                            force_copy=False)

            backup_count_before = len(vintagebackup.last_n_backups(backup_location, "all"))
            self.assertEqual(backup_count_before, 1)

            vintagebackup.delete_last_backup(backup_location)
            backup_count_after = len(vintagebackup.last_n_backups(backup_location, "all"))
            self.assertEqual(backup_count_after, 0)

    def test_deleting_backup_with_read_only_folder(self) -> None:
        """Test deleting a backup containing a readonly file."""
        with (tempfile.TemporaryDirectory() as user_folder,
              tempfile.TemporaryDirectory() as backup_folder):
            user_data = Path(user_folder)
            create_user_data(user_data)
            os.chmod(user_data/"sub_directory_1"/"sub_sub_directory_1", stat.S_IRUSR | stat.S_IXUSR)

            backup_location = Path(backup_folder)
            vintagebackup.create_new_backup(user_data,
                                            backup_location,
                                            alter_file=None,
                                            examine_whole_file=False,
                                            force_copy=False)

            backup_count_before = len(vintagebackup.last_n_backups(backup_location, "all"))
            self.assertEqual(backup_count_before, 1)

            vintagebackup.delete_last_backup(backup_location)
            backup_count_after = len(vintagebackup.last_n_backups(backup_location, "all"))
            self.assertEqual(backup_count_after, 0)

    def test_space_deletion(self) -> None:
        """Test deleting backups until there is a given amount of free space."""
        with tempfile.TemporaryDirectory() as backup_folder:
            backup_location = Path(backup_folder)
            before_backup_space = shutil.disk_usage(backup_location).free
            backups_created = 30
            create_old_backups(backup_location, backups_created)
            file_size = 10_000_000
            data = "A"*file_size
            for directory_name, sub_directory_names, _ in os.walk(backup_location):
                if not sub_directory_names:
                    with open(Path(directory_name)/"file.txt", "w") as file:
                        file.write(data)
            after_backup_space = shutil.disk_usage(backup_location).free
            backups_after_deletion = 10
            backup_size = after_backup_space - before_backup_space
            backup_size_after_deletion = backup_size*(backups_after_deletion/backups_created)
            goal_space = before_backup_space + backup_size_after_deletion - file_size/2
            vintagebackup.delete_oldest_backups_for_space(backup_location, f"{goal_space}B")
            backups_left = len(vintagebackup.last_n_backups(backup_location, "all"))
            self.assertEqual(backups_left, backups_after_deletion)

    def test_space_percent_deletion(self) -> None:
        """Test deleting backups until there is a given percent of free space."""
        with tempfile.TemporaryDirectory() as backup_folder:
            backup_location = Path(backup_folder)
            before_backup_space = shutil.disk_usage(backup_location).free
            backups_created = 30
            create_old_backups(backup_location, backups_created)
            file_size = 10_000_000
            data = "A"*file_size
            for directory_name, sub_directory_names, _ in os.walk(backup_location):
                if not sub_directory_names:
                    with open(Path(directory_name)/"file.txt", "w") as file:
                        file.write(data)
            after_backup_space = shutil.disk_usage(backup_location).free
            backups_after_deletion = 10
            backup_size = after_backup_space - before_backup_space
            backup_size_after_deletion = backup_size*(backups_after_deletion/backups_created)
            goal_space = before_backup_space + backup_size_after_deletion - file_size/2
            goal_space_percent = 100*goal_space/shutil.disk_usage(backup_location).total
            vintagebackup.delete_oldest_backups_for_space(backup_location, f"{goal_space_percent}%")
            backups_left = len(vintagebackup.last_n_backups(backup_location, "all"))
            self.assertEqual(backups_left, backups_after_deletion)

    def test_date_deletion(self) -> None:
        """Test that backups older than a given date can be deleted."""
        with tempfile.TemporaryDirectory() as backup_folder:
            backup_location = Path(backup_folder)
            create_old_backups(backup_location, 30)
            vintagebackup.delete_backups_older_than(backup_location, "1y")
            self.assertEqual(len(vintagebackup.last_n_backups(backup_location, "all")), 12)

    def test_deleting_all_backups_leaves_one(self) -> None:
        """Test that trying to delete all backups actually leaves the last one."""
        with tempfile.TemporaryDirectory() as backup_folder:
            backup_location = Path(backup_folder)
            create_old_backups(backup_location, 30)
            vintagebackup.delete_last_backup(backup_location)
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
            self.assertRaises(vintagebackup.CommandLineError,
                              vintagebackup.delete_oldest_backups_for_space,
                              backup_location,
                              f"{too_much_space}B")

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
              tempfile.TemporaryDirectory() as backup_folder,
              tempfile.TemporaryDirectory() as new_backup_folder):
            user_data = Path(user_data_folder)
            create_user_data(user_data)
            backup_location = Path(backup_folder)
            backup_count = 10
            for _ in range(backup_count):
                vintagebackup.create_new_backup(user_data,
                                                backup_location,
                                                alter_file=None,
                                                examine_whole_file=False,
                                                force_copy=False)
                time.sleep(1)

            backups_to_move = vintagebackup.last_n_backups(backup_location, "all")
            self.assertEqual(len(backups_to_move), backup_count)
            new_backup_location = Path(new_backup_folder)
            vintagebackup.move_backups(backup_location, new_backup_location, backups_to_move)
            self.assertTrue(directories_are_completely_copied(backup_location,
                                                              new_backup_location))
            self.assertEqual(vintagebackup.backup_source(backup_location),
                             vintagebackup.backup_source(new_backup_location))

    def test_move_n_backups(self) -> None:
        """Test that moving N backups works."""
        with (tempfile.TemporaryDirectory() as user_data_folder,
              tempfile.TemporaryDirectory() as backup_folder,
              tempfile.TemporaryDirectory() as new_backup_folder):
            user_data = Path(user_data_folder)
            create_user_data(user_data)
            backup_location = Path(backup_folder)
            for _ in range(10):
                vintagebackup.create_new_backup(user_data,
                                                backup_location,
                                                alter_file=None,
                                                examine_whole_file=False,
                                                force_copy=False)
                time.sleep(1)

            move_count = 5
            backups_to_move = vintagebackup.last_n_backups(backup_location, move_count)
            self.assertEqual(len(backups_to_move), move_count)
            new_backup_location = Path(new_backup_folder)
            vintagebackup.move_backups(backup_location, new_backup_location, backups_to_move)
            backups_at_new_location = vintagebackup.last_n_backups(new_backup_location, "all")
            self.assertEqual(len(backups_at_new_location), move_count)
            old_backups = [p.relative_to(backup_location)
                           for p in vintagebackup.last_n_backups(backup_location, move_count)]
            new_backups = [p.relative_to(new_backup_location)
                           for p in vintagebackup.last_n_backups(new_backup_location, "all")]
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


class ConfigurationFileTest(unittest.TestCase):
    """Test configuration file functionality."""

    def test_configuration_file(self) -> None:
        """Test that a properly formatted configuration file is accepted."""
        with tempfile.NamedTemporaryFile("w+", delete_on_close=False) as config_file:
            config_file.write(r"""
User Folder:     C:\Files
Backup Folder:   D:\Backup
Delete  on   error:

# Extra options
aLtEr:           alter_file.txt
force-copy:
""")
            config_file.close()
            command_line = vintagebackup.read_configuation_file(config_file.name)
            self.assertEqual(command_line,
                             ["--user-folder", r"C:\Files",
                              "--backup-folder", r"D:\Backup",
                              "--delete-on-error",
                              "--alter", "alter_file.txt",
                              "--force-copy"])
            arg_parser = vintagebackup.argument_parser()
            args = arg_parser.parse_args(command_line)
            self.assertEqual(args.user_folder, r"C:\Files")
            self.assertEqual(args.backup_folder, r"D:\Backup")
            self.assertTrue(args.delete_on_error)
            self.assertEqual(args.alter, "alter_file.txt")
            self.assertTrue(args.force_copy)

    def test_override_config_file_with_command_line(self) -> None:
        """Test that command line options override file configurations."""
        with tempfile.NamedTemporaryFile("w+", delete_on_close=False) as config_file:
            config_file.write(r"""
# Test configuration file
User Folder : C:\Users\Test User\
Backup Folder: temp_back
alter: alter.txt
log: temp_log.txt
whole file:
Debug:""")
            config_file.close()
            command_line_options = ["-b", "temp_back2",
                                    "-c", config_file.name,
                                    "-l", "temporary_log.log"]
            actual_backup_folder = command_line_options[1]
            actual_log_file = command_line_options[-1]
            file_commands = vintagebackup.read_configuation_file(config_file.name)
            arg_parser = vintagebackup.argument_parser()
            options = arg_parser.parse_args(file_commands + command_line_options)
            self.assertEqual(options.backup_folder, actual_backup_folder)
            self.assertEqual(options.log, actual_log_file)

    def test_negating_config_file_with_command_line(self) -> None:
        """Test that command line options override file configurations."""
        with tempfile.NamedTemporaryFile("w+", delete_on_close=False) as config_file:
            config_file.write(r"""
# Test configuration file
User Folder : C:\Users\Test User\
Backup Folder: temp_back
alter: alter.txt
log: temp_log.txt
whole file:
Debug:""")
            config_file.close()
            command_line_options = ["-c", config_file.name,
                                    "--no-whole-file",
                                    "--no-debug"]
            file_commands = vintagebackup.read_configuation_file(config_file.name)
            arg_parser = vintagebackup.argument_parser()
            options = arg_parser.parse_args(file_commands + command_line_options)
            self.assertFalse(vintagebackup.toggle_is_set(options, "whole_file"))
            self.assertFalse(vintagebackup.toggle_is_set(options, "debug"))


if __name__ == "__main__":
    vintagebackup.logger.setLevel(logging.ERROR)
    unittest.main()
