import os
import shutil
import datetime
import platform
import argparse
import sys
import logging
import glob
import filecmp
import tempfile
from collections import Counter

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.setLevel(logging.INFO)


def byte_units(size: float, prefixes: list[str] | None = None) -> str:
    if prefixes is None:
        prefixes = ["", "k", "M", "G", "T"]

    if size >= 1000 and len(prefixes) > 1:
        return byte_units(size/1000, prefixes[1:])
    else:
        return f"{size:.1f} {prefixes[0]}B"


def last_directory(dir_name: str) -> str:
    return sorted(d.path for d in os.scandir(dir_name) if d.is_dir())[-1]


def find_previous_backup(backup_location: str) -> str:
    try:
        last_year_dir = last_directory(backup_location)
        return last_directory(last_year_dir)
    except IndexError:
        with tempfile.TemporaryDirectory() as tempdir:
            # Return a directory that definately does
            # not exist once this function returns.
            return tempdir


def create_exclusion_list(exclude_file: str, user_data_location: str) -> list[str]:
    if not exclude_file:
        return []

    logger.info(f"Reading exclude file: {exclude_file}")
    exclusions: list[str] = []
    with open(exclude_file) as exclude_list:
        for line in exclude_list:
            line = line.rstrip("\n")
            path_list = glob.glob(os.path.join(user_data_location, line))
            if path_list:
                for path in path_list:
                    exclusions.append(os.path.relpath(path, user_data_location))
            else:
                logger.info(f"Ignoring exclude line: {line}")

    return exclusions


def filter_excluded_paths(base_dir: str,
                          exclusions: list[str],
                          current_dir: str,
                          name_list: list[str]) -> list[str]:
    def norm(path: str) -> str:
        return os.path.normcase(os.path.normpath(path))

    original_names = {os.path.normcase(name): name for name in name_list}
    exclusion_set = set(norm(os.path.join(base_dir, path)) for path in exclusions)
    current_set = set(norm(os.path.join(current_dir, name)) for name in name_list)
    allowed_set = current_set - exclusion_set
    return [original_names[os.path.basename(path)] for path in allowed_set]


def get_user_location_record(backup_location: str) -> str:
    return os.path.join(backup_location, "vintagebackup.source.txt")


def record_user_location(user_location: str, backup_location: str) -> None:
    user_folder_record = get_user_location_record(backup_location)
    with open(user_folder_record, "w") as user_record:
        user_record.write(user_location + "\n")


def confirm_user_location_is_unchanged(user_data_location: str, backup_location: str) -> None:
    user_folder_record = get_user_location_record(backup_location)
    try:
        with open(user_folder_record) as user_record:
            recorded_user_folder = user_record.read().rstrip("\n")
        if not os.path.samefile(recorded_user_folder, user_data_location):
            raise RuntimeError("Previous backup stored a different user folder."
                               f" Previously: {recorded_user_folder}; Now: {user_data_location}")
    except FileNotFoundError:
        # This is probably the first backup, hence no user folder record.
        pass


def create_new_backup(user_data_location: str,
                      backup_location: str,
                      exclude_file: str,
                      examine_whole_file: bool) -> None:
    now = datetime.datetime.now()
    backup_date = now.strftime("%Y-%m-%d %H-%M-%S")
    os_name = f"{platform.system()} {platform.release()}".strip()
    new_backup_path = os.path.join(backup_location, str(now.year), f"{backup_date} ({os_name})")

    logger.info("")
    logger.info("=====================")
    logger.info(" Starting new backup")
    logger.info("=====================")
    logger.info("")

    confirm_user_location_is_unchanged(user_data_location, backup_location)
    record_user_location(user_data_location, backup_location)

    exclusions = create_exclusion_list(exclude_file, user_data_location)
    logger.info("")
    logger.info(f"User's data     : {os.path.abspath(user_data_location)}")
    logger.info(f"Backup location : {os.path.abspath(new_backup_path)}")

    last_backup_path = find_previous_backup(backup_location)
    if not os.path.isdir(last_backup_path):
        logger.info("No previous backups. Copying everything ...")
    else:
        logger.info(f"Previous backup : {os.path.abspath(last_backup_path)}")
    logger.info("")

    action_counter: Counter[str] = Counter()

    for current_user_path, user_dir_names, user_file_names in os.walk(user_data_location):
        user_file_names[:] = filter_excluded_paths(user_data_location,
                                                   exclusions,
                                                   current_user_path,
                                                   user_file_names)
        user_dir_names[:] = filter_excluded_paths(user_data_location,
                                                  exclusions,
                                                  current_user_path,
                                                  user_dir_names)

        relative_path = os.path.relpath(current_user_path, user_data_location)
        new_backup_directory = os.path.join(new_backup_path, relative_path)
        os.makedirs(new_backup_directory)

        previous_backup_directory = os.path.join(last_backup_path, relative_path)
        matching, mismatching, errors = filecmp.cmpfiles(current_user_path,
                                                         previous_backup_directory,
                                                         user_file_names,
                                                         shallow=not examine_whole_file)

        for file_name in matching:
            previous_backup = os.path.join(previous_backup_directory, file_name)
            new_backup = os.path.join(new_backup_directory, file_name)
            try:
                os.link(previous_backup, new_backup, follow_symlinks=False)
                action_counter["linked file"] += 1
            except Exception as error:
                logger.error(f"Could not link {previous_backup} to {new_backup} ({error})")
                action_counter["failed link"] += 1

        for file_name in mismatching + errors:
            new_backup_file = os.path.join(new_backup_directory, file_name)
            user_file = os.path.join(current_user_path, file_name)
            try:
                shutil.copy2(user_file, new_backup_file, follow_symlinks=False)
                action_counter["copied file"] += 1
            except Exception as error:
                logger.error(f"Could not copy {user_file} to {new_backup_file} ({error})")
                action_counter["failed copy"] += 1

    total_files = sum(count for action, count in action_counter.items()
                      if not action.startswith("failed"))
    action_counter["Backed up files"] = total_files
    name_column_size = max(len(name) for name in action_counter.keys())
    count_column_size = len(str(max(action_counter.values())))
    logger.info("")
    for action, count in action_counter.items():
        logger.info(f"{action.capitalize():<{name_column_size}} : {count:>{count_column_size}}")


def is_valid_folder(path: str, label: str | None = None) -> bool:
    is_valid = bool(path) and os.path.isdir(path)
    if not is_valid and label:
        logger.error(f"The {label} folder does not exist: {path or 'None given'}")
    return is_valid


def set_log_location(logger: logging.Logger, log_file_path: str, backup_path: str) -> None:
    log_file = logging.FileHandler(log_file_path)
    log_file_format = logging.Formatter(fmt="%(asctime)s %(levelname)s    %(message)s")
    log_file.setFormatter(log_file_format)
    logger.addHandler(log_file)

    if is_valid_folder(backup_path):
        backup_log_file_name = os.path.join(backup_path, os.path.basename(log_file_path))
        backup_log_file = logging.FileHandler(backup_log_file_name)
        backup_log_file.setFormatter(log_file_format)
        logger.addHandler(backup_log_file)


if __name__ == "__main__":
    user_input = argparse.ArgumentParser(prog="vintagebackup.py",
                                         description="""
A backup utility that combines the best aspects of full and incremental backups.""",
                                         epilog="""
Every time Vintage Backup runs, a new folder is created at the backup location
that contains copies of all of the files in the directory being backed up.
If a file in the directory being backed up is unchanged since the last
back up, a hard link to the same file in the previous backup is created.
This way, unchanged files do not take up more storage space in the backup
location, allowing for possible years of daily backups, all while having
each folder in the backup location contain a full backup.""")

    user_input.add_argument("-u", "--user-folder", help="""
The directory to be backed up. The contents of this
folder and all subfolders will be backed up recursively.""")

    user_input.add_argument("-b", "--backup-folder", help="""
The destination of the backed up files. This folder will
contain a set of folders labeled by year, and each year's
folder will contain all of that year's backups.""")

    user_input.add_argument("-e", "--exclude", help="""
The path of a text file containing a list of files and folders
to exclude from backups. Each line in the file should contain
one exclusion. Wildcard characters like * and ? are allowed.""")

    user_input.add_argument("-w", "--whole-file", action="store_true", help="""
Examine the entire contents of a file to determine if it has
changed and needs to be copied to the new backup. Without this
option, only the file's size, type, and modification date are
checked for differences. Using this option will make backups
take considerably longer.""")

    default_log_file_name = os.path.join(os.path.expanduser("~"), "vintagebackup.log")
    user_input.add_argument("-l", "--log", default=default_log_file_name, help=f"""
Where to log the activity of this program. A file of the same
name will be written to the backup folder. The default is
{os.path.basename(default_log_file_name)} in the user's home folder.""")

    args = user_input.parse_args(args=None if sys.argv[1:] else ["--help"])

    set_log_location(logger, args.log, args.backup_folder)

    if not (is_valid_folder(args.user_folder, "user")
            and is_valid_folder(args.backup_folder, "backup")):
        user_input.print_usage()
        sys.exit(1)

    if args.exclude is not None and not os.path.isfile(args.exclude):
        logger.error(f"Exclude file not found: {args.exclude}")
        user_input.print_usage()
        sys.exit(1)

    start = datetime.datetime.now()

    try:
        create_new_backup(args.user_folder, args.backup_folder, args.exclude, args.whole_file)
        exit_code = 0
    except Exception as error:
        logger.error(f"An error prevented the backup from completing: {error}")
        exit_code = 1
    finally:
        finish = datetime.datetime.now()
        logger.info("")
        logger.info(f"Time taken = {finish - start}")

        backup_storage = shutil.disk_usage(args.backup_folder)
        percent_used = round(100*backup_storage.used/backup_storage.total)
        percent_free = round(100*backup_storage.free/backup_storage.total)
        logger.info("")
        logger.info("Backup storage space: "
                    f"Total = {byte_units(backup_storage.total)}  "
                    f"Used = {byte_units(backup_storage.used)} ({percent_used}%)  "
                    f"Free = {byte_units(backup_storage.free)} ({percent_free}%)")
        sys.exit(exit_code)
