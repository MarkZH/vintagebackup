import os
import shutil
import datetime
import platform
import argparse
import sys
import logging
import glob
import filecmp
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


def find_previous_backup(backup_location: str) -> str | None:
    try:
        last_year_dir = last_directory(backup_location)
        return last_directory(last_year_dir)
    except IndexError:
        return None


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
    def norm(path):
        return os.path.normcase(os.path.normpath(path))

    original_names = {os.path.normcase(name): name for name in name_list}
    exclusion_set = set(norm(os.path.join(base_dir, path)) for path in exclusions)
    current_set = set(norm(os.path.join(current_dir, name)) for name in name_list)
    allowed_set = current_set - exclusion_set
    return [original_names[os.path.basename(path)] for path in allowed_set]


def create_new_backup(user_data_location: str, backup_location: str, exclude_file: str) -> None:
    now = datetime.datetime.now()
    backup_date = now.strftime("%Y-%m-%d %H-%M-%S")
    os_name = f"{platform.system()} {platform.release()}".strip()
    new_backup_path = os.path.join(backup_location, str(now.year), f"{backup_date} ({os_name})")

    logger.info("")
    logger.info("=====================")
    logger.info(" Starting new backup")
    logger.info("=====================")
    logger.info("")
    exclusions = create_exclusion_list(exclude_file, user_data_location)
    logger.info("")
    logger.info(f"User's data     : {os.path.abspath(user_data_location)}")
    logger.info(f"Backup location : {os.path.abspath(new_backup_path)}")

    last_backup_path = find_previous_backup(backup_location)
    if not last_backup_path:
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

        if last_backup_path:
            previous_backup_directory = os.path.join(last_backup_path, relative_path)
            matching, mismatching, errors = filecmp.cmpfiles(current_user_path,
                                                             previous_backup_directory,
                                                             user_file_names)
        else:
            matching = []
            mismatching = user_file_names
            errors = []

        for file_name in matching:
            previous_backup = os.path.join(previous_backup_directory, file_name)
            new_backup = os.path.join(new_backup_directory, file_name)
            try:
                os.link(previous_backup, new_backup)
                action_counter["linked file"] += 1
            except Exception as error:
                logger.error(f"Could not link {previous_backup} to {new_backup} ({error})")
                action_counter["failed link"] += 1

        for file_name in mismatching + errors:
            new_backup_file = os.path.join(new_backup_directory, file_name)
            user_file = os.path.join(current_user_path, file_name)
            try:
                shutil.copy2(user_file, new_backup_file)
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


def verify_last_backup(user_data_location: str, backup_location: str, exclude_file: str) -> None:
    matches_found = 0
    mismatches_found = 0
    missing_files = 0

    last_backup_directory = find_previous_backup(backup_location)
    if not last_backup_directory:
        raise RuntimeError(f"Could not find previous backup in {backup_location}")

    logger.info("")
    logger.info(f"Verifying previous backup: {last_backup_directory}")
    exclusions = create_exclusion_list(exclude_file, user_data_location)
    for user_directory, user_directory_names, user_file_names in os.walk(user_data_location):
        user_file_names[:] = filter_excluded_paths(user_data_location,
                                                   exclusions,
                                                   user_directory,
                                                   user_file_names)
        user_directory_names[:] = filter_excluded_paths(user_data_location,
                                                        exclusions,
                                                        user_directory,
                                                        user_directory_names)

        relative_path = os.path.relpath(user_directory, user_data_location)
        backup_path = os.path.join(last_backup_directory, relative_path)

        matches, mismatches, errors = filecmp.cmpfiles(user_directory, backup_path, user_file_names)
        matches_found += len(matches)
        mismatches_found += len(mismatches)
        missing_files += len(errors)

        for name in mismatches:
            user_file_path = os.path.join(user_directory, name)
            backup_file_path = os.path.join(backup_path, name)
            os.utime(backup_file_path)
            logger.warning(f"Files do not match: {user_file_path} / {backup_file_path}")

        for name in errors:
            backup_file_path = os.path.join(backup_path, name)
            logger.warning(f"Could not verify file: {backup_file_path}")

    logger.info("")
    logger.info(f"Found {matches_found} matching files.")

    def plural(count: int, text: str) -> str:
        return f"{count} {text}{'' if count == 1 else 's'}"

    if mismatches_found:
        logger.warning(f"Found {plural(mismatches_found, 'backup file')} different from the"
                       " user's. Will copy anew in the next backup.")
    if missing_files:
        logger.warning(f"Did not find {plural(missing_files, 'files')} in backup. There were"
                       " probably errors during the original backup run.")


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

    user_input.add_argument("-v", "--verify", action="store_true", help="""
Verify the integrity of the just completed backup by comparing
the contents of the backed up files with the contents of the
files in the user folder. Names of backup files that don't match
will be printed to the terminal screen and logged.""")

    default_log_file_name = os.path.join(os.path.expanduser("~"), "backup.log")
    user_input.add_argument("-l", "--log", default=default_log_file_name, help="""
Where to log the activity of this program. A file of the same
name will be written to the backup folder. The default is backup.log
in the user's home folder.""")

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
        action = "backup"
        create_new_backup(args.user_folder, args.backup_folder, args.exclude)
        if args.verify:
            action = "verification"
            verify_last_backup(args.user_folder, args.backup_folder, args.exclude)
    except Exception:
        logger.exception(f"An error prevented the {action} from completing.")
    finally:
        finish = datetime.datetime.now()
        logger.info("")
        logger.info(f"Time taken = {finish - start}")

        backup_storage = shutil.disk_usage(args.backup_folder)
        logger.info("")
        logger.info("Backup storage space: "
                    f"Total = {byte_units(backup_storage.total)}  "
                    f"Used = {byte_units(backup_storage.used)}  "
                    f"Free = {byte_units(backup_storage.free)}")
