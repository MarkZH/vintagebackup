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


class CommandLineError(ValueError):
    pass


def byte_units(size: float, prefixes: list[str] | None = None) -> str:
    if prefixes is None:
        prefixes = ["", "k", "M", "G"]

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
            # Return a directory that definitely does
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
    if not os.path.isdir(user_data_location or ""):
        raise CommandLineError("The user folder does not exist: "
                               f"{user_data_location or 'None given'}")

    if not backup_location:
        raise CommandLineError("No backup destination was given.")

    if exclude_file is not None and not os.path.isfile(exclude_file):
        raise CommandLineError(f"Exclude file not found: {exclude_file}")

    os.makedirs(backup_location, exist_ok=True)

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
                logger.warning(f"Could not link {previous_backup} to {new_backup} ({error})")
                action_counter["failed link"] += 1

        for file_name in mismatching + errors:
            new_backup_file = os.path.join(new_backup_directory, file_name)
            user_file = os.path.join(current_user_path, file_name)
            try:
                shutil.copy2(user_file, new_backup_file, follow_symlinks=False)
                action_counter["copied file"] += 1
            except Exception as error:
                logger.warning(f"Could not copy {user_file} to {new_backup_file} ({error})")
                action_counter["failed copy"] += 1

    total_files = sum(count for action, count in action_counter.items()
                      if not action.startswith("failed"))
    action_counter["Backed up files"] = total_files
    name_column_size = max(len(name) for name in action_counter.keys())
    count_column_size = len(str(max(action_counter.values())))
    logger.info("")
    for action, count in action_counter.items():
        logger.info(f"{action.capitalize():<{name_column_size}} : {count:>{count_column_size}}")


def setup_log_file(logger: logging.Logger, log_file_path: str) -> None:
    log_file = logging.FileHandler(log_file_path)
    log_file_format = logging.Formatter(fmt="%(asctime)s %(levelname)s    %(message)s")
    log_file.setFormatter(log_file_format)
    logger.addHandler(log_file)


def recover_file(recovery_file_name: str, backup_location: str) -> None:
    try:
        with open(get_user_location_record(backup_location)) as location_file:
            user_data_location = location_file.readline().rstrip("\n")
    except FileNotFoundError:
        raise CommandLineError(f"No backups found at {backup_location}")

    recovery_path = os.path.abspath(recovery_file_name)
    recovery_relative_path = os.path.relpath(recovery_path, user_data_location)
    if recovery_relative_path.startswith(".."):
        raise CommandLineError(f"{recovery_path} is not contained in the backup set "
                               f"{backup_location}, which contains {user_data_location}.")

    unique_backups = {}
    for path in sorted(glob.glob(os.path.join(backup_location, "*", "*", recovery_relative_path))):
        inode = os.stat(path).st_ino
        if inode not in unique_backups:
            unique_backups[inode] = path[:-len(recovery_relative_path)]

    number_column_size = len(unique_backups)
    for choice, backup_copy in enumerate(unique_backups.values(), 1):
        print(f"{choice:>{number_column_size}}: {os.path.relpath(backup_copy, backup_location)}")

    while True:
        try:
            user_choice = int(input("Version to recover (Ctrl-C to quit): "))
            if user_choice < 1:
                continue
            chosen_file = os.path.join(backup_location,
                                       unique_backups[user_choice - 1],
                                       recovery_relative_path)
            break
        except (ValueError, IndexError):
            pass

    recovered_path = recovery_path
    unique_id = 0
    while os.path.lexists(recovered_path):
        unique_id += 1
        root, ext = os.path.splitext(recovery_path)
        recovered_path = f"{root}.{unique_id}{ext}"

    logger.info(f"Copying {chosen_file} to {recovered_path}")
    shutil.copy2(chosen_file, recovered_path)


def delete_last_backup(backup_location: str) -> None:
    last_backup_directory = find_previous_backup(backup_location)
    logger.info(f"Deleting failed backup: {last_backup_directory}")
    shutil.rmtree(last_backup_directory)


def print_backup_storage_stats(backup_location: str) -> None:
    try:
        backup_storage = shutil.disk_usage(backup_location)
        percent_used = round(100*backup_storage.used/backup_storage.total)
        percent_free = round(100*backup_storage.free/backup_storage.total)
        logger.info("")
        logger.info("Backup storage space: "
                    f"Total = {byte_units(backup_storage.total)}  "
                    f"Used = {byte_units(backup_storage.used)} ({percent_used}%)  "
                    f"Free = {byte_units(backup_storage.free)} ({percent_free}%)")
    except Exception:
        pass


def print_time_and_space_usage(start: datetime.datetime) -> None:
    finish = datetime.datetime.now()
    logger.info("")
    logger.info(f"Time taken = {finish - start}")
    print_backup_storage_stats(args.backup_folder)


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

    user_input.add_argument("--delete-on-error", action="store_true", help="""
If an error causes a backup to fail to complete, delete that
backup. If this option does not appear, then the incomplete
backup is left in place. Users may want to use this option
so that files that were not part of the failed backup do not
get copied anew during the next backup. NOTE: Individual files
not being copied or linked (e.g., for lack of permission) are
not errors, and will only be noted in the log.""")

    user_input.add_argument("-r", "--recover", help="""
Recover a file from the backup. The user will be able to pick
which version of the file to recover by choosing from dates
where the backup has a new copy the file due to the file being
modified. This option requires the -u option to specify which
backup location to search.""")

    default_log_file_name = os.path.join(os.path.expanduser("~"), "vintagebackup.log")
    user_input.add_argument("-l", "--log", default=default_log_file_name, help=f"""
Where to log the activity of this program. A file of the same
name will be written to the backup folder. The default is
{os.path.basename(default_log_file_name)} in the user's home folder.""")

    args = user_input.parse_args(args=None if sys.argv[1:] else ["--help"])

    setup_log_file(logger, args.log)

    start = datetime.datetime.now()

    exit_code = 1
    try:
        if args.recover:
            action = "recovery"
            args.delete_on_error = False
            recover_file(args.recover, args.backup_folder)
        else:
            action = "backup"
            create_new_backup(args.user_folder, args.backup_folder, args.exclude, args.whole_file)
        exit_code = 0
        print_time_and_space_usage(start)
    except CommandLineError as error:
        logger.error(error)
        logger.info("")
        user_input.print_usage()
    except Exception as error:
        logger.error(f"An error prevented the {action} from completing: {error}")
        if args.delete_on_error:
            delete_last_backup(args.backup_folder)
        print_time_and_space_usage(start)
    finally:
        sys.exit(exit_code)
