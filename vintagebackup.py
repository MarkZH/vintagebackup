import os
import shutil
import datetime
import platform
import argparse
import sys
import logging
import glob
import filecmp
import stat
import itertools
from collections import Counter
from typing import Iterator

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.setLevel(logging.INFO)


class CommandLineError(ValueError):
    pass


def byte_units(size: float, prefixes: list[str] | None = None) -> str:
    if not prefixes:
        prefixes = ["", "k", "M", "G"]

    if size >= 1000 and len(prefixes) > 1:
        return byte_units(size / 1000, prefixes[1:])
    else:
        return f"{size:.1f} {prefixes[0]}B"


def last_directory(dir_name: str) -> str:
    with os.scandir(dir_name) as scan:
        return sorted(d.path for d in scan if d.is_dir())[-1]


def find_previous_backup(backup_location: str) -> str | None:
    try:
        last_year_dir = last_directory(backup_location)
        return last_directory(last_year_dir)
    except IndexError:
        return None


def create_exclusion_list(exclude_file: str | None, user_data_location: str) -> list[str]:
    if not exclude_file:
        return []

    logger.info(f"Reading exclude file: {exclude_file}")
    exclusions: list[str] = []
    with open(exclude_file) as exclude_list:
        for line in exclude_list:
            line = line.rstrip("\n")
            path_list = glob.glob(os.path.join(user_data_location, line))
            if path_list:
                exclusions.extend(os.path.relpath(path, user_data_location) for path in path_list)
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


def path_contained_inside(query_path: str, containing_path: str) -> bool:
    def canonical_path(path: str) -> str:
        return os.path.normcase(os.path.normpath(os.path.abspath(path)))

    try:
        container_canon = canonical_path(containing_path)
        query_canon = canonical_path(query_path)
        commonality = os.path.commonpath([query_canon, container_canon])
        return os.path.samefile(commonality, container_canon)
    except ValueError:
        # query_path and possible_container are on different drives
        return False


def shallow_stats(stats: os.stat_result) -> tuple[int, int, int]:
    return (stats.st_size, stat.S_IFMT(stats.st_mode), stats.st_mtime_ns)


def get_stat_info(directory: str, file_name: str) -> tuple[int, int, int]:
    stats = os.stat(os.path.join(directory, file_name), follow_symlinks=False)
    return shallow_stats(stats)


def compare_to_backup(user_directory: str,
                      backup_directory: str | None,
                      file_names: list[str],
                      examine_whole_file: bool) -> tuple[list[str], list[str], list[str]]:
    if not backup_directory:
        return [], [], file_names
    elif examine_whole_file:
        return filecmp.cmpfiles(user_directory, backup_directory, file_names, shallow=False)
    else:
        matches: list[str] = []
        mismatches: list[str] = []
        errors: list[str] = []
        try:
            with os.scandir(backup_directory) as scan:
                backup_files = {entry.name: entry.stat(follow_symlinks=False) for entry in scan}
        except OSError:
            backup_files = {}

        for file_name in file_names:
            try:
                user_file_stats = get_stat_info(user_directory, file_name)
                backup_file_stats = shallow_stats(backup_files[file_name])
                if user_file_stats == backup_file_stats:
                    matches.append(file_name)
                else:
                    mismatches.append(file_name)
            except Exception:
                errors.append(file_name)

        return matches, mismatches, errors


def create_hard_link(previous_backup: str, new_backup: str) -> bool:
    try:
        os.link(previous_backup, new_backup, follow_symlinks=False)
        return True
    except Exception as error:
        logger.debug(f"Could not create hard link due to error: {error}")
        logger.debug(f"Previous backed up file: {previous_backup}")
        logger.debug(f"Attempted link         : {new_backup}")
        return False


def include_walk(include_file_name: str | None,
                 user_directory: str) -> Iterator[tuple[str, list[str], list[str]]]:
    if not include_file_name:
        return

    with open(include_file_name) as include_file:
        for line in include_file:
            line = line.rstrip("\n")
            path_entries = glob.glob(os.path.join(user_directory, line))
            if not path_entries:
                logger.info(f"No files or directories found for include line: {line}")
                continue

            for path in path_entries:
                if not path_contained_inside(path, user_directory):
                    logger.warning(f"Skipping include path outside of backup directory: {path}")
                    continue

                if os.path.isdir(path):
                    yield from os.walk(path)
                elif os.path.isfile(path):
                    yield os.path.dirname(path), [], [os.path.basename(path)]
                else:
                    logger.info(f"Skipping non-existant include line: {path}")


def backup_directory(user_data_location: str,
                     new_backup_path: str,
                     last_backup_path: str | None,
                     current_user_path: str,
                     user_dir_names: list[str],
                     user_file_names: list[str],
                     exclusions: list[str],
                     examine_whole_file: bool,
                     action_counter: Counter[str],
                     is_include_backup: bool):
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
    os.makedirs(new_backup_directory, exist_ok=is_include_backup)

    previous_backup_directory = (os.path.join(last_backup_path, relative_path)
                                 if last_backup_path else None)

    matching, mismatching, errors = compare_to_backup(current_user_path,
                                                      previous_backup_directory,
                                                      user_file_names,
                                                      examine_whole_file)

    for file_name in matching:
        assert previous_backup_directory
        previous_backup = os.path.join(previous_backup_directory, file_name)
        new_backup = os.path.join(new_backup_directory, file_name)
        if os.path.lexists(new_backup):
            logger.debug("Skipping include file that is already backed up: "
                         + os.path.join(current_user_path, file_name))
            continue

        if create_hard_link(previous_backup, new_backup):
            action_counter["linked files"] += 1
            logger.debug(f"Linked {previous_backup} to {new_backup}")
        else:
            errors.append(file_name)

    for file_name in itertools.chain(mismatching, errors):
        new_backup_file = os.path.join(new_backup_directory, file_name)
        user_file = os.path.join(current_user_path, file_name)
        try:
            shutil.copy2(user_file, new_backup_file, follow_symlinks=False)
            action_counter["copied files"] += 1
            logger.debug(f"Copied {user_file} to {new_backup_file}")
        except Exception as error:
            logger.warning(f"Could not copy {user_file} to {new_backup_file} ({error})")
            action_counter["failed copies"] += 1


def create_new_backup(user_data_location: str,
                      backup_location: str,
                      exclude_file: str | None,
                      include_file: str | None,
                      examine_whole_file: bool,
                      force_copy: bool) -> None:
    if not os.path.isdir(user_data_location or ""):
        raise CommandLineError("The user folder does not exist: "
                               f"{user_data_location or 'None given'}")

    if not backup_location:
        raise CommandLineError("No backup destination was given.")

    if path_contained_inside(backup_location, user_data_location):
        raise CommandLineError("Backup destination cannot be inside user's folder:\n"
                               f"User data      : {user_data_location}\n"
                               f"Backup location: {backup_location}")

    if exclude_file and not os.path.isfile(exclude_file):
        raise CommandLineError(f"Exclude file not found: {exclude_file}")

    if include_file and not os.path.isfile(include_file):
        raise CommandLineError(f"Include file not found: {include_file}")

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

    if include_file:
        logger.info(f"Reading include file: {include_file}")

    logger.info("")
    logger.info(f"User's data     : {os.path.abspath(user_data_location)}")
    logger.info(f"Backup location : {os.path.abspath(new_backup_path)}")

    last_backup_path = None if force_copy else find_previous_backup(backup_location)
    if last_backup_path:
        logger.info(f"Previous backup : {os.path.abspath(last_backup_path)}")
    else:
        logger.info("No previous backups. Copying everything ...")

    logger.info("")
    logger.info(f"Deep file inspection = {examine_whole_file}")
    logger.info("")

    action_counter: Counter[str] = Counter()

    for current_user_path, user_dir_names, user_file_names in os.walk(user_data_location):
        backup_directory(user_data_location,
                         new_backup_path,
                         last_backup_path,
                         current_user_path,
                         user_dir_names,
                         user_file_names,
                         exclusions,
                         examine_whole_file,
                         action_counter,
                         False)

    for include_path, _, include_file_list in include_walk(include_file, user_data_location):
        backup_directory(user_data_location,
                         new_backup_path,
                         last_backup_path,
                         include_path,
                         [],
                         include_file_list,
                         [],
                         examine_whole_file,
                         action_counter,
                         True)

    total_files = sum(count for action, count in action_counter.items()
                      if not action.startswith("failed"))
    action_counter["Backed up files"] = total_files
    name_column_size = max(len(name) for name in action_counter.keys())
    count_column_size = len(str(max(action_counter.values())))
    for action, count in action_counter.items():
        logger.info(f"{action.capitalize():<{name_column_size}} : {count:>{count_column_size}}")


def setup_log_file(logger: logging.Logger, log_file_path: str) -> None:
    log_file = logging.FileHandler(log_file_path, encoding="utf8")
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

    number_column_size = len(str(len(unique_backups)))
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
    if last_backup_directory:
        logger.info(f"Deleting failed backup: {last_backup_directory}")
        shutil.rmtree(last_backup_directory)
    else:
        logger.info("No previous backup to delete")


def print_backup_storage_stats(backup_location: str) -> None:
    try:
        backup_storage = shutil.disk_usage(backup_location)
        percent_used = round(100 * backup_storage.used / backup_storage.total)
        percent_free = round(100 * backup_storage.free / backup_storage.total)
        logger.info("Backup storage space: "
                    f"Total = {byte_units(backup_storage.total)}  "
                    f"Used = {byte_units(backup_storage.used)} ({percent_used}%)  "
                    f"Free = {byte_units(backup_storage.free)} ({percent_free}%)")
    except Exception:
        pass


if __name__ == "__main__":
    user_input = argparse.ArgumentParser(prog="vintagebackup.py",
                                         add_help=False,
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

    user_input.add_argument("-h", "--help", action="store_true", help="""
Show this help message and exit.""")

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
one exclusion. Wildcard characters like * and ? are allowed.
The path should either be an absolute path or one relative to
the directory being backed up (from the -u option).""")

    user_input.add_argument("-i", "--include", help="""
The path of a text file containing a list of files and folders
to include in the backups. The entries in this text file
override the exclusions from the --exclude argument. Each line
should contain one file or directory to include. Wildcard
characters like * and ? are allowed. The paths should either
be absolute paths or paths relative to the directory being backed
up (from the -u option). Included paths must be contained within
the directory being backed up.""")

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
modified. This option requires the -b option to specify which
backup location to search.""")

    user_input.add_argument("--force-copy", action="store_true", help="""
Copy all files instead of linking to files previous backups. The
new backup will contain new copies of all of the user's files,
so the backup location will require much more space than a normal
backup.""")

    user_input.add_argument("--debug", action="store_true", help="""
Log information on all action of a backup.""")

    default_log_file_name = os.path.join(os.path.expanduser("~"), "vintagebackup.log")
    user_input.add_argument("-l", "--log", default=default_log_file_name, help=f"""
Where to log the activity of this program. A file of the same
name will be written to the backup folder. The default is
{os.path.basename(default_log_file_name)} in the user's home folder.""")

    args = user_input.parse_args(args=None if sys.argv[1:] else ["--help"])
    if args.help:
        user_input.print_help()
        sys.exit(0)

    exit_code = 1
    delete_last_backup_on_error = False
    action = ""

    try:
        setup_log_file(logger, args.log)
        if args.debug:
            logger.setLevel(logging.DEBUG)
        logger.debug(args)

        if args.recover:
            action = "recovery"
            recover_file(args.recover, args.backup_folder)
        else:
            action = "backup"
            delete_last_backup_on_error = args.delete_on_error
            create_new_backup(args.user_folder,
                              args.backup_folder,
                              args.exclude,
                              args.include,
                              args.whole_file,
                              args.force_copy)
        logger.info("")
        print_backup_storage_stats(args.backup_folder)
        exit_code = 0
    except CommandLineError as error:
        logger.error(error)
        logger.info("")
        user_input.print_usage()
    except Exception:
        if action:
            logger.error(f"An error prevented the {action} from completing.")
        else:
            logger.error("An error occurred before any action could take place.")
        logger.exception("Error:")
        if delete_last_backup_on_error:
            delete_last_backup(args.backup_folder)
        print_backup_storage_stats(args.backup_folder)
    finally:
        sys.exit(exit_code)
