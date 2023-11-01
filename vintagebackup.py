import os
import shutil
import datetime
import platform
import argparse
import sys
import logging
import filecmp
import stat
import itertools
from collections import Counter
from typing import Iterator
from pathlib import Path

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.setLevel(logging.INFO)

new_backup_directory_created = False


class CommandLineError(ValueError):
    pass


def byte_units(size: float, prefixes: list[str] | None = None) -> str:
    if not prefixes:
        prefixes = ["", "k", "M", "G"]

    if size >= 1000 and len(prefixes) > 1:
        return byte_units(size / 1000, prefixes[1:])
    else:
        return f"{size:.1f} {prefixes[0]}B"


def last_directory(containing_directory: Path) -> Path:
    with os.scandir(containing_directory) as scan:
        return Path(sorted(d.path for d in scan if d.is_dir())[-1])


def find_previous_backup(backup_location: Path) -> Path | None:
    try:
        last_year_dir = last_directory(backup_location)
        return last_directory(last_year_dir)
    except IndexError:
        return None


def create_exclusion_list(exclude_file: Path | None, user_data_location: Path) -> set[Path]:
    if not exclude_file:
        return set()

    logger.info(f"Reading exclude file: {exclude_file}")
    exclusions: set[Path] = set()
    with open(exclude_file) as exclude_list:
        for line in exclude_list:
            line = line.rstrip("\n")
            path_list = user_data_location.glob(line)
            original_count = len(exclusions)
            exclusions.update(path_list)
            if len(exclusions) == original_count:
                logger.info(f"Ignoring exclude line: {line}")

    return exclusions


def filter_excluded_paths(exclusions: set[Path],
                          current_dir: Path,
                          name_list: list[str]) -> list[str]:
    current_set = set(current_dir / name for name in name_list)
    return [path.name for path in current_set - exclusions]


def get_user_location_record(backup_location: Path) -> Path:
    return backup_location / "vintagebackup.source.txt"


def record_user_location(user_location: Path, backup_location: Path) -> None:
    user_folder_record = get_user_location_record(backup_location)
    with open(user_folder_record, "w") as user_record:
        user_record.write(str(user_location) + "\n")


def confirm_user_location_is_unchanged(user_data_location: Path, backup_location: Path) -> None:
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


def shallow_stats(stats: os.stat_result) -> tuple[int, int, int]:
    return (stats.st_size, stat.S_IFMT(stats.st_mode), stats.st_mtime_ns)


def get_stat_info(path: Path) -> tuple[int, int, int]:
    stats = os.stat(path, follow_symlinks=False)
    return shallow_stats(stats)


def compare_to_backup(user_directory: Path,
                      backup_directory: Path | None,
                      file_names: list[str],
                      examine_whole_file: bool) -> tuple[list[str], list[str], list[str]]:
    if not backup_directory:
        return [], [], file_names
    elif examine_whole_file:
        return filecmp.cmpfiles(user_directory, backup_directory, file_names, shallow=False)
    else:
        try:
            with os.scandir(backup_directory) as scan:
                backup_files = {entry.name: entry.stat(follow_symlinks=False) for entry in scan}
        except OSError:
            return [], [], file_names

        matches: list[str] = []
        mismatches: list[str] = []
        errors: list[str] = []
        for file_name in file_names:
            try:
                user_file_stats = get_stat_info(user_directory / file_name)
                backup_file_stats = shallow_stats(backup_files[file_name])
                if user_file_stats == backup_file_stats:
                    matches.append(file_name)
                else:
                    mismatches.append(file_name)
            except Exception:
                errors.append(file_name)

        return matches, mismatches, errors


def create_hard_link(previous_backup: Path, new_backup: Path) -> bool:
    try:
        os.link(previous_backup, new_backup, follow_symlinks=False)
        return True
    except Exception as error:
        logger.debug(f"Could not create hard link due to error: {error}")
        logger.debug(f"Previous backed up file: {previous_backup}")
        logger.debug(f"Attempted link         : {new_backup}")
        return False


def include_walk(include_file_name: Path | None,
                 user_directory: Path) -> Iterator[tuple[str, list[str], list[str]]]:
    if not include_file_name:
        return

    logger.info(f"Backing up locations from include file: {include_file_name} ...")
    with open(include_file_name) as include_file:
        for line in include_file:
            line = line.rstrip("\n")
            path_entries = user_directory.glob(line)
            if not path_entries:
                logger.info(f"No files or directories found for include line: {line}")
                continue

            for path in path_entries:
                if not path.is_relative_to(user_directory):
                    logger.warning(f"Skipping include path outside of backup directory: {path}")
                    continue

                if not os.path.islink(path) and os.path.isdir(path):
                    yield from os.walk(path)
                elif os.path.lexists(path):
                    yield str(path.parent), [], [path.name]
                else:
                    logger.info(f"Skipping non-existant include line: {path}")


def backup_directory(user_data_location: Path,
                     new_backup_path: Path,
                     last_backup_path: Path | None,
                     current_user_path: Path,
                     user_dir_names: list[str],
                     user_file_names: list[str],
                     exclusions: set[Path],
                     examine_whole_file: bool,
                     action_counter: Counter[str],
                     is_include_backup: bool):
    user_file_names[:] = filter_excluded_paths(exclusions,
                                               current_user_path,
                                               user_file_names)
    user_dir_names[:] = filter_excluded_paths(exclusions,
                                              current_user_path,
                                              user_dir_names)

    relative_path = current_user_path.relative_to(user_data_location)
    new_backup_directory = new_backup_path / relative_path
    os.makedirs(new_backup_directory, exist_ok=is_include_backup)
    global new_backup_directory_created
    new_backup_directory_created = True

    previous_backup_directory = last_backup_path / relative_path if last_backup_path else None

    matching, mismatching, errors = compare_to_backup(current_user_path,
                                                      previous_backup_directory,
                                                      user_file_names,
                                                      examine_whole_file)

    for file_name in matching:
        assert previous_backup_directory
        previous_backup = previous_backup_directory / file_name
        new_backup = new_backup_directory / file_name
        if os.path.lexists(new_backup):
            logger.debug(f"Skipping backed up include file: {current_user_path / file_name}")
            continue

        if create_hard_link(previous_backup, new_backup):
            action_counter["linked files"] += 1
            logger.debug(f"Linked {previous_backup} to {new_backup}")
        else:
            errors.append(file_name)

    for file_name in itertools.chain(mismatching, errors):
        new_backup_file = new_backup_directory / file_name
        user_file = current_user_path / file_name
        try:
            shutil.copy2(user_file, new_backup_file, follow_symlinks=False)
            action_counter["copied files"] += 1
            logger.debug(f"Copied {user_file} to {new_backup_file}")
        except Exception as error:
            logger.warning(f"Could not copy {user_file} to {new_backup_file} ({error})")
            action_counter["failed copies"] += 1


def create_new_backup(user_data_location: Path,
                      backup_location: Path,
                      exclude_file: Path | None,
                      include_file: Path | None,
                      examine_whole_file: bool,
                      force_copy: bool) -> None:
    if not os.path.isdir(user_data_location):
        raise CommandLineError(f"The user folder path is not a folder: {user_data_location}")

    if not backup_location:
        raise CommandLineError("No backup destination was given.")

    if backup_location.is_relative_to(user_data_location):
        raise CommandLineError("Backup destination cannot be inside user's folder:"
                               f" User data: {user_data_location}"
                               f"; Backup location: {backup_location}")

    if exclude_file and not os.path.isfile(exclude_file):
        raise CommandLineError(f"Exclude file not found: {exclude_file}")

    if include_file and not os.path.isfile(include_file):
        raise CommandLineError(f"Include file not found: {include_file}")

    os.makedirs(backup_location, exist_ok=True)

    now = datetime.datetime.now()
    backup_date = now.strftime("%Y-%m-%d %H-%M-%S")
    os_name = f"{platform.system()} {platform.release()}".strip()
    new_backup_path = backup_location / str(now.year) / f"{backup_date} ({os_name})"

    logger.info("")
    logger.info("=====================")
    logger.info(" Starting new backup")
    logger.info("=====================")
    logger.info("")

    confirm_user_location_is_unchanged(user_data_location, backup_location)
    record_user_location(user_data_location, backup_location)

    logger.info(f"User's data     : {user_data_location}")
    logger.info(f"Backup location : {new_backup_path}")

    last_backup_path = None if force_copy else find_previous_backup(backup_location)
    if last_backup_path:
        logger.info(f"Previous backup : {last_backup_path}")
    else:
        logger.info("No previous backups. Copying everything ...")

    logger.info("")
    logger.info(f"Deep file inspection = {examine_whole_file}")

    action_counter: Counter[str] = Counter()
    exclusions = create_exclusion_list(exclude_file, user_data_location)
    logger.info("Running backup ...")
    for current_user_path, user_dir_names, user_file_names in os.walk(user_data_location):
        backup_directory(user_data_location,
                         new_backup_path,
                         last_backup_path,
                         Path(current_user_path),
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
                         Path(include_path),
                         [],
                         include_file_list,
                         set(),
                         examine_whole_file,
                         action_counter,
                         True)

    logger.info("")
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


def recover_file(recovery_path: Path, backup_location: Path) -> None:
    try:
        with open(get_user_location_record(backup_location)) as location_file:
            user_data_location = Path(location_file.readline().rstrip("\n")).resolve(strict=True)
    except FileNotFoundError:
        raise CommandLineError(f"No backups found at {backup_location}")

    if not recovery_path.is_relative_to(user_data_location):
        raise CommandLineError(f"{recovery_path} is not contained in the backup set "
                               f"{backup_location}, which contains {user_data_location}.")

    unique_backups: dict[int, Path] = {}
    recovery_relative_path = recovery_path.relative_to(user_data_location)
    glob_pattern = str(Path("*") / "*" / recovery_relative_path)
    for path in sorted(backup_location.glob(glob_pattern)):
        inode = os.stat(path).st_ino
        unique_backups.setdefault(inode, path)

    backup_choices = sorted(unique_backups.values())
    number_column_size = len(str(len(backup_choices)))
    for choice, backup_copy in enumerate(backup_choices, 1):
        backup_date = backup_copy.relative_to(backup_location).parts[1]
        print(f"{choice:>{number_column_size}}: {backup_date}")

    while True:
        try:
            user_choice = int(input("Version to recover (Ctrl-C to quit): "))
            if user_choice < 1:
                continue
            chosen_file = backup_choices[user_choice - 1]
            break
        except (ValueError, IndexError):
            pass

    recovered_path = recovery_path
    unique_id = 0
    while os.path.lexists(recovered_path):
        unique_id += 1
        new_file_name = f"{recovery_path.stem}.{unique_id}{recovery_path.suffix}"
        recovered_path = recovery_path.parent / new_file_name

    logger.info(f"Copying {chosen_file} to {recovered_path}")
    shutil.copy2(chosen_file, recovered_path)


def delete_last_backup(backup_location: Path) -> None:
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

    default_log_file_name = Path.home() / "vintagebackup.log"
    user_input.add_argument("-l", "--log", default=default_log_file_name, help=f"""
Where to log the activity of this program. A file of the same
name will be written to the backup folder. The default is
{default_log_file_name.name} in the user's home folder.""")

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
            if not args.backup_folder:
                raise CommandLineError("Backup folder needed to recover file.")

            try:
                backup_folder = Path(args.backup_folder).absolute()
            except FileNotFoundError:
                raise CommandLineError(f"Could not find backup folder: {args.backup_folder}")

            action = "recovery"
            recover_file(Path(args.recover).absolute(), backup_folder)
        else:
            def path_or_none(arg: str) -> Path | None:
                return Path(arg).absolute() if arg else None

            try:
                user_folder = Path(args.user_folder).resolve(strict=True)
            except FileNotFoundError:
                raise CommandLineError(f"Could not find users folder: {args.user_folder}")

            backup_folder = Path(args.backup_folder).absolute()

            action = "backup"
            delete_last_backup_on_error = args.delete_on_error
            create_new_backup(user_folder,
                              backup_folder,
                              path_or_none(args.exclude),
                              path_or_none(args.include),
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
        if delete_last_backup_on_error and new_backup_directory_created:
            delete_last_backup(args.backup_folder)
        print_backup_storage_stats(args.backup_folder)
    finally:
        sys.exit(exit_code)
