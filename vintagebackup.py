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
import re
from collections import Counter
from typing import Iterator
from pathlib import Path

backup_date_format = "%Y-%m-%d %H-%M-%S"

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.setLevel(logging.INFO)

new_backup_directory_created = False


class CommandLineError(ValueError):
    pass


storage_prefixes = ["", "k", "M", "G", "T", "P", "E", "Z", "Y", "R", "Q"]


def byte_units(size: float, prefixes: list[str] | None = None) -> str:
    if not prefixes:
        prefixes = storage_prefixes

    if size >= 10_000 and len(prefixes) > 1:
        return byte_units(size / 1000, prefixes[1:])
    else:
        return f"{size:.1f} {prefixes[0]}B"


def all_backups(backup_location: Path) -> list[Path]:
    year_pattern = re.compile(r"\d\d\d\d")
    backup_pattern = re.compile(r"\d\d\d\d-\d\d-\d\d \d\d-\d\d-\d\d (.*)")

    def is_valid_directory(dir: os.DirEntry, pattern: re.Pattern) -> bool:
        return not dir.is_symlink() and dir.is_dir() and bool(pattern.fullmatch(dir.name))

    all_backup_list: list[Path] = []
    with os.scandir(backup_location) as year_scan:
        for year in (y for y in year_scan if is_valid_directory(y, year_pattern)):
            with os.scandir(year) as dated_backup_scan:
                all_backup_list.extend(Path(dated_backup)
                                       for dated_backup in dated_backup_scan
                                       if is_valid_directory(dated_backup, backup_pattern))

    return sorted(all_backup_list)


def find_previous_backup(backup_location: Path) -> Path | None:
    try:
        return all_backups(backup_location)[-1]
    except IndexError:
        return None


def glob_file(glob_file_path: Path | None,
              category: str,
              user_data_location: Path) -> Iterator[tuple[int, str, Iterator[Path]]]:
    if not glob_file_path:
        return

    logger.info(f"Reading {category} file: {glob_file_path}")
    with open(glob_file_path) as glob_file:
        for line_number, line in enumerate(glob_file, 1):
            line = line.rstrip("\n")
            glob_path = Path(line)
            if glob_path.is_absolute():
                try:
                    pattern = str(glob_path.relative_to(user_data_location))
                except ValueError:
                    logger.info(f"Ignoring {category} line #{line_number} outside of user folder:"
                                f" {glob_path}")
                    continue
            else:
                pattern = str(glob_path)

            yield line_number, line, user_data_location.glob(pattern)


def create_exclusion_list(exclude_file: Path | None, user_data_location: Path) -> set[Path]:
    exclusions: set[Path] = set()
    for line_number, line, exclusion_set in glob_file(exclude_file, "exclude", user_data_location):
        original_count = len(exclusions)
        exclusions.update(exclusion_set)
        if len(exclusions) == original_count:
            logger.info(f"Nothing found for exclude line #{line_number}: {line}")
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


def backup_source(backup_location: Path) -> Path:
    user_folder_record = get_user_location_record(backup_location)
    with open(user_folder_record) as user_record:
        return Path(user_record.read().rstrip("\n"))


def confirm_user_location_is_unchanged(user_data_location: Path, backup_location: Path) -> None:
    try:
        recorded_user_folder = backup_source(backup_location)
        if not os.path.samefile(recorded_user_folder, user_data_location):
            raise RuntimeError("Previous backup stored a different user folder."
                               f" Previously: {recorded_user_folder}; Now: {user_data_location}")
    except FileNotFoundError:
        # This is probably the first backup, hence no user folder record.
        pass


def shallow_stats(stats: os.stat_result) -> tuple[int, int, int]:
    return (stats.st_size, stat.S_IFMT(stats.st_mode), stats.st_mtime_ns)


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
        with os.scandir(user_directory) as scan:
            user_files = {entry.name: entry.stat(follow_symlinks=False) for entry in scan}
        for file_name in file_names:
            try:
                user_file_stats = shallow_stats(user_files[file_name])
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
    for line_number, line, inclusions in glob_file(include_file_name, "include", user_directory):
        found_paths = False
        for path in inclusions:
            found_paths = True
            if not os.path.islink(path) and os.path.isdir(path):
                yield from os.walk(path)
            else:
                yield str(path.parent), [], [path.name]
        if not found_paths:
            logger.info(f"Nothing found for include line #{line_number}: {line}")


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
    backup_date = now.strftime(backup_date_format)
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


def search_backups(search_directory: Path, backup_folder: Path) -> Path:
    if search_directory.is_symlink() or not search_directory.is_dir():
        raise CommandLineError(f"The given search path is not a directory: {search_directory}")
    try:
        user_data_location = backup_source(backup_folder)
    except FileNotFoundError:
        raise CommandLineError(f"There are no backups in {backup_folder}")

    try:
        target_relative_path = search_directory.relative_to(user_data_location)
    except ValueError:
        raise CommandLineError(f"The path {search_directory} is not in the backup at"
                               f" {backup_folder}, which contains {user_data_location}")

    all_paths: set[tuple[str, str]] = set()
    for backup in all_backups(backup_folder):
        backup_search_directory = backup / target_relative_path
        try:
            with os.scandir(backup_search_directory) as backup_scan:
                for item in backup_scan:
                    path_type = ("Symlink" if item.is_symlink()
                                 else "File" if item.is_file()
                                 else "Folder" if item.is_dir()
                                 else "?")
                    all_paths.add((item.name, path_type))
        except FileNotFoundError:
            continue

    menu_list = sorted(all_paths)
    number_column_size = len(str(len(menu_list)))
    for index, (name, path_type) in enumerate(menu_list, 1):
        print(f"{index:>{number_column_size}}: {name} ({path_type})")

    while True:
        try:
            user_choice = int(input("Which path to recover (Ctrl-C to quit): "))
            if user_choice >= 1:
                recovery_target_name = menu_list[user_choice - 1][0]
                return search_directory / recovery_target_name
        except (ValueError, IndexError):
            continue


def recover_path(recovery_path: Path, backup_location: Path) -> None:
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
    for backup in all_backups(backup_location):
        path = backup / recovery_relative_path
        inode = os.stat(path).st_ino
        unique_backups.setdefault(inode, path)

    backup_choices = sorted(unique_backups.values())
    number_column_size = len(str(len(backup_choices)))
    for choice, backup_copy in enumerate(backup_choices, 1):
        backup_date = backup_copy.relative_to(backup_location).parts[1]
        path_type = ("Symlink" if backup_copy.is_symlink()
                     else "File" if backup_copy.is_file()
                     else "Folder" if backup_copy.is_dir()
                     else "?")
        print(f"{choice:>{number_column_size}}: {backup_date} ({path_type})")

    while True:
        try:
            user_choice = int(input("Version to recover (Ctrl-C to quit): "))
            if user_choice < 1:
                continue
            chosen_path = backup_choices[user_choice - 1]
            break
        except (ValueError, IndexError):
            pass

    recovered_path = recovery_path
    unique_id = 0
    while os.path.lexists(recovered_path):
        unique_id += 1
        new_file_name = f"{recovery_path.stem}.{unique_id}{recovery_path.suffix}"
        recovered_path = recovery_path.parent / new_file_name

    logger.info(f"Copying {chosen_path} to {recovered_path}")
    if chosen_path.is_symlink() or chosen_path.is_file():
        shutil.copy2(chosen_path, recovered_path, follow_symlinks=False)
    else:
        shutil.copytree(chosen_path, recovered_path, symlinks=True)


def delete_last_backup(backup_location: Path) -> None:
    last_backup_directory = find_previous_backup(backup_location)
    if last_backup_directory:
        logger.info(f"Deleting failed backup: {last_backup_directory}")
        shutil.rmtree(last_backup_directory)
    else:
        logger.info("No previous backup to delete")


def delete_oldest_backups_for_space(backup_location: Path, space_requirement: str) -> None:
    space_text = "".join(space_requirement.lower().split())
    prefixes = [p.lower() for p in storage_prefixes]
    prefix_pattern = "".join(prefixes)
    pattern = rf"\d+(.\d*)?[{prefix_pattern}]?b?"
    total_storage = shutil.disk_usage(backup_location).total
    if not re.fullmatch(pattern, space_text):
        raise CommandLineError(f"Incorrect format of free-up space: {space_text}")
    if space_text.endswith("%"):
        free_fraction_required = float(space_text[:-1]) / 100
        if free_fraction_required > 1:
            raise CommandLineError(f"Percent cannot be greater than 100: {space_text}")
        free_storage_required = total_storage * free_fraction_required
    else:
        space_text = space_text.rstrip('b')
        if space_text[-1].isalpha():
            prefix = space_text[-1]
            space_text = space_text[:-1]
        else:
            prefix = ""

        multiplier = 1000**prefixes.index(prefix)
        free_storage_required = float(space_text) * multiplier

    if free_storage_required > shutil.disk_usage(backup_location).total:
        raise CommandLineError(f"Cannot free more storage ({byte_units(free_storage_required)})"
                               f" than exists at {backup_location} ({byte_units(total_storage)})")

    any_deletions = False
    backups = all_backups(backup_location)
    for backup in backups[:-1]:
        if shutil.disk_usage(backup_location).free > free_storage_required:
            break

        if not any_deletions:
            logger.info(f"Deleting old backups until {byte_units(free_storage_required)} is free.")

        logger.info(f"Deleting backup: {backup}")
        shutil.rmtree(backup)
        any_deletions = True

    if any_deletions:
        remaining_backups = all_backups(backup_location)
        logger.info(f"Stopped deletions. {len(remaining_backups)} backups remain,"
                    f" earliest: {remaining_backups[0]}")

    if shutil.disk_usage(backup_location).free < free_storage_required:
        logger.warning(f"Could not free up {byte_units(free_storage_required)} of storage"
                       " without deleting most recent backup.")


def delete_backups_older_than(backup_folder: Path, time_span: str) -> None:
    time_span = ''.join(time_span.split())
    try:
        number = int(time_span[:-1])
    except ValueError:
        raise CommandLineError(f"Invalid number in time span (must be whole number): {time_span}")

    letter = time_span[-1]
    day = datetime.timedelta(days=1)
    time_units = {"d": day, "w": 7 * day, "m": 30 * day, "y": 365 * day}
    try:
        span = number * time_units[letter]
        date_to_keep = datetime.datetime.now() - span
    except KeyError:
        raise CommandLineError(f"Invalid time (valid units: {list(time_units)}): {time_span}")

    any_deletions = False
    backups = all_backups(backup_folder)
    for backup in backups[:-1]:
        date_portion = " ".join(backup.name.split()[0:1])
        backup_date = datetime.datetime.strptime(date_portion, backup_date_format)
        if backup_date >= date_to_keep:
            break

        if not any_deletions:
            logger.info(f"Deleting backups prior to {date_to_keep.strftime('%Y-%m-%d %H:%M:%S')}.")

        logger.info(f"Deleting oldest backup: {backup}")
        shutil.rmtree(backup)
        any_deletions = True

    if any_deletions:
        remaining_backups = all_backups(backup_folder)
        logger.info(f"Stopped deletions. {len(remaining_backups)} backups remain,"
                    f" earliest: {remaining_backups[0]}")


def print_backup_storage_stats(backup_location: str | Path) -> None:
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

    user_input.add_argument("--free-up", metavar="SPACE", help="""
Automatically delete old backups when space runs low on the
backup destination. The SPACE argument can be in one of two forms.
If the argument is a number followed by a percent sign (%%), then
the number is interpreted as a percent of the total storage space
of the destination. Old backups will be deleted until that
percentage of the destination storage space is free.

If the argument ends with one letter or one letter followed by
a 'B', then the number will be interpreted as a number of bytes.
Case does not matter, so all of the following specify 15 megabytes:
15MB, 15Mb, 15mB, 15mb, 15M, 15m. Old backups will be deleted until
at least that much space is free.

In either of the above cases, there should be no space between the
number and subsequent symbol.

No matter what, the most recent backup will not be deleted.""")

    user_input.add_argument("--delete-after", metavar="TIME", help="""
Delete backups if they are older than the time span in the argument.
The format of the argument is Nt, where N is a whole number and
t is a single letter: d for days, w for weeks (7 days), m for months
(30 days), or y for years (365 days). There should be no space between
the number and letter.""")

    user_input.add_argument("-r", "--recover", help="""
Recover a file or folder from the backup. The user will be able
to pick which version to recover by choosing the backup date as
the source. If a file is being recovered, only backup dates where
the file was modified will be presented. If a folder is being
recovered, then all available backup dates will be options.
This option requires the -b option to specify which
backup location to search.""")

    user_input.add_argument("--list", metavar="DIRECTORY", help="""
Recover a file or folder in the directory specified by the argument
by first choosing what to recover from a list of everything that's
ever been backed up. If no argument is given, the current directory
is used. The backup location argument (-b) is required.""")

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

    args = user_input.parse_args(args=sys.argv[1:] or ["--help"])

    action_count = [bool(a) for a in (args.help, args.recover, args.list)].count(True)
    if action_count > 1:
        print("Only one action (--help, --recover, --list) may be performed at one time.")
        print("If none of these options are used, a backup will start, which requires the -u and -b parameters.")
        user_input.print_usage()
        sys.exit(1)

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
                backup_folder = Path(args.backup_folder).resolve(strict=True)
            except FileNotFoundError:
                raise CommandLineError(f"Could not find backup folder: {args.backup_folder}")

            action = "recovery"
            recover_path(Path(args.recover).absolute(), backup_folder)
        elif args.list:
            if not args.backup_folder:
                raise CommandLineError("Backup folder needed to list backed up items.")

            try:
                backup_folder = Path(args.backup_folder).resolve(strict=True)
            except FileNotFoundError:
                raise CommandLineError(f"Could not find backup folder: {args.backup_folder}")
            action = "backup listing"
            search_directory = Path(args.list).resolve()
            print(search_directory)
            chosen_recovery_path = search_backups(search_directory, backup_folder)
            recover_path(chosen_recovery_path, backup_folder)
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

            if args.free_up:
                delete_oldest_backups_for_space(backup_folder, args.free_up)

            if args.delete_after:
                delete_backups_older_than(backup_folder, args.delete_after)

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
