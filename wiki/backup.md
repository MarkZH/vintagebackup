# Backup

The default action of running this program is to create a backup.
If run repeatedly with the same source (user) and destination (backup) folders, unchanged files will be hard-linked together to save disk space.

## Required options

### `--user-folder`, `-u`

Specify the folder containing the data the user wants to back up.
For example,

`--user-folder C:\Users\Alice\`

or

`-u /home/bob/`

### `--backup-folder`, `-b`

Specify the destination for the backed up files.
If this folder does not exist, it will be created.
For example,

`--backup-folder "D:\Backup Files\"`

or

`-b /mnt/backup_drive/bobs_backups`

## Recommended options

### `--free-up`

Specify how much disk space should be kept free at the backup location.
If there is less space after a backup, old backups will be deleted until this amount of space is free.
This parameter can be just a number or a number with a byte unit.
For example,

`--free-up "10 GB"`

will delete old backups until at least 10 GB are free at the backup location.

Spacing and capitalization don't matter for the unit, and the `B` is optional.
All of the following are equivalent: `10 GB`, `10GB`, `10gb`, `10G`.

This size of this parameter should be an overestimate of the space needed for each backup.
This depends on how much new data is added between backups and how often files are copied instead of hard-linked (see the `--hard-link-count` and `--copy-probability` parameters, below).

The most recent backup will never be deleted, even if the remaining free disk space is less than the parameter.

### `--hard-link-count`

Specify how many times--on average--a file should be hard-linked in new backups before a new copy of the file is made.

This has two benefits.

First, it safeguards against backed up files being corrupted by having multiple copies of a file at the backup location.
If no new copies were ever created, then there would be only one actual copy of the backed up data, with every other backup containing hard links to that one copy.
If anything should happen to that one copy (disk sector goes bad, errant write, cosmic ray), then the backup is no good.
Copying a file to the backup location, even if it hasn't changed, mitigates this risk.

Second, on some file systems, adding new hard links to a file that already has many hard links gets slower and slower over time.
On an NTFS (Windows) file system where I was backing up 150,000 files, each backup after the tenth took a minute longer than the previous one.
After four months (~120 backups), each backup was taking two hours while copying every file would only take one hour.

There is a trade-off.
Smaller values of `--hard-link-count` will cause each backup to take more space.
Larger values will make each back up (and backup deletion) take a longer time to complete.

Technical detail: this parameter specifies an average since files will be randomly chosen for copying based on a probability calculated from this parameter.
If the user adds the parameter `--hard-link-count N`, where $N$ is some positive number, the probability of copying an unchanged file is $1/(N + 1)$.
So, `--hard-link-count 9` would have unchanged files copied with a probability of 10%.

The option `--copy-probability` is an alternative way of controlling how often files get copied.

### `--filter`

Specify a file with filters that more finely determine which files are backed up.
See the [filter file](filter_files.md) page for details on how to create filters.

## Other options

### `--whole-file`, `-w`

By default, the program determines whether a file has changed by looking only at its size, modification date, and type.
This is a fast check and is sufficient for most situations.
If the user wants to be really sure that files are unchanged before making hard links instead of copying, this option will cause the program to compare every byte of each file with the corresponding file in the previous backup.
If there is any difference in the file data, the file will be copied.
This option can also be used as an occasional check on the integrity of backups.

This option is overridden by `--no-whole-file`.

### `--delete-after`

Delete backups that are older than the specified age after a backup completes.
The parameter takes the form of `Nt`, where `N` is a positive whole number and `t` is a letter specifying a time span.

- `y` for years
- `m` for calendar months
- `w` for weeks
- `d` for days

For example,

`--delete-after 6m`

will delete all backups that are older than six months.

### `--max-deletions`

Limit the number of backup deletions per program run.
Since deleting backups with lots of hard links can take a substantial amount of time, this option limits the number of deletions.

The option `--max-deletions 5` will delete no more than 5 of the oldest backups per program run.

The value of the parameter must be a positive whole number.

### `--delete-first`

Delete old backups according to `--free-up` or `--delete-after` before creating a new backup.
This can be useful if the next backup is expected to contain more new data than the free space on the backup location.

This option is overridden by `--no-delete-first`.

### `--force-copy`

Copy every file regardless of whether the file has changed since the last backup.

This option is overridden by `--no-force-copy`.

### `--copy-probability`

Specify the probability that an unchanged file will be copied instead of hard-linked.
The parameter is specified either as a decimal (0.25) or as a percent (25%).
This is an alternative to `--hard-link-count` and has the same trade-offs.

## Other details

- If a folder contains no files and none of its subfolders contain files, that folder will not appear in the backup.
- Symbolic links are copied as symbolic links.
The data they point to will not be copied to the backup.
- Windows junction points are excluded by default.
They may be added with a filter file.
- Hard links in the user's data are not preserved.
They will be copied or hard-linked separately.
- Backing up to a location with a different file system may prevent hard links from forming due to discrepancies in how file metadata is recorded. The option `--whole-file` can mitigate this by examining file contents, but this is much slower.
