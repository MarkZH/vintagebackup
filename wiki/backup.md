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

### Deleting old backups

Eventually, whatever media one uses to backup files, it will fill up with backed up data.
Read the section on [deleting old backups](delete.md) to learn how to manage the storage space of the backup media by deleting old backups.

### `--hard-link-count`

Specify how many times--on average--a file should be hard-linked in new backups before a new copy of the file is made.

This has two benefits.

First, it safeguards against backed up files being corrupted by having multiple copies of a file at the backup location.
If no new copies were ever created, then there would be only one actual copy of the backed up data, with every other backup containing hard links to that one copy.
If anything should happen to that one copy (disk sector goes bad, errant write, cosmic ray), then every backup of that file is no longer good.
Copying a file to the backup location, even if it hasn't changed, mitigates this risk.

Second, on some file systems, adding new hard links to a file that already has many hard links gets slower and slower over time.
On an NTFS (Windows) file system where I was backing up 150,000 files, each backup after the tenth took a minute longer than the previous one.
After four months (~120 backups), each backup was taking two hours while copying every file would only take one hour.

There is a trade-off.
Smaller values of `--hard-link-count` will cause each backup to take more space.
Larger values will result in fewer independent copies of data at the backup location (and, on some systems, make each back up and deletion take a longer time to complete).

If the `--free-up` option is being used, check the logs after a backup to see how much space the backup took.
If the space taken by the backup is larger than the `--free-up` parameter, either increase the `--hard-link-count` parameter (to create more hard links and shrink the backup size) or increase the `--free-up` paramter to keep more space free.

Technical detail: this parameter specifies an average since files will be randomly chosen for copying based on a probability calculated from this parameter.
If the user adds the parameter `--hard-link-count N`, where $N$ is some positive number, the probability of copying an unchanged file is $1/(N + 1)$.
So, `--hard-link-count 9` would have unchanged files copied with a probability of 10%.

The option `--copy-probability` is an alternative way of controlling how often files get copied.

### `--filter`

Specify a file with filters that more finely determine which files are backed up.
See the [filter file](filter_files.md) page for details on how to create filters.

## Other options

### `--compare-contents`

By default, the program determines whether a file has changed by looking only at its size, modification date, and type.
This is a fast check and is sufficient for most situations.
If the user wants to be really sure that files are unchanged before making hard links instead of copying, this option will cause the program to compare every byte of each file with the corresponding file in the previous backup.
If there is any difference in the file data, the file will be copied.
This option can also be used as an occasional check on the integrity of backups.

This option is overridden by `--no-compare-contents`.

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
- Backing up to a location with a different file system may prevent hard links from forming due to discrepancies in how file metadata is recorded. The option `--compare-contents` can mitigate this by examining file contents, but this is much slower.
