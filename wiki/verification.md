# Checking backup integrity

There are two ways to check whether backups are being created and stored correctly: verification and checksumming.

## Verification

The integrity of the last backup can be checked to verify that backups are running successfully.
The verification process will generate three files (all prefixed with the date and time of the start of the verification process in `YYYY-MM-DD HH-MM-SS` format):
  1. `matching files.txt` - list of files that are in both the user folder and the backup and are identical.
  2. `mismatching files.txt` - list of files that are in both the user folder and the backup folder but are different in content.
  3. `error files.txt` - list of files that are in the user folder but could not be compared with files in the backup, usually because the file is not in the backup.

### Required options

#### `--verify`

This option starts the verification process.
The parameter to this option specifies the folder where the result files list above should be placed.

#### `--backup-folder`, `-b`

The backup that should be verified.

### Recommended options

#### `--filter`

If a filter file is used to create the backups, then it should be used when verifying to make sure that the right set of files are being checked.

## Checksumming

This process checks that the backed up data does not change while it is on the disk.
This can happen for many reasons:
  1. Accidentally writing to a backup instead of a personal folder
  2. Errors in the backup storage media
  3. Cosmic rays hitting the backup storage media

To detect when this occurs, Vintage Backup can create a checksum file after a backup completes.

A checksum file is a list of all of every file in the backup along with a checksum, which is a random-looking number that is calculated from the data in the file.
The idea of a checksum is that, if any change occurs within a file, even a single byte change, the checksum of that file will be very different.
So, at any time in the future, one can verify that the data within a backup has not changed by recalculating the checksums of the files in a backup and comparing them to the checksums in the file.
If a difference is found, then there can be further investigation to determine what data was changed and what was the cause.

*Technical details*: The algorithm used for calculating checksums is SHA-3, which outputs 256-bit checksums as hexadecimal strings.

### Options for creating a checksum

These are not standalone options.
They are added onto options when running a backup.

#### `--checksum`

Create a checksum file after a backup completes.
The file will be put in the root of the dated backup folder.
For example:
```
python vintagebackup.py --user-folder C:\Users\Alice --backup-folder E:\backups --checksum
```
will create a checksum file in the new backup:
```
E:\backups\2026\2026-01-12 02-00-00\checksums.sha3
```
Each line of `checksums.sha3` consists of a file name, followed by a single space, followed by the checksum.

If a file named `checksums.sha3` already exists in the root of the backup, the file will be renamed `checksums.1.sha3`, `checksums.2.sha3`, etc. as needed to not overwrite an existing file.

Creating a checksum can take a very long time, much longer than a backup where every file is copied.
If regular checksums are desired, the `--checksum-every` option can do this periodically with long intervals in between.

#### `--checksum-every`

Create a new checksum file (as if by `--checksum`) for the latest backup if one hasn't been created in the timespan given in the argument.
The argument has the same format as [`--delete-after`](delete.md#--delete-after).
So, `--checksum-every 6m` will create a checksum file for the latest backup if one has not been created in six months.
The date of the last checksum will be found by searching through all previous backups for the `checksums.sha3` or a number-renamed version.

#### `--no-checksum`

Do not create a checksum file, even if `--checksum` or `--checksum-every` arguments are also present.
This is usually used to override options in a [configuration file](configuration_file.md).

### Option for verifying a backups checksums

#### `--verify-checksum`

Recalculate checksums of a backup, compare them with the checksum file, and write the result into a file.
The file will be put into a directory named in the argument of this option.
The user will be given a choice of which backups with a checksum file to verify from an on-screen menu, or according to `--oldest` or `--newest` (see below).
The result of this comparison will be a list of files which have different checksums from when the backup occured.
This file will be written into the directory
If no files have changed, no result file will be written.

Like creating checksum files, verifying a backup's checksum will also take a long time.

#### `--oldest`

Instead of showing a menu of backups with checksums, verify the oldest backup with a checksum file.

#### `--newest`

Instead of showing a menu of backups with checksums, verify the newest backup with a checksum file.
