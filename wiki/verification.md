# Verifying backups

The integrity of the last backup can be checked to verify that backups are running successfully.
The verification process will generate three files (all prefixed with the date and time of the start of the verification process in `YYYY-MM-DD HH-MM-SS` format):
  1. `matching files.txt` - list of files that are in both the user folder and the backup and are identical.
  2. `mismatching files.txt` - list of files that are in both the user folder and the backup folder but are different in content.
  3. `error files.txt` - list of files that are in the user folder but could not be compared with files in the backup, usually because the file is not in the backup.

## Required options

### `--verify`

This option starts the verification process.
The parameter to this option specifies the folder where the result files list above should be placed.

### `--backup-folder`, `-b`

The backup that should be verified.

## Recommended options

### `--filter`

If a filter file is used to create the backups, then it should be used when verifying to make sure that the right set of files are being checked.
