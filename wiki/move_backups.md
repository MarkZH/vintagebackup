# Moving backups to another location

If the user wants to change where their files are backed up, they can move their existing backups to the new location.
The backups will not be deleted from their current location.
The hard links between unchanged files will be preserved, so moving each backup will take as long as the original backup.

## Required options

### `--move-backup`

This option indicates that backups are being moved.
The parameter to this option is the new location for the backups.

### `--backup-folder`

This parameter to this option specifies the current location of the backups.
This should be the same folder as when backups are run.

*Exactly one of the following options must be chosen:*

### `--move-count`

Use this option to specify how many of the most recent backups should be moved.
The argument may be a positive whole number or the word "all" to move all backups.

### `--move-age`

Use this option to specify the maximum age of the backups to move.
The format is the same as the `--delete-after` option for [backups](backup.md#--delete-after).

### `--move-since`

Use this option to specify that all backups on or after the date in the argument should be moved.
The format of the date should be `YYYY-MM-DD`.
