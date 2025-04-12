# Restore all data from backup

This function can revert user data to a previous state by restore an entire backup to its original location.
Or, it can copy all of the user's backed up data to a new location, such as a new computer to replace an old one.

## Required option

### `--restore`

This option begins the process of restoring an entire backup.
It takes no parameters.

## Restoration destination

*Exactly one of the following two choices is required.*

### `--user-folder`

The backup will be restored to the user's folder specified by the parameter to this option.
All files that exist on the backup will be overwritten by the backed up data.
Other files may or may not be deleted according to other options listed below.

### `--destination`

The backup will be restored to the folder in this option's parameter.
If the folder is not empty, it will overwrite files that exist on the backup.
If other files exist, they may be deleted according to other options listed below.

## Whether to keep files already in destination

*Exactly one of the following two choices is required.*

### `--delete-extra`

If there are files in the restoration destination, they will be deleted so that the destination is identical to the backup.

### `--keep-extra`

If there are files in the restoration destination, they will be left alone.

## Which backup to restore from

*Exactly one of the following two choices is required.*

### `--last-backup`

Restore from the most recent backup.

### `--choose-backup`

Choose which backup to restore from in a menu.
