# Restore all data from backup

This function will restore an entire backup to a location of the user's choosing.
This can be to the original folder that was being backed up or to a new location, such as a new computer.

## Main options

*All of these options are required.*

### `--restore`

This option begins the process of restoring an entire backup.
It takes no parameters.

### `--destination`

The backup will be restored to the folder in this option's parameter.
If the folder is not empty, it will overwrite files that exist on the backup.
If other files exist, they may be deleted according to other options listed below.

### `--backup-folder`

This is the folder containing the backups from which the user's folder will be restored.

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
