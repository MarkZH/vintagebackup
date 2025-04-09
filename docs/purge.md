# Purging data from backups

These functions can be used to remove data from all backups, either to free up space or to remove unwanted data.
There are two functions for removing all instances of a file or folder from all backups.

### `--purge`

The parameter to this option is the name of a file or folder that should be removed from all backups.
If multiple types (files, folders, and/or symlinks) of entries with that name are found, a menu will be presented for choosing which to purge.

### `--purge-list`

The parameter to this option is a folder.
A list of everything that has been backed up from this folder will be displayed in a menu.
Once a selection is made, that path will be purged from all backups as if `--purge` had been run.

## Required option

### `--backup-folder`, `-b`

The purging function needs to know where the backups are stored.
