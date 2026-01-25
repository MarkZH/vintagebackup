# Find missing files

A missing file is one that exists in backups but not in the [user's folder](backup.md#--user-folder--u).
This can be useful for checking if any files have gone missing from the user's data.
Also, if a folder has lots of missing files, a user may decide that the data in that folder is too ephemeral to be worth backing up and to adjust the [backup filter](backup.md#--filter).

The search for missing files covers the entire backup as specified with the [`--user-folder`](backup.md#--user-folder--u) parameter.
Every backup is searched, as well.
So, this process may take a long time depending on how many files are backed up and how many backups there are.

## Required options

### `--find-missing`

This function prints a list of files that are in a backup folder but no longer in the user's folder.
The list will be copied to a file named `missing_files.txt` or renamed with a number so that existing files are not overwritten.
The parameter of this option is a directory where the file will be created.

### `--backup-folder`, `-b`

The backup that should be searched.

## Other option

### `--filter`

If a lot of files are excluded from backups, using the same filter with `--find-missing` can speed up the search through the user's folder.
