# Recovering data from backups

Every backup contains every file from the user's folder, so the backup location can be explored just like any other folder on the computer without using Vintage Backup.
In addition, these functions can be used to find and recover data by copying it from backups to its original location.
There are two functions for choosing which file to restore and which version to restore.

### `--recover`, `-r`

The parameter to this option is the name of a file or folder to be restored to its original location.
A menu will be presented for choosing which backup to restore from.
The type of the path (file, folder, or symlink) will also be listed.

If something with the same name as the recovery target is already at the target's original location, the recovered copy will be renamed to avoid overwriting data.
For example, if the file `important.docx` is being recovered and there is already a `important.docx` in the recovery destination, then the recovered file will be renamed to `important.1.docx`.
The number will be incremented as needed to avoid overwriting any files.

### `--list`

The parameter to this option is a folder.
If no folder is specified, the current directory is used.
A list of everything that has been backed up from this folder will be displayed in a menu.
Once a selection is made, that path will be recovered as if `--recover` had been run.

## Required option

### `--backup-folder`, `-b`

The recovery functions need to know where the backups are stored.
