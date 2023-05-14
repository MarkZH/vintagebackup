# Vintage Backup

Vintage Backup is a backup utility that uses hard links to unchanged (vintage) files to create full backups with the storage savings of incremental backups.
It is written in Python and requires Python 3.11 or later to be installed to run.

Why "Vintage" Backup?
When something old is still of good quality--whether cars, wines, books, or art--they are brought forward into the present instead of being replaced with something new.
These are vintage items.
When a backup of a file is still good--the original file hasn't changed since the last backup--it is brought forward to the newest backup instead of being replaced with a new copy.
These are vintage files.

The program can be run from the command line or automatically with tools like Linux cron or Windows Task Scheduler.
To start the program, use the following command: `python vintagebackup.py -u "C:\Users\Anon Y Mous" -b "E:\backups"`
This will backup the Windows home directory to an external storage mounted as drive E.
Every time this program is run with the same options, a new dated backup folder will be created.
Unchanged files will be linked to earlier backups so they don't take up more space, while new or changed files will be copied to the backup.
This can result in years of daily backups fitting on a single external drive with every backup folder containing every backed up file, making restoring these files as easy as drag-and-drop.

Running `python vintagebackup.py -h` displays the help message with more options:
```
usage: vintagebackup.py [-h] [-u USER_FOLDER] [-b BACKUP_FOLDER] [-e EXCLUDE] [-w]
                        [--delete-on-error] [-r RECOVER] [-l LOG]

A backup utility that combines the best aspects of full and incremental backups.

options:
  -h, --help            show this help message and exit
  -u USER_FOLDER, --user-folder USER_FOLDER
                        The directory to be backed up. The contents of this folder and
                        all subfolders will be backed up recursively.
  -b BACKUP_FOLDER, --backup-folder BACKUP_FOLDER
                        The destination of the backed up files. This folder will contain
                        a set of folders labeled by year, and each year's folder will
                        contain all of that year's backups.
  -e EXCLUDE, --exclude EXCLUDE
                        The path of a text file containing a list of files and folders
                        to exclude from backups. Each line in the file should contain
                        one exclusion. Wildcard characters like * and ? are allowed.
  -w, --whole-file      Examine the entire contents of a file to determine if it has
                        changed and needs to be copied to the new backup. Without this
                        option, only the file's size, type, and modification date are
                        checked for differences. Using this option will make backups
                        take considerably longer.
  --delete-on-error     If an error causes a backup to fail to complete, delete that
                        backup. If this option does not appear, then the incomplete
                        backup is left in place. Users may want to use this option so
                        that files that were not part of the failed backup do not get
                        copied anew during the next backup. NOTE: Individual files not
                        being copied or linked (e.g., for lack of permission) are not
                        errors, and will only be noted in the log.
  -r RECOVER, --recover RECOVER
                        Recover a file from the backup. The user will be able to pick
                        which version of the file to recover by choosing from dates
                        where the backup has a new copy the file due to the file being
                        modified. This option requires the -u option to specify which
                        backup location to search.
  -l LOG, --log LOG     Where to log the activity of this program. A file of the same
                        name will be written to the backup folder. The default is
                        vintagebackup.log in the user's home folder.

Every time Vintage Backup runs, a new folder is created at the backup location that
contains copies of all of the files in the directory being backed up. If a file in the
directory being backed up is unchanged since the last back up, a hard link to the same
file in the previous backup is created. This way, unchanged files do not take up more
storage space in the backup location, allowing for possible years of daily backups, all
while having each folder in the backup location contain a full backup.
```
