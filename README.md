# Vintage Backup

Vintage Backup is a backup utility that uses hard links to unchanged (vintage) files to create full backups with the storage savings of incremental backups.
It is written in Python and requires Python 3.11 or later to be installed to run.

The program can be run from the command line or automatically with tools like Linux cron or Windows Task Scheduler.
To start the program, use the following command: `python vintagebackup.py -u "C:\Users\Anon Y Mous" -b "E:\backups"`
This will backup the Windows home directory to an external storage mounted as drive E.
Every time this program is run with the same options, a new dated backup folder will be created.
Unchanged files will be linked to earlier backups so they don't take up more space, while new or changed files will be copied to the backup.
This can result in years of daily backups fitting on a single external drive with every backup folder containing every backed up file, making restoring these files as easy as drag-and-drop.

Running `python vintagebackup.py -h` displays the help message with more options:
```
usage: vintagebackup.py [-h] [-u USER_FOLDER] [-b BACKUP_FOLDER] [-e EXCLUDE] [-l LOG]

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
  -l LOG, --log LOG     Where to log the activity of this program. A file of the same
                        name will be written to the backup folder. The default is
                        backup.log in the user's home folder.

Every time Vintage Backup runs, a new folder is created at the backup location that
contains copies of all of the files in the directory being backed up. If a file in the
directory being backed up is unchanged since the last back up, a hard link to the same
file in the previous backup is created. This way, unchanged files do not take up more
storage space in the backup location, allowing for possible years of daily backups, all
while having each folder in the backup location contain a full backup.
```
