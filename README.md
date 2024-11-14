# Vintage Backup

Vintage Backup is a backup utility that uses hard links to unchanged (vintage) files to create full backups with the storage savings of incremental backups.
It is written in Python and requires Python 3.13 or later to be installed to run.

Why "Vintage" Backup?
When something old is still of good quality--whether cars, wines, books, or art--they are brought forward into the present instead of being replaced with something new.
These are vintage items.
When a backup of a file is still good--the original file hasn't changed since the last backup--it is brought forward to the newest backup instead of being replaced with a new copy.
These are vintage files.

The program can be run from the command line or automatically with tools like Linux cron or Windows Task Scheduler.
To start the program to back up, for example, Alice's Windows 10 home directory to a folder named `backups` on an external hard drive connected as drive E, the following command could be used:
```
python vintagebackup.py --user-folder C:\Users\Alice --backup-folder E:\backups
```
Every time this program is run with the same options, a new dated backup folder will be created. For example, the above command would create a new backup folder that might look like this:
```
E:\backups\2023\2023-09-02 17-25-33 (Windows 10)
```
Bob, on a Linux or Mac, might run it like this:
```
python vintagebackup.py --user-folder /home/bob/ --backup-folder /mnt/backup_drive/bob/
```
which might create
```
/mnt/backup_drive/bob/2024/2024-08-27 22-10-12 (Ubuntu 22.04.4 LTS)
```
as the first backup.

Unchanged files will be linked to earlier backups so they don't take up more space, while new or changed files will be copied to the backup.
This can result in years of daily backups fitting on a single external drive with every backup folder containing every backed up file, making restoring these files as easy as drag-and-drop.

Running `python vintagebackup.py -h` displays help about all the options.
