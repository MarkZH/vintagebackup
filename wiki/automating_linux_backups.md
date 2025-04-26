# Automating Linux backups with crontab

The easiest way to create a regular backup schedule in Linux is to use the [`cron` utility](https://en.wikipedia.org/wiki/Cron).
As an example, let's say Bob has the backup script at `/home/bob/vintagebackup/vintagebackup.py` and the backup configuration file at `/home/bob/vintagebackup/config.txt`. He wants to run a backup every night at 2AM.

1. Open a terminal and run `crontab -e` to open the `cron` table editor.
This starts a text editor for editing the table.
2. On a new line, type the schedule and the command to run Vintage Backup.
```
0 2 * * * python3.13 "/home/bob/vintagebackup/vintagebackup.py" --config "/home/bob/vintagebackup/config.txt"
```
3. Exit the text editor.

### Notes

- The quotes in the command are required in case there are spaces in the paths.
- Using a configuration file is recommended to keep the `cron` line short and to make it easier to change settings in the future.
- Other schedules can be implemented by changing the first five entries in the line.
See the [Wikipedia article](https://en.wikipedia.org/wiki/Cron) for more information.
