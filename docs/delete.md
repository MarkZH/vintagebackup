# Deleting old backups

Eventually, whatever media one uses to backup files, it will fill up with backed up data.
It is necessary to manage the storage space of the backup media by deleting old backups.
Most of this management will be handled with [backup options](backup.md) like `--free-up`, `--delete-after`, `--max-deletions`, and `--delete-first`.

Another option that may be useful is `--delete-only`.
This option skips the backup process and only deletes old backups according to `--free-up` and `--delete-after`.
This may be useful if there is no enough space for a new backup to complete.
