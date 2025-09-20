# Deleting old backups

Most of this management will be handled with [backup options](backup.md) like `--free-up`, `--delete-after`, `--max-deletions`, and `--delete-first`.

Another option that may be useful is `--delete-only`.
This option skips the backup process and only deletes old backups according to `--free-up` and `--delete-after`.
This may be useful if there is no enough space for a new backup to complete.

## Options

### `--free-up`

Specify how much disk space should be kept free at the backup location.
If there is less space after a backup, old backups will be deleted until this amount of space is free.
This parameter can be just a number or a number with a byte unit.
For example,

`--free-up "10 GB"`

will delete old backups until at least 10 GB are free at the backup location.

Spacing and capitalization don't matter for the unit, and the `B` is optional.
All of the following are equivalent: `10 GB`, `10GB`, `10gb`, `10G`.

This size of this parameter should be an overestimate of the space needed for each backup.
This depends on how much new data is added between backups and how often files are copied instead of hard-linked (see the `--hard-link-count` and `--copy-probability` parameters, below).

The most recent backup will never be deleted, even if the remaining free disk space is less than the parameter.

### `--delete-after`

Delete backups that are older than the specified age after a backup completes.
The parameter takes the form of `Nt`, where `N` is a positive whole number and `t` is a letter specifying a time span.

- `y` for years
- `m` for calendar months
- `w` for weeks
- `d` for days

For example,

`--delete-after 6m`

will delete all backups that are older than six months.

### `--max-deletions`

Limit the number of backup deletions per program run.
Since deleting backups with lots of hard links can take a substantial amount of time, this option limits the number of deletions.

The option `--max-deletions 5` will delete no more than 5 of the oldest backups per program run.

The value of the parameter must be a positive whole number.

### `--delete-first`

Delete old backups according to `--free-up` or `--delete-after` before creating a new backup.
This can be useful if the next backup is expected to contain more new data than the free space on the backup location.

This option is overridden by `--no-delete-first`.

### `--delete-only`

The option is the same as `--delete-first`, but no backup is created afterwards.
