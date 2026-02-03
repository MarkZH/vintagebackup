# Deleting old backups

Deleting old backups is necessary for keeping space available for new backups.

## Deletion actions

Any and all of the options in this section can be used at the same time.
They will run after a backup completes successfully.

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

### `--keep-weekly-after`, `--keep-monthly-after`, `--keep-yearly-after`

These options specify how long to retain all backups before deleting most of them to leave only a less frequent sample.
For example, the option `--keep-weekly-after 6m` specifies that backups older than six months will be deleted except for those spaced out by at least a week.
This goes similarly for the monthly and yearly options.
The purpose of these options is to allow the user to extend how long backups are retained by only keeping the oldest backups at a less frequent interval.

All three of these can be used together or in any combination of one or two.

The parameter of these options is a time span to specify after what period to begin deleting the frequent backups.
The format is the same as the `--delete-after` option.
If they are used together, the time span for `--keep-weekly-after` must be the shortest, followed by `--keep-monthly-after`, then `--keep-yearly-after`.

## Other options

These options modify the behavior of the deletion action in the previous section.

### `--max-deletions`

Limit the number of backup deletions per program run.
Since deleting backups with lots of hard links can take a substantial amount of time, this option limits the number of deletions.

The option `--max-deletions 5` will delete no more than 5 of the oldest backups per program run.

The value of the parameter must be a positive whole number.

### `--delete-only`

Only delete old backups. Do not create a backup.
