# Configuration Files

Instead of configuring Vintage Backup with command line parameters, which may result in a very long command, one may want to put all of the parameters in a file and then run the program like this:

`python3 vintagebackup.py --config config.txt`

## Format

### Options and parameters

Within a configuration file, put one parameter per line like so:

`<parameter>: <value>`

The `<parameter>` is the name of an option without the leading `--`. The parameter `--user-folder "C:\Users\Alice"` becomes `user-folder: C:\Users\Alice`.
To make the file more readable, dashes may be replaced with spaces and captilization is ignored.
So, all of the following are equivalent:

```
user-folder: C:\Users\Alice
user folder: C:\Users\Alice
User Folder : C:\Users\Alice
```

If a parameter does not take a value (`--whole-file`, `--delete-first`, `--force-copy`, or `--debug`), it should be written with the same format but with a blank value.

```
Force copy:
```

### Comments

Lines starting with `#` are ignored by Vintage Backup, so they may be used for comments and descriptions of settings.

### Quoting to preserve leading and trailing spaces in file names

Normally, and leading or trailing spaces will be trimmed from the parameter values.
If the value needs to retain those spaces, then put double quotes around the value.

```
Filter: " name with spaces at both ends.txt  "
```

In this example, the single space before name and two spaces at the end will be kept as part of the file name.
File and folder names with newlines will not be handled correctly.
Use names without newlines or use the command line if they cannot be renamed.

If a parameter value begins and ends with quotation marks that need to be kept, use another set of quotations marks.

```
Filter: ""file name that is quoted.txt""
```

In this example, the file name that will be passed to Vintage Backup is `"file name that is quoted.txt"`.

Quotes will only be removed in they are at both the start and end of a parameter value, and only one pair will be removed. Otherwise, quotes are not removed. For example,

```
Filter: the "alleged" filter file.txt
```

will result in `the "alleged" filter file.txt` being passed as the `--filter` argument to Vintage Backup.


### Example configuration file

Here's an example of a complete configuration file that can be used for regular backups:

```
# Main options
User folder: /home/bob
Backup folder: /mnt/backups/bobs backups
Filter: /home/bob/backup filter.txt

# Save lots of space
Hard link count: 50
Free up: 20GB

# Write a lot more information to the log file
Debug:
```

This is equivalent to the following command line:

```
python3 vintagebackup.py --user-folder /home/bob --backup-folder "/mnt/backups/bobs backups" --filter "/home/bob/backup filter.txt" --hard-link-count 50 --free-up 20GB --debug
```

Like the command line, the order of different parameters does not matter. If a parameter appears more than once, the last parameter value is used.

## Overriding configuration files

If command line parameters are used in addition to a configuration file, the command line parameters take precedence over file options.
Let's say the above complete configuration file example is stored in `/home/bob/backup_config.txt`.
The following command line

```
python3 vintagebackup.py --config /home/bob/backup_config.txt --no-debug --free-up 10GB
```

would override the `Free up: 20GB` value with `10GB` and would add the `--no-debug` parameter, resulting in the following equivalent command line:

```
python3 vintagebackup.py --user-folder /home/bob --backup-folder "/mnt/backups/bobs backups" --filter "/home/bob/backup filter.txt" --hard-link-count 50 --debug --no-debug --free-up 10GB
```

The `--no-debug` parameter negates the `--debug` parameter.

## No recursion

The only command line parameter that is not supported is `--config`.
This means that recursive configuration files that contain `Config: another_config_file.txt` parameter are not allowed.
