# Other options

## Logging

### `--log`

Specify an alternate location for the log file.
The parameter is a file name.

### `--error-log`

Specify a log file that only receives warnings and errors.
The parameter is a file name.
If a backup or other process runs without errors, this file will not be created.
If the file in the parameter is placed in a prominent location (like the Desktop), then the full log does not need to be inspected as often--only when the error log appears to indicate something went wrong.


## Troubleshooting

### `--preview-filter`

Check that a [filter file](filter_files.md) is correct by printing a list of everything that will be backed up.
The list will be printed to a file name in the parameter of this option, or the console if the parameter is blank.
The `--user-folder` option is required. This procedure can be run without a `--filter` option, which will result in everything in the user folder being printed.

### `--debug`

Write extra information about more actions to the log file.
For example, running a backup with `--debug` will print whether a file was copied or hard linked for every backed up file.
