# Filtering

When running a backup, every file in the `--user-folder` and all subfolders will be backed up to the `--backup-folder`.
If there are files that should be excluded, a filter file can be used with the `--filter` command line parameter to precisely control which files get backed up.

## Format

Each line of the filter file should contain one rule of the following form:

```
<sign> <pattern>
```

The `<sign>` is one of `-`, `+`, or `#`.

- `-` will exlude all files that match the pattern.
- `+` will include files that match the pattern.
- `#` causes the line to be ignored.
This can be used for comments.

The `<pattern>` consists of text that specifies a set of files to exclude or include.
It can be a file path or a [glob pattern](https://docs.python.org/3/library/pathlib.html#pathlib-pattern-language) to specify a set of files.
The specified files must be inside the `--user-folder` directory tree.

## Pattern format

Glob patterns have the following characters with special meaning:
- `?` - Matches any single character: `c?b` matches `cab` and `cub` but not `club`
- `*` - Matches zero or more characters with a single directory or file name: `c*b` matches `cb`, `cab`, `curb`, and `climb`
- `**` - Matches any sequence of nested directories: `a/**/b.txt` matches `a/b.txt`, `a/d1/b.txt``a/d1/d2/b.txt`, `a/d1/d2/d3/b.txt`
- `[sequence]` - Matches a single character between the brackets: `[bcdr]ough` matches `bough`, `cough`, `dough`, and `rough`.
  - This can also be used to make characters be no longer treated as special. `why[?].csv` will only match the file named `why?.csv`.
- `[!sequence]` - Matches a single character that is not between the brackets: `[!b]at` matches `cat`, `mat`, and `sat`, but not `bat`.

## Example

For example, when backing up C:\\Users\\Alice, the following filter file:

```
# Ignore AppData except Firefox
- AppData/**
+ AppData/Roaming/Mozilla/Firefox/**
```

The first line starts with `#`, so it is ignored.
The second line excludes everything in the `AppData` directory by using the `**` pattern.
The third line includes everything in the `AppData/Roaming/Mozilla/Firefox` directory.
This shows the usual way of using `+` lines: to include a subset of files inside of a directory that should be mostly excluded.

The order matters when writing a filter file.
If the second and third line of the example were reversed, then the Firefox directory would not be backed up because it would be excluded by the `- AppData/**` line.
Before adding a `+` line, make sure that the files in the pattern are excluded by earlier lines.
Otherwise, it will be redundant.

## Possible unexpected behavior

The lines in the filter file only filter files, not directories.
If an entire directory is to be excluded, then the pattern must end with `**` to exclude all of the directory's contents.
To be specific:
- `- dir/**` excludes the directory `dir` from the backup.
- `- dir/*` excludes all files in `dir`, but if there are subdirectories with files, those will be included.
- `- dir` does nothing.

## Notes

A pattern can be an absolute path or a relative path that starts at the folder being backed up.
Either way, the files must be contained within the directory tree of the folder being backed up.

On Windows, patterns are not case sensitive. Other systems like Mac and Linux are case sensitive.

A warning will be printed/logged if a filter line has no effect on the backup.
