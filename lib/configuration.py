"""Functions for reading configuration files."""

from pathlib import Path

def read_configuation_file(config_file: Path) -> list[str]:
    """Parse a configuration file into command line arguments."""
    try:
        with config_file.open(encoding="utf8") as file:
            arguments: list[str] = []
            for line_raw in file:
                line = line_raw.strip()
                if not line or line.startswith("#"):
                    continue
                parameter_raw, value_raw = line.split(":", maxsplit=1)

                parameter = "-".join(parameter_raw.lower().split())
                if parameter == "config":
                    raise CommandLineError(
                        "The parameter `config` within a configuration file has no effect.")
                arguments.append(f"--{parameter}")

                value = remove_quotes(value_raw)
                if value:
                    arguments.append(value)
            return arguments
    except FileNotFoundError:
        raise CommandLineError(f"Configuation file does not exist: {config_file}") from None


def remove_quotes(s: str) -> str:
    """
    After stripping a string of outer whitespace, remove pairs of quotes from the start and end.

    >>> remove_quotes('  "  a b c  "   ')
    '  a b c  '

    Strings without quotes are stripped of outer whitespace.

    >>> remove_quotes(' abc  ')
    'abc'

    All other strings are unchanged.

    >>> remove_quotes('Inner "quoted strings" are not affected.')
    'Inner "quoted strings" are not affected.'

    >>> remove_quotes('"This quote will stay," he said.')
    '"This quote will stay," he said.'

    If a string (usually a file name read from a file) has leading or trailing spaces,
    then the user should surround this file name with quotations marks to make sure the
    spaces are included in the return value.

    If the file name actually does begin and end with quotation marks, then surround the
    file name with another pair of quotation marks. Only one pair will be removed.

    >>> remove_quotes('""abc""')
    '"abc"'
    """
    s = s.strip()
    if len(s) > 1 and (s[0] == s[-1] == '"'):
        return s[1:-1]
    return s
