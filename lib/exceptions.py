"""Exceptions to report errors."""


class CommandLineError(ValueError):
    """An exception raised when invalid command line parameters are used."""


class ConcurrencyError(RuntimeError):
    """An exception raised when another process is using the same backup location."""


class OutOfSpaceError(RuntimeError):
    """An exception raised when the backup media does not have enough space for the operation."""


class FilterFileError(ValueError):
    """An exception raised when an error occurs when reading a filter file."""
