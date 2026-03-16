"""Exceptions to report errors."""


class CommandLineError(ValueError):
    """An exception class to catch invalid command line parameters."""


class ConcurrencyError(RuntimeError):
    """An exception thrown when another process is using the same backup location."""


class OutOfSpaceError(RuntimeError):
    """An exception thrown when the backup media does not have enough space for the operation."""
