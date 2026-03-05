"""Exceptions to report errors."""


class CommandLineError(ValueError):
    """An exception class to catch invalid command line parameters."""


class ConcurrencyError(RuntimeError):
    """An exception thrown when another process is using the same backup location."""


class NotEnoughStorageSpaceError(CommandLineError):
    """An exception thrown when there is not enough room on the backup media to copy a user file."""
