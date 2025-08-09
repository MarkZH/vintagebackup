"""A class for preventing more than one operation from simultaneously modifying backups."""

import os
from pathlib import Path

from lib.exceptions import ConcurrencyError


class Backup_Lock:
    """
    Lock out other Vintage Backup instances from accessing the same backup location.

    This class should be used as a context manager like so:
    ```
    with Lock_File(backup_path, "backup"):
        # Code that uses backup path
    ```
    """

    def __init__(self, backup_location: Path, operation: str) -> None:
        """Set up the lock."""
        self.lock_file_path = backup_location/"vintagebackup.lock"
        self.pid = str(os.getpid())
        self.operation = operation

    def __enter__(self) -> None:
        """
        Attempt to take possession of the file lock.

        If unsuccessful, a ConcurrencyError is raised.
        """
        while not self.acquire_lock():
            try:
                other_pid, other_operation = self.read_lock_data()
            except FileNotFoundError:
                continue

            raise ConcurrencyError(
                f"Vintage Backup is already running {other_operation} on "
                f"{self.lock_file_path.parent} (PID {other_pid})")

    def __exit__(self, *_: object) -> None:
        """Release the file lock."""
        self.lock_file_path.unlink()

    def acquire_lock(self) -> bool:
        """
        Attempt to create the lock file.

        Returns whether locking was successful.
        """
        try:
            self.create_lock()
            return True
        except FileExistsError:
            return False

    def create_lock(self) -> None:
        """Write PID and operation to the lock file."""
        with self.lock_file_path.open("x", encoding="utf8") as lock_file:
            lock_file.write(f"{self.pid}\n")
            lock_file.write(f"{self.operation}\n")

    def read_lock_data(self) -> tuple[str, str]:
        """Get all data from lock file."""
        with self.lock_file_path.open(encoding="utf8") as lock_file:
            pid = lock_file.readline().strip()
            operation = lock_file.readline().strip()
            return (pid, operation)
