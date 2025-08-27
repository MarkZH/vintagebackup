"""A backup utility that uses hardlinks to save space when making full backups."""

import sys
import logging

minimum_python_version = (3, 13)
if sys.version_info < minimum_python_version:
    print(f"Vintage Backup requires Python {".".join(map(str, minimum_python_version))} or later.")
    sys.exit(1)

from lib.main import main  # noqa: E402

if __name__ == "__main__":
    try:
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)
        logger.addHandler(logging.StreamHandler(sys.stdout))
        sys.exit(main(sys.argv, testing=False))
    except KeyboardInterrupt:
        sys.exit(1)
