"""A backup utility that uses hardlinks to save space when making full backups."""

import sys
import logging

if sys.version_info < (3, 13):
    print("Vintage Backup requires Python 3.13 or later.")
    sys.exit(1)

from lib.main import main

if __name__ == "__main__":
    try:
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)
        logger.addHandler(logging.StreamHandler(sys.stdout))
        sys.exit(main(sys.argv, testing=False))
    except KeyboardInterrupt:
        sys.exit(1)
