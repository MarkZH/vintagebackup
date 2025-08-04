"""A backup utility that uses hardlinks to save space when making full backups."""

import sys
import logging

from lib.main import main

if __name__ == "__main__":
    try:
        logger = logging.getLogger(__name__)
        logger.addHandler(logging.StreamHandler(sys.stdout))
        sys.exit(main(sys.argv))
    except KeyboardInterrupt:
        sys.exit(1)
