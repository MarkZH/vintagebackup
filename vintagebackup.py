
import sys
import logging

if __name__ == "__main__":
    try:
        logger.addHandler(logging.StreamHandler(sys.stdout))
        sys.exit(main(sys.argv))
    except KeyboardInterrupt:
        sys.exit(1)
