"""Functions that generate files for automating backup procedures."""

import logging
import argparse
import sys
from pathlib import Path

from lib.configuration import generate_config
from lib.filesystem import absolute_path, unique_path_name

logger = logging.getLogger()


def generate_windows_scripts(args: argparse.Namespace) -> None:
    """Generate files for use with Windows Task Scheduler."""
    destination = absolute_path(args.generate_windows_scripts)
    args.generate_config = str(destination/"config.txt")
    config_path = generate_config(args)

    batch_file = unique_path_name(destination/"batch_script.bat")
    script_location = Path(__file__).parent/"vintagebackup.py"
    python_version = f"{sys.version_info[0]}.{sys.version_info[1]}"
    batch_file.write_text(
        f'py -{python_version} "{script_location}" --config "{config_path}"\n',
        encoding="utf8")
    logger.info("Generated batch script: %s", batch_file)

    vb_script_file = unique_path_name(destination/"vb_script.vbs")
    vb_script_file.write_text(
f'''Dim Shell
Set Shell = CreateObject("WScript.Shell")
Shell.Run """{batch_file}""", 0, true
Set Shell = Nothing
''',
encoding="utf8")
    logger.info("Generated VB script: %s", vb_script_file)
