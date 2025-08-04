
import os
import argparse
import sys
from pathlib import Path
from inspect import getsourcefile
from typing import Any, cast

def generate_config(args: argparse.Namespace) -> Path:
    """Generate a configuration file from the arguments and return the path of that file."""
    no_arguments: set[str] = set()
    no_prefix = "no_"
    arguments: list[tuple[str, Any]] = []
    for option, value in vars(args).items():
        if not value or option in {"generate_config", "generate_windows_scripts", "config"}:
            continue

        if option.startswith(no_prefix) and value:
            no_arguments.add(option.removeprefix(no_prefix))
            continue

        arguments.append((option, value))

    arguments = [(arg, val) for arg, val in arguments if arg not in no_arguments]
    config_path = unique_path_name(Path(args.generate_config))
    with config_path.open("w", encoding="utf8") as config_file:
        for option, value in arguments:
            parameter = option.replace("_", " ").capitalize()
            value_string = "" if value is True else str(value)
            is_path = option in {"user_folder", "backup_folder", "filter", "destination"}
            is_non_null_log = option == "log" and value_string != os.devnull
            if is_path or is_non_null_log:
                value_string = str(absolute_path(value_string))
            needs_quotes = (value_string.strip() != value_string)
            parameter_value = f'"{value_string}"' if needs_quotes else value_string
            config_file.write(f"{parameter}: {parameter_value}".strip() + "\n")

    logger.info("Generated configuration file: %s", config_path)
    return config_path


def generate_windows_scripts(args: argparse.Namespace) -> None:
    """Generate files for use with Windows Task Scheduler."""
    destination = absolute_path(args.generate_windows_scripts)
    args.generate_config = str(destination/"config.txt")
    config_path = generate_config(args)

    batch_file = unique_path_name(destination/"batch_script.bat")
    script_path = cast(str, getsourcefile(main))
    script_location = absolute_path(script_path)
    python_version = f"{sys.version_info[0]}.{sys.version_info[1]}"
    batch_file.write_text(f'py -{python_version} "{script_location}" --config "{config_path}"\n')
    logger.info("Generated batch script: %s", batch_file)

    vb_script_file = unique_path_name(destination/"vb_script.vbs")
    vb_script_file.write_text(
f'''Dim Shell
Set Shell = CreateObject("WScript.Shell")
Shell.Run """{batch_file}""", 0, true
Set Shell = Nothing
''')
    logger.info("Generated VB script: %s", vb_script_file)
