"""Utility functions for ftarc command-line interface.

This module provides shared utilities for Luigi task building, executable detection,
and common CLI operations used across different ftarc commands.
"""

import logging
import os
import re
import shutil
from datetime import datetime
from pathlib import Path
from pprint import pformat
from typing import Any

import luigi
import yaml
from jinja2 import Environment, FileSystemLoader


def write_config_yml(
    path: str | os.PathLike[str], src_yml: str = "example_ftarc.yml"
) -> None:
    """Write a configuration YAML file from template.

    Args:
        path: Destination path for the configuration file
        src_yml: Source template filename (defaults to example_ftarc.yml)
    """
    if Path(path).is_file():
        print_log(f"The file exists:\t{path}")
    else:
        print_log(f"Create a config YAML:\t{path}")
        shutil.copyfile(
            str(Path(__file__).parent.joinpath("../static").joinpath(src_yml)),
            Path(path).resolve(),
        )


def print_log(message: str) -> None:
    """Print a log message to both logger and stdout.

    Args:
        message: Message to log and print
    """
    logger = logging.getLogger(__name__)
    logger.debug(message)
    print(f">>\t{message}", flush=True)


def fetch_executable(cmd: str, ignore_errors: bool = False) -> str | None:
    """Find executable command in PATH.

    Args:
        cmd: Command name to search for
        ignore_errors: Return None instead of raising error if not found

    Returns:
        Path to executable or None if not found and ignore_errors is True

    Raises:
        RuntimeError: If command not found and ignore_errors is False
    """
    executables = [
        cp
        for cp in [
            str(Path(p).joinpath(cmd)) for p in os.environ["PATH"].split(os.pathsep)
        ]
        if os.access(cp, os.X_OK)
    ]
    if executables:
        return executables[0]
    elif ignore_errors:
        return None
    else:
        msg = f"command not found: {cmd}"
        raise RuntimeError(msg)


def read_yml(path: str | os.PathLike[str]) -> dict[str, Any]:
    """Read and parse a YAML file.

    Args:
        path: Path to the YAML file

    Returns:
        Parsed YAML data as dictionary
    """
    logger = logging.getLogger(__name__)
    with open(str(path), encoding="utf-8") as f:
        d = yaml.load(f, Loader=yaml.FullLoader)
    logger.debug("YAML data:" + os.linesep + pformat(d))
    return d


def print_yml(data: object) -> None:
    """Print data in YAML format.

    Args:
        data: Data to serialize and print as YAML
    """
    print(yaml.dump(data))


def render_luigi_log_cfg(
    log_cfg_path: str | os.PathLike[str],
    log_dir_path: str | os.PathLike[str] | None = None,
    console_log_level: str = "WARNING",
    file_log_level: str = "DEBUG",
) -> None:
    """Render Luigi logging configuration file from template.

    Args:
        log_cfg_path: Path to write the logging configuration file
        log_dir_path: Directory for log files (defaults to config directory)
        console_log_level: Log level for console output
        file_log_level: Log level for file output
    """
    log_cfg = Path(str(log_cfg_path)).resolve()
    cfg_dir = log_cfg.parent
    log_dir = Path(str(log_dir_path)).resolve() if log_dir_path else cfg_dir
    log_txt = log_dir.joinpath(
        "luigi.{}.{}.log.txt".format(
            file_log_level, datetime.now().strftime("%Y%m%d_%H%M%S")
        )
    )
    for d in {cfg_dir, log_dir}:
        if not d.is_dir():
            print_log(f"Make a directory:\t{d}")
            d.mkdir(parents=True, exist_ok=True)
    print_log(
        "{} a file:\t{}".format(
            ("Overwrite" if log_cfg.exists() else "Render"), log_cfg
        )
    )
    with log_cfg.open(mode="w") as f:
        f.write(
            Environment(
                loader=FileSystemLoader(
                    str(Path(__file__).parent.joinpath("../template")), encoding="utf8"
                )
            )
            .get_template("luigi.log.cfg.j2")
            .render({
                "console_log_level": console_log_level,
                "file_log_level": file_log_level,
                "log_txt_path": str(log_txt),
            })
            + os.linesep
        )


def load_default_dict(stem: str) -> dict[str, Any]:
    """Load default configuration from static YAML file.

    Args:
        stem: Base filename (without .yml extension) in static directory

    Returns:
        Configuration dictionary
    """
    return read_yml(path=Path(__file__).parent.parent.joinpath(f"static/{stem}.yml"))


def build_luigi_tasks(
    check_scheduling_succeeded: bool = True,
    hide_summary: bool = False,
    **kwargs: object,
) -> None:
    """Build and execute Luigi tasks.

    Args:
        check_scheduling_succeeded: Assert that scheduling succeeded
        hide_summary: Skip printing execution summary
        **kwargs: Additional arguments passed to luigi.build()
    """
    r = luigi.build(local_scheduler=True, detailed_summary=True, **kwargs)
    if not hide_summary:
        print(
            os.linesep + os.linesep.join(["Execution summary:", r.summary_text, str(r)])
        )
    if check_scheduling_succeeded:
        assert r.scheduling_succeeded, r.one_line_summary


def parse_fq_id(fq_path: str | os.PathLike[str]) -> str:
    """Extract sample ID from FASTQ file path.

    Removes common FASTQ file suffixes and read pair indicators to extract
    the base sample identifier.

    Args:
        fq_path: Path to FASTQ file

    Returns:
        Extracted sample ID string
    """
    fq_stem = Path(fq_path).name
    for _ in range(3):
        if fq_stem.endswith(("fq", "fastq")):
            fq_stem = Path(fq_stem).stem
            break
        else:
            fq_stem = Path(fq_stem).stem
    return (
        re.sub(
            r"[\._](read[12]|r[12]|[12]|[a-z0-9]+_val_[12]|r[12]_[0-9]+)$",
            "",
            fq_stem,
            flags=re.IGNORECASE,
        )
        or fq_stem
    )
