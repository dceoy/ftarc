"""Pipeline orchestration and configuration management for ftarc.

This module handles the high-level pipeline execution, including configuration
file parsing, resource allocation, and coordination of Luigi tasks for the
complete genomic data processing workflow.
"""

import logging
import os
import re
import shutil
from collections.abc import Mapping, Sequence
from math import floor
from pathlib import Path
from pprint import pformat
from typing import Any

from psutil import cpu_count, virtual_memory

from ftarc.cli.constants import MAX_FQ_FILES, MEMORY_THRESHOLD_MB
from ftarc.cli.util import (
    build_luigi_tasks,
    fetch_executable,
    load_default_dict,
    parse_fq_id,
    print_log,
    print_yml,
    read_yml,
    render_luigi_log_cfg,
)
from ftarc.task.controller import PrintEnvVersions, RunPreprocessingPipeline


def run_processing_pipeline(
    config_yml_path: str | os.PathLike[str],
    dest_dir_path: str | os.PathLike[str] | None = None,
    max_n_cpu: int | None = None,
    max_n_worker: int | None = None,
    skip_cleaning: bool = False,
    print_subprocesses: bool = False,
    console_log_level: str = "WARNING",
    file_log_level: str = "DEBUG",
    use_spark: bool = False,
    use_bwa_mem2: bool = True,
) -> None:
    """Run the complete genomic data processing pipeline.

    Orchestrates the full FASTQ-to-CRAM workflow including configuration parsing,
    resource allocation, and Luigi task coordination for genomic data processing.

    Args:
        config_yml_path: Path to the YAML configuration file
        dest_dir_path: Output directory path (defaults to current directory)
        max_n_cpu: Maximum number of CPUs to use (defaults to system CPU count)
        max_n_worker: Maximum number of parallel workers (defaults to max_n_cpu)
        skip_cleaning: Skip cleaning temporary files and cache directories
        print_subprocesses: Print subprocess commands and output
        console_log_level: Console logging level (WARNING, INFO, DEBUG, etc.)
        file_log_level: File logging level (WARNING, INFO, DEBUG, etc.)
        use_spark: Use Spark-enabled GATK tools for large-scale processing
        use_bwa_mem2: Use bwa-mem2 instead of bwa for read alignment
    """
    logger = logging.getLogger(__name__)
    logger.info("config_yml_path:\t%s", config_yml_path)
    config = _read_config_yml(path=config_yml_path)
    runs = config.get("runs")
    logger.info("dest_dir_path:\t%s", dest_dir_path)
    dest_dir = Path(dest_dir_path).resolve()
    log_dir = dest_dir.joinpath("log")

    adapter_removal = config.get("adapter_removal", True)
    logger.debug("adapter_removal:\t%s", adapter_removal)

    default_dict = load_default_dict(stem="example_ftarc")
    metrics_collectors = (
        [
            k
            for k in default_dict["metrics_collectors"]
            if config["metrics_collectors"].get(k)
        ]
        if "metrics_collectors" in config
        else []
    )
    logger.debug("metrics_collectors:%s%s", os.linesep, pformat(metrics_collectors))

    command_dict = {
        "bwa": fetch_executable("bwa-mem2" if use_bwa_mem2 else "bwa"),
        **{
            c: fetch_executable(c)
            for c in {
                "bgzip",
                "gatk",
                "java",
                "pbzip2",
                "pigz",
                "samtools",
                "tabix",
                *(["gnuplot"] if "samtools" in metrics_collectors else []),
                *({"cutadapt", "fastqc", "trim_galore"} if adapter_removal else set()),
            }
        },
    }
    logger.debug("command_dict:%s%s", os.linesep, pformat(command_dict))

    n_cpu = cpu_count()
    n_worker = min(int(max_n_worker or max_n_cpu or n_cpu), (len(runs) or 1))
    n_cpu_per_worker = max(1, floor(int(max_n_cpu or n_cpu) / n_worker))
    memory_mb = virtual_memory().total / 1024 / 1024 / 2
    memory_mb_per_worker = int(memory_mb / n_worker)
    cf_dict = {
        "reference_name": (config.get("reference_name") or ""),
        "use_bwa_mem2": use_bwa_mem2,
        "use_spark": use_spark,
        "adapter_removal": adapter_removal,
        "plot_bamstats": (
            fetch_executable("plot-bamstats")
            if "samtools" in metrics_collectors
            else ""
        ),
        "metrics_collectors": metrics_collectors,
        "save_memory": (memory_mb_per_worker < MEMORY_THRESHOLD_MB),
        **{f"{k}_dir_path": str(dest_dir.joinpath(k)) for k in ("trim", "align", "qc")},
        **{k: v for k, v in command_dict.items() if k != "java"},
    }
    logger.debug("cf_dict:%s%s", os.linesep, pformat(cf_dict))

    sh_config = {
        "log_dir_path": str(log_dir),
        "remove_if_failed": (not skip_cleaning),
        "quiet": (not print_subprocesses),
        "executable": fetch_executable("bash"),
    }
    logger.debug("sh_config:%s%s", os.linesep, pformat(sh_config))

    resource_path_dict = _resolve_input_file_paths(
        path_dict={
            "fa": config["resources"]["reference_fa"],
            "known_sites_vcf": config["resources"]["known_sites_vcf"],
        }
    )
    logger.debug("resource_path_dict:%s%s", os.linesep, pformat(resource_path_dict))

    sample_dict_list = (
        [
            {**_determine_input_samples(run_dict=r), "priority": p}
            for p, r in zip(
                [i * 1000 for i in range(1, (len(runs) + 1))[::-1]], runs, strict=False
            )
        ]
        if runs
        else []
    )
    logger.debug("sample_dict_list:%s%s", os.linesep, pformat(sample_dict_list))

    print_log(f"Prepare analysis-ready CRAM files:\t{dest_dir}")
    print_yml([
        {
            "config": [
                {"adapter_removal": adapter_removal},
                {"metrics_collectors": metrics_collectors},
                {"n_worker": n_worker},
                {"n_cpu": n_cpu},
                {"memory_mb": memory_mb},
            ]
        },
        {
            "input": [
                {"n_sample": len(runs)},
                {"samples": [d["sample_name"] for d in sample_dict_list]},
            ]
        },
    ])
    log_cfg_path = str(log_dir.joinpath("luigi.log.cfg"))
    render_luigi_log_cfg(
        log_cfg_path=log_cfg_path,
        console_log_level=console_log_level,
        file_log_level=file_log_level,
    )

    build_luigi_tasks(
        tasks=[
            PrintEnvVersions(
                command_paths=list(command_dict.values()), sh_config=sh_config
            )
        ],
        workers=1,
        log_level=console_log_level,
        logging_conf_file=log_cfg_path,
        hide_summary=True,
    )
    build_luigi_tasks(
        tasks=[
            RunPreprocessingPipeline(
                **d,
                **resource_path_dict,
                **cf_dict,
                n_cpu=n_cpu_per_worker,
                memory_mb=memory_mb_per_worker,
                sh_config=sh_config,
            )
            for d in sample_dict_list
        ],
        workers=n_worker,
        log_level=console_log_level,
        logging_conf_file=log_cfg_path,
    )
    if not skip_cleaning:
        for a in Path(cf_dict["align_dir_path"]).iterdir():
            c = a.joinpath(".ref_cache")
            if c.is_dir():
                print_log(f"Remove a cache directory:\t{c}")
                shutil.rmtree(str(c))


def _read_config_yml(path: str | os.PathLike[str]) -> dict[str, Any]:
    """Read and validate the YAML configuration file.

    Args:
        path: Path to the YAML configuration file

    Returns:
        Validated configuration dictionary

    Raises:
        ValueError: If the configuration is invalid or malformed
        TypeError: If configuration values have incorrect types
    """
    config = read_yml(path=Path(path).resolve())
    if not (isinstance(config, dict) and config.get("resources")):
        msg = f"Invalid config structure: {config}"
        raise ValueError(msg)
    if not isinstance(config["resources"], dict):
        msg = f"Invalid resources structure: {config['resources']}"
        raise TypeError(msg)
    for k in ["reference_fa", "known_sites_vcf"]:
        v = config["resources"].get(k)
        if k == "known_sites_vcf":
            if not isinstance(v, list):
                msg = f"Expected list for {k}, got {type(v)}"
                raise ValueError(msg)
            if len(v) == 0:
                msg = f"Empty list not allowed for {k}"
                raise ValueError(msg)
            if not _has_unique_elements(v):
                msg = f"Duplicate elements found in {k}"
                raise ValueError(msg)
            for s in v:
                if not isinstance(s, str):
                    msg = f"Expected string in {k}, got {type(s)}"
                    raise TypeError(msg)
        elif not isinstance(v, str):
            msg = f"Expected string for {k}, got {type(v)}"
            raise TypeError(msg)
    if not config.get("runs"):
        msg = f"Missing 'runs' in config: {config}"
        raise ValueError(msg)
    if not isinstance(config["runs"], list):
        msg = f"Expected list for runs, got {type(config['runs'])}"
        raise TypeError(msg)
    for r in config["runs"]:
        if not isinstance(r, dict):
            msg = f"Expected dict for run, got {type(r)}: {r}"
            raise TypeError(msg)
        if not r.get("fq"):
            msg = f"Missing 'fq' in run: {r}"
            raise ValueError(msg)
        if not isinstance(r["fq"], list):
            msg = f"Expected list for fq, got {type(r['fq'])}: {r}"
            raise TypeError(msg)
        if not _has_unique_elements(r["fq"]):
            msg = f"Duplicate fq files found: {r}"
            raise ValueError(msg)
        if len(r["fq"]) > MAX_FQ_FILES:
            msg = f"Too many fq files (max {MAX_FQ_FILES}): {r}"
            raise ValueError(msg)
        for p in r["fq"]:
            if not p.endswith((".gz", ".bz2")):
                msg = f"fq file must be compressed (.gz or .bz2): {p}"
                raise ValueError(msg)
        if r.get("read_group"):
            if not isinstance(r["read_group"], dict):
                msg = f"Expected dict for read_group: {r}"
                raise ValueError(msg)
            for k, v in r["read_group"].items():
                if not re.fullmatch(r"[A-Z]{2}", k):
                    msg = (
                        f"Invalid read group key format "
                        f"(expected 2 uppercase letters): {k}"
                    )
                    raise ValueError(msg)
                if not isinstance(v, str):
                    msg = f"Expected string value for read group key {k}, got {type(v)}"
                    raise TypeError(msg)
    return config


def _has_unique_elements(elements: Sequence[object]) -> bool:
    """Check if all elements in a sequence are unique.

    Args:
        elements: Sequence to check for uniqueness

    Returns:
        True if all elements are unique, False otherwise
    """
    return len(set(elements)) == len(tuple(elements))


def _resolve_file_path(path: str | os.PathLike[str]) -> str:
    """Resolve and validate a file path.

    Args:
        path: File path to resolve

    Returns:
        Absolute path string

    Raises:
        FileNotFoundError: If the file does not exist
    """
    p = Path(path).resolve()
    if not p.is_file():
        msg = f"file not found: {p}"
        raise FileNotFoundError(msg)
    return str(p)


def _resolve_input_file_paths(
    path_list: Sequence[str] | None = None,
    path_dict: Mapping[str, Any] | None = None,
) -> list[str] | dict[str, list[str] | str]:
    """Resolve input file paths from list or dictionary format.

    Args:
        path_list: Optional sequence of file paths
        path_dict: Optional mapping of keys to file paths or lists of paths

    Returns:
        List of resolved paths or dictionary with resolved paths
    """
    if path_list is not None:
        return [_resolve_file_path(s) for s in path_list]
    elif path_dict is not None:
        new_dict: dict[str, list[str] | str] = {}
        for k, v in path_dict.items():
            if isinstance(v, str):
                new_dict[f"{k}_path"] = _resolve_file_path(v)
            elif v:
                new_dict[f"{k}_paths"] = [_resolve_file_path(s) for s in v]
        return new_dict


def _determine_input_samples(run_dict: Mapping[str, Any]) -> dict[str, Any]:
    """Extract and process sample information from run configuration.

    Args:
        run_dict: Run configuration dictionary containing FASTQ paths and read
            group info

    Returns:
        Dictionary with processed sample information including paths and metadata
    """
    g = run_dict.get("read_group") or {}
    return {
        "fq_paths": _resolve_input_file_paths(path_list=run_dict["fq"]),
        "read_group": g,
        "sample_name": (g.get("SM") or parse_fq_id(fq_path=run_dict["fq"][0])),
    }
