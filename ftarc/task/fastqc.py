#!/usr/bin/env python
"""FastQC quality control tasks for the ftarc pipeline.

This module provides Luigi tasks for running FastQC quality control analysis on
FASTQ and SAM/BAM/CRAM files to generate comprehensive quality metrics reports.
"""

import os
import re
from collections.abc import Iterable
from pathlib import Path

import luigi

from .core import FtarcTask


class CollectFqMetricsWithFastqc(FtarcTask):
    """Luigi task for collecting FASTQ quality metrics using FastQC.

    This task runs FastQC on FASTQ files to generate comprehensive quality control
    reports including per-base quality scores, sequence content, adapter contamination,
    and other important metrics.

    Parameters:
        fq_paths: List of FASTQ file paths to analyze.
        dest_dir_path: Output directory for FastQC reports.
        fastqc: Path to the FastQC executable.
        add_fastqc_args: Additional arguments for FastQC.
        n_cpu: Number of CPU threads to use.
        memory_mb: Memory allocation in MB.
        sh_config: Shell configuration parameters.
    """

    fq_paths = luigi.ListParameter()
    dest_dir_path = luigi.Parameter(default=".")
    fastqc = luigi.Parameter(default="fastqc")
    add_fastqc_args = luigi.ListParameter(default=["--nogroup"])
    n_cpu = luigi.IntParameter(default=1)
    memory_mb = luigi.FloatParameter(default=4096)
    sh_config = luigi.DictParameter(default={})
    priority = 10

    def output(self) -> list[luigi.LocalTarget]:
        """Return the output targets for the FastQC task.

        Returns:
            list[luigi.LocalTarget]: List of output file targets including HTML and ZIP reports.
        """
        return [
            luigi.LocalTarget(o) for o in self._generate_output_files(*self.fq_paths)
        ]

    def run(self) -> None:
        """Execute FastQC quality control analysis on FASTQ files.

        This method runs FastQC on each FASTQ file in the input list, generating
        HTML reports and ZIP archives containing detailed quality metrics. The
        analysis includes per-base quality scores, sequence content analysis,
        adapter contamination detection, and other important QC metrics.

        Raises:
            subprocess.CalledProcessError: If FastQC execution fails.
            FileNotFoundError: If input FASTQ files are not found.
        """
        dest_dir = Path(self.dest_dir_path).resolve()
        for p in self.fq_paths:
            fq = Path(p).resolve()
            run_id = fq.stem
            self.print_log(f"Collect FASTQ metrics using FastQC:\t{run_id}")
            self.setup_shell(
                run_id=run_id,
                commands=self.fastqc,
                cwd=dest_dir,
                **self.sh_config,
                env={"JAVA_TOOL_OPTIONS": f"-Xmx{int(self.memory_mb)}m"},
            )
            self.run_shell(
                args=(
                    f"set -e && {self.fastqc}"
                    f" --threads {self.n_cpu}"
                    + "".join(f" {a}" for a in self.add_fastqc_args)
                    + f" --outdir {dest_dir} {p}"
                ),
                input_files_or_dirs=p,
                output_files_or_dirs=list(self._generate_output_files(p)),
            )
        tmp_dir = dest_dir.joinpath("?")
        self.remove_files_and_dirs(tmp_dir)

    def _generate_output_files(self, *paths: str | os.PathLike[str]) -> Iterable[Path]:
        """Generate expected output file paths for FastQC analysis.

        Args:
            *paths: Variable number of input FASTQ file paths.

        Yields:
            Path: Expected output file paths for FastQC HTML and ZIP reports.

        Note:
            FastQC generates two output files for each input FASTQ file:
            - HTML report for visualization
            - ZIP archive containing detailed metrics data
        """
        dest_dir = Path(self.dest_dir_path).resolve()
        for p in paths:
            stem = re.sub(r"\.(fq|fastq)$", "", Path(str(p)).stem)
            for e in ["html", "zip"]:
                yield dest_dir.joinpath(f"{stem}_fastqc.{e}")


if __name__ == "__main__":
    luigi.run()
