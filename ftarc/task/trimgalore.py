"""Trim Galore adapter trimming tasks for the ftarc pipeline.

This module provides Luigi tasks for adapter removal and quality trimming of FASTQ files
using Trim Galore, which wraps Cutadapt and FastQC.
"""

import re
from itertools import product
from pathlib import Path
from typing import Any

import luigi

from .core import FtarcTask


class TrimAdapters(FtarcTask):
    """Luigi task for adapter removal and quality trimming of FASTQ files.

    This task uses Trim Galore to automatically detect and remove sequencing adapters,
    perform quality trimming, and optionally run FastQC on trimmed reads.

    Parameters:
        fq_paths: List of input FASTQ file paths.
        dest_dir_path: Output directory for trimmed files.
        sample_name: Sample identifier for logging.
        pigz: Path to the pigz executable.
        pbzip2: Path to the pbzip2 executable.
        trim_galore: Path to the trim_galore executable.
        cutadapt: Path to the cutadapt executable.
        fastqc: Path to the fastqc executable.
        add_trim_galore_args: Additional arguments for trim_galore.
        n_cpu: Number of CPU threads to use.
        memory_mb: Memory allocation in MB.
        sh_config: Shell configuration parameters.
    """

    fq_paths: list[str] = luigi.ListParameter()
    dest_dir_path: str = luigi.Parameter(default=".")
    sample_name: str = luigi.Parameter(default="")
    pigz: str = luigi.Parameter(default="pigz")
    pbzip2: str = luigi.Parameter(default="pbzip2")
    trim_galore: str = luigi.Parameter(default="trim_galore")
    cutadapt: str = luigi.Parameter(default="cutadapt")
    fastqc: str = luigi.Parameter(default="fastqc")
    add_trim_galore_args: list[str] = luigi.ListParameter(default=[])
    n_cpu: int = luigi.IntParameter(default=1)
    memory_mb: float = luigi.FloatParameter(default=4096)
    sh_config: dict[str, Any] = luigi.DictParameter(default={})
    priority: int = 50

    def output(self) -> list[luigi.LocalTarget]:
        """Return the output targets for trimmed FASTQ files.

        Returns:
            list[luigi.LocalTarget]: List of output file targets with trimmed reads.

        Note:
            Output files follow Trim Galore naming convention:
            - Single-end: {basename}_val_1.fq.gz
            - Paired-end: {basename}_val_1.fq.gz, {basename}_val_2.fq.gz
        """
        dest_dir = Path(self.dest_dir_path).resolve()
        standard_suffixes = tuple(
            "".join(t) for t in product([".fastq", ".fq"], [".gz", ""])
        )
        return [
            luigi.LocalTarget(
                dest_dir.joinpath(
                    (
                        re.sub(r"\.(fastq|fq)$", "", Path(p).stem)
                        if p.endswith(standard_suffixes)
                        else Path(p).name
                    )
                    + f"_val_{i + 1}.fq.gz"
                )
            )
            for i, p in enumerate(self.fq_paths)
        ]

    def run(self) -> None:
        """Execute adapter trimming and quality filtering using Trim Galore.

        This method performs comprehensive read preprocessing including:
        1. Automatic adapter detection and removal
        2. Quality trimming based on Phred scores
        3. Length filtering to remove short reads
        4. Optional FastQC quality control reports

        The method handles both single-end and paired-end data, automatically
        detecting the mode based on the number of input files. It also manages
        file format conversion from bzip2 to gzip when needed.
        """
        run_id = self.sample_name or Path(Path(Path(self.fq_paths[0]).stem).stem).stem
        self.print_log(f"Trim adapters:\t{run_id}")
        output_fq_paths = [o.path for o in self.output()]
        run_dir = Path(output_fq_paths[0]).parent
        work_fq_paths = [
            (str(run_dir.joinpath(Path(p).stem + ".gz")) if p.endswith(".bz2") else p)
            for p in self.fq_paths
        ]
        self.setup_shell(
            run_id=run_id,
            commands=[
                self.pigz,
                self.pbzip2,
                self.trim_galore,
                self.cutadapt,
                self.fastqc,
            ],
            cwd=run_dir,
            **self.sh_config,
            env={"JAVA_TOOL_OPTIONS": f"-Xmx{int(self.memory_mb)}m"},
        )
        for i, o in zip(self.fq_paths, work_fq_paths, strict=False):
            if i.endswith(".bz2"):
                self.bzip2_to_gzip(
                    src_bz2_path=i,
                    dest_gz_path=o,
                    pbzip2=self.pbzip2,
                    pigz=self.pigz,
                    n_cpu=self.n_cpu,
                )
        self.run_shell(
            args=(
                f"set -e && {self.trim_galore}"
                f" --path_to_cutadapt {self.cutadapt}"
                f" --cores {self.n_cpu}"
                + (" --paired" if len(work_fq_paths) > 1 else "")
                + "".join(f" {a}" for a in self.add_trim_galore_args)
                + f" --output_dir {run_dir}"
                + "".join(f" {p}" for p in work_fq_paths)
            ),
            input_files_or_dirs=work_fq_paths,
            output_files_or_dirs=[*output_fq_paths, run_dir],
        )


class LocateFastqs(FtarcTask):
    """Luigi task for locating and organizing FASTQ files for processing.

    This task identifies FASTQ files and prepares them for downstream analysis,
    handling both single and paired-end sequencing data.

    Parameters:
        fq_paths: Path pattern or list of FASTQ files.
        dest_dir_path: Output directory for organized files.
        sample_name: Sample identifier.
    """

    fq_paths: list[str] = luigi.Parameter()
    dest_dir_path: str = luigi.Parameter(default=".")
    sample_name: str = luigi.Parameter(default="")
    pigz: str = luigi.Parameter(default="pigz")
    pbzip2: str = luigi.Parameter(default="pbzip2")
    n_cpu: int = luigi.IntParameter(default=1)
    sh_config: dict[str, Any] = luigi.DictParameter(default={})
    priority: int = 50

    def output(self) -> list[luigi.LocalTarget]:
        """Return the output targets for located and processed FASTQ files.

        Returns:
            list[luigi.LocalTarget]: List of output file targets, with bzip2 files
                converted to gzip format and other files passed through unchanged.
        """
        dest_dir = Path(self.dest_dir_path).resolve()
        return [
            luigi.LocalTarget(
                dest_dir.joinpath(Path(p).stem + ".gz")
                if p.endswith(".bz2")
                else Path(p)
            )
            for p in self.fq_paths
        ]

    def run(self) -> None:
        """Execute FASTQ file location and format standardization.

        This method processes FASTQ files to ensure consistent format:
        1. Identifies bzip2-compressed files (.bz2)
        2. Converts bzip2 files to gzip format (.gz) using parallel tools
        3. Leaves already properly formatted files unchanged

        The conversion uses pbzip2 for decompression and pigz for recompression,
        utilizing multiple CPU cores for improved performance on large files.
        """
        run_id = Path(Path(Path(self.fq_paths[0]).stem).stem).stem
        self.print_log(f"Bunzip2 and Gzip a file:\t{run_id}")
        self.setup_shell(
            run_id=run_id,
            commands=[self.pigz, self.pbzip2],
            cwd=self.dest_dir_path,
            **self.sh_config,
        )
        for p, o in zip(self.fq_paths, self.output(), strict=False):
            if p != o.path:
                self.bzip2_to_gzip(
                    src_bz2_path=p,
                    dest_gz_path=o.path,
                    pbzip2=self.pbzip2,
                    pigz=self.pigz,
                    n_cpu=self.n_cpu,
                )


if __name__ == "__main__":
    luigi.run()
