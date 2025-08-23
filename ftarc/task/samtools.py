"""Samtools operations for the ftarc pipeline.

This module provides Luigi tasks for samtools operations including FASTA indexing,
BAM/CRAM file manipulation, duplicate removal, and quality metrics collection.
"""

import re
from pathlib import Path

import luigi
from luigi.util import requires

from .core import FtarcTask


class SamtoolsFaidx(FtarcTask):
    """Luigi task for creating FASTA index files using samtools faidx.

    This task generates .fai index files for reference FASTA files, enabling
    efficient random access to sequence regions.

    Parameters:
        fa_path: Path to the FASTA file to index.
        samtools: Path to the samtools executable.
        add_faidx_args: Additional arguments for samtools faidx.
        sh_config: Shell configuration parameters.
    """

    fa_path = luigi.Parameter()
    samtools = luigi.Parameter(default="samtools")
    add_faidx_args = luigi.ListParameter(default=[])
    sh_config = luigi.DictParameter(default={})
    priority = 70

    def output(self) -> luigi.LocalTarget:
        """Return the output target for the FASTA index file.

        Returns:
            luigi.LocalTarget: Path to the generated FASTA index file (.fai).
        """
        fa = Path(self.fa_path).resolve()
        return luigi.LocalTarget(f"{fa}.fai")

    def run(self) -> None:
        """Execute FASTA file indexing using samtools faidx.

        This method creates a .fai index file that enables fast random access
        to sequences in the FASTA file by genomic coordinates.
        """
        run_id = Path(self.fa_path).stem
        self.print_log(f"Index FASTA:\t{run_id}")
        fa = Path(self.fa_path).resolve()
        self.setup_shell(
            run_id=run_id, commands=self.samtools, cwd=fa.parent, **self.sh_config
        )
        self.run_shell(
            args=(
                f"set -e && {self.samtools} faidx"
                + "".join(f" {a}" for a in self.add_faidx_args)
                + f" {fa}"
            ),
            input_files_or_dirs=fa,
            output_files_or_dirs=f"{fa}.fai",
        )


@requires(SamtoolsFaidx)
class SamtoolsView(FtarcTask):
    """Luigi task for converting and filtering SAM/BAM/CRAM files.

    This task provides flexible conversion between SAM formats, filtering options,
    and indexing capabilities using samtools view.

    Parameters:
        input_sam_path: Path to the input SAM/BAM/CRAM file.
        fa_path: Path to the reference FASTA file.
        output_sam_path: Path for the output file.
        samtools: Path to the samtools executable.
        n_cpu: Number of CPU threads to use.
        add_view_args: Additional arguments for samtools view.
        message: Custom log message for the operation.
        remove_input: Remove input file after successful conversion.
        index_sam: Create index for the output file.
        sh_config: Shell configuration parameters.
    """

    input_sam_path = luigi.Parameter()
    fa_path = luigi.Parameter()
    output_sam_path = luigi.Parameter()
    samtools = luigi.Parameter(default="samtools")
    n_cpu = luigi.IntParameter(default=1)
    add_view_args = luigi.ListParameter(default=[])
    message = luigi.Parameter(default="")
    remove_input = luigi.BoolParameter(default=True)
    index_sam = luigi.BoolParameter(default=False)
    sh_config = luigi.DictParameter(default={})
    priority = 90

    def output(self) -> list[luigi.LocalTarget]:
        """Return the output targets for the samtools view operation.

        Returns:
            list[luigi.LocalTarget]: List of output file targets including:
                - Converted/filtered SAM/BAM/CRAM file
                - Index file (.bai/.crai) if indexing is enabled
        """
        output_sam = Path(self.output_sam_path).resolve()
        return [
            luigi.LocalTarget(output_sam),
            *(
                [
                    luigi.LocalTarget(
                        re.sub(r"\.(cr|b)am$", ".\\1am.\\1ai", str(output_sam))
                    )
                ]
                if self.index_sam
                else []
            ),
        ]

    def run(self) -> None:
        """Execute SAM/BAM/CRAM file conversion and filtering using samtools view.

        This method handles various operations including:
        - Format conversion (SAM <-> BAM <-> CRAM)
        - Read filtering based on flags or regions
        - Compression and decompression
        - Index creation for random access

        The method automatically detects the operation type based on input/output
        file extensions and parameters, providing appropriate logging messages.
        """
        target_sam = Path(self.input_sam_path)
        run_id = target_sam.stem
        input_sam = target_sam.resolve()
        fa = Path(self.fa_path).resolve()
        output_sam = Path(self.output_sam_path).resolve()
        only_index = self.input_sam_path == self.output_sam_path and self.index_sam
        if self.message:
            message = self.message
        elif only_index:
            message = f"Index {input_sam.suffix[1:].upper()}"
        elif input_sam.suffix == output_sam.suffix:
            message = None
        else:
            message = "Convert {} to {}".format(*[
                s.suffix[1:].upper() for s in [input_sam, output_sam]
            ])
        if message:
            self.print_log(f"{message}:\t{run_id}")
        dest_dir = output_sam.parent
        self.setup_shell(
            run_id=run_id,
            commands=self.samtools,
            cwd=dest_dir,
            **self.sh_config,
            env={"REF_CACHE": str(dest_dir.joinpath(".ref_cache"))},
        )
        if only_index:
            self.samtools_index(
                sam_path=input_sam, samtools=self.samtools, n_cpu=self.n_cpu
            )
        else:
            self.samtools_view(
                input_sam_path=input_sam,
                fa_path=fa,
                output_sam_path=output_sam,
                samtools=self.samtools,
                n_cpu=self.n_cpu,
                add_args=self.add_view_args,
                index_sam=self.index_sam,
                remove_input=self.remove_input,
            )


class RemoveDuplicates(luigi.WrapperTask):
    """Luigi wrapper task for removing duplicate reads from aligned data.

    This task filters out reads marked as duplicates (flag 1024) using samtools view,
    producing a deduplicated CRAM file.

    Parameters:
        input_sam_path: Path to the input SAM/BAM/CRAM file.
        fa_path: Path to the reference FASTA file.
        dest_dir_path: Output directory for deduplicated files.
        samtools: Path to the samtools executable.
        add_view_args: Additional filtering arguments (default: -F 1024).
        n_cpu: Number of CPU threads to use.
        remove_input: Remove input file after deduplication.
        index_sam: Create index for the output file.
        sh_config: Shell configuration parameters.
    """

    input_sam_path = luigi.Parameter()
    fa_path = luigi.Parameter()
    dest_dir_path = luigi.Parameter(default=".")
    samtools = luigi.Parameter(default="samtools")
    add_view_args = luigi.ListParameter(default=["-F", "1024"])
    n_cpu = luigi.IntParameter(default=1)
    remove_input = luigi.BoolParameter(default=False)
    index_sam = luigi.BoolParameter(default=True)
    sh_config = luigi.DictParameter(default={})
    priority = 90

    def requires(self) -> luigi.Task:
        """Return the dependency task for duplicate removal.

        Returns:
            luigi.Task: SamtoolsView task configured to remove duplicates.
        """
        return SamtoolsView(
            input_sam_path=str(Path(self.input_sam_path).resolve()),
            fa_path=str(Path(self.fa_path).resolve()),
            output_sam_path=str(
                Path(self.dest_dir_path)
                .resolve()
                .joinpath(Path(self.input_sam_path).stem + ".dedup.cram")
            ),
            samtools=self.samtools,
            n_cpu=self.n_cpu,
            add_view_args=self.add_view_args,
            message="Remove duplicates",
            remove_input=self.remove_input,
            index_sam=self.index_sam,
            sh_config=self.sh_config,
        )

    def output(self) -> list[luigi.Target]:
        """Return the output targets from the dependency task.

        Returns:
            list[luigi.Target]: Output targets from the SamtoolsView task.
        """
        return self.input()


class CollectSamMetricsWithSamtools(FtarcTask):
    """Luigi task for collecting comprehensive alignment metrics using samtools.

    This task runs multiple samtools commands to generate various quality metrics
    including coverage, flagstat, idxstats, and stats reports.

    Parameters:
        sam_path: Path to the SAM/BAM/CRAM file to analyze.
        fa_path: Optional path to the reference FASTA file.
        dest_dir_path: Output directory for metrics files.
        samtools_commands: List of samtools commands to run.
        samtools: Path to the samtools executable.
        plot_bamstats: Path to the plot-bamstats executable.
        gnuplot: Path to the gnuplot executable.
        add_samtools_command_args: Additional arguments per command.
        add_faidx_args: Additional arguments for faidx.
        n_cpu: Number of CPU threads to use.
        sh_config: Shell configuration parameters.
    """

    sam_path = luigi.Parameter()
    fa_path = luigi.Parameter(default="")
    dest_dir_path = luigi.Parameter(default=".")
    samtools_commands = luigi.ListParameter(
        default=["coverage", "flagstat", "idxstats", "stats"]
    )
    samtools = luigi.Parameter(default="samtools")
    plot_bamstats = luigi.Parameter(default="plot-bamstats")
    gnuplot = luigi.Parameter(default="gnuplot")
    add_samtools_command_args = luigi.DictParameter(default={"depth": ["-a"]})
    add_faidx_args = luigi.ListParameter(default=[])
    n_cpu = luigi.IntParameter(default=1)
    sh_config = luigi.DictParameter(default={})
    priority = 10

    def requires(self) -> luigi.Task:
        """Return the dependency task for FASTA indexing if needed.

        Returns:
            luigi.Task: SamtoolsFaidx task if reference FASTA is provided,
                otherwise None.
        """
        if self.fa_path:
            return SamtoolsFaidx(
                fa_path=self.fa_path,
                samtools=self.samtools,
                add_faidx_args=self.add_faidx_args,
                sh_config=self.sh_config,
            )
        else:
            return super().requires()

    def output(self) -> list[luigi.LocalTarget]:
        """Return the output targets for samtools metrics collection.

        Returns:
            list[luigi.LocalTarget]: List of output file targets including:
                - Text files for each samtools command (coverage, flagstat, etc.)
                - HTML plots directory for stats command (if enabled)
        """
        sam_name = Path(self.sam_path).name
        dest_dir = Path(self.dest_dir_path).resolve()
        return [
            luigi.LocalTarget(dest_dir.joinpath(f"{sam_name}.{c}.txt"))
            for c in self.samtools_commands
        ] + (
            [luigi.LocalTarget(dest_dir.joinpath(f"{sam_name}.stats"))]
            if "stats" in self.samtools_commands
            else []
        )

    def run(self) -> None:
        """Execute comprehensive alignment metrics collection using samtools.

        This method runs multiple samtools commands to generate various quality
        metrics and statistics including:
        - coverage: Per-base and per-region coverage statistics
        - flagstat: Summary of alignment flags and read counts
        - idxstats: Alignment statistics per reference sequence
        - stats: Comprehensive alignment statistics with optional plots

        For the stats command, it also generates HTML visualization plots
        using plot-bamstats if gnuplot is available.
        """
        run_id = Path(self.sam_path).name
        self.print_log(f"Collect SAM metrics using Samtools:\t{run_id}")
        sam = Path(self.sam_path).resolve()
        fa = Path(self.fa_path).resolve() if self.fa_path else None
        dest_dir = Path(self.dest_dir_path).resolve()
        ref_cache = str(sam.parent.joinpath(".ref_cache"))
        for c in self.samtools_commands:
            output_txt = dest_dir.joinpath(Path(self.sam_path).name + f".{c}.txt")
            self.setup_shell(
                run_id=f"{run_id}.{c}",
                commands=(
                    [self.samtools, self.gnuplot] if c == "stats" else self.samtools
                ),
                cwd=dest_dir,
                **self.sh_config,
                env={"REF_CACHE": ref_cache},
            )
            self.run_shell(
                args=(
                    f"set -eo pipefail && {self.samtools} {c}"
                    + (
                        f" --reference {fa}"
                        if (fa is not None and c in {"coverage", "depth", "stats"})
                        else ""
                    )
                    + (
                        f" -@ {self.n_cpu}"
                        if c in {"flagstat", "idxstats", "stats"}
                        else ""
                    )
                    + "".join(
                        f" {a}" for a in (self.add_samtools_command_args.get(c) or [])
                    )
                    + f" {sam} | tee {output_txt}"
                ),
                input_files_or_dirs=sam,
                output_files_or_dirs=output_txt,
            )
            if c == "stats":
                plot_dir = dest_dir.joinpath(output_txt.stem)
                self.run_shell(
                    args=(
                        f"set -e && {self.plot_bamstats}"
                        + "".join(
                            f" {a}"
                            for a in (
                                self.add_samtools_command_args.get("plot-bamstats")
                                or []
                            )
                        )
                        + f" --prefix {plot_dir}/index {output_txt}"
                    ),
                    input_files_or_dirs=output_txt,
                    output_files_or_dirs=[plot_dir, plot_dir.joinpath("index.html")],
                )


class SoundReadDepthsWithSamtools(FtarcTask):
    """Luigi task for calculating read depth statistics using samtools depth.

    This task computes per-base read depth across the genome or specified regions,
    useful for coverage analysis and quality assessment.

    Parameters:
        sam_path: Path to the SAM/BAM/CRAM file to analyze.
        fa_path: Optional path to the reference FASTA file.
        bed_path: Optional BED file specifying regions of interest.
        dest_dir_path: Output directory for depth statistics.
        samtools: Path to the samtools executable.
        add_samtools_depth_args: Additional arguments for samtools depth.
        add_faidx_args: Additional arguments for faidx.
        n_cpu: Number of CPU threads to use.
        sh_config: Shell configuration parameters.
    """

    sam_path = luigi.Parameter()
    fa_path = luigi.Parameter(default="")
    bed_path = luigi.Parameter(default="")
    dest_dir_path = luigi.Parameter(default=".")
    samtools = luigi.Parameter(default="samtools")
    add_samtools_depth_args = luigi.ListParameter(default=["-a"])
    add_faidx_args = luigi.ListParameter(default=[])
    n_cpu = luigi.IntParameter(default=1)
    sh_config = luigi.DictParameter(default={})
    priority = 10

    def requires(self) -> luigi.Task:
        """Return the dependency task for FASTA indexing if needed.

        Returns:
            luigi.Task: SamtoolsFaidx task if reference FASTA is provided,
                otherwise None.
        """
        if self.fa_path:
            return SamtoolsFaidx(
                fa_path=self.fa_path,
                samtools=self.samtools,
                add_faidx_args=self.add_faidx_args,
                sh_config=self.sh_config,
            )
        else:
            return super().requires()

    def output(self) -> luigi.LocalTarget:
        """Return the output target for the depth statistics file.

        Returns:
            luigi.LocalTarget: Path to the gzipped TSV file containing depth statistics.

        Note:
            The filename encodes the analysis parameters:
            - '_a' suffix indicates all positions were included (-a flag)
            - '_b.<bed_file>' suffix indicates BED file was used for regions
        """
        return luigi.LocalTarget(
            Path(self.dest_dir_path)
            .resolve()
            .joinpath(
                Path(self.sam_path).name
                + ".depth"
                + ("_a" if "-a" in self.add_samtools_depth_args else "")
                + (("_b." + Path(self.bed_path).name) if self.bed_path else "")
                + ".tsv.gz"
            )
        )

    def run(self) -> None:
        """Execute read depth calculation using samtools depth.

        This method computes per-base read depth statistics across genomic regions.
        The analysis can be customized using various parameters:
        - Reference FASTA for CRAM files requiring sequence data
        - BED file to restrict analysis to specific regions
        - -a flag to include positions with zero coverage

        The output is a tab-separated file with columns:
        - Chromosome/reference name
        - Position (1-based)
        - Read depth

        The output file is automatically compressed with gzip for storage efficiency.
        """
        run_id = Path(self.sam_path).name
        self.print_log(f"Sound read depths using Samtools:\t{run_id}")
        sam = Path(self.sam_path).resolve()
        fa = Path(self.fa_path).resolve() if self.fa_path else None
        bed = Path(self.bed_path).resolve() if self.bed_path else None
        dest_dir = Path(self.dest_dir_path).resolve()
        ref_cache = str(sam.parent.joinpath(".ref_cache"))
        output_tsv_gz = Path(self.output().path)
        self.setup_shell(
            run_id=run_id,
            commands=self.samtools,
            cwd=dest_dir,
            **self.sh_config,
            env={"REF_CACHE": ref_cache},
        )
        self.run_shell(
            args=(
                f"set -eo pipefail && {self.samtools} depth"
                + (f" --reference {fa}" if fa is not None else "")
                + (f" -b {bed}" if bed is not None else "")
                + (f" -@ {self.n_cpu}" if self.n_cpu > 1 else "")
                + "".join(f" {a}" for a in self.add_samtools_depth_args)
                + f" {sam} | gzip -c - > {output_tsv_gz}"
            ),
            input_files_or_dirs=[sam, *[f for f in [fa, bed] if f]],
            output_files_or_dirs=output_tsv_gz,
        )


if __name__ == "__main__":
    luigi.run()
