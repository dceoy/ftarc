"""High-level pipeline controller tasks for ftarc.

This module provides controller tasks that orchestrate the complete preprocessing
pipeline, coordinating multiple processing stages for genomic data from FASTQ to
analysis-ready CRAM.
"""

import re
import sys
from collections.abc import Generator
from pathlib import Path
from socket import gethostname

import luigi
from luigi.util import requires

from .bwa import AlignReads
from .core import FtarcTask
from .fastqc import CollectFqMetricsWithFastqc
from .gatk import ApplyBqsr, MarkDuplicates
from .picard import CollectSamMetricsWithPicard, ValidateSamFile
from .resource import FetchKnownSitesVcfs, FetchReferenceFasta
from .samtools import CollectSamMetricsWithSamtools, RemoveDuplicates
from .trimgalore import LocateFastqs, TrimAdapters


class PrintEnvVersions(FtarcTask):
    """Luigi task for printing environment and tool versions.

    This task displays version information for all tools used in the pipeline,
    helping with reproducibility and debugging.

    Parameters:
        command_paths: List of command executables to check versions.
        run_id: Identifier for this run (defaults to hostname).
        sh_config: Shell configuration parameters.
    """

    command_paths = luigi.ListParameter(default=[])
    run_id = luigi.Parameter(default=gethostname())
    sh_config = luigi.DictParameter(default={})
    __is_completed: bool = False

    def complete(self) -> bool:
        """Check if the task has been completed.

        Returns:
            True if the task has completed, False otherwise.
        """
        return self.__is_completed

    def run(self) -> None:
        """Execute version printing for all configured tools."""
        self.print_log(f"Print environment versions:\t{self.run_id}")
        self.setup_shell(
            run_id=self.run_id, commands=self.command_paths, **self.sh_config
        )
        self.print_env_versions()
        self.__is_completed = True


class PrepareFastqs(luigi.WrapperTask):
    """Luigi wrapper task for preparing FASTQ files for alignment.

    This task coordinates either adapter trimming or direct FASTQ location
    based on the adapter_removal parameter.

    Parameters:
        fq_paths: List of input FASTQ file paths.
        sample_name: Sample identifier.
        trim_dir_path: Directory for trimmed files.
        align_dir_path: Directory for alignment files.
        pigz: Path to the pigz executable.
        pbzip2: Path to the pbzip2 executable.
        trim_galore: Path to the trim_galore executable.
        cutadapt: Path to the cutadapt executable.
        fastqc: Path to the FastQC executable.
        adapter_removal: Whether to perform adapter trimming.
        n_cpu: Number of CPU threads to use.
        memory_mb: Memory allocation in MB.
        sh_config: Shell configuration parameters.
    """

    fq_paths = luigi.ListParameter()
    sample_name = luigi.Parameter()
    trim_dir_path = luigi.Parameter(default=".")
    align_dir_path = luigi.Parameter(default=".")
    pigz = luigi.Parameter(default="pigz")
    pbzip2 = luigi.Parameter(default="pbzip2")
    trim_galore = luigi.Parameter(default="trim_galore")
    cutadapt = luigi.Parameter(default="cutadapt")
    fastqc = luigi.Parameter(default="fastqc")
    adapter_removal = luigi.BoolParameter(default=True)
    n_cpu = luigi.IntParameter(default=1)
    memory_mb = luigi.FloatParameter(default=4096)
    sh_config = luigi.DictParameter(default={})
    priority: int = 50

    def requires(self) -> luigi.Task:
        """Determine prerequisite tasks based on adapter removal configuration.

        Returns:
            TrimAdapters task if adapter_removal is True, otherwise LocateFastqs task.
        """
        if self.adapter_removal:
            return TrimAdapters(
                fq_paths=self.fq_paths,
                dest_dir_path=str(Path(self.trim_dir_path).joinpath(self.sample_name)),
                sample_name=self.sample_name,
                pigz=self.pigz,
                pbzip2=self.pbzip2,
                trim_galore=self.trim_galore,
                cutadapt=self.cutadapt,
                fastqc=self.fastqc,
                n_cpu=self.n_cpu,
                memory_mb=self.memory_mb,
                sh_config=self.sh_config,
            )
        else:
            return LocateFastqs(
                fq_paths=self.fq_paths,
                dest_dir_path=str(Path(self.align_dir_path).joinpath(self.sample_name)),
                sample_name=self.sample_name,
                pigz=self.pigz,
                pbzip2=self.pbzip2,
                n_cpu=self.n_cpu,
                sh_config=self.sh_config,
            )

    def output(self) -> list[luigi.Target]:
        """Return the output targets for the FASTQ preparation task.

        The output is identical to the input, as this is a wrapper task that
        delegates actual processing to either trimming or locating subtasks.

        Returns:
            list[luigi.Target]: List of prepared FASTQ file targets.
        """
        return self.input()


@requires(PrepareFastqs, FetchReferenceFasta, FetchKnownSitesVcfs)
class PrepareAnalysisReadyCram(luigi.Task):
    """Luigi task for preparing analysis-ready CRAM files from FASTQ inputs.

    This task orchestrates the complete preprocessing workflow including alignment,
    duplicate marking, BQSR, and duplicate removal to produce analysis-ready CRAM files.

    Parameters:
        sample_name: Sample identifier.
        read_group: Read group dictionary for SAM header.
        align_dir_path: Directory for alignment outputs.
        bwa: Path to the BWA executable.
        samtools: Path to the samtools executable.
        gatk: Path to the GATK executable.
        reference_name: Reference genome identifier.
        adapter_removal: Whether adapter trimming was performed.
        use_bwa_mem2: Use BWA-MEM2 instead of standard BWA.
        use_spark: Use Spark-enabled GATK tools.
        n_cpu: Number of CPU threads to use.
        memory_mb: Memory allocation in MB.
        sh_config: Shell configuration parameters.
    """

    sample_name = luigi.Parameter()
    read_group = luigi.DictParameter()
    align_dir_path = luigi.Parameter(default=".")
    bwa = luigi.Parameter(default="bwa")
    samtools = luigi.Parameter(default="samtools")
    gatk = luigi.Parameter(default="gatk")
    reference_name = luigi.Parameter(default="")
    adapter_removal = luigi.BoolParameter(default=True)
    use_bwa_mem2 = luigi.BoolParameter(default=False)
    use_spark = luigi.BoolParameter(default=False)
    save_memory = luigi.BoolParameter(default=False)
    n_cpu = luigi.IntParameter(default=1)
    memory_mb = luigi.FloatParameter(default=4096)
    sh_config = luigi.DictParameter(default={})
    priority: int = 70

    def output(self) -> list[luigi.LocalTarget]:
        """Define output targets for analysis-ready CRAM preparation.

        Creates output paths for the final analysis-ready CRAM files including
        the main CRAM file, its index, validation report, and deduplicated version.

        Returns:
            list[luigi.LocalTarget]: List of output file targets including:
                - Main CRAM file with BQSR applied
                - CRAM index file (.crai)
                - SAM validation report
                - Deduplicated CRAM file
                - Deduplicated CRAM index
        """
        dest_dir = Path(self.align_dir_path).resolve().joinpath(self.sample_name)
        output_stem = (
            self.sample_name
            + (".trim." if self.adapter_removal else ".")
            + (self.reference_name or Path(self.input()[1][0].path).stem)
            + ".markdup.bqsr"
        )
        return [
            luigi.LocalTarget(dest_dir.joinpath(f"{output_stem}.{s}"))
            for s in [
                "cram",
                "cram.crai",
                "cram.ValidateSamFile.txt",
                "dedup.cram",
                "dedup.cram.crai",
            ]
        ]

    def run(self) -> Generator[luigi.Task, None, None]:
        """Execute the complete analysis-ready CRAM preparation pipeline.

        Orchestrates the sequential execution of genomic preprocessing steps:
        1. Read alignment using BWA or BWA-MEM2
        2. Duplicate marking with GATK MarkDuplicates
        3. Base Quality Score Recalibration (BQSR) with GATK
        4. Duplicate removal and SAM file validation

        Each step uses the output of the previous step as input, creating a
        complete preprocessing pipeline from raw aligned reads to analysis-ready
        CRAM files suitable for variant calling and other downstream analyses.

        Yields:
            AlignReads: Aligns FASTQ reads to reference genome
            MarkDuplicates: Marks duplicate reads in aligned data
            BaseRecalibration: Performs BQSR on marked reads
            RemoveDuplicates: Removes marked duplicates
            ValidateSamFile: Validates final CRAM file
        """
        fa_path = self.input()[1][0].path
        output_cram = Path(self.output()[0].path)
        dest_dir_path = str(output_cram.parent)
        align_target: AlignReads = yield AlignReads(
            fq_paths=[i.path for i in self.input()[0]],
            fa_path=fa_path,
            dest_dir_path=dest_dir_path,
            sample_name=self.sample_name,
            read_group=self.read_group,
            output_stem=Path(Path(output_cram.stem).stem).stem,
            bwa=self.bwa,
            samtools=self.samtools,
            use_bwa_mem2=self.use_bwa_mem2,
            n_cpu=self.n_cpu,
            memory_mb=self.memory_mb,
            sh_config=self.sh_config,
        )
        markdup_target: MarkDuplicates = yield MarkDuplicates(
            input_sam_path=align_target[0].path,
            fa_path=fa_path,
            dest_dir_path=dest_dir_path,
            gatk=self.gatk,
            samtools=self.samtools,
            use_spark=self.use_spark,
            n_cpu=self.n_cpu,
            memory_mb=self.memory_mb,
            sh_config=self.sh_config,
        )
        bqsr_target: ApplyBqsr = yield ApplyBqsr(
            input_sam_path=markdup_target[0].path,
            fa_path=fa_path,
            known_sites_vcf_paths=[i[0].path for i in self.input()[2]],
            dest_dir_path=dest_dir_path,
            gatk=self.gatk,
            samtools=self.samtools,
            use_spark=self.use_spark,
            save_memory=self.save_memory,
            n_cpu=self.n_cpu,
            memory_mb=self.memory_mb,
            sh_config=self.sh_config,
        )
        yield [
            RemoveDuplicates(
                input_sam_path=bqsr_target[0].path,
                fa_path=self.fa_path,
                dest_dir_path=dest_dir_path,
                samtools=self.samtools,
                n_cpu=self.n_cpu,
                sh_config=self.sh_config,
            ),
            ValidateSamFile(
                sam_path=bqsr_target[0].path,
                fa_path=self.fa_path,
                dest_dir_path=dest_dir_path,
                picard=self.gatk,
                n_cpu=self.n_cpu,
                memory_mb=self.memory_mb,
                sh_config=self.sh_config,
            ),
        ]


@requires(PrepareAnalysisReadyCram, FetchReferenceFasta, PrepareFastqs)
class RunPreprocessingPipeline(luigi.Task):
    """Luigi task for running the complete preprocessing and QC pipeline.

    This task coordinates the full workflow from FASTQ to analysis-ready CRAM,
    including all quality control metrics collection steps.

    Parameters:
        sample_name: Sample identifier.
        qc_dir_path: Directory for QC output files.
        fastqc: Path to the FastQC executable.
        gatk: Path to the GATK executable.
        samtools: Path to the samtools executable.
        plot_bamstats: Path to the plot-bamstats executable.
        gnuplot: Path to the gnuplot executable.
        metrics_collectors: List of QC tools to run.
        picard_qc_commands: List of Picard QC commands.
        samtools_qc_commands: List of samtools QC commands.
        n_cpu: Number of CPU threads to use.
        memory_mb: Memory allocation in MB.
        sh_config: Shell configuration parameters.
    """

    sample_name = luigi.Parameter()
    qc_dir_path = luigi.Parameter(default=".")
    fastqc = luigi.Parameter(default="fastqc")
    gatk = luigi.Parameter(default="gatk")
    samtools = luigi.Parameter(default="samtools")
    plot_bamstats = luigi.Parameter(default="plot_bamstats")
    gnuplot = luigi.Parameter(default="gnuplot")
    metrics_collectors = luigi.ListParameter(default=["fastqc", "picard", "samtools"])
    picard_qc_commands = luigi.ListParameter(
        default=[
            "CollectRawWgsMetrics",
            "CollectAlignmentSummaryMetrics",
            "CollectInsertSizeMetrics",
            "QualityScoreDistribution",
            "MeanQualityByCycle",
            "CollectBaseDistributionByCycle",
            "CollectGcBiasMetrics",
        ]
    )
    samtools_qc_commands = luigi.ListParameter(
        default=["coverage", "flagstat", "idxstats", "stats"]
    )
    n_cpu = luigi.IntParameter(default=1)
    memory_mb = luigi.FloatParameter(default=4096)
    sh_config = luigi.DictParameter(default={})
    priority = luigi.IntParameter(default=sys.maxsize)

    def output(self) -> list[luigi.LocalTarget]:
        """Define output targets for the complete preprocessing pipeline.

        Creates output paths for all pipeline products including the analysis-ready
        CRAM files and all quality control metrics from the specified collectors.

        Returns:
            list[luigi.LocalTarget]: Comprehensive list of output targets including:
                - Analysis-ready CRAM files and indices from PrepareAnalysisReadyCram
                - FastQC HTML reports (if fastqc in metrics_collectors)
                - Picard metrics files (if picard in metrics_collectors)
                - Samtools statistics files (if samtools in metrics_collectors)
        """
        cram = Path(self.input()[0][0].path)
        qc_dir = Path(self.qc_dir_path)
        return (
            self.input()[0]
            + [
                luigi.LocalTarget(
                    qc_dir.joinpath("fastqc")
                    .joinpath(self.sample_name)
                    .joinpath(re.sub(r"\.(fq|fastq)$", "", s) + "_fastqc.html")
                )
                for s in (
                    [Path(i.path).stem for i in self.input()[2]]
                    if "fastqc" in self.metrics_collectors
                    else []
                )
            ]
            + [
                luigi.LocalTarget(
                    qc_dir.joinpath("picard")
                    .joinpath(self.sample_name)
                    .joinpath(f"{cram.stem}.{c}.txt")
                )
                for c in (
                    self.picard_qc_commands
                    if "picard" in self.metrics_collectors
                    else []
                )
            ]
            + [
                luigi.LocalTarget(
                    qc_dir.joinpath("samtools")
                    .joinpath(self.sample_name)
                    .joinpath(f"{cram.stem}.{c}.txt")
                )
                for c in (
                    self.samtools_qc_commands
                    if "samtools" in self.metrics_collectors
                    else []
                )
            ]
        )

    def run(self) -> Generator[list[luigi.Task], None, None]:
        """Execute the complete preprocessing pipeline with quality control.

        Coordinates the execution of quality control tasks based on the configured
        metrics collectors. Runs FastQC on FASTQ files and Picard/samtools metrics
        on the final CRAM files to provide comprehensive quality assessment.

        The quality control includes:
        - FastQC analysis of input FASTQ files (read quality, adapter content, etc.)
        - Picard metrics collection (alignment summary, insert size, GC bias, etc.)
        - Samtools statistics (coverage, flagstat, idxstats)

        Yields:
            CollectFqMetricsWithFastqc: Collects FASTQ quality metrics
            CollectSamMetricsWithPicard: Collects alignment metrics with Picard
            CollectSamMetricsWithSamtools: Collects alignment metrics with samtools
        """
        qc_dir = Path(self.qc_dir_path)
        if "fastqc" in self.metrics_collectors:
            yield CollectFqMetricsWithFastqc(
                fq_paths=[i.path for i in self.input()[2]],
                dest_dir_path=str(qc_dir.joinpath("fastqc").joinpath(self.sample_name)),
                fastqc=self.fastqc,
                n_cpu=self.n_cpu,
                memory_mb=self.memory_mb,
                sh_config=self.sh_config,
            )
        if {"picard", "samtools"} & set(self.metrics_collectors):
            yield [
                CollectMultipleSamMetrics(
                    sam_path=self.input()[0][0].path,
                    fa_path=self.input()[1][0].path,
                    dest_dir_path=str(qc_dir.joinpath(m).joinpath(self.sample_name)),
                    metrics_collectors=[m],
                    picard_qc_commands=self.picard_qc_commands,
                    samtools_qc_commands=self.samtools_qc_commands,
                    picard=self.gatk,
                    samtools=self.samtools,
                    plot_bamstats=self.plot_bamstats,
                    gnuplot=self.gnuplot,
                    n_cpu=self.n_cpu,
                    memory_mb=self.memory_mb,
                    sh_config=self.sh_config,
                )
                for m in self.metrics_collectors
            ]


class CollectMultipleSamMetrics(luigi.WrapperTask):
    """Luigi wrapper task for collecting multiple SAM/BAM/CRAM metrics.

    This task coordinates running various metrics collection tools (Picard, samtools)
    on aligned sequencing data.

    Parameters:
        sam_path: Path to the SAM/BAM/CRAM file to analyze.
        fa_path: Path to the reference FASTA file.
        dest_dir_path: Output directory for metrics files.
        metrics_collectors: List of metrics tools to run.
        picard_qc_commands: List of Picard QC commands.
        samtools_qc_commands: List of samtools QC commands.
        picard: Path to the Picard executable.
        samtools: Path to the samtools executable.
        plot_bamstats: Path to the plot-bamstats executable.
        gnuplot: Path to the gnuplot executable.
        n_cpu: Number of CPU threads to use.
        memory_mb: Memory allocation in MB.
        sh_config: Shell configuration parameters.
    """

    sam_path = luigi.Parameter()
    fa_path = luigi.Parameter()
    dest_dir_path = luigi.Parameter(default=".")
    metrics_collectors = luigi.ListParameter(default=["picard", "samtools"])
    picard_qc_commands = luigi.ListParameter(
        default=[
            "CollectRawWgsMetrics",
            "CollectAlignmentSummaryMetrics",
            "CollectInsertSizeMetrics",
            "QualityScoreDistribution",
            "MeanQualityByCycle",
            "CollectBaseDistributionByCycle",
            "CollectGcBiasMetrics",
        ]
    )
    samtools_qc_commands = luigi.ListParameter(
        default=["coverage", "flagstat", "idxstats", "stats"]
    )
    picard = luigi.Parameter(default="picard")
    samtools = luigi.Parameter(default="samtools")
    plot_bamstats = luigi.Parameter(default="plot-bamstats")
    gnuplot = luigi.Parameter(default="gnuplot")
    n_cpu = luigi.IntParameter(default=1)
    memory_mb = luigi.FloatParameter(default=4096)
    sh_config = luigi.DictParameter(default={})
    priority: int = 10

    def requires(self) -> list[luigi.Task]:
        """Define prerequisite tasks for multiple SAM metrics collection.

        Creates individual collection tasks for each requested Picard and samtools
        command based on the metrics_collectors configuration. This allows for
        fine-grained control over which QC metrics are generated.

        Returns:
            list[luigi.Task]: List of metrics collection tasks including:
                - Individual Picard tasks for each command in picard_qc_commands
                - Individual samtools tasks for each command in samtools_qc_commands
                Only tasks for collectors specified in metrics_collectors are included.
        """
        return [
            CollectSamMetricsWithPicard(
                sam_path=self.sam_path,
                fa_path=self.fa_path,
                dest_dir_path=self.dest_dir_path,
                picard_commands=[c],
                picard=self.picard,
                n_cpu=self.n_cpu,
                memory_mb=self.memory_mb,
                sh_config=self.sh_config,
            )
            for c in (
                self.picard_qc_commands if "picard" in self.metrics_collectors else []
            )
        ] + [
            CollectSamMetricsWithSamtools(
                sam_path=self.sam_path,
                fa_path=self.fa_path,
                dest_dir_path=self.dest_dir_path,
                samtools_commands=[c],
                samtools=self.samtools,
                plot_bamstats=self.plot_bamstats,
                gnuplot=self.gnuplot,
                n_cpu=self.n_cpu,
                sh_config=self.sh_config,
            )
            for c in (
                self.samtools_qc_commands
                if "samtools" in self.metrics_collectors
                else []
            )
        ]

    def output(self) -> list[luigi.Target]:
        """Return the output targets for the multiple metrics collection task.

        The output is identical to the input from prerequisite tasks, as this is
        a wrapper task that delegates actual processing to individual metrics
        collection subtasks.

        Returns:
            list[luigi.Target]: List of metrics file targets from all subtasks.
        """
        return self.input()


if __name__ == "__main__":
    luigi.run()
