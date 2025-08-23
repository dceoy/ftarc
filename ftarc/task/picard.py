"""Picard tools tasks for the ftarc pipeline.

This module provides Luigi tasks for Picard tools operations including sequence dictionary
creation, SAM file validation, and various quality metrics collection.
"""

from pathlib import Path

import luigi
from luigi.util import requires

from .core import FtarcTask


class CreateSequenceDictionary(FtarcTask):
    """Luigi task for creating a sequence dictionary for reference FASTA files.

    This task generates a .dict file containing sequence information required by
    GATK and other tools for efficient reference sequence access.

    Parameters:
        fa_path: Path to the reference FASTA file.
        gatk: Path to the GATK executable.
        add_createsequencedictionary_args: Additional arguments for CreateSequenceDictionary.
        n_cpu: Number of CPU threads to use.
        memory_mb: Memory allocation in MB.
        sh_config: Shell configuration parameters.
    """

    fa_path = luigi.Parameter()
    gatk = luigi.Parameter(default="gatk")
    add_createsequencedictionary_args = luigi.ListParameter(default=[])
    n_cpu = luigi.IntParameter(default=1)
    memory_mb = luigi.FloatParameter(default=4096)
    sh_config = luigi.DictParameter(default={})
    priority = 70

    def output(self) -> luigi.LocalTarget:
        """Return the output target for the sequence dictionary.

        Returns:
            luigi.LocalTarget: Path to the generated sequence dictionary file (.dict).
        """
        fa = Path(self.fa_path).resolve()
        return luigi.LocalTarget(fa.parent.joinpath(f"{fa.stem}.dict"))

    def run(self) -> None:
        """Execute sequence dictionary creation for a reference FASTA file.

        This method creates a sequence dictionary (.dict) file that contains metadata
        about the reference sequences, including sequence names, lengths, MD5 checksums,
        and other information required by GATK and other genomics tools.

        Raises:
            subprocess.CalledProcessError: If GATK CreateSequenceDictionary execution fails.
            FileNotFoundError: If the reference FASTA file is not found.
        """
        run_id = Path(self.fa_path).stem
        self.print_log(f"Create a sequence dictionary:\t{run_id}")
        fa = Path(self.fa_path).resolve()
        seq_dict_path = self.output().path
        self.setup_shell(
            run_id=run_id,
            commands=self.gatk,
            cwd=fa.parent,
            **self.sh_config,
            env={
                "JAVA_TOOL_OPTIONS": self.generate_gatk_java_options(
                    n_cpu=self.n_cpu, memory_mb=self.memory_mb
                )
            },
        )
        self.run_shell(
            args=(
                f"set -e && {self.gatk} CreateSequenceDictionary"
                f" --REFERENCE {fa}"
                + "".join(f" {a}" for a in self.add_createsequencedictionary_args)
                + f" --OUTPUT {seq_dict_path}"
            ),
            input_files_or_dirs=fa,
            output_files_or_dirs=seq_dict_path,
        )


@requires(CreateSequenceDictionary)
class ValidateSamFile(FtarcTask):
    """Luigi task for validating SAM/BAM/CRAM file integrity and format.

    This task uses Picard's ValidateSamFile to check for format compliance,
    data consistency, and potential issues in alignment files.

    Parameters:
        sam_path: Path to the SAM/BAM/CRAM file to validate.
        fa_path: Path to the reference FASTA file.
        dest_dir_path: Output directory for validation report.
        picard: Path to the Picard executable.
        add_validatesamfile_args: Additional arguments for ValidateSamFile.
        n_cpu: Number of CPU threads to use.
        memory_mb: Memory allocation in MB.
        sh_config: Shell configuration parameters.
    """

    sam_path = luigi.Parameter()
    fa_path = luigi.Parameter()
    dest_dir_path = luigi.Parameter(default=".")
    picard = luigi.Parameter(default="picard")
    add_validatesamfile_args = luigi.ListParameter(
        default=["--MODE", "VERBOSE", "--IGNORE", "MISSING_TAG_NM"]
    )
    n_cpu = luigi.IntParameter(default=1)
    memory_mb = luigi.FloatParameter(default=4096)
    sh_config = luigi.DictParameter(default={})
    priority = luigi.IntParameter(default=100)

    def output(self) -> luigi.LocalTarget:
        """Return the output target for the SAM validation report.

        Returns:
            luigi.LocalTarget: Path to the validation report text file.
        """
        return luigi.LocalTarget(
            Path(self.dest_dir_path)
            .resolve()
            .joinpath(Path(self.sam_path).name + ".ValidateSamFile.txt")
        )

    def run(self) -> None:
        """Execute SAM/BAM/CRAM file validation using Picard ValidateSamFile.

        This method performs comprehensive validation of alignment files, checking
        for format compliance, data consistency, proper sorting, and potential issues.
        The validation report includes errors, warnings, and summary statistics.

        Common validation checks include:
        - Format specification compliance
        - Coordinate sorting verification
        - Reference sequence consistency
        - Read group validation
        - Tag format verification

        Raises:
            subprocess.CalledProcessError: If Picard ValidateSamFile execution fails.
            FileNotFoundError: If input files or reference files are not found.
        """
        run_id = Path(self.sam_path).name
        self.print_log(f"Validate a SAM file:\t{run_id}")
        sam = Path(self.sam_path).resolve()
        fa = Path(self.fa_path).resolve()
        fa_dict = fa.parent.joinpath(f"{fa.stem}.dict")
        dest_dir = Path(self.dest_dir_path).resolve()
        output_txt = Path(self.output().path)
        self.setup_shell(
            run_id=run_id,
            commands=self.picard,
            cwd=dest_dir,
            **self.sh_config,
            env={
                "JAVA_TOOL_OPTIONS": self.generate_gatk_java_options(
                    n_cpu=self.n_cpu, memory_mb=self.memory_mb
                )
            },
        )
        self.run_shell(
            args=(
                f"set -e && {self.picard} ValidateSamFile"
                f" --INPUT {sam}"
                f" --REFERENCE_SEQUENCE {fa}"
                + "".join(f" {a}" for a in self.add_validatesamfile_args)
                + f" --OUTPUT {output_txt}"
            ),
            input_files_or_dirs=[sam, fa, fa_dict],
            output_files_or_dirs=output_txt,
        )


@requires(CreateSequenceDictionary)
class CollectSamMetricsWithPicard(FtarcTask):
    """Luigi task for collecting comprehensive alignment metrics using Picard tools.

    This task runs multiple Picard metrics collectors to generate detailed quality
    metrics including alignment summary, insert size distribution, GC bias, and
    quality score distributions.

    Parameters:
        sam_path: Path to the SAM/BAM/CRAM file to analyze.
        fa_path: Path to the reference FASTA file.
        dest_dir_path: Output directory for metrics files.
        picard_commands: List of Picard metrics commands to run.
        picard: Path to the Picard executable.
        add_picard_command_args: Additional arguments per command.
        n_cpu: Number of CPU threads to use.
        memory_mb: Memory allocation in MB.
        sh_config: Shell configuration parameters.
    """

    sam_path = luigi.Parameter()
    fa_path = luigi.Parameter()
    dest_dir_path = luigi.Parameter(default=".")
    picard_commands = luigi.ListParameter(
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
    picard = luigi.Parameter(default="picard")
    add_picard_command_args = luigi.DictParameter(
        default={"CollectRawWgsMetrics": ["--INCLUDE_BQ_HISTOGRAM", "true"]}
    )
    n_cpu = luigi.IntParameter(default=1)
    memory_mb = luigi.FloatParameter(default=4096)
    sh_config = luigi.DictParameter(default={})
    priority = 100

    def output(self) -> list[luigi.LocalTarget]:
        """Return the output targets for all Picard metrics collection.

        Returns:
            list[luigi.LocalTarget]: List of output file targets including:
                - Text files with metrics data for each command
                - PDF chart files for visualization (when applicable)
                - Summary files for specific metrics (e.g., GC bias)
        """
        sam_name = Path(self.sam_path).name
        dest_dir = Path(self.dest_dir_path).resolve()
        return (
            [
                luigi.LocalTarget(dest_dir.joinpath(f"{sam_name}.{c}.txt"))
                for c in self.picard_commands
            ]
            + [
                luigi.LocalTarget(dest_dir.joinpath(f"{sam_name}.{c}.summary.txt"))
                for c in self.picard_commands
                if c == "CollectGcBiasMetrics"
            ]
            + [
                luigi.LocalTarget(dest_dir.joinpath(f"{sam_name}.{c}.pdf"))
                for c in self.picard_commands
                if c
                in {
                    "MeanQualityByCycle",
                    "QualityScoreDistribution",
                    "CollectBaseDistributionByCycle",
                    "CollectInsertSizeMetrics",
                }
            ]
        )

    def run(self) -> None:
        """Execute comprehensive alignment metrics collection using Picard tools.

        This method runs multiple Picard metrics collectors to generate detailed
        quality control reports including:
        - CollectRawWgsMetrics: Whole genome sequencing metrics
        - CollectAlignmentSummaryMetrics: Alignment statistics
        - CollectInsertSizeMetrics: Insert size distribution
        - QualityScoreDistribution: Base quality score distribution
        - MeanQualityByCycle: Quality scores by sequencing cycle
        - CollectBaseDistributionByCycle: Base composition by cycle
        - CollectGcBiasMetrics: GC bias assessment

        Each command generates text-based metrics files and, when applicable,
        PDF visualization charts.

        Raises:
            subprocess.CalledProcessError: If any Picard command execution fails.
            FileNotFoundError: If input files or reference files are not found.
        """
        run_id = Path(self.sam_path).name
        self.print_log(f"Collect SAM metrics using Picard:\t{run_id}")
        sam = Path(self.sam_path).resolve()
        fa = Path(self.fa_path).resolve()
        fa_dict = fa.parent.joinpath(f"{fa.stem}.dict")
        for c, o in zip(self.picard_commands, self.output(), strict=False):
            output_txt = Path(o.path)
            self.setup_shell(
                run_id=f"{run_id}.{c}",
                commands=f"{self.picard} {c}",
                cwd=output_txt.parent,
                **self.sh_config,
                env={
                    "JAVA_TOOL_OPTIONS": self.generate_gatk_java_options(
                        n_cpu=self.n_cpu, memory_mb=self.memory_mb
                    )
                },
            )
            prefix = str(output_txt.parent.joinpath(output_txt.stem))
            if c in {
                "MeanQualityByCycle",
                "QualityScoreDistribution",
                "CollectBaseDistributionByCycle",
            }:
                output_args = [
                    "--CHART_OUTPUT",
                    f"{prefix}.pdf",
                    "--OUTPUT",
                    f"{prefix}.txt",
                ]
            elif c == "CollectInsertSizeMetrics":
                output_args = [
                    "--Histogram_FILE",
                    f"{prefix}.pdf",
                    "--OUTPUT",
                    f"{prefix}.txt",
                ]
            elif c == "CollectGcBiasMetrics":
                output_args = [
                    "--CHART_OUTPUT",
                    f"{prefix}.pdf",
                    "--SUMMARY_OUTPUT",
                    f"{prefix}.summary.txt",
                    "--OUTPUT",
                    f"{prefix}.txt",
                ]
            else:
                output_args = ["--OUTPUT", f"{prefix}.txt"]
            self.run_shell(
                args=(
                    f"set -e && {self.picard} {c}"
                    f" --INPUT {sam}"
                    f" --REFERENCE_SEQUENCE {fa}"
                    + "".join(
                        f" {a}"
                        for a in [
                            *(self.add_picard_command_args.get(c) or []),
                            *output_args,
                        ]
                    )
                ),
                input_files_or_dirs=[sam, fa, fa_dict],
                output_files_or_dirs=[
                    a for a in output_args if a.endswith((".txt", ".pdf"))
                ],
            )


if __name__ == "__main__":
    luigi.run()
