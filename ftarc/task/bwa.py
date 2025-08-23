#!/usr/bin/env python
"""BWA alignment tasks for the ftarc pipeline.

This module provides Luigi tasks for BWA and BWA-MEM2 read alignment operations,
including index creation and read mapping to reference genomes.
"""

from pathlib import Path

import luigi
from luigi.util import requires

from .core import FtarcTask
from .samtools import SamtoolsFaidx


class CreateBwaIndices(FtarcTask):
    """Luigi task for creating BWA or BWA-MEM2 indices for reference genomes.

    This task generates the necessary index files required for BWA alignment,
    supporting both original BWA and the faster BWA-MEM2 variant.

    Parameters:
        fa_path: Path to the reference FASTA file.
        bwa: Path to the BWA executable.
        use_bwa_mem2: Use BWA-MEM2 instead of standard BWA.
        add_index_args: Additional arguments for the index command.
        sh_config: Shell configuration parameters.
    """

    fa_path = luigi.Parameter()
    bwa = luigi.Parameter(default="bwa")
    use_bwa_mem2 = luigi.BoolParameter(default=False)
    add_index_args = luigi.ListParameter(default=[])
    sh_config = luigi.DictParameter(default={})
    priority = 100

    def output(self) -> list[luigi.LocalTarget]:
        """Define the output targets for BWA index files.

        Creates Luigi targets for all BWA index files that will be generated.
        The specific index files depend on whether BWA-MEM2 is used:
        - BWA-MEM2: .0123, .amb, .ann, .pac, .bwt.2bit.64
        - Standard BWA: .pac, .bwt, .ann, .amb, .sa

        Returns:
            list[luigi.LocalTarget]: List of Luigi targets for each index file.
        """
        return [
            luigi.LocalTarget(f"{self.fa_path}.{s}")
            for s in (
                ["0123", "amb", "ann", "pac", "bwt.2bit.64"]
                if self.use_bwa_mem2
                else ["pac", "bwt", "ann", "amb", "sa"]
            )
        ]

    def run(self) -> None:
        """Execute the BWA index creation task.

        Generates BWA or BWA-MEM2 index files for the reference genome FASTA file.
        The indexing process creates multiple auxiliary files that enable efficient
        alignment of sequencing reads to the reference.

        The task:
        1. Sets up the shell environment with BWA executable
        2. Runs the BWA index command with optional additional arguments
        3. Creates all required index files in the same directory as the FASTA

        Raises:
            subprocess.CalledProcessError: If BWA indexing command fails.
            FileNotFoundError: If the reference FASTA file or BWA executable is not found.
        """
        fa = Path(self.fa_path)
        run_id = fa.stem
        self.print_log(f"Create BWA indices:\t{run_id}")
        self.setup_shell(
            run_id=run_id, commands=self.bwa, cwd=fa.parent, **self.sh_config
        )
        self.run_shell(
            args=(
                f"set -e && {self.bwa} index"
                + "".join(f" {a}" for a in self.add_index_args)
                + f" {fa}"
            ),
            input_files_or_dirs=fa,
            output_files_or_dirs=[o.path for o in self.output()],
        )


@requires(SamtoolsFaidx, CreateBwaIndices)
class AlignReads(FtarcTask):
    """Luigi task for aligning sequencing reads to a reference genome using BWA.

    This task performs read alignment using BWA MEM algorithm, generating sorted
    and indexed CRAM files with proper read group information.

    Parameters:
        fq_paths: List of FASTQ file paths to align.
        fa_path: Path to the reference FASTA file.
        dest_dir_path: Output directory for aligned files.
        sample_name: Sample identifier for read group.
        read_group: Read group dictionary for SAM header.
        output_stem: Custom output filename stem.
        bwa: Path to the BWA executable.
        samtools: Path to the samtools executable.
        use_bwa_mem2: Use BWA-MEM2 instead of standard BWA.
        add_mem_args: Additional arguments for BWA MEM.
        n_cpu: Number of CPU threads to use.
        memory_mb: Memory allocation in MB.
        sh_config: Shell configuration parameters.
    """

    fq_paths = luigi.ListParameter()
    fa_path = luigi.Parameter()
    dest_dir_path = luigi.Parameter(default=".")
    sample_name = luigi.Parameter()
    read_group = luigi.DictParameter(default={})
    output_stem = luigi.Parameter(default="")
    bwa = luigi.Parameter(default="bwa")
    samtools = luigi.Parameter(default="samtools")
    use_bwa_mem2 = luigi.BoolParameter(default=False)
    add_mem_args = luigi.ListParameter(default=["-P", "-T", "0"])
    n_cpu = luigi.IntParameter(default=1)
    memory_mb = luigi.FloatParameter(default=4096)
    sh_config = luigi.DictParameter(default={})
    priority = 70

    def output(self) -> list[luigi.LocalTarget]:
        """Define the output targets for aligned reads.

        Creates Luigi targets for the CRAM file and its associated index file.
        The CRAM filename is constructed from either the provided output_stem
        or a combination of sample_name and reference genome stem.

        Returns:
            list[luigi.LocalTarget]: List containing:
                - CRAM file target (compressed alignment format)
                - CRAI index file target (CRAM index for random access)
        """
        dest_dir = Path(self.dest_dir_path).resolve()
        output_cram_name = "{}.cram".format(
            self.output_stem or (self.sample_name + "." + Path(self.fa_path).stem)
        )
        return [
            luigi.LocalTarget(dest_dir.joinpath(output_cram_name)),
            luigi.LocalTarget(dest_dir.joinpath(output_cram_name + ".crai")),
        ]

    def run(self) -> None:
        """Execute the BWA read alignment task.

        Performs the complete read alignment workflow:
        1. Constructs read group information for SAM header
        2. Runs BWA MEM to align FASTQ reads to reference genome
        3. Pipes output through samtools for CRAM conversion and sorting
        4. Creates CRAM index for random access

        The alignment pipeline uses BWA MEM algorithm for fast and accurate
        alignment of sequencing reads, with output in compressed CRAM format
        to save storage space while maintaining full information.

        Memory is automatically allocated per thread for efficient sorting,
        and a reference cache directory is set up to improve CRAM compression.

        Raises:
            subprocess.CalledProcessError: If BWA alignment or samtools processing fails.
            FileNotFoundError: If FASTQ files, reference FASTA, or executables are not found.
            ValueError: If read group parameters are invalid or memory allocation fails.
        """
        output_cram = Path(self.output()[0].path)
        run_id = output_cram.stem
        self.print_log(f"Align reads:\t{run_id}")
        memory_mb_per_thread = int(self.memory_mb / self.n_cpu / 8)
        fqs = [Path(p).resolve() for p in self.fq_paths]
        rg = "\\t".join(
            [
                "@RG",
                "ID:{}".format(self.read_group.get("ID") or 0),
                "PU:{}".format(self.read_group.get("PU") or "UNIT-0"),
                "SM:{}".format(
                    self.read_group.get("SM") or self.sample_name or "SAMPLE"
                ),
                "PL:{}".format(self.read_group.get("PL") or "ILLUMINA"),
                "LB:{}".format(self.read_group.get("LB") or "LIBRARY-0"),
            ]
            + [
                f"{k}:{v}"
                for k, v in self.read_group.items()
                if k not in {"ID", "PU", "SM", "PL", "LB"}
            ]
        )
        fa = Path(self.fa_path).resolve()
        bwa_indices = [
            Path(f"{fa}.{s}")
            for s in (
                ["0123", "amb", "ann", "pac", "bwt.2bit.64"]
                if self.use_bwa_mem2
                else ["pac", "bwt", "ann", "amb", "sa"]
            )
        ]
        dest_dir = output_cram.parent
        self.setup_shell(
            run_id=run_id,
            commands=[self.bwa, self.samtools],
            cwd=dest_dir,
            **self.sh_config,
            env={"REF_CACHE": str(dest_dir.joinpath(".ref_cache"))},
        )
        self.run_shell(
            args=(
                f"set -eo pipefail && {self.bwa} mem"
                f" -t {self.n_cpu}"
                + "".join(f" {a}" for a in self.add_mem_args)
                + f" -R '{rg}'"
                + f" {fa}"
                + "".join(f" {a}" for a in fqs)
                + f" | {self.samtools} view -T {fa} -CS -o - -"
                + f" | {self.samtools} sort -@ {self.n_cpu}"
                + f" -m {memory_mb_per_thread}M -O CRAM"
                + f" -T {output_cram}.sort -o {output_cram} -"
            ),
            input_files_or_dirs=[*fqs, fa, *bwa_indices],
            output_files_or_dirs=[output_cram, dest_dir],
        )
        self.samtools_index(
            sam_path=output_cram, samtools=self.samtools, n_cpu=self.n_cpu
        )


if __name__ == "__main__":
    luigi.run()
