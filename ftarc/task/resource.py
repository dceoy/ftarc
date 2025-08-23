"""Resource management utilities for the ftarc pipeline.

This module provides Luigi tasks for managing genomic reference resources,
including creating necessary indices and preparing known variant sites for BQSR.
"""

import re
from pathlib import Path

import luigi

from .core import FtarcTask
from .picard import CreateSequenceDictionary
from .samtools import SamtoolsFaidx


class FetchReferenceFasta(luigi.Task):
    """Luigi task for fetching and indexing reference FASTA files.

    This task prepares reference genome files by decompressing if needed and
    creating both samtools and GATK indices for efficient access.

    Parameters:
        fa_path: Path to the reference FASTA file.
        pigz: Path to the pigz executable.
        pbzip2: Path to the pbzip2 executable.
        samtools: Path to the samtools executable.
        gatk: Path to the GATK executable.
        n_cpu: Number of CPU threads to use.
        memory_mb: Memory allocation in MB.
        sh_config: Shell configuration parameters.
    """

    fa_path = luigi.Parameter()
    pigz = luigi.Parameter(default="pigz")
    pbzip2 = luigi.Parameter(default="pbzip2")
    samtools = luigi.Parameter(default="samtools")
    gatk = luigi.Parameter(default="gatk")
    n_cpu = luigi.IntParameter(default=1)
    memory_mb = luigi.FloatParameter(default=4096)
    sh_config = luigi.DictParameter(default={})
    priority = 100

    def requires(self) -> luigi.Task:
        """Return the dependency task for fetching the resource file.

        Returns:
            luigi.Task: FetchResourceFile task to prepare the FASTA file.
        """
        return FetchResourceFile(
            src_path=self.fa_path,
            pigz=self.pigz,
            pbzip2=self.pbzip2,
            n_cpu=self.n_cpu,
            sh_config=self.sh_config,
        )

    def output(self) -> list[luigi.LocalTarget]:
        """Return the output targets for the reference FASTA file and indices.

        Returns:
            list[luigi.LocalTarget]: List of output file targets including:
                - Reference FASTA file
                - Samtools FASTA index (.fai)
                - GATK sequence dictionary (.dict)
        """
        fa = Path(self.input().path)
        return [
            luigi.LocalTarget(fa),
            luigi.LocalTarget(f"{fa}.fai"),
            luigi.LocalTarget(fa.parent.joinpath(f"{fa.stem}.dict")),
        ]

    def run(self) -> None:
        """Execute reference FASTA file preparation with indexing.

        This method yields tasks to create both samtools and GATK indices for the
        reference FASTA file, enabling efficient random access for genomics tools.

        Yields:
            list[luigi.Task]: List of indexing tasks (SamtoolsFaidx,
                CreateSequenceDictionary).
        """
        fa_path = self.input().path
        yield [
            SamtoolsFaidx(
                fa_path=fa_path, samtools=self.samtools, sh_config=self.sh_config
            ),
            CreateSequenceDictionary(
                fa_path=fa_path,
                gatk=self.gatk,
                n_cpu=self.n_cpu,
                memory_mb=self.memory_mb,
                sh_config=self.sh_config,
            ),
        ]


class FetchResourceFile(FtarcTask):
    """Luigi task for fetching and decompressing resource files.

    This task handles downloading or copying resource files and decompressing
    them if they are in gzip or bzip2 format.

    Parameters:
        src_path: Path to the source resource file.
        pigz: Path to the pigz executable.
        pbzip2: Path to the pbzip2 executable.
        n_cpu: Number of CPU threads to use.
        sh_config: Shell configuration parameters.
    """

    src_path = luigi.Parameter()
    pigz = luigi.Parameter(default="pigz")
    pbzip2 = luigi.Parameter(default="pbzip2")
    n_cpu = luigi.IntParameter(default=1)
    sh_config = luigi.DictParameter(default={})
    priority = 70

    def output(self) -> luigi.LocalTarget:
        """Return the output target for the decompressed resource file.

        Returns:
            luigi.LocalTarget: Path to the decompressed resource file.

        Note:
            The output filename is derived by removing .gz or .bz2 extensions.
        """
        return luigi.LocalTarget(
            Path(self.src_path).parent.joinpath(
                re.sub(r"\.(gz|bz2)$", "", Path(self.src_path).name)
            )
        )

    def run(self) -> None:
        """Execute resource file fetching and decompression.

        This method handles resource file preparation by:
        1. Decompressing gzipped files using pigz
        2. Decompressing bzip2 files using pbzip2
        3. Copying uncompressed files directly

        The method automatically detects compression format based on file extension
        and uses parallel decompression tools for better performance.
        """
        dest_file = Path(self.output().path)
        run_id = dest_file.stem
        self.print_log(f"Create a resource:\t{run_id}")
        self.setup_shell(
            run_id=run_id,
            commands=[self.pigz, self.pbzip2],
            cwd=dest_file.parent,
            **self.sh_config,
        )
        if self.src_path.endswith(".gz"):
            a = (
                f"set -e && {self.pigz} -p {self.n_cpu} -dc {self.src_path}"
                f" > {dest_file}"
            )
        elif self.src_path.endswith(".bz2"):
            a = (
                f"set -e && {self.pbzip2} -p{self.n_cpu} -dc {self.src_path}"
                f" > {dest_file}"
            )
        else:
            a = f"set -e && cp {self.src_path} {dest_file}"
        self.run_shell(
            args=a, input_files_or_dirs=self.src_path, output_files_or_dirs=dest_file
        )


class FetchResourceVcf(FtarcTask):
    """Luigi task for fetching and indexing VCF files.

    This task prepares VCF files for use in variant calling and BQSR by
    ensuring proper bgzip compression and creating tabix indices.

    Parameters:
        src_path: Path to the source VCF file.
        bgzip: Path to the bgzip executable.
        tabix: Path to the tabix executable.
        n_cpu: Number of CPU threads to use.
        sh_config: Shell configuration parameters.
    """

    src_path = luigi.Parameter()
    bgzip = luigi.Parameter(default="bgzip")
    tabix = luigi.Parameter(default="tabix")
    n_cpu = luigi.IntParameter(default=1)
    sh_config = luigi.DictParameter(default={})
    priority = 70

    def output(self) -> list[luigi.LocalTarget]:
        """Return the output targets for the VCF file and index.

        Returns:
            list[luigi.LocalTarget]: List of output file targets including:
                - BGzip-compressed VCF file (.gz)
                - Tabix index file (.tbi)
        """
        dest_vcf = Path(self.src_path).parent.joinpath(
            re.sub(r"\.(gz|bgz)$", ".gz", Path(self.src_path).name)
        )
        return [luigi.LocalTarget(f"{dest_vcf}{s}") for s in ["", ".tbi"]]

    def run(self) -> None:
        """Execute VCF file preparation with bgzip compression and tabix indexing.

        This method prepares VCF files for use in genomics workflows by:
        1. Ensuring proper bgzip compression for efficient random access
        2. Creating tabix indices for fast querying by genomic coordinates

        The method handles both compressed and uncompressed input VCF files,
        applying bgzip compression when needed and always creating tabix indices.
        """
        dest_vcf = Path(self.output()[0].path)
        run_id = Path(dest_vcf.stem).stem
        self.print_log(f"Create a VCF:\t{run_id}")
        self.setup_shell(
            run_id=run_id,
            commands=[self.bgzip, self.tabix],
            cwd=dest_vcf.parent,
            **self.sh_config,
        )
        self.run_shell(
            args=(
                f"set -e && cp {self.src_path} {dest_vcf}"
                if self.src_path.endswith((".gz", ".bgz"))
                else (
                    f"set -e && {self.bgzip} -@ {self.n_cpu}"
                    f" -c {self.src_path} > {dest_vcf}"
                )
            ),
            input_files_or_dirs=self.src_path,
            output_files_or_dirs=dest_vcf,
        )
        self.run_shell(
            args=f"set -e && {self.tabix} --preset vcf {dest_vcf}",
            input_files_or_dirs=dest_vcf,
            output_files_or_dirs=f"{dest_vcf}.tbi",
        )


class FetchKnownSitesVcfs(luigi.WrapperTask):
    """Luigi wrapper task for fetching multiple known variant VCF files.

    This task coordinates fetching and indexing of multiple VCF files containing
    known variant sites used for Base Quality Score Recalibration (BQSR).

    Parameters:
        known_sites_vcf_paths: List of paths to known variant VCF files.
        bgzip: Path to the bgzip executable.
        tabix: Path to the tabix executable.
        n_cpu: Number of CPU threads to use.
        sh_config: Shell configuration parameters.
    """

    known_sites_vcf_paths = luigi.ListParameter()
    bgzip = luigi.Parameter(default="bgzip")
    tabix = luigi.Parameter(default="tabix")
    n_cpu = luigi.IntParameter(default=1)
    sh_config = luigi.DictParameter(default={})
    priority = 70

    def requires(self) -> list[luigi.Task]:
        """Return the dependency tasks for fetching all known sites VCF files.

        Returns:
            list[luigi.Task]: List of FetchResourceVcf tasks for each VCF file.
        """
        return [
            FetchResourceVcf(
                src_path=p,
                bgzip=self.bgzip,
                tabix=self.tabix,
                n_cpu=self.n_cpu,
                sh_config=self.sh_config,
            )
            for p in self.known_sites_vcf_paths
        ]

    def output(self) -> list[luigi.Target]:
        """Return the output targets from the required VCF preparation tasks.

        Returns:
            list[luigi.Target]: Combined output targets from all VCF preparation tasks.
        """
        return self.input()


if __name__ == "__main__":
    luigi.run()
