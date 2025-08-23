"""Resource downloading and processing tasks for the ftarc pipeline.

This module provides Luigi tasks for downloading and preparing reference genome
resources, including FASTA files, known variant databases, and associated indices.
"""

import re
from collections.abc import Generator
from itertools import product
from pathlib import Path
from socket import gethostname

import luigi
from luigi.util import requires

from .bwa import CreateBwaIndices
from .core import FtarcTask
from .picard import CreateSequenceDictionary
from .resource import FetchResourceVcf
from .samtools import SamtoolsFaidx


class DownloadResourceFiles(FtarcTask):
    """Luigi task for downloading genomic resource files from URLs.

    This task downloads resource files such as reference genomes and variant databases,
    automatically handling compression formats and file type conversions.

    Parameters:
        src_urls: List of URLs to download resources from.
        dest_dir_path: Destination directory for downloaded files.
        run_id: Identifier for this download run.
        wget: Path to the wget executable.
        pigz: Path to the pigz executable.
        pbzip2: Path to the pbzip2 executable.
        bgzip: Path to the bgzip executable.
        n_cpu: Number of CPU threads to use.
        sh_config: Shell configuration parameters.
    """

    src_urls = luigi.ListParameter()
    dest_dir_path = luigi.Parameter(default=".")
    run_id = luigi.Parameter(default=gethostname())
    wget = luigi.Parameter(default="wget")
    pigz = luigi.Parameter(default="pigz")
    pbzip2 = luigi.Parameter(default="pbzip2")
    bgzip = luigi.Parameter(default="bgzip")
    n_cpu = luigi.IntParameter(default=1)
    sh_config = luigi.DictParameter(default={})
    priority: int = 10

    def output(self) -> list[luigi.LocalTarget]:
        """Define output file targets based on input URLs and file types.

        Determines the expected output file paths after downloading and processing
        resource files. Handles different compression formats and file extensions,
        automatically adjusting paths based on the processing that will be performed.

        Returns:
            list[luigi.LocalTarget]: List of Luigi local file targets representing
                the expected output files after download and processing.

        Note:
            - Compressed FASTA/text files (.gz/.bz2) are decompressed
            - BGZ files are converted to GZ format
            - VCF and BED files are compressed with bgzip
            - Other files retain their original format
        """
        dest_dir = Path(self.dest_dir_path).resolve()
        target_paths = []
        for u in self.src_urls:
            p = str(dest_dir.joinpath(Path(u).name))
            if u.endswith(
                tuple(
                    f".{a}.{b}"
                    for a, b in product(("fa", "fna", "fasta", "txt"), ("gz", "bz2"))
                )
            ):
                target_paths.append(re.sub(r"\.(gz|bz2)$", "", p))
            elif u.endswith(".bgz"):
                target_paths.append(re.sub(r"\.bgz$", ".gz", p))
            elif u.endswith((".vcf", ".bed")):
                target_paths.append(f"{p}.gz")
            else:
                target_paths.append(p)
        return [luigi.LocalTarget(p) for p in target_paths]

    def run(self) -> None:
        """Download and process genomic resource files from URLs.

        Downloads files from the specified URLs and processes them according to
        their file types and compression formats. Sets up shell environment and
        executes appropriate compression/decompression commands.

        Process:
            1. Downloads each file using wget
            2. Applies format-specific processing:
               - BGZ files: converted to GZ format
               - VCF/BED files: compressed with bgzip
               - Compressed FASTA/text: decompressed appropriately
               - Other files: left as-is
        """
        dest_dir = Path(self.dest_dir_path).resolve()
        self.print_log(f"Download resource files:\t{dest_dir}")
        self.setup_shell(
            run_id=self.run_id,
            commands=[self.wget, self.bgzip, self.pigz, self.pbzip2],
            cwd=dest_dir,
            **self.sh_config,
        )
        for u, o in zip(self.src_urls, self.output(), strict=False):
            t = dest_dir.joinpath(
                (Path(u).stem + ".gz") if u.endswith(".bgz") else Path(u).name
            )
            self.run_shell(
                args=f"set -e && {self.wget} -qSL -O {t} {u}", output_files_or_dirs=t
            )
            if str(t) == o.path:
                pass
            elif t.suffix != ".gz" and o.path.endswith(".gz"):
                self.run_shell(
                    args=f"set -e && {self.bgzip} -@ {self.n_cpu} {t}",
                    input_files_or_dirs=t,
                    output_files_or_dirs=o.path,
                )
            elif o.path.endswith((".fa", ".fna", ".fasta", ".txt")):
                self.run_shell(
                    args=(
                        f"set -e && {self.pbzip2} -p{self.n_cpu} -d {t}"
                        if t.suffix == ".bz2"
                        else f"set -e && {self.pigz} -p {self.n_cpu} -d {t}"
                    ),
                    input_files_or_dirs=t,
                    output_files_or_dirs=o.path,
                )


@requires(DownloadResourceFiles)
class DownloadAndIndexReferenceFasta(luigi.Task):
    """Luigi task for downloading and indexing reference FASTA files.

    This task coordinates downloading reference genomes and creating all necessary
    indices for BWA alignment, samtools, and GATK operations.

    Parameters:
        samtools: Path to the samtools executable.
        gatk: Path to the GATK executable.
        bwa: Path to the BWA executable.
        use_bwa_mem2: Use BWA-MEM2 instead of standard BWA.
        n_cpu: Number of CPU threads to use.
        memory_mb: Memory allocation in MB.
        sh_config: Shell configuration parameters.
    """

    samtools = luigi.Parameter(default="samtools")
    gatk = luigi.Parameter(default="gatk")
    bwa = luigi.Parameter(default="bwa")
    use_bwa_mem2 = luigi.BoolParameter(default=False)
    n_cpu = luigi.IntParameter(default=1)
    memory_mb = luigi.FloatParameter(default=4096)
    sh_config = luigi.DictParameter(default={})
    priority: int = 10

    def output(self) -> list[luigi.LocalTarget]:
        """Define output targets including downloaded FASTA and all required indices.

        Determines the expected output files after downloading reference FASTA files
        and creating all necessary indices for genome alignment and analysis tools.

        Returns:
            list[luigi.LocalTarget]: List of file targets including:
                - Original downloaded files
                - FASTA index (.fai) for samtools
                - Sequence dictionary (.dict) for GATK
                - BWA index files (varies by BWA version)

        Note:
            BWA-MEM2 uses different index file suffixes than standard BWA:
            - BWA: .pac, .bwt, .ann, .amb, .sa
            - BWA-MEM2: .0123, .amb, .ann, .pac, .bwt.2bit.64
        """
        bwa_suffixes = (
            ["0123", "amb", "ann", "pac", "bwt.2bit.64"]
            if self.use_bwa_mem2
            else ["pac", "bwt", "ann", "amb", "sa"]
        )
        fa = next(
            Path(i.path)
            for i in self.input()
            if i.path.endswith((".fa", ".fna", ".fasta"))
        )
        return self.input() + [
            luigi.LocalTarget(p)
            for p in [
                f"{fa}.fai",
                fa.parent.joinpath(f"{fa.stem}.dict"),
                *[f"{fa}.{s}" for s in bwa_suffixes],
            ]
        ]

    def run(self) -> Generator[list[luigi.Task], None, None]:
        """Execute indexing tasks for the downloaded reference FASTA file.

        Yields Luigi tasks to create all necessary indices for the reference genome,
        enabling efficient access by various bioinformatics tools in the pipeline.

        Yields:
            SamtoolsFaidx: Creates .fai index for random access
            CreateSequenceDictionary: Creates .dict file for GATK
            CreateBwaIndices: Creates BWA alignment indices

        Note:
            Uses yield instead of return to enable Luigi's dynamic dependency
            resolution, allowing these indexing tasks to run in parallel.
        """
        fa_path = self.input()[0].path
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
            CreateBwaIndices(
                fa_path=fa_path,
                bwa=self.bwa,
                use_bwa_mem2=self.use_bwa_mem2,
                sh_config=self.sh_config,
            ),
        ]


@requires(DownloadResourceFiles)
class DownloadAndIndexResourceVcfs(luigi.Task):
    """Luigi task for downloading and indexing VCF resource files.

    This task ensures VCF files are properly compressed with bgzip and indexed
    with tabix for efficient random access during variant calling.

    Parameters:
        bgzip: Path to the bgzip executable.
        tabix: Path to the tabix executable.
        n_cpu: Number of CPU threads to use.
        sh_config: Shell configuration parameters.
    """

    bgzip = luigi.Parameter(default="bgzip")
    tabix = luigi.Parameter(default="tabix")
    n_cpu = luigi.IntParameter(default=1)
    sh_config = luigi.DictParameter(default={})
    priority: int = 10

    def output(self) -> list[luigi.LocalTarget]:
        """Define output targets including downloaded VCFs and their tabix indices.

        Determines the expected output files after downloading VCF files and
        creating tabix indices for efficient random access during variant calling.

        Returns:
            list[luigi.LocalTarget]: List of file targets including:
                - Original downloaded VCF files
                - Tabix index files (.tbi) for bgzip-compressed VCF files

        Note:
            Only VCF files ending with .vcf.gz get tabix indices created,
            as tabix requires bgzip compression.
        """
        return self.input() + [
            luigi.LocalTarget(f"{i.path}.tbi")
            for i in self.input()
            if i.path.endswith(".vcf.gz")
        ]

    def run(self) -> Generator[list[luigi.Task], None, None]:
        """Execute tabix indexing tasks for downloaded VCF files.

        Yields Luigi tasks to create tabix indices for all bgzip-compressed VCF files,
        enabling efficient random access by genomic coordinate during variant analysis.

        Yields:
            FetchResourceVcf: Creates .tbi tabix index for each .vcf.gz file

        Note:
            Uses yield instead of return to enable Luigi's dynamic dependency
            resolution, allowing indexing tasks to run in parallel for multiple VCFs.
        """
        yield [
            FetchResourceVcf(
                src_path=i.path,
                bgzip=self.bgzip,
                tabix=self.tabix,
                n_cpu=self.n_cpu,
                sh_config=self.sh_config,
            )
            for i in self.input()
            if i.path.endswith(".vcf.gz")
        ]


class DownloadAndProcessResourceFiles(luigi.WrapperTask):
    """Luigi wrapper task for downloading and processing all genomic resources.

    This task orchestrates the complete download and indexing workflow for all
    resources needed by the ftarc pipeline, including reference genomes and known
    variant databases.

    Parameters:
        src_url_dict: Dictionary mapping resource types to URLs.
        dest_dir_path: Destination directory for resources.
        wget: Path to the wget executable.
        pigz: Path to the pigz executable.
        pbzip2: Path to the pbzip2 executable.
        bgzip: Path to the bgzip executable.
        tabix: Path to the tabix executable.
        samtools: Path to the samtools executable.
        gatk: Path to the GATK executable.
        bwa: Path to the BWA executable.
        n_cpu: Number of CPU threads to use.
        memory_mb: Memory allocation in MB.
        use_bwa_mem2: Use BWA-MEM2 instead of standard BWA.
        sh_config: Shell configuration parameters.
    """

    src_url_dict = luigi.DictParameter()
    dest_dir_path = luigi.Parameter(default=".")
    wget = luigi.Parameter(default="wget")
    pigz = luigi.Parameter(default="pigz")
    pbzip2 = luigi.Parameter(default="pbzip2")
    bgzip = luigi.Parameter(default="bgzip")
    tabix = luigi.Parameter(default="tabix")
    samtools = luigi.Parameter(default="samtools")
    gatk = luigi.Parameter(default="gatk")
    bwa = luigi.Parameter(default="bwa")
    n_cpu = luigi.IntParameter(default=1)
    memory_mb = luigi.FloatParameter(default=4096)
    use_bwa_mem2 = luigi.BoolParameter(default=False)
    sh_config = luigi.DictParameter(default={})
    priority: int = 10

    def requires(self) -> list[luigi.Task]:
        """Define prerequisite tasks for downloading and processing all resources.

        Creates two main task dependencies that handle different types of genomic
        resources required by the ftarc pipeline.

        Returns:
            list[luigi.Task]: List of required tasks:
                - DownloadAndIndexReferenceFasta: Downloads and indexes reference
                  genome FASTA files (primary and ALT sequences)
                - DownloadAndIndexResourceVcfs: Downloads and indexes known variant
                  databases and other non-FASTA resources

        Note:
            The URL dictionary is partitioned to separate reference FASTA files
            from other resources (VCFs, BED files, etc.) for appropriate processing.
        """
        return [
            DownloadAndIndexReferenceFasta(
                src_urls=[
                    self.src_url_dict["reference_fa"],
                    self.src_url_dict["reference_fa_alt"],
                ],
                dest_dir_path=self.dest_dir_path,
                run_id=Path(self.src_url_dict["reference_fa"]).stem,
                wget=self.wget,
                pigz=self.pigz,
                pbzip2=self.pbzip2,
                bgzip=self.bgzip,
                samtools=self.samtools,
                gatk=self.gatk,
                bwa=self.bwa,
                use_bwa_mem2=self.use_bwa_mem2,
                n_cpu=self.n_cpu,
                memory_mb=self.memory_mb,
                sh_config=self.sh_config,
            ),
            DownloadAndIndexResourceVcfs(
                src_urls=[
                    v
                    for k, v in self.src_url_dict.items()
                    if k not in {"reference_fa", "reference_fa_alt"}
                ],
                dest_dir_path=self.dest_dir_path,
                run_id="others",
                wget=self.wget,
                pigz=self.pigz,
                pbzip2=self.pbzip2,
                bgzip=self.bgzip,
                tabix=self.tabix,
                n_cpu=self.n_cpu,
                sh_config=self.sh_config,
            ),
        ]

    def output(self) -> list[luigi.Target]:
        """Return output targets from prerequisite tasks.

        As a wrapper task, this returns the combined outputs from all required
        subtasks, representing all downloaded and indexed genomic resources.

        Returns:
            list[luigi.Target]: All file targets from the required download and
                indexing tasks, including:
                - Reference FASTA files and their indices
                - Known variant VCF files and their tabix indices
        """
        return self.input()


if __name__ == "__main__":
    luigi.run()
