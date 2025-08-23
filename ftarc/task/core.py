"""Core base classes and shared functionality for ftarc Luigi tasks.

This module provides the base task classes, common utilities, and shared patterns
used by all specialized task implementations in the ftarc pipeline.
"""

import logging
import os
import re
import sys
from abc import ABC, abstractmethod
from collections.abc import Iterable, Mapping, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path

import luigi
from shoper.shelloperator import ShellOperator


class ShellTask(luigi.Task, ABC):
    """Abstract base class for Luigi tasks that execute shell commands.

    This class provides a framework for running shell commands with proper logging,
    error handling, and execution time tracking. It serves as the foundation for
    all command-line tool integration in the ftarc pipeline.

    Attributes:
        retry_count: Number of times to retry the task on failure (default: 0).
    """

    retry_count = 0

    def __init__(self, *args: object, **kwargs: object) -> None:
        """Initialize the ShellTask with shell environment setup."""
        super().__init__(*args, **kwargs)
        self.initialize_shell()

    @luigi.Task.event_handler(luigi.Event.PROCESSING_TIME)
    def print_execution_time(self, processing_time: float) -> None:
        """Print task execution time when task completes.

        Args:
            processing_time: Total processing time in seconds
        """
        logger = logging.getLogger("task-timer")
        message = (
            f"{self.__class__.__module__}.{self.__class__.__name__} - "
            f"total elapsed time:\t{timedelta(seconds=processing_time)}"
        )
        logger.info(message)
        print(message, flush=True)

    @classmethod
    def print_log(cls, message: str, new_line: bool = True) -> None:
        """Print log message to both logger and stdout.

        Args:
            message: Message to log and print
            new_line: Prepend newline to output
        """
        logger = logging.getLogger(cls.__name__)
        logger.info(message)
        print((os.linesep if new_line else "") + f">>\t{message}", flush=True)

    @classmethod
    def initialize_shell(cls) -> None:
        """Initialize shell-related class attributes to None."""
        cls.__log_txt_path = None
        cls.__sh = None
        cls.__run_kwargs = None

    @classmethod
    def setup_shell(
        cls,
        run_id: str | int | None = None,
        log_dir_path: str | os.PathLike[str] | None = None,
        commands: str | Sequence[str] | None = None,
        cwd: str | os.PathLike[str] | None = None,
        remove_if_failed: bool = True,
        clear_log_txt: bool = False,
        print_command: bool = True,
        quiet: bool = True,
        executable: str = "/bin/bash",
        env: Mapping[str, str] | None = None,
        **kwargs: object,
    ) -> None:
        """Configure shell execution environment and run version commands.

        Args:
            run_id: Unique identifier for this run (used in log filename)
            log_dir_path: Directory to store log files
            commands: Commands to run for version checking
            cwd: Working directory for shell execution
            remove_if_failed: Remove output files if command fails
            clear_log_txt: Clear log file before writing
            print_command: Print commands before execution
            quiet: Suppress stdout from commands
            executable: Shell executable to use
            env: Environment variables to set
            **kwargs: Additional arguments for shell execution
        """
        cls.__log_txt_path = (
            str(
                Path(log_dir_path)
                .joinpath(f"{cls.__module__}.{cls.__name__}.{run_id}.sh.log.txt")
                .resolve()
            )
            if log_dir_path and run_id
            else None
        )
        cls.__sh = ShellOperator(
            log_txt=cls.__log_txt_path,
            quiet=quiet,
            clear_log_txt=clear_log_txt,
            logger=logging.getLogger(cls.__name__),
            print_command=print_command,
            executable=executable,
        )
        cls.__run_kwargs = {
            "cwd": cwd,
            "remove_if_failed": remove_if_failed,
            "env": (
                {**env, **{k: v for k, v in os.environ.items() if k not in env}}
                if env
                else dict(os.environ)
            ),
            **kwargs,
        }
        cls.make_dirs(log_dir_path, cwd)
        if commands:
            cls.run_shell(args=list(cls.generate_version_commands(commands)))

    @classmethod
    def make_dirs(cls, *paths: object) -> None:
        """Create directories for the given paths if they don't exist.

        Args:
            *paths: Paths to create as directories
        """
        for p in paths:
            if p:
                d = Path(str(p)).resolve()
                if not d.is_dir():
                    cls.print_log(f"Make a directory:\t{d}", new_line=False)
                    d.mkdir(parents=True, exist_ok=True)

    @classmethod
    def run_shell(cls, *args: object, **kwargs: object) -> None:
        """Execute shell command using configured ShellOperator.

        Args:
            *args: Arguments to pass to shell operator
            **kwargs: Keyword arguments to pass to shell operator
        """
        logger = logging.getLogger(cls.__name__)
        start_datetime = datetime.now(UTC)
        cls.__sh.run(
            *args,
            **kwargs,
            **{k: v for k, v in cls.__run_kwargs.items() if k not in kwargs},
        )
        if "asynchronous" in kwargs:
            cls.__sh.wait()
        elapsed_timedelta = datetime.now(UTC) - start_datetime
        message = f"shell elapsed time:\t{elapsed_timedelta}"
        logger.info(message)
        if cls.__log_txt_path:
            with Path(cls.__log_txt_path).open("a", encoding="utf-8") as f:
                f.write(f"### {message}{os.linesep}")

    @classmethod
    def remove_files_and_dirs(cls, *paths: str | os.PathLike[str]) -> None:
        """Remove files and directories using shell rm command.

        Args:
            *paths: File and directory paths to remove
        """
        targets = [Path(str(p)) for p in paths if Path(str(p)).exists()]
        if targets:
            cls.run_shell(
                args=" ".join([
                    "rm",
                    ("-rf" if [t for t in targets if t.is_dir()] else "-f"),
                    *[str(t) for t in targets],
                ])
            )

    @classmethod
    def print_env_versions(cls) -> None:
        """Print version information for Python and system environment."""
        python = sys.executable
        version_files = [
            Path("/proc/version"),
            *[
                o
                for o in Path("/etc").iterdir()
                if o.name.endswith(("-release", "_version"))
            ],
        ]
        cls.run_shell(
            args=[
                f"{python} --version",
                f"{python} -m pip --version",
                f"{python} -m pip freeze --no-cache-dir",
                "uname -a",
                *[f"cat {o}" for o in version_files if o.is_file()],
            ]
        )

    @staticmethod
    @abstractmethod
    def generate_version_commands(commands: str | Sequence[str]) -> Iterable[str]:
        """Generate version checking commands for the given executables."""
        raise NotImplementedError


class FtarcTask(ShellTask):
    """Base task class for ftarc pipeline operations.

    This class extends ShellTask with ftarc-specific functionality including
    version command generation for various bioinformatics tools, file format
    conversion utilities, and GATK Java options configuration.
    """

    def __init__(self, *args: object, **kwargs: object) -> None:
        """Initialize FtarcTask with parent class initialization."""
        super().__init__(*args, **kwargs)

    @staticmethod
    def generate_version_commands(commands: str | Sequence[str]) -> Iterable[str]:
        """Generate version checking commands for bioinformatics tools.

        Args:
            commands: Command name(s) to generate version commands for

        Yields:
            Version checking command strings
        """
        for c in [commands] if isinstance(commands, str) else commands:
            n = Path(c).name
            if n == "java" or n.endswith(".jar"):
                yield f"{c} -version"
            elif n == "bwa":
                yield f'{c} 2>&1 | grep -e "Program:" -e "Version:"'
            elif n == "wget":
                yield f"{c} --version | head -1"
            elif n == "bwa-mem2":
                yield f"{c} version"
            elif n == "picard":
                yield f"{c} CreateSequenceDictionary --version"
            else:
                yield f"{c} --version"

    @classmethod
    def bzip2_to_gzip(
        cls,
        src_bz2_path: str | os.PathLike[str],
        dest_gz_path: str | os.PathLike[str],
        pbzip2: str = "pbzip2",
        pigz: str = "pigz",
        n_cpu: int = 1,
    ) -> None:
        """Convert bzip2 file to gzip format.

        Args:
            src_bz2_path: Source bzip2 file path
            dest_gz_path: Destination gzip file path
            pbzip2: Path to pbzip2 executable
            pigz: Path to pigz executable
            n_cpu: Number of CPU threads to use
        """
        cls.run_shell(
            args=(
                f"set -eo pipefail && {pbzip2} -p{n_cpu} -dc {src_bz2_path}"
                f" | {pigz} -p {n_cpu} -c - > {dest_gz_path}"
            ),
            input_files_or_dirs=src_bz2_path,
            output_files_or_dirs=dest_gz_path,
        )

    @staticmethod
    def generate_gatk_java_options(n_cpu: int = 1, memory_mb: int = 4096) -> str:
        """Generate optimized Java options for GATK tools.

        Args:
            n_cpu: Number of CPU threads for parallel GC
            memory_mb: Maximum memory in megabytes

        Returns:
            String of Java options for GATK execution
        """
        return " ".join([
            "-Dsamjdk.compression_level=5",
            "-Dsamjdk.use_async_io_read_samtools=true",
            "-Dsamjdk.use_async_io_write_samtools=true",
            "-Dsamjdk.use_async_io_write_tribble=false",
            f"-Xmx{int(memory_mb)}m",
            "-XX:+UseParallelGC",
            f"-XX:ParallelGCThreads={int(n_cpu)}",
        ])

    @classmethod
    def samtools_index(
        cls,
        sam_path: str | os.PathLike[str],
        samtools: str = "samtools",
        n_cpu: int = 1,
    ) -> None:
        """Index SAM/BAM/CRAM file using samtools.

        Args:
            sam_path: Path to SAM/BAM/CRAM file
            samtools: Path to samtools executable
            n_cpu: Number of CPU threads to use
        """
        cls.run_shell(
            args=(
                f"set -e && {samtools} quickcheck -v {sam_path}"
                f" && {samtools} index -@ {n_cpu} {sam_path}"
            ),
            input_files_or_dirs=sam_path,
            output_files_or_dirs=re.sub(r"\.(cr|b)am$", ".\\1am.\\1ai", str(sam_path)),
        )

    @classmethod
    def samtools_view(
        cls,
        input_sam_path: str | os.PathLike[str],
        fa_path: str | os.PathLike[str],
        output_sam_path: str | os.PathLike[str],
        samtools: str = "samtools",
        n_cpu: int = 1,
        add_args: str | Sequence[str] | None = None,
        index_sam: bool = False,
        remove_input: bool = False,
    ) -> None:
        """Convert SAM/BAM/CRAM files using samtools view.

        Args:
            input_sam_path: Input SAM/BAM/CRAM file path
            fa_path: Reference genome FASTA file path
            output_sam_path: Output SAM/BAM/CRAM file path
            samtools: Path to samtools executable
            n_cpu: Number of CPU threads to use
            add_args: Additional arguments for samtools view
            index_sam: Create index for output file
            remove_input: Remove input file after conversion
        """
        cls.run_shell(
            args=(
                f"set -e && {samtools} quickcheck -v {input_sam_path}"
                f" && {samtools} view -@ {n_cpu} -T {fa_path}"
                + " -{}S".format("C" if str(output_sam_path).endswith(".cram") else "b")
                + (
                    "".join(
                        f" {a}"
                        for a in ([add_args] if isinstance(add_args, str) else add_args)
                    )
                    if add_args
                    else ""
                )
                + f" -o {output_sam_path} {input_sam_path}"
            ),
            input_files_or_dirs=[input_sam_path, fa_path, f"{fa_path}.fai"],
            output_files_or_dirs=output_sam_path,
        )
        if index_sam:
            cls.samtools_index(sam_path=output_sam_path, samtools=samtools, n_cpu=n_cpu)
        if remove_input:
            cls.remove_files_and_dirs(input_sam_path)
