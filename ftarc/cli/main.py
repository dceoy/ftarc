#!/usr/bin/env python
"""
FASTQ-to-analysis-ready-CRAM Workflow Executor for Human Genome Sequencing

Usage:
    ftarc init [--debug|--info] [--yml=<path>]
    ftarc download [--debug|--info] [--dest-dir=<path>]
    ftarc run [--debug|--info] [--yml=<path>] [--cpus=<int>]
        [--workers=<int>] [--skip-cleaning] [--print-subprocesses]
        [--use-bwa-mem2] [--dest-dir=<path>]
    ftarc samqc [--debug|--info] [--cpus=<int>] [--dest-dir=<path>] <fa_path>
        <sam_path>...
    ftarc -h|--help
    ftarc --version

Commands:
    init                    Create a config YAML template
    download                Download GRCh38 resource data via FTP
    run                     Create analysis-ready CRAM files from FASTQ files
                            (Trim adapters, align reads, mark duplicates, and
                             apply BQSR)
    samqc                   Collect multiple metrics from CRAM or BAM files

Options:
    -h, --help              Print help and exit
    --version               Print version and exit
    --debug, --info         Execute a command with debug|info messages
    --yml=<path>            Specify a config YAML path [default: ftarc.yml]
    --cpus=<int>            Limit CPU cores used
    --workers=<int>         Specify the maximum number of workers [default: 1]
    --skip-cleaning         Skip incomlete file removal when a task fails
    --print-subprocesses    Print STDOUT/STDERR outputs from subprocesses
    --use-bwa-mem2          Use BWA-MEM2 for read alignment
    --dest-dir=<path>       Specify a destination directory path [default: .]

Args:
    <fa_path>               Path to an reference FASTA file
                            (The index and sequence dictionary are required.)
    <sam_path>              Path to a CRAM or BAM file
"""

import logging
import os
from math import floor
from pathlib import Path

from docopt import docopt
from psutil import cpu_count, virtual_memory

from .. import __version__
from ..task.controller import CollectMultipleSamMetrics
from ..task.wget import DownloadResourceFilesRecursively
from .builder import build_luigi_tasks, run_processing_pipeline
from .util import fetch_executable, load_default_dict, write_config_yml


def main():
    args = docopt(__doc__, version=__version__)
    if args['--debug']:
        log_level = 'DEBUG'
    elif args['--info']:
        log_level = 'INFO'
    else:
        log_level = 'WARNING'
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S', level=log_level
    )
    logger = logging.getLogger(__name__)
    logger.debug(f'args:{os.linesep}{args}')
    if args['init']:
        write_config_yml(path=args['--yml'])
    elif args['run']:
        run_processing_pipeline(
            config_yml_path=args['--yml'], dest_dir_path=args['--dest-dir'],
            max_n_cpu=args['--cpus'], max_n_worker=args['--workers'],
            skip_cleaning=args['--skip-cleaning'],
            print_subprocesses=args['--print-subprocesses'],
            console_log_level=log_level, use_bwa_mem2=args['--use-bwa-mem2']
        )
    else:
        dest_dir = Path(args['--dest-dir']).resolve()
        if args['download']:
            build_luigi_tasks(
                tasks=[
                    DownloadResourceFilesRecursively(
                        ftp_src_url=v, dest_path=str(dest_dir.joinpath(k)),
                        wget=fetch_executable('wget')
                    ) for k, v in load_default_dict(stem='urls').items()
                ],
                log_level=log_level
            )
        elif args['samqc']:
            n_cpu = int(args['--cpus'] or cpu_count())
            n_sam = len(args['<sam_path>'])
            n_worker = min(n_sam, n_cpu)
            kwargs = {
                'fa_path': str(Path(args['<fa_path>']).resolve()),
                'dest_dir_path': str(dest_dir),
                'samtools': fetch_executable('samtools'),
                'pigz': fetch_executable('pigz'),
                'picard': (
                    fetch_executable('gatk', ignore_errors=True)
                    or fetch_executable('picard')
                ),
                'n_cpu': (floor(n_cpu / n_sam) if n_cpu > n_sam else 1),
                'java_tool_options': '-Xmx{}m'.format(
                    int(virtual_memory().total / 1024 / 1024 / n_worker)
                )
            }
            build_luigi_tasks(
                tasks=[
                    CollectMultipleSamMetrics(
                        input_sam_path=str(Path(p).resolve()), **kwargs
                    ) for p in args['<sam_path>']
                ],
                workers=n_worker, log_level=log_level
            )
