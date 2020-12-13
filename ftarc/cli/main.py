#!/usr/bin/env python
"""
FASTQ-to-analysis-ready-CRAM Workflow Executor for Human Genome Sequencing

Usage:
    ftarc init [--debug|--info] [--yml=<path>]
    ftarc download [--debug|--info] [--dest-dir=<path>]
    ftarc run [--debug|--info] [--yml=<path>] [--cpus=<int>]
        [--workers=<int>] [--skip-cleaning] [--print-subprocesses]
        [--use-bwa-mem2] [--dest-dir=<path>]
    ftarc -h|--help
    ftarc --version

Commands:
    init                    Create a config YAML template
    download                Download GRCh38 resource data via FTP
    run                     Create analysis-ready CRAM files from FASTQ files
                            (Trim adapters, align reads, mark duplicates, and
                             apply BQSR)

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
"""

import logging
import os
from pathlib import Path

from docopt import docopt

from .. import __version__
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
    elif args['download']:
        build_luigi_tasks(
            tasks=[
                DownloadResourceFilesRecursively(
                    ftp_src_url=v,
                    dest_path=str(
                        Path(args['--dest-dir']).resolve().joinpath(k)
                    ),
                    wget=fetch_executable('wget')
                ) for k, v in load_default_dict(stem='urls').items()
            ],
            log_level=log_level
        )
    elif args['run']:
        run_processing_pipeline(
            config_yml_path=args['--yml'], dest_dir_path=args['--dest-dir'],
            max_n_cpu=args['--cpus'], max_n_worker=args['--workers'],
            skip_cleaning=args['--skip-cleaning'],
            print_subprocesses=args['--print-subprocesses'],
            console_log_level=log_level, use_bwa_mem2=args['--use-bwa-mem2']
        )
