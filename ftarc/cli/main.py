#!/usr/bin/env python
"""
FASTQ-to-analysis-ready-CRAM Workflow Executor for Human Genome Sequencing

Usage:
    ftarc init [--debug|--info] [--yml=<path>]
    ftarc download [--debug|--info] [--cpus=<int>] [--use-bwa-mem2]
        [--skip-cleaning] [--dest-dir=<path>]
    ftarc run [--debug|--info] [--yml=<path>] [--cpus=<int>] [--workers=<int>]
        [--skip-cleaning] [--print-subprocesses] [--use-bwa-mem2]
        [--dest-dir=<path>]
    ftarc fastqc [--debug|--info] [--cpus=<int>] [--dest-dir=<path>]
        [--skip-cleaning] <fq_path>...
    ftarc samqc [--debug|--info] [--cpus=<int>] [--workers=<int>]
        [--dest-dir=<path>] [--skip-cleaning] <fa_path> <sam_path>...
    ftarc validate [--debug|--info] [--cpus=<int>] [--workers=<int>]
        [--dest-dir=<path>] [--summary] <fa_path> <sam_path>...
    ftarc bqsr [--debug|--info] [--cpus=<int>] [--workers=<int>]
        [--dest-dir=<path>] [--skip-cleaning] (--known-sites=<vcf_path>)...
        <fa_path> <sam_path>...
    ftarc -h|--help
    ftarc --version

Commands:
    init                    Create a config YAML template
    download                Download and process GRCh38 resource data
    run                     Create analysis-ready CRAM files from FASTQ files
                            (Trim adapters, align reads, mark duplicates, and
                             apply BQSR)
    fastqc                  Collect metrics from FASTQ files using FastQC
    samqc                   Collect metrics from CRAM or BAM files using Picard
                            and Samtools
    validate                Validate BAM or CRAM files using Picard
    bqsr                    Apply BQSR to BAM or CRAM files using GATK

Options:
    -h, --help              Print help and exit
    --version               Print version and exit
    --debug, --info         Execute a command with debug|info messages
    --yml=<path>            Specify a config YAML path [default: ftarc.yml]
    --cpus=<int>            Limit CPU cores used
    --workers=<int>         Specify the maximum number of workers [default: 1]
    --skip-cleaning         Skip incomlete file removal when a task fails
    --print-subprocesses    Print STDOUT/STDERR outputs from subprocesses
    --use-bwa-mem2          Use Bwa-mem2 for read alignment
    --dest-dir=<path>       Specify a destination directory path [default: .]
    --summary               Set SUMMARY to the mode of output
    --known-sites=<vcf_path>
                            Specify paths of known polymorphic sites VCF files

Args:
    <fa_path>               Path to an reference FASTA file
                            (The index and sequence dictionary are required.)
    <sam_path>              Path to a CRAM or BAM file
    <fq_path>               Path to a FASTQ file
"""

import logging
import os
from math import floor

from docopt import docopt
from psutil import cpu_count, virtual_memory

from .. import __version__
from ..task.controller import CollectMultipleSamMetrics
from ..task.downloader import DownloadAndProcessResourceFiles
from ..task.fastqc import CollectFqMetricsWithFastqc
from ..task.gatk import ApplyBQSR
from ..task.picard import ValidateSamFile
from .pipeline import run_processing_pipeline
from .util import (build_luigi_tasks, fetch_executable, load_default_dict,
                   print_log, write_config_yml)


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
    print_log(f'Start the workflow of ftarc {__version__}')
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
        n_cpu = int(args['--cpus'] or cpu_count())
        memory_mb = virtual_memory().total / 1024 / 1024 / 2
        gatk_or_picard = (
            fetch_executable('gatk', ignore_errors=(not args['bqsr']))
            or fetch_executable('picard')
        )
        sh_config = {
            'log_dir_path': None,
            'remove_if_failed': (not args['--skip-cleaning']), 'quiet': False,
            'executable': fetch_executable('bash')
        }
        if args['download']:
            build_luigi_tasks(
                tasks=[
                    DownloadAndProcessResourceFiles(
                        src_urls=list(load_default_dict(stem='urls').values()),
                        dest_dir_path=args['--dest-dir'],
                        **{
                            c: fetch_executable(c) for c in [
                                'wget', 'pbzip2', 'bgzip', 'pigz', 'samtools',
                                'tabix'
                            ]
                        },
                        bwa=fetch_executable(
                            'bwa-mem2' if args['--use-bwa-mem2'] else 'bwa'
                        ),
                        gatk=gatk_or_picard, n_cpu=n_cpu, memory_mb=memory_mb,
                        use_bwa_mem2=args['--use-bwa-mem2'],
                        sh_config=sh_config
                    )
                ],
                log_level=log_level
            )
        elif args['fastqc']:
            build_luigi_tasks(
                tasks=[
                    CollectFqMetricsWithFastqc(
                        input_fq_paths=args['<fq_path>'],
                        dest_dir_path=args['--dest-dir'],
                        fastqc=fetch_executable('fastqc'), n_cpu=n_cpu,
                        memory_mb=memory_mb, sh_config=sh_config
                    )
                ],
                log_level=log_level
            )
        elif args['samqc']:
            n_worker = min(
                int(args['--workers']), n_cpu, (len(args['<sam_path>']) * 11)
            )
            kwargs = {
                'fa_path': args['<fa_path>'],
                'dest_dir_path': args['--dest-dir'],
                **{c: fetch_executable(c) for c in ['samtools', 'pigz']},
                'picard': gatk_or_picard,
                **_calculate_cpus_n_memory_per_worker(
                    n_cpu=n_cpu, memory_mb=memory_mb, n_worker=n_worker
                ),
                'n_cpu': max(floor(n_cpu / n_worker), 1),
                'memory_mb': (memory_mb / n_worker), 'sh_config': sh_config
            }
            build_luigi_tasks(
                tasks=[
                    CollectMultipleSamMetrics(input_sam_path=p, **kwargs)
                    for p in args['<sam_path>']
                ],
                workers=n_worker, log_level=log_level
            )
        elif args['validate']:
            n_worker = min(
                int(args['--workers']), n_cpu, len(args['<sam_path>'])
            )
            kwargs = {
                'fa_path': args['<fa_path>'],
                'dest_dir_path': args['--dest-dir'], 'picard': gatk_or_picard,
                'mode_of_output':
                ('SUMMARY' if args['--summary'] else 'VERBOSE'),
                **_calculate_cpus_n_memory_per_worker(
                    n_cpu=n_cpu, memory_mb=memory_mb, n_worker=n_worker
                ),
                'sh_config': sh_config
            }
            build_luigi_tasks(
                tasks=[
                    ValidateSamFile(input_sam_path=p, **kwargs)
                    for p in args['<sam_path>']
                ],
                workers=n_worker, log_level=log_level
            )
        elif args['bqsr']:
            n_worker = min(
                int(args['--workers']), n_cpu, len(args['<sam_path>'])
            )
            worker_cpus_n_memory = _calculate_cpus_n_memory_per_worker(
                n_cpu=n_cpu, memory_mb=memory_mb, n_worker=n_worker
            )
            kwargs = {
                'fa_path': args['<fa_path>'],
                'known_sites_vcf_paths': args['--known-sites'],
                'dest_dir_path': args['--dest-dir'], 'gatk': gatk_or_picard,
                'samtools': fetch_executable('samtools'),
                'save_memory': (worker_cpus_n_memory['memory_mb'] < 8192),
                'sh_config': sh_config, **worker_cpus_n_memory
            }
            build_luigi_tasks(
                tasks=[
                    ApplyBQSR(input_sam_path=p, **kwargs)
                    for p in args['<sam_path>']
                ],
                workers=n_worker, log_level=log_level
            )


def _calculate_cpus_n_memory_per_worker(n_cpu, memory_mb, n_worker=1):
    return {
        'n_cpu': max(floor(n_cpu / n_worker), 1),
        'memory_mb': (memory_mb / n_worker)
    }
