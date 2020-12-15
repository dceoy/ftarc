#!/usr/bin/env python

import re
import sys
from pathlib import Path

import luigi

from .base import BaseTask, ShellTask
from .bwa import CreateBWAIndices
from .picard import CreateSequenceDictionary
from .resource import FetchResourceVCF


class DownloadAndProcessResourceFiles(BaseTask):
    ref_fa_url = luigi.Parameter()
    ref_fa_alt_url = luigi.Parameter()
    dbsnp_vcf_url = luigi.Parameter()
    mills_indel_vcf_url = luigi.Parameter()
    known_indel_vcf_url = luigi.Parameter()
    dest_dir_path = luigi.Parameter(default='.')
    wget = luigi.Parameter(default='wget')
    pbzip2 = luigi.Parameter(default='pbzip2')
    bgzip = luigi.Parameter(default='bgzip')
    pigz = luigi.Parameter(default='pigz')
    bwa = luigi.Parameter(default='bwa')
    samtools = luigi.Parameter(default='samtools')
    tabix = luigi.Parameter(default='tabix')
    picard = luigi.Parameter(default='picard')
    n_cpu = luigi.IntParameter(default=1)
    java_tool_options = luigi.Parameter(default='')
    use_bwa_mem2 = luigi.BoolParameter(default=False)
    log_dir_path = luigi.Parameter(default='')
    remove_if_failed = luigi.BoolParameter(default=True)
    quiet = luigi.BoolParameter(default=False)
    priority = luigi.IntParameter(default=sys.maxsize)

    def required(self):
        return [
            DownloadResourceFile(
                src_url=u, dest_dir_path=self.dest_dir_path, n_cpu=self.n_cpu,
                wget=self.wget, pbzip2=self.pbzip2, bgzip=self.bgzip
            ) for u in [
                self.ref_fa_url, self.ref_fa_alt_url, self.dbsnp_vcf_url,
                self.mills_indel_vcf_url, self.known_indel_vcf_url
            ]
        ]

    def run(self):
        input_paths = [i.path for i in self.input()]
        cf = {
            'pigz': self.pigz, 'pbzip2': self.pbzip2, 'bgzip': self.bgzip,
            'bwa': self.bwa, 'samtools': self.samtools, 'tabix': self.tabix,
            'picard': self.picard, 'n_cpu_per_worker': self.n_cpu,
            'gatk_java_options': self.java_tool_options,
            'use_bwa_mem2': self.use_bwa_mem2,
            'ref_dir_path': self.dest_dir_path,
            'log_dir_path': self.log_dir_path,
            'remove_if_failed': self.remove_if_failed, 'quiet': self.quiet
        }
        yield [
            CreateSequenceDictionary(ref_fa_path=input_paths[0], cf=cf),
            CreateBWAIndices(ref_fa_path=input_paths[0], cf=cf),
            *[FetchResourceVCF(src_path=p, cf=cf) for p in input_paths[2:]]
        ]


class DownloadResourceFile(ShellTask):
    src_url = luigi.Parameter()
    dest_dir_path = luigi.Parameter(default='.')
    n_cpu = luigi.IntParameter(default=1)
    wget = luigi.Parameter(default='wget')
    pbzip2 = luigi.Parameter(default='pbzip2')
    bgzip = luigi.Parameter(default='bgzip')
    priority = 10

    def output(self):
        p = str(Path(self.dest_dir_path).joinpath(Path(self.src_url).name))
        if p.endswith('.bgz'):
            return luigi.LocalTarget(re.sub(r'\.bgz$', '.gz', p))
        elif p.endswith(('.vcf', '.bed')):
            return luigi.LocalTarget(f'{p}.gz')
        else:
            return luigi.LocalTarget(p)

    def run(self):
        dest_file = Path(self.output().path)
        self.print_log(f'Download resource files:\t{dest_file}')
        if (self.src_url.endswith(('.bgz', '.gz', '.bz2'))
                or (dest_file.suffix == Path(self.src_url).suffix)):
            tmp_path = str(dest_file)
            commands = self.wget
            postproc_args = None
        elif dest_file.name.endswith('.gz'):
            tmp_path = re.sub(r'\.gz$', '', str(dest_file))
            commands = [self.wget, self.bgzip]
            postproc_args = f'{self.bgzip} -@ {self.n_cpu} {tmp_path}'
        elif dest_file.name.endswith('.bz2'):
            tmp_path = re.sub(r'\.bz2$', '', str(dest_file))
            commands = [self.wget, self.pbzip2]
            postproc_args = f'{self.pbzip2} -p{self.n_cpu} {tmp_path}'
        else:
            tmp_path = str(dest_file)
            commands = self.wget
            postproc_args = None
        self.setup_shell(commands=commands, cwd=dest_file.parent, quiet=False)
        self.run_shell(
            args=f'set -e && {self.wget} -qSL {self.src_url} -O {tmp_path}',
            output_files_or_dirs=tmp_path
        )
        if postproc_args:
            self.run_shell(
                args=f'set -e && {postproc_args}',
                input_files_or_dirs=tmp_path, output_files_or_dirs=dest_file
            )


if __name__ == '__main__':
    luigi.run()
