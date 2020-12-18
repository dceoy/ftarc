#!/usr/bin/env python

import re
from pathlib import Path

import luigi

from .bwa import CreateBwaIndices
from .core import FtarcTask
from .picard import CreateSequenceDictionary
from .resource import FetchResourceVcf


class DownloadAndProcessResourceFiles(luigi.Task):
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
    gatk = luigi.Parameter(default='gatk')
    n_cpu = luigi.IntParameter(default=1)
    memory_mb = luigi.FloatParameter(default=4096)
    use_bwa_mem2 = luigi.BoolParameter(default=False)
    sh_config = luigi.DictParameter(default=dict())
    priority = 10

    def requires(self):
        return DownloadResourceFiles(
            src_urls=[
                self.ref_fa_url, self.ref_fa_alt_url, self.dbsnp_vcf_url,
                self.mills_indel_vcf_url, self.known_indel_vcf_url
            ],
            dest_dir_path=self.dest_dir_path, n_cpu=self.n_cpu,
            wget=self.wget, bgzip=self.bgzip, sh_config=self.sh_config
        )

    def output(self):
        dest_dir = Path(self.dest_dir_path).resolve()
        bwa_suffixes = (
            ['0123', 'amb', 'ann', 'pac', 'bwt.2bit.64', 'bwt.8bit.32']
            if self.use_bwa_mem2 else ['pac', 'bwt', 'ann', 'amb', 'sa']
        )
        for u in [self.ref_fa_url, self.ref_fa_alt_url, self.dbsnp_vcf_url,
                  self.mills_indel_vcf_url, self.known_indel_vcf_url]:
            file = dest_dir.joinpath(
                Path(u).name + ('.gz' if u.endswith('.vcf') else '')
            )
            if file.name.endswith('.fa'):
                for p in [file, f'{file}.fai', (Path(file).stem + '.dict'),
                          *[f'{file}.{s}' for s in bwa_suffixes]]:
                    yield luigi.LocalTarget(p)
            elif file.name.endswith('.vcf.gz'):
                for p in [file, f'{file}.tbi']:
                    yield luigi.LocalTarget(p)
            else:
                yield luigi.LocalTarget(file)

    def run(self):
        input_paths = [str(Path(i.path).resolve()) for i in self.input()]
        cf = {
            'pigz': self.pigz, 'pbzip2': self.pbzip2, 'bgzip': self.bgzip,
            'bwa': self.bwa, 'samtools': self.samtools, 'tabix': self.tabix,
            'gatk': self.gatk, 'n_cpu_per_worker': self.n_cpu,
            'memory_mb_per_worker': self.memory_mb,
            'use_bwa_mem2': self.use_bwa_mem2,
            'ref_dir_path': Path(self.dest_dir_path).resolve(),
            'sh_config': self.sh_config
        }
        yield [
            CreateSequenceDictionary(ref_fa_path=input_paths[0], cf=cf),
            CreateBwaIndices(ref_fa_path=input_paths[0], cf=cf),
            *[FetchResourceVcf(src_path=p, cf=cf) for p in input_paths[2:]]
        ]


class DownloadResourceFiles(FtarcTask):
    src_urls = luigi.ListParameter()
    dest_dir_path = luigi.Parameter(default='.')
    run_id = luigi.Parameter(default='data')
    n_cpu = luigi.IntParameter(default=1)
    wget = luigi.Parameter(default='wget')
    bgzip = luigi.Parameter(default='bgzip')
    sh_config = luigi.DictParameter(default=dict())
    priority = 10

    def output(self):
        dest_dir = Path(self.dest_dir_path)
        for u in self.src_urls:
            p = str(dest_dir.joinpath(Path(u).name))
            if u.endswith('.bgz'):
                yield luigi.LocalTarget(re.sub(r'\.bgz$', '.gz', p))
            elif u.endswith(('.vcf', '.bed')):
                yield luigi.LocalTarget(f'{p}.gz')
            else:
                yield luigi.LocalTarget(p)

    def run(self):
        dest_dir = Path(self.dest_dir_path).resolve()
        self.print_log(f'Download resource files:\t{dest_dir}')
        self.setup_shell(
            run_id=self.run_id, commands=[self.wget, self.bgzip], cwd=dest_dir,
            **self.sh_config
        )
        for u, o in zip(self.src_urls, self.output()):
            t = dest_dir.joinpath(
                (Path(u).stem + '.gz') if u.endswith('.bgz') else Path(u).name
            )
            p = o.path
            self.run_shell(
                args=f'set -e && {self.wget} -qSL {u} -O {t}',
                output_files_or_dirs=t
            )
            if t == p:
                pass
            elif p.endswith('.gz'):
                self.run_shell(
                    args=f'set -e && {self.bgzip} -@ {self.n_cpu} {t}',
                    input_files_or_dirs=t, output_files_or_dirs=p
                )


if __name__ == '__main__':
    luigi.run()
