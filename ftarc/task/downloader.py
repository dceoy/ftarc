#!/usr/bin/env python

import re
from pathlib import Path

import luigi
from luigi.util import requires

from .bwa import CreateBwaIndices
from .core import FtarcTask
from .picard import CreateSequenceDictionary
from .resource import FetchResourceVcf


class DownloadResourceFiles(FtarcTask):
    src_urls = luigi.ListParameter()
    dest_dir_path = luigi.Parameter(default='.')
    run_id = luigi.Parameter(default='data')
    wget = luigi.Parameter(default='wget')
    bgzip = luigi.Parameter(default='bgzip')
    n_cpu = luigi.IntParameter(default=1)
    sh_config = luigi.DictParameter(default=dict())
    priority = 10

    def output(self):
        dest_dir = Path(self.dest_dir_path).resolve()
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
            self.run_shell(
                args=f'set -e && {self.wget} -qSL -O {t} {u}',
                output_files_or_dirs=t
            )
            if t.suffix != '.gz' and o.path.endswith('.gz'):
                self.run_shell(
                    args=f'set -e && {self.bgzip} -@ {self.n_cpu} {t}',
                    input_files_or_dirs=t, output_files_or_dirs=o.path
                )


@requires(DownloadResourceFiles)
class DownloadAndProcessResourceFiles(luigi.Task):
    dest_dir_path = luigi.Parameter(default='.')
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

    def output(self):
        bwa_suffixes = (
            ['0123', 'amb', 'ann', 'pac', 'bwt.2bit.64', 'bwt.8bit.32']
            if self.use_bwa_mem2 else ['pac', 'bwt', 'ann', 'amb', 'sa']
        )
        for i in self.input():
            file = Path(i.path)
            if file.name.endswith(('.fa', '.fasta')):
                for p in [file, f'{file}.fai',
                          file.parent.joinpath(f'{file.stem}.dict'),
                          *[f'{file}.{s}' for s in bwa_suffixes]]:
                    yield luigi.LocalTarget(p)
            elif file.name.endswith('.vcf.gz'):
                for p in [file, f'{file}.tbi']:
                    yield luigi.LocalTarget(p)
            else:
                yield luigi.LocalTarget(file)

    def run(self):
        common_kwargs = {
            'cf': {
                'pigz': self.pigz, 'pbzip2': self.pbzip2, 'bgzip': self.bgzip,
                'bwa': self.bwa, 'samtools': self.samtools,
                'tabix': self.tabix, 'gatk': self.gatk,
                'n_cpu_per_worker': self.n_cpu,
                'memory_mb_per_worker': self.memory_mb,
                'use_bwa_mem2': self.use_bwa_mem2, 'sh_config': self.sh_config
            },
            'sh_config': self.sh_config
        }
        for i in self.input():
            p = i.path
            if p.endswith(('.fa', '.fasta')):
                yield [
                    CreateSequenceDictionary(ref_fa_path=p, **common_kwargs),
                    CreateBwaIndices(ref_fa_path=p, **common_kwargs)
                ]
            elif p.endswith('.vcf.gz'):
                yield FetchResourceVcf(src_path=p, **common_kwargs)


if __name__ == '__main__':
    luigi.run()
