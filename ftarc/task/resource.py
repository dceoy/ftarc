#!/usr/bin/env python

import re
from pathlib import Path

import luigi
from luigi.util import requires

from .core import FtarcTask


class FetchReferenceFasta(luigi.WrapperTask):
    ref_fa_path = luigi.Parameter()
    cf = luigi.DictParameter()
    sh_config = luigi.DictParameter(default=dict())
    priority = 100

    def requires(self):
        return FetchResourceFasta(
            src_path=self.ref_fa_path, cf=self.cf, sh_config=self.sh_config
        )

    def output(self):
        return self.input()


class FetchResourceFile(FtarcTask):
    src_path = luigi.Parameter()
    cf = luigi.DictParameter()
    n_cpu = luigi.IntParameter(default=1)
    sh_config = luigi.DictParameter(default=dict())
    priority = 70

    def output(self):
        return luigi.LocalTarget(
            Path(self.src_path).parent.joinpath(
                re.sub(r'\.(gz|bz2)$', '', Path(self.src_path).name)
            )
        )

    def run(self):
        dest_file = Path(self.output().path)
        run_id = dest_file.stem
        self.print_log(f'Create a resource:\t{run_id}')
        pigz = self.cf['pigz']
        pbzip2 = self.cf['pbzip2']
        self.setup_shell(
            run_id=run_id, commands=[pigz, pbzip2], cwd=dest_file.parent,
            **self.sh_config
        )
        if self.src_path.endswith('.gz'):
            a = f'{pigz} -p {self.n_cpu} -dc {self.src_path} > {dest_file}'
        elif self.src_path.endswith('.bz2'):
            a = f'{pbzip2} -p{self.n_cpu} -dc {self.src_path} > {dest_file}'
        else:
            a = f'cp {self.src_path} {dest_file}'
        self.run_shell(
            args=f'set -e && {a}', input_files_or_dirs=self.src_path,
            output_files_or_dirs=dest_file
        )


@requires(FetchResourceFile)
class FetchResourceFasta(FtarcTask):
    cf = luigi.DictParameter()
    sh_config = luigi.DictParameter(default=dict())
    priority = 70

    def output(self):
        fa_path = self.input().path
        return [luigi.LocalTarget(fa_path + s) for s in ['', '.fai']]

    def run(self):
        fa = Path(self.input().path)
        run_id = fa.stem
        self.print_log(f'Index FASTA:\t{run_id}')
        samtools = self.cf['samtools']
        self.setup_shell(
            run_id=run_id,  commands=samtools, cwd=fa.parent, **self.sh_config
        )
        self.run_shell(
            args=f'set -e && {samtools} faidx {fa}',
            input_files_or_dirs=fa, output_files_or_dirs=f'{fa}.fai'
        )


class FetchResourceVcf(FtarcTask):
    src_path = luigi.Parameter()
    cf = luigi.DictParameter()
    n_cpu = luigi.IntParameter(default=1)
    sh_config = luigi.DictParameter(default=dict())
    priority = 70

    def output(self):
        dest_vcf = Path(self.src_path).parent.joinpath(
            re.sub(r'\.(gz|bgz)$', '.gz', Path(self.src_path).name)
        )
        return [luigi.LocalTarget(f'{dest_vcf}{s}') for s in ['', '.tbi']]

    def run(self):
        dest_vcf = Path(self.output()[0].path)
        run_id = Path(dest_vcf.stem).stem
        self.print_log(f'Create a VCF:\t{run_id}')
        bgzip = self.cf['bgzip']
        tabix = self.cf['tabix']
        self.setup_shell(
            run_id=run_id, commands=[bgzip, tabix], cwd=dest_vcf.parent,
            **self.sh_config
        )
        self.run_shell(
            args=(
                'set -e && ' + (
                    f'cp {self.src_path} {dest_vcf}'
                    if self.src_path.endswith(('.gz', '.bgz')) else
                    f'{bgzip} -@ {self.n_cpu} -c {self.src_path} > {dest_vcf}'
                )
            ),
            input_files_or_dirs=self.src_path, output_files_or_dirs=dest_vcf
        )
        self.run_shell(
            args=f'set -e && {tabix} --preset vcf {dest_vcf}',
            input_files_or_dirs=dest_vcf,
            output_files_or_dirs=f'{dest_vcf}.tbi'
        )


class FetchKnownSitesVcfs(luigi.WrapperTask):
    known_sites_vcf_paths = luigi.ListParameter()
    sh_config = luigi.DictParameter(default=dict())
    cf = luigi.DictParameter()
    priority = 70

    def requires(self):
        return [
            FetchResourceVcf(src_path=p, cf=self.cf, sh_config=self.sh_config)
            for p in self.known_sites_vcf_paths
        ]

    def output(self):
        return self.input()


if __name__ == '__main__':
    luigi.run()
