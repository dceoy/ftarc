#!/usr/bin/env python

import re
from pathlib import Path

import luigi
from luigi.util import requires

from .base import ShellTask
from .samtools import samtools_faidx, tabix_tbi


class FetchReferenceFASTA(luigi.WrapperTask):
    ref_fa_path = luigi.Parameter()
    cf = luigi.DictParameter()
    priority = 100

    def requires(self):
        return FetchResourceFASTA(src_path=self.ref_fa_path, cf=self.cf)

    def output(self):
        return self.input()


class FetchResourceFile(ShellTask):
    src_path = luigi.Parameter()
    cf = luigi.DictParameter()
    priority = 70

    def output(self):
        return luigi.LocalTarget(
            (
                Path(self.cf['ref_dir_path']) if self.cf.get('ref_dir_path')
                else Path(self.src_path).parent
            ).joinpath(re.sub(r'\.(gz|bz2)$', '', Path(self.src_path).name))
        )

    def run(self):
        dest_file = Path(self.output().path)
        run_id = dest_file.stem
        self.print_log(f'Create a resource:\t{run_id}')
        pigz = self.cf['pigz']
        pbzip2 = self.cf['pbzip2']
        n_cpu = self.cf['n_cpu_per_worker']
        self.setup_shell(
            run_id=run_id, log_dir_path=self.cf['log_dir_path'],
            commands=[pigz, pbzip2], cwd=dest_file.parent,
            remove_if_failed=self.cf['remove_if_failed'],
            quiet=self.cf['quiet']
        )
        if self.src_path.endswith('.gz'):
            a = f'{pigz} -p {n_cpu} -dc {self.src_path} > {dest_file}'
        elif self.src_path.endswith('.bz2'):
            a = f'{pbzip2} -p{n_cpu} -dc {self.src_path} > {dest_file}'
        else:
            a = f'cp {self.src_path} {dest_file}'
        self.run_shell(
            args=f'set -e && {a}', input_files_or_dirs=self.src_path,
            output_files_or_dirs=dest_file
        )


@requires(FetchResourceFile)
class FetchResourceFASTA(ShellTask):
    cf = luigi.DictParameter()
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
            run_id=run_id, log_dir_path=self.cf['log_dir_path'],
            commands=samtools, cwd=fa.parent,
            remove_if_failed=self.cf['remove_if_failed'],
            quiet=self.cf['quiet']
        )
        samtools_faidx(shelltask=self, samtools=samtools, fa_path=fa)


class FetchResourceVCF(ShellTask):
    src_path = luigi.ListParameter()
    cf = luigi.DictParameter()
    priority = 70

    def output(self):
        dest_vcf = (
            Path(self.cf['ref_dir_path']) if self.cf.get('ref_dir_path')
            else Path(self.src_path).parent
        ).joinpath(re.sub(r'\.(gz|bgz)$', '.gz', Path(self.src_path).name))
        return [luigi.LocalTarget(f'{dest_vcf}{s}') for s in ['', '.tbi']]

    def run(self):
        dest_vcf = Path(self.output()[0].path)
        run_id = Path(dest_vcf.stem).stem
        self.print_log(f'Create a VCF:\t{run_id}')
        bgzip = self.cf['bgzip']
        tabix = self.cf['tabix']
        n_cpu = self.cf['n_cpu_per_worker']
        self.setup_shell(
            run_id=run_id, log_dir_path=self.cf['log_dir_path'],
            commands=[bgzip, tabix], cwd=dest_vcf.parent,
            remove_if_failed=self.cf['remove_if_failed'],
            quiet=self.cf['quiet']
        )
        self.run_shell(
            args=(
                'set -e && ' + (
                    f'cp {self.src_path} {dest_vcf}'
                    if self.src_path.endswith(('.gz', '.bgz')) else
                    f'{bgzip} -@ {n_cpu} -c {self.src_path} > {dest_vcf}'
                )
            ),
            input_files_or_dirs=self.src_path, output_files_or_dirs=dest_vcf
        )
        tabix_tbi(shelltask=self, tabix=tabix, tsv_path=dest_vcf, preset='vcf')


class FetchDbsnpVCF(luigi.WrapperTask):
    dbsnp_vcf_path = luigi.Parameter()
    cf = luigi.DictParameter()
    priority = 70

    def requires(self):
        return FetchResourceVCF(src_path=self.dbsnp_vcf_path, cf=self.cf)

    def output(self):
        return self.input()


class FetchMillsIndelVCF(luigi.WrapperTask):
    mills_indel_vcf_path = luigi.Parameter()
    cf = luigi.DictParameter()
    priority = 70

    def requires(self):
        return FetchResourceVCF(src_path=self.mills_indel_vcf_path, cf=self.cf)

    def output(self):
        return self.input()


class FetchKnownIndelVCF(luigi.WrapperTask):
    known_indel_vcf_path = luigi.Parameter()
    cf = luigi.DictParameter()
    priority = 70

    def requires(self):
        return FetchResourceVCF(src_path=self.known_indel_vcf_path, cf=self.cf)

    def output(self):
        return self.input()


if __name__ == '__main__':
    luigi.run()
