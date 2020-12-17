#!/usr/bin/env python

from pathlib import Path

import luigi
from luigi.util import requires

from .core import FtarcTask
from .resource import FetchReferenceFASTA
from .samtools import samtools_index
from .trimgalore import PrepareFASTQs


@requires(FetchReferenceFASTA)
class CreateBWAIndices(FtarcTask):
    cf = luigi.DictParameter()
    priority = 100

    def output(self):
        fa_path = self.input()[0].path
        return [
            luigi.LocalTarget(f'{fa_path}.{s}') for s in (
                ['0123', 'amb', 'ann', 'pac', 'bwt.2bit.64', 'bwt.8bit.32']
                if self.cf['use_bwa_mem2'] else
                ['pac', 'bwt', 'ann', 'amb', 'sa']
            )
        ]

    def run(self):
        fa = Path(self.input()[0].path)
        run_id = fa.stem
        self.print_log(f'Create BWA indices:\t{run_id}')
        bwa = self.cf['bwa']
        self.setup_shell(
            run_id=run_id, log_dir_path=self.cf['log_dir_path'],
            commands=bwa, cwd=fa.parent,
            remove_if_failed=self.cf['remove_if_failed'],
            quiet=self.cf['quiet']
        )
        self.run_shell(
            args=f'set -e && {bwa} index {fa}', input_files_or_dirs=fa,
            output_files_or_dirs=[o.path for o in self.output()]
        )


@requires(PrepareFASTQs, FetchReferenceFASTA, CreateBWAIndices)
class AlignReads(FtarcTask):
    sample_name = luigi.Parameter()
    read_group = luigi.DictParameter()
    cf = luigi.DictParameter()
    priority = 70

    def output(self):
        output_cram = Path(self.cf['align_dir_path']).joinpath(
            '{0}/{0}{1}.{2}.cram'.format(
                self.sample_name,
                ('.trim' if self.cf['adapter_removal'] else ''),
                (
                    self.cf['reference_name']
                    or Path(self.input()[1][0].path).stem
                )
            )
        )
        return [luigi.LocalTarget(f'{output_cram}{s}') for s in ['', '.crai']]

    def run(self):
        output_cram = Path(self.output()[0].path)
        run_id = output_cram.stem
        self.print_log(f'Align reads:\t{run_id}')
        bwa = self.cf['bwa']
        samtools = self.cf['samtools']
        n_cpu = self.cf['n_cpu_per_worker']
        memory_mb_per_thread = int(self.cf['memory_mb_per_worker'] / n_cpu / 8)
        fq_paths = [i.path for i in self.input()[0]]
        rg = '\\t'.join(
            [
                '@RG',
                'ID:{}'.format(self.read_group.get('ID') or 1),
                'PU:{}'.format(self.read_group.get('PU') or 'UNIT-1'),
                'SM:{}'.format(self.read_group.get('SM') or self.sample_name),
                'PL:{}'.format(self.read_group.get('PL') or 'ILLUMINA'),
                'LB:{}'.format(self.read_group.get('LB') or 'LIBRARY-1')
            ] + [
                f'{k}:{v}' for k, v in self.read_group.items()
                if k not in ['ID', 'PU', 'SM', 'PL', 'LB']
            ]
        )
        fa_path = self.input()[1][0].path
        index_paths = [o.path for o in self.input()[2]]
        self.setup_shell(
            run_id=run_id, log_dir_path=self.cf['log_dir_path'],
            commands=[bwa, samtools], cwd=output_cram.parent,
            remove_if_failed=self.cf['remove_if_failed'],
            quiet=self.cf['quiet'], env={'REF_CACHE': '.ref_cache'}
        )
        self.run_shell(
            args=(
                'set -eo pipefail && '
                + f'{bwa} mem -t {n_cpu} -R \'{rg}\' -T 0 -P {fa_path}'
                + ''.join([f' {a}' for a in fq_paths])
                + f' | {samtools} sort -@ {n_cpu} -m {memory_mb_per_thread}M'
                + f' -O BAM -l 0 -T {output_cram}.sort -'
                + f' | {samtools} view -@ {n_cpu} -T {fa_path} -CS'
                + f' -o {output_cram} -'
            ),
            input_files_or_dirs=[fa_path, *index_paths, *fq_paths],
            output_files_or_dirs=[output_cram, output_cram.parent]
        )
        samtools_index(
            shelltask=self, samtools=samtools, sam_path=output_cram,
            n_cpu=n_cpu
        )


if __name__ == '__main__':
    luigi.run()
