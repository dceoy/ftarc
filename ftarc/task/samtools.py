#!/usr/bin/env python

import re
from pathlib import Path

import luigi

from .base import ShellTask


class SamtoolsView(ShellTask):
    input_sam_path = luigi.Parameter()
    output_sam_path = luigi.Parameter()
    fa_path = luigi.Parameter()
    samtools = luigi.Parameter()
    n_cpu = luigi.IntParameter(default=1)
    add_args = luigi.Parameter(default='')
    message = luigi.Parameter(default='')
    remove_input = luigi.BoolParameter(default=True)
    index_sam = luigi.BoolParameter(default=False)
    log_dir_path = luigi.Parameter(default='')
    remove_if_failed = luigi.BoolParameter(default=True)
    quiet = luigi.BoolParameter(default=False)
    priority = 100

    def output(self):
        return [
            luigi.LocalTarget(self.output_sam_path),
            *(
                [
                    luigi.LocalTarget(
                        re.sub(
                            r'\.(cr|b)am$', '.\\1am.\\1ai',
                            self.output_sam_path
                        )
                    )
                ] if self.index_sam else list()
            )
        ]

    def run(self):
        run_id = Path(self.input_sam_path).stem
        if self.message:
            message = self.message
        elif (self.input_sam_path.endswith('.bam')
              and self.output_sam_path.endswith('.cram')):
            message = 'Convert BAM to CRAM'
        elif (self.input_sam_path.endswith('.cram')
              and self.output_sam_path.endswith('.bam')):
            message = 'Convert CRAM to BAM'
        else:
            message = None
        if message:
            self.print_log(f'{message}:\t{run_id}')
        self.setup_shell(
            run_id=run_id, log_dir_path=(self.log_dir_path or None),
            commands=self.samtools, cwd=Path(self.output_sam_path).parent,
            remove_if_failed=self.remove_if_failed, quiet=self.quiet,
            env={'REF_CACHE': '.ref_cache'}
        )
        if self.index_sam:
            samtools_view_and_index(
                shelltask=self, samtools=self.samtools,
                input_sam_path=self.input_sam_path, fa_path=self.fa_path,
                output_sam_path=self.output_sam_path, n_cpu=self.n_cpu,
                add_args=self.add_args
            )
        else:
            samtools_view(
                shelltask=self, samtools=self.samtools,
                input_sam_path=self.input_sam_path, fa_path=self.fa_path,
                output_sam_path=self.output_sam_path, n_cpu=self.n_cpu,
                add_args=self.add_args
            )
        if self.remove_input:
            self.run_shell(
                args=f'rm -f {self.input_sam_path}',
                input_files_or_dirs=self.input_sam_path
            )


class CollectSamMetricsWithSamtools(ShellTask):
    input_sam_path = luigi.Parameter()
    fa_path = luigi.Parameter()
    dest_dir_path = luigi.Parameter(default='.')
    samtools = luigi.Parameter(default='samtools')
    pigz = luigi.Parameter(default='pigz')
    n_cpu = luigi.IntParameter(default=1)
    samtools_commands = luigi.ListParameter(
        default=['coverage', 'depth', 'flagstat', 'idxstats', 'stats']
    )
    log_dir_path = luigi.Parameter(default='')
    remove_if_failed = luigi.BoolParameter(default=True)
    quiet = luigi.BoolParameter(default=False)
    priority = 10

    def output(self):
        output_path_prefix = str(
            Path(self.dest_dir_path).joinpath(Path(self.input_sam_path).stem)
        )
        return [
            luigi.LocalTarget(
                f'{output_path_prefix}.{c}.txt'
                + ('.gz' if c in {'depth', 'stats'} else '')
            ) for c in self.samtools_commands
        ]

    def run(self):
        input_sam = Path(self.input_sam_path)
        run_id = input_sam.stem
        self.print_log(f'Collect SAM metrics using Samtools:\t{run_id}')
        output_file_paths = [o.path for o in self.output()]
        self.setup_shell(
            run_id=run_id, log_dir_path=(self.log_dir_path or None),
            commands=[self.samtools, self.pigz], cwd=self.dest_dir_path,
            remove_if_failed=self.remove_if_failed, quiet=self.quiet,
            env={'REF_CACHE': '.ref_cache'}
        )
        for c, p in zip(self.samtools_commands, output_file_paths):
            self.run_shell(
                args=(
                    f'set -e && {self.samtools} {c}'
                    + (
                        f' --reference {self.fa_path}'
                        if c in {'coverage', 'depth', 'stats'} else ''
                    ) + (
                        ' -a' if c == 'depth' else ''
                    ) + (
                        f' -@ {self.n_cpu}'
                        if c in {'flagstat', 'idxstats', 'stats'} else ''
                    )
                    + (
                        f' {input_sam} | {self.pigz} -p {self.n_cpu} -c -'
                        if p.endswith('.gz') else f' {input_sam}'
                    )
                    + f' > {p}'
                ),
                input_files_or_dirs=input_sam, output_files_or_dirs=p
            )


def samtools_faidx(shelltask, samtools, fa_path):
    shelltask.run_shell(
        args=f'set -e && {samtools} faidx {fa_path}',
        input_files_or_dirs=fa_path, output_files_or_dirs=f'{fa_path}.fai'
    )


def samtools_index(shelltask, samtools, sam_path, n_cpu=1):
    shelltask.run_shell(
        args=(
            f'set -e && {samtools} quickcheck -v {sam_path}'
            + f' && {samtools} index -@ {n_cpu} {sam_path}'
        ),
        input_files_or_dirs=sam_path,
        output_files_or_dirs=re.sub(
            r'\.(cr|b)am$', '.\\1am.\\1ai', str(sam_path)
        )
    )


def samtools_view_and_index(shelltask, samtools, input_sam_path, fa_path,
                            output_sam_path, n_cpu=1, add_args=None):
    samtools_view(
        shelltask=shelltask, samtools=samtools, input_sam_path=input_sam_path,
        fa_path=fa_path, output_sam_path=output_sam_path, n_cpu=n_cpu,
        add_args=add_args
    )
    samtools_index(
        shelltask=shelltask, samtools=samtools, sam_path=output_sam_path,
        n_cpu=n_cpu
    )


def samtools_view(shelltask, samtools, input_sam_path, fa_path,
                  output_sam_path, n_cpu=1, add_args=None):
    shelltask.run_shell(
        args=(
            f'set -e && {samtools} quickcheck -v {input_sam_path}'
            + f' && {samtools} view -@ {n_cpu} -T {fa_path}'
            + ' -{0}S{1}'.format(
                ('C' if output_sam_path.endswith('.cram') else 'b'),
                (f' {add_args}' if add_args else '')
            )
            + f' -o {output_sam_path} {input_sam_path}'
        ),
        input_files_or_dirs=[
            input_sam_path, fa_path, f'{fa_path}.fai'
        ],
        output_files_or_dirs=output_sam_path
    )


def tabix_tbi(shelltask, tabix, tsv_path, preset='vcf'):
    shelltask.run_shell(
        args=f'set -e && {tabix} --preset {preset} {tsv_path}',
        input_files_or_dirs=tsv_path, output_files_or_dirs=f'{tsv_path}.tbi'
    )


if __name__ == '__main__':
    luigi.run()
