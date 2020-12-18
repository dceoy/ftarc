#!/usr/bin/env python

import re
from pathlib import Path

import luigi

from .core import FtarcTask


class SamtoolsView(FtarcTask):
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
        output_sam = Path(self.output_sam_path).resolve()
        return [
            luigi.LocalTarget(output_sam),
            *(
                [
                    luigi.LocalTarget(
                        re.sub(r'\.(cr|b)am$', '.\\1am.\\1ai', str(output_sam))
                    )
                ] if self.index_sam else list()
            )
        ]

    def run(self):
        input_sam = Path(self.input_sam_path).resolve()
        fa = Path(self.fa_path).resolve()
        output_sam = Path(self.output_sam_path).resolve()
        run_id = input_sam.stem
        if self.message:
            message = self.message
        elif input_sam.suffix == output_sam.suffix:
            message = None
        else:
            message = 'Convert {0} to {1}'.format(
                *[s.suffix.upper() for s in [input_sam, output_sam]]
            )
        if message:
            self.print_log(f'{message}:\t{run_id}')
        self.setup_shell(
            run_id=run_id, log_dir_path=self.log_dir_path,
            commands=self.samtools, cwd=output_sam.parent,
            remove_if_failed=self.remove_if_failed, quiet=self.quiet,
            env={'REF_CACHE': '.ref_cache'}
        )
        self.samtools_view(
            input_sam_path=input_sam, fa_path=fa, output_sam_path=output_sam,
            samtools=self.samtools, n_cpu=self.n_cpu, add_args=self.add_args,
            index_sam=self.index_sam
        )
        if self.remove_input:
            self.remove_files_and_dirs(input_sam)


class CollectSamMetricsWithSamtools(FtarcTask):
    input_sam_path = luigi.Parameter()
    fa_path = luigi.Parameter()
    dest_dir_path = luigi.Parameter(default='.')
    samtools_commands = luigi.ListParameter(
        default=['coverage', 'flagstat', 'idxstats', 'stats', 'depth']
    )
    samtools = luigi.Parameter(default='samtools')
    pigz = luigi.Parameter(default='pigz')
    n_cpu = luigi.IntParameter(default=1)
    log_dir_path = luigi.Parameter(default='')
    remove_if_failed = luigi.BoolParameter(default=True)
    quiet = luigi.BoolParameter(default=False)
    priority = 10

    def output(self):
        output_path_prefix = str(
            Path(self.dest_dir_path).resolve().joinpath(
                Path(self.input_sam_path).stem
            )
        )
        return [
            luigi.LocalTarget(
                f'{output_path_prefix}.{c}.txt'
                + ('.gz' if c == 'depth' else '')
            ) for c in self.samtools_commands
        ]

    def run(self):
        input_sam = Path(self.input_sam_path).resolve()
        run_id = input_sam.stem
        self.print_log(f'Collect SAM metrics using Samtools:\t{run_id}')
        fa = Path(self.fa_path).resolve()
        dest_dir = Path(self.dest_dir_path).resolve()
        for c, o in zip(self.samtools_commands, self.output()):
            self.setup_shell(
                run_id=f'{run_id}.{c}', log_dir_path=self.log_dir_path,
                commands=[self.samtools, self.pigz], cwd=dest_dir,
                remove_if_failed=self.remove_if_failed, quiet=self.quiet,
                env={'REF_CACHE': '.ref_cache'}
            )
            p = o.path
            self.run_shell(
                args=(
                    f'set -eo pipefail && {self.samtools} {c}'
                    + (
                        f' --reference {fa}'
                        if c in {'coverage', 'depth', 'stats'} else ''
                    ) + (
                        ' -a' if c == 'depth' else ''
                    ) + (
                        f' -@ {self.n_cpu}'
                        if c in {'flagstat', 'idxstats', 'stats'} else ''
                    )
                    + f' {input_sam}'
                    + (
                        f' | {self.pigz} -p {self.n_cpu} -c - > {p}'
                        if p.endswith('.gz') else f' | tee {p}'
                    )
                ),
                input_files_or_dirs=input_sam, output_files_or_dirs=p
            )


if __name__ == '__main__':
    luigi.run()
