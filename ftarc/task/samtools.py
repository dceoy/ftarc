#!/usr/bin/env python

import re
from pathlib import Path

import luigi

from .core import FtarcTask


class SamtoolsView(FtarcTask):
    input_sam_path = luigi.Parameter()
    fa_path = luigi.Parameter()
    output_sam_path = luigi.Parameter()
    samtools = luigi.Parameter(default='samtools')
    n_cpu = luigi.IntParameter(default=1)
    add_args = luigi.Parameter(default='')
    message = luigi.Parameter(default='')
    remove_input = luigi.BoolParameter(default=True)
    index_sam = luigi.BoolParameter(default=False)
    sh_config = luigi.DictParameter(default=dict())
    priority = 90

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
        target_sam = Path(self.input_sam_path)
        run_id = target_sam.stem
        input_sam = target_sam.resolve()
        fa = Path(self.fa_path).resolve()
        output_sam = Path(self.output_sam_path).resolve()
        only_index = (
            self.input_sam_path == self.output_sam_path and self.index_sam
        )
        if self.message:
            message = self.message
        elif only_index:
            message = 'Index {}'.format(input_sam.suffix.upper())
        elif input_sam.suffix == output_sam.suffix:
            message = None
        else:
            message = 'Convert {0} to {1}'.format(
                *[s.suffix.upper() for s in [input_sam, output_sam]]
            )
        if message:
            self.print_log(f'{message}:\t{run_id}')
        dest_dir = output_sam.parent
        self.setup_shell(
            run_id=run_id, commands=self.samtools, cwd=dest_dir,
            **self.sh_config,
            env={'REF_CACHE': str(dest_dir.joinpath('.ref_cache'))}
        )
        if only_index:
            self.samtools_index(
                sam_path=input_sam, samtools=self.samtools, n_cpu=self.n_cpu
            )
        else:
            self.samtools_view(
                input_sam_path=input_sam, fa_path=fa,
                output_sam_path=output_sam, samtools=self.samtools,
                n_cpu=self.n_cpu, add_args=self.add_args,
                index_sam=self.index_sam, remove_input=self.remove_input
            )


class RemoveDuplicates(luigi.WrapperTask):
    input_sam_path = luigi.Parameter()
    fa_path = luigi.Parameter()
    dest_dir_path = luigi.Parameter(default='.')
    samtools = luigi.Parameter(default='samtools')
    n_cpu = luigi.IntParameter(default=1)
    remove_input = luigi.BoolParameter(default=False)
    index_sam = luigi.BoolParameter(default=True)
    sh_config = luigi.DictParameter(default=dict())
    priority = 90

    def requires(self):
        return SamtoolsView(
            input_sam_path=str(Path(self.input_sam_path).resolve()),
            fa_path=str(Path(self.fa_path).resolve()),
            output_sam_path=str(
                Path(self.dest_dir_path).resolve().joinpath(
                    Path(self.input_sam_path).stem + '.dedup.cram'
                )
            ),
            samtools=self.samtools, n_cpu=self.n_cpu, add_args='-F 1024',
            message='Remove duplicates', remove_input=self.remove_input,
            index_sam=self.index_sam, sh_config=self.sh_config
        )

    def output(self):
        return self.input()


class CollectSamMetricsWithSamtools(FtarcTask):
    input_sam_path = luigi.Parameter()
    fa_path = luigi.Parameter(default='')
    dest_dir_path = luigi.Parameter(default='.')
    samtools_commands = luigi.ListParameter(
        default=['coverage', 'flagstat', 'idxstats', 'stats']
    )
    samtools = luigi.Parameter(default='samtools')
    plot_bamstats = luigi.Parameter(default='plot-bamstats')
    gnuplot = luigi.Parameter(default='gnuplot')
    n_cpu = luigi.IntParameter(default=1)
    sh_config = luigi.DictParameter(default=dict())
    priority = 10

    def output(self):
        sam_name = Path(self.input_sam_path).name
        dest_dir = Path(self.dest_dir_path).resolve()
        return (
            [
                luigi.LocalTarget(dest_dir.joinpath(f'{sam_name}.{c}.txt'))
                for c in self.samtools_commands
            ] + (
                [luigi.LocalTarget(dest_dir.joinpath(f'{sam_name}.stats'))]
                if 'stats' in self.samtools_commands else list()
            )
        )

    def run(self):
        target_sam = Path(self.input_sam_path)
        run_id = target_sam.name
        self.print_log(f'Collect SAM metrics using Samtools:\t{run_id}')
        input_sam = target_sam.resolve()
        fa = (Path(self.fa_path).resolve() if self.fa_path else None)
        dest_dir = Path(self.dest_dir_path).resolve()
        output_txts = [
            Path(o.path) for o in self.output() if o.path.endswith('.txt')
        ]
        ref_cache = str(input_sam.parent.joinpath('.ref_cache'))
        for t in output_txts:
            cmd = t.stem.split('.')[-1]
            self.setup_shell(
                run_id=f'{run_id}.{cmd}',
                commands=(
                    [self.samtools, self.gnuplot] if cmd == 'stats'
                    else self.samtools
                ),
                cwd=dest_dir, **self.sh_config, env={'REF_CACHE': ref_cache}
            )
            self.run_shell(
                args=(
                    f'set -eo pipefail && {self.samtools} {cmd}'
                    + (
                        f' --reference {fa}' if (
                            fa is not None
                            and cmd in {'coverage', 'depth', 'stats'}
                        ) else ''
                    )
                    + (' -a' if cmd == 'depth' else '')
                    + (
                        f' -@ {self.n_cpu}'
                        if cmd in {'flagstat', 'idxstats', 'stats'} else ''
                    )
                    + f' {input_sam} | tee {t}'
                ),
                input_files_or_dirs=input_sam, output_files_or_dirs=t
            )
            if cmd == 'stats':
                plot_dir = t.parent.joinpath(t.stem)
                self.run_shell(
                    args=(
                        f'set -e && {self.plot_bamstats}'
                        + f' --prefix {plot_dir}/index {t}'
                    ),
                    input_files_or_dirs=t,
                    output_files_or_dirs=[
                        plot_dir, plot_dir.joinpath('index.html')
                    ]
                )


if __name__ == '__main__':
    luigi.run()
