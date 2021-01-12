#!/usr/bin/env python

import re
from itertools import chain
from pathlib import Path

import luigi

from .core import FtarcTask


class CollectFqMetricsWithFastqc(FtarcTask):
    input_fq_paths = luigi.ListParameter()
    dest_dir_path = luigi.Parameter(default='.')
    fastqc = luigi.Parameter(default='fastqc')
    n_cpu = luigi.IntParameter(default=1)
    memory_mb = luigi.FloatParameter(default=4096)
    sh_config = luigi.DictParameter(default=dict())
    priority = 10

    def output(self):
        dest_dir = Path(self.dest_dir_path).resolve()
        fq_stems = [
            re.sub(r'\.(fq|fastq)$', '', Path(p).stem)
            for p in self.input_fq_paths
        ]
        return [
            luigi.LocalTarget(dest_dir.joinpath(n))
            for n in chain.from_iterable(
                [f'{s}_fastqc.{e}' for e in ['html', 'zip']] for s in fq_stems
            )
        ]

    def run(self):
        input_fqs = [Path(p).resolve() for p in self.input_fq_paths]
        run_id = Path(Path(Path(self.input_fq_paths[0]).stem).stem).stem
        self.print_log(f'Collect FASTQ metrics using FastQC:\t{run_id}')
        dest_dir = Path(self.dest_dir_path).resolve()
        self.setup_shell(
            run_id=run_id, commands=self.fastqc, cwd=dest_dir,
            **self.sh_config,
            env={'JAVA_TOOL_OPTIONS': '-Xmx{}m'.format(int(self.memory_mb))}
        )
        self.run_shell(
            args=(
                f'set -e && {self.fastqc} --nogroup'
                + f' --threads {self.n_cpu} --outdir {dest_dir}'
                + ''.join(f' {f}' for f in input_fqs)
            ),
            input_files_or_dirs=input_fqs,
            output_files_or_dirs=[o.path for o in self.output()]
        )
        tmp_dir = dest_dir.joinpath('?')
        self.remove_files_and_dirs(tmp_dir)


if __name__ == '__main__':
    luigi.run()
