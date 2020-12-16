#!/usr/bin/env python

import re
from itertools import chain
from pathlib import Path

import luigi

from .base import ShellTask


class CollectFqMetricsWithFastqc(ShellTask):
    input_fq_paths = luigi.ListParameter()
    dest_dir_path = luigi.Parameter(default='.')
    fastqc = luigi.Parameter(default='fastqc')
    n_cpu = luigi.IntParameter(default=1)
    memory_mb = luigi.FloatParameter(default=4096)
    log_dir_path = luigi.Parameter(default='')
    remove_if_failed = luigi.BoolParameter(default=True)
    quiet = luigi.BoolParameter(default=False)
    priority = 10

    def output(self):
        dest_dir = Path(self.dest_dir_path)
        fq_stems = [
            re.sub(r'\.(fq|fastq)$', '', Path(p).stem)
            for p in self.input_fq_paths
        ]
        return [
            dest_dir.joinpath(n) for n in chain.from_iterable(
                [f'{s}_fastqc.{e}' for e in ['html', 'zip']] for s in fq_stems
            )
        ]

    def run(self):
        run_id = self.sample_name
        self.print_log(f'Collect FASTQ metrics using FastQC:\t{run_id}')
        output_file_paths = [o.path for o in self.output()]
        self.setup_shell(
            run_id=run_id, log_dir_path=(self.log_dir_path or None),
            commands=self.picard, cwd=self.dest_dir_path,
            remove_if_failed=self.remove_if_failed, quiet=self.quiet,
            env={'JAVA_TOOL_OPTIONS': '-Xmx{}m'.format(int(self.memory_mb))}
        )
        self.run_shell(
            args=(
                f'set -e && {self.fastqc} --nogroup'
                + f' --threads {self.n_cpu} --outdir {self.dest_dir_path}'
                + ''.join([f' {p}' for p in self.input_fq_paths])
            ),
            input_files_or_dirs=self.input_fq_paths,
            output_files_or_dirs=output_file_paths
        )


if __name__ == '__main__':
    luigi.run()
