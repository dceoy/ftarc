#!/usr/bin/env python

import re
from pathlib import Path

import luigi

from .core import FtarcTask


class PrepareFastqs(luigi.Task):
    fq_paths = luigi.ListParameter()
    sample_name = luigi.Parameter()
    cf = luigi.DictParameter()
    priority = 50

    def output(self):
        if self.cf['adapter_removal']:
            dest_dir = Path(self.cf['trim_dir_path']).joinpath(
                self.sample_name
            )
            return [
                luigi.LocalTarget(o) for o in _generate_trimmed_fqs(
                    raw_fq_paths=self.fq_paths, dest_dir_path=str(dest_dir)
                )
            ]
        else:
            dest_dir = Path(self.cf['align_dir_path']).joinpath(
                self.sample_name
            )
            return [
                luigi.LocalTarget(
                    dest_dir.joinpath(Path(p).stem + '.gz')
                    if p.endswith('.bz2') else p
                ) for p in self.fq_paths
            ]

    def run(self):
        if self.cf['adapter_removal']:
            yield TrimAdapters(
                fq_paths=self.fq_paths,
                dest_dir_path=str(
                    Path(self.cf['trim_dir_path']).joinpath(self.sample_name)
                ),
                sample_name=self.sample_name, pigz=self.cf['pigz'],
                pbzip2=self.cf['pbzip2'], trim_galore=self.cf['trim_galore'],
                cutadapt=self.cf['cutadapt'], fastqc=self.cf['fastqc'],
                n_cpu=self.cf['n_cpu_per_worker'],
                memory_mb=self.cf['memory_mb_per_worker'],
                log_dir_path=self.cf['log_dir_path'],
                remove_if_failed=self.cf['remove_if_failed'],
                quiet=self.cf['quiet']
            )
        else:
            yield [
                Bunzip2AndGzip(bz2_path=p, gz_path=o.path, cf=self.cf)
                for p, o in zip(self.fq_paths, self.output())
                if p.endswith('.bz2')
            ]


def _generate_trimmed_fqs(raw_fq_paths, dest_dir_path):
    for i, p in enumerate(raw_fq_paths):
        yield Path(dest_dir_path).joinpath(
            re.sub(
                r'\.(fastq|fq)\.(gz|bz2)$', f'_val_{i + 1}.fq.gz', Path(p).name
            )
        )


class TrimAdapters(FtarcTask):
    fq_paths = luigi.ListParameter()
    dest_dir_path = luigi.Parameter(default='.')
    sample_name = luigi.Parameter(default='')
    pigz = luigi.Parameter(default='pigz')
    pbzip2 = luigi.Parameter(default='pbzip2')
    trim_galore = luigi.Parameter(default='trim_galore')
    cutadapt = luigi.Parameter(default='cutadapt')
    fastqc = luigi.Parameter(default='fastqc')
    n_cpu = luigi.IntParameter(default=1)
    memory_mb = luigi.FloatParameter(default=4096)
    log_dir_path = luigi.Parameter(default='')
    remove_if_failed = luigi.BoolParameter(default=True)
    quiet = luigi.BoolParameter(default=False)
    priority = 50

    def output(self):
        return [
            luigi.LocalTarget(p) for p in _generate_trimmed_fqs(
                raw_fq_paths=self.fq_paths, dest_dir_path=self.dest_dir_path
            )
        ]

    def run(self):
        run_id = (
            self.sample_name
            or Path(Path(Path(self.fq_paths[0]).stem).stem).stem
        )
        self.print_log(f'Trim adapters:\t{run_id}')
        output_fq_paths = [o.path for o in self.output()]
        run_dir = Path(output_fq_paths[0]).parent
        work_fq_paths = [
            (
                str(run_dir.joinpath(Path(p).stem + '.gz'))
                if p.endswith('.bz2') else p
            ) for p in self.fq_paths
        ]
        self.setup_shell(
            run_id=run_id, log_dir_path=self.log_dir_path,
            commands=[
                self.pigz, self.pbzip2, self.trim_galore, self.cutadapt,
                self.fastqc
            ],
            cwd=run_dir,
            remove_if_failed=self.remove_if_failed,
            quiet=self.quiet,
            env={'JAVA_TOOL_OPTIONS': '-Xmx{}m'.format(int(self.memory_mb))}
        )
        for i, o in zip(self.fq_paths, work_fq_paths):
            if i.endswith('.bz2'):
                _bunzip2_and_gzip(
                    shelltask=self, pbzip2=self.pbzip2, pigz=self.pigz,
                    src_bz2_path=i, dest_gz_path=o, n_cpu=self.n_cpu
                )
        self.run_shell(
            args=(
                f'set -e && {self.trim_galore}'
                + f' --path_to_cutadapt {self.cutadapt}'
                + f' --cores {self.n_cpu}'
                + f' --output_dir {run_dir}'
                + (' --paired' if len(work_fq_paths) > 1 else '')
                + ''.join([f' {p}' for p in work_fq_paths])
            ),
            input_files_or_dirs=work_fq_paths,
            output_files_or_dirs=[*output_fq_paths, run_dir]
        )


class Bunzip2AndGzip(FtarcTask):
    bz2_path = luigi.Parameter()
    gz_path = luigi.Parameter()
    cf = luigi.DictParameter()
    priority = 50

    def output(self):
        return luigi.LocalTarget(self.gz_path)

    def run(self):
        run_id = Path(self.bz2_path).stem
        self.print_log(f'Bunzip2 and Gzip a file:\t{run_id}')
        pigz = self.cf['pigz']
        pbzip2 = self.cf['pbzip2']
        n_cpu = self.cf['n_cpu_per_worker']
        self.setup_shell(
            run_id=run_id, log_dir_path=self.cf['log_dir_path'],
            commands=[pigz, pbzip2], cwd=Path(self.gz_path).parent,
            remove_if_failed=self.cf['remove_if_failed'],
            quiet=self.cf['quiet']
        )
        _bunzip2_and_gzip(
            shelltask=self, pbzip2=pbzip2, pigz=pigz,
            src_bz2_path=self.bz2_path, dest_gz_path=self.gz_path, n_cpu=n_cpu
        )


def _bunzip2_and_gzip(shelltask, pbzip2, pigz, src_bz2_path, dest_gz_path,
                      n_cpu=1):
    shelltask.run_shell(
        args=(
            f'set -eo pipefail && {pbzip2} -p{n_cpu} -dc {src_bz2_path}'
            + f' | {pigz} -p {n_cpu} -c - > {dest_gz_path}'
        ),
        input_files_or_dirs=src_bz2_path, output_files_or_dirs=dest_gz_path
    )


if __name__ == '__main__':
    luigi.run()
