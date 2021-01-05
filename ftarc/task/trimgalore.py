#!/usr/bin/env python

import re
from pathlib import Path

import luigi

from .core import FtarcTask


class PrepareFastqs(luigi.WrapperTask):
    fq_paths = luigi.ListParameter()
    sample_name = luigi.Parameter()
    cf = luigi.DictParameter()
    n_cpu = luigi.IntParameter(default=1)
    memory_mb = luigi.FloatParameter(default=4096)
    sh_config = luigi.DictParameter(default=dict())
    priority = 50

    def requires(self):
        if self.cf['adapter_removal']:
            return TrimAdapters(
                fq_paths=self.fq_paths,
                dest_dir_path=str(
                    Path(self.cf['trim_dir_path']).joinpath(self.sample_name)
                ),
                sample_name=self.sample_name, pigz=self.cf['pigz'],
                pbzip2=self.cf['pbzip2'], trim_galore=self.cf['trim_galore'],
                cutadapt=self.cf['cutadapt'], fastqc=self.cf['fastqc'],
                n_cpu=self.n_cpu, memory_mb=self.memory_mb,
                sh_config=self.sh_config
            )
        else:
            return LocateFastqs(
                fq_paths=self.fq_paths, cf=self.cf, n_cpu=self.n_cpu,
                sh_config=self.sh_config
            )

    def output(self):
        return self.input()


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
    sh_config = luigi.DictParameter(default=dict())
    priority = 50

    def output(self):
        return [
            luigi.LocalTarget(
                Path(self.dest_dir_path).joinpath(
                    re.sub(r'\.(fastq|fq)$', '', Path(p).stem)
                    + f'_val_{i + 1}.fq.gz'
                )
            ) for i, p in enumerate(self.fq_paths)
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
            run_id=run_id,
            commands=[
                self.pigz, self.pbzip2, self.trim_galore, self.cutadapt,
                self.fastqc
            ],
            cwd=run_dir, **self.sh_config,
            env={'JAVA_TOOL_OPTIONS': '-Xmx{}m'.format(int(self.memory_mb))}
        )
        for i, o in zip(self.fq_paths, work_fq_paths):
            if i.endswith('.bz2'):
                self.bzip2_to_gzip(
                    src_bz2_path=i, dest_gz_path=o, pbzip2=self.pbzip2,
                    pigz=self.pigz, n_cpu=self.n_cpu
                )
        self.run_shell(
            args=(
                f'set -e && {self.trim_galore}'
                + f' --path_to_cutadapt {self.cutadapt}'
                + f' --cores {self.n_cpu}'
                + f' --output_dir {run_dir}'
                + (' --paired' if len(work_fq_paths) > 1 else '')
                + ''.join(f' {p}' for p in work_fq_paths)
            ),
            input_files_or_dirs=work_fq_paths,
            output_files_or_dirs=[*output_fq_paths, run_dir]
        )


class LocateFastqs(FtarcTask):
    fq_paths = luigi.Parameter()
    cf = luigi.DictParameter()
    n_cpu = luigi.IntParameter(default=1)
    sh_config = luigi.DictParameter(default=dict())
    priority = 50

    def output(self):
        return [
            luigi.LocalTarget(
                Path(self.cf['align_dir_path']).joinpath(Path(p).stem + '.gz')
                if p.endswith('.bz2') else Path(p).resolve()
            ) for p in self.fq_paths
        ]

    def run(self):
        run_id = Path(Path(Path(self.fq_paths[0]).stem).stem).stem
        self.print_log(f'Bunzip2 and Gzip a file:\t{run_id}')
        pigz = self.cf['pigz']
        pbzip2 = self.cf['pbzip2']
        self.setup_shell(
            run_id=run_id, commands=[pigz, pbzip2],
            cwd=self.cf['align_dir_path'], **self.sh_config
        )
        for p, o in zip(self.fq_paths, self.output()):
            if p != o.path:
                self.bzip2_to_gzip(
                    src_bz2_path=p, dest_gz_path=o.path, pbzip2=pbzip2,
                    pigz=pigz, n_cpu=self.n_cpu
                )


if __name__ == '__main__':
    luigi.run()
