#!/usr/bin/env python

import sys
from pathlib import Path

import luigi
from luigi.util import requires

from .base import ShellTask
from .gatk import ApplyBQSR
from .picard import CollectSamMetricsWithPicard
from .resource import FetchReferenceFASTA
from .samtools import CollectSamMetricsWithSamtools, SamtoolsView


class PrintEnvVersions(ShellTask):
    log_dir_path = luigi.Parameter()
    command_paths = luigi.ListParameter(default=list())
    run_id = luigi.Parameter(default='env')
    quiet = luigi.BoolParameter(default=False)
    priority = luigi.IntParameter(default=sys.maxsize)
    __is_completed = False

    def complete(self):
        return self.__is_completed

    def run(self):
        python = sys.executable
        self.print_log(f'Print environment versions: {python}')
        version_files = [
            Path('/proc/version'),
            *[
                o for o in Path('/etc').iterdir()
                if o.name.endswith(('-release', '_version'))
            ]
        ]
        self.setup_shell(
            run_id=self.run_id, log_dir_path=self.log_dir_path,
            commands=[python, *self.command_paths], quiet=self.quiet
        )
        self.run_shell(
            args=[
                f'{python} -m pip --version',
                f'{python} -m pip freeze --no-cache-dir'
            ]
        )
        self.run_shell(
            args=[
                'uname -a',
                *[f'cat {o}' for o in version_files if o.is_file()]
            ]
        )
        self.__is_completed = True


@requires(ApplyBQSR, FetchReferenceFASTA)
class PrepareAnalysisReadyCRAM(luigi.Task):
    sample_name = luigi.Parameter()
    cf = luigi.DictParameter()
    priority = luigi.IntParameter(default=sys.maxsize)

    def output(self):
        input_cram = Path(self.input()[0][0].path)
        return [
            luigi.LocalTarget(
                input_cram.parent.joinpath(f'{input_cram.stem}.dedup.cram{s}')
            ) for s in ['', '.crai']
        ]

    def run(self):
        yield SamtoolsView(
            input_sam_path=self.input()[0][0].path,
            output_sam_path=self.output()[0].path,
            fa_path=self.input()[1][0].path, samtools=self.cf['samtools'],
            n_cpu=self.cf['n_cpu_per_worker'], add_args='-F 1024',
            message='Remove duplicates', remove_input=False, index_sam=True,
            log_dir_path=self.cf['log_dir_path'],
            remove_if_failed=self.cf['remove_if_failed'],
            quiet=self.cf['quiet']
        )
        qc_dir = Path(self.cf['qc_dir_path'])
        if 'picard' in self.cf['metrics_collectors']:
            yield CollectSamMetricsWithPicard(
                input_sam_path=self.input()[0][0].path,
                fa_path=self.input()[1][0].path,
                dest_dir_path=str(
                    qc_dir.joinpath('picard').joinpath(self.sample_name)
                ),
                picard=(self.cf.get('gatk') or self.cf['picard']),
                java_tool_options=self.cf['gatk_java_options'],
                log_dir_path=self.cf['log_dir_path'],
                remove_if_failed=self.cf['remove_if_failed'],
                quiet=self.cf['quiet']
            )
        if 'samtools' in self.cf['metrics_collectors']:
            yield CollectSamMetricsWithSamtools(
                input_sam_path=self.input()[0][0].path,
                fa_path=self.input()[1][0].path,
                dest_dir_path=str(
                    qc_dir.joinpath('samtools').joinpath(self.sample_name)
                ),
                samtools=self.cf['samtools'], pigz=self.cf['pigz'],
                n_cpu=self.cf['n_cpu_per_worker'],
                log_dir_path=self.cf['log_dir_path'],
                remove_if_failed=self.cf['remove_if_failed'],
                quiet=self.cf['quiet']
            )


class CollectMultipleSamMetrics(luigi.WrapperTask):
    input_sam_path = luigi.Parameter()
    fa_path = luigi.Parameter()
    dest_dir_path = luigi.Parameter(default='.')
    samtools = luigi.Parameter()
    pigz = luigi.Parameter()
    picard = luigi.Parameter()
    n_cpu = luigi.IntParameter(default=1)
    java_tool_options = luigi.Parameter(default='')
    log_dir_path = luigi.Parameter(default='')
    remove_if_failed = luigi.BoolParameter(default=True)
    quiet = luigi.BoolParameter(default=False)
    priority = luigi.IntParameter(default=sys.maxsize)

    def requires(self):
        return [
            CollectSamMetricsWithPicard(
                input_sam_path=self.input_sam_path, fa_path=self.fa_path,
                dest_dir_path=self.dest_dir_path, picard=self.picard,
                java_tool_options=self.java_tool_options,
                log_dir_path=self.log_dir_path,
                remove_if_failed=self.remove_if_failed, quiet=self.quiet
            ),
            CollectSamMetricsWithSamtools(
                input_sam_path=self.input_sam_path, fa_path=self.fa_path,
                dest_dir_path=self.dest_dir_path, samtools=self.samtools,
                pigz=self.pigz, n_cpu=self.n_cpu,
                log_dir_path=self.log_dir_path,
                remove_if_failed=self.remove_if_failed, quiet=self.quiet
            )
        ]

    def output(self):
        return self.input()


if __name__ == '__main__':
    luigi.run()
