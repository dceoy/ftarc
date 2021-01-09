#!/usr/bin/env python

import re
import sys
from pathlib import Path
from socket import gethostname

import luigi
from luigi.util import requires

from .core import FtarcTask
from .fastqc import CollectFqMetricsWithFastqc
from .gatk import RecalibrateBaseQualityScores
from .picard import CollectSamMetricsWithPicard, ValidateSamFile
from .resource import FetchReferenceFasta
from .samtools import CollectSamMetricsWithSamtools, SamtoolsView
from .trimgalore import PrepareFastqs


class PrintEnvVersions(FtarcTask):
    command_paths = luigi.ListParameter(default=list())
    run_id = luigi.Parameter(default=gethostname())
    sh_config = luigi.DictParameter(default=dict())
    __is_completed = False

    def complete(self):
        return self.__is_completed

    def run(self):
        self.print_log(f'Print environment versions:\t{self.run_id}')
        self.setup_shell(
            run_id=self.run_id, commands=self.command_paths, **self.sh_config
        )
        self.print_env_versions()
        self.__is_completed = True


@requires(RecalibrateBaseQualityScores, FetchReferenceFasta, PrepareFastqs)
class PrepareAnalysisReadyCram(luigi.Task):
    sample_name = luigi.Parameter()
    cf = luigi.DictParameter()
    picard_qc_commands = luigi.ListParameter(
        default=[
            'CollectRawWgsMetrics', 'CollectAlignmentSummaryMetrics',
            'CollectInsertSizeMetrics', 'QualityScoreDistribution',
            'MeanQualityByCycle', 'CollectBaseDistributionByCycle',
            'CollectGcBiasMetrics'
        ]
    )
    samtools_qc_commands = luigi.ListParameter(
        default=['coverage', 'flagstat', 'idxstats', 'stats']
    )
    n_cpu = luigi.IntParameter(default=1)
    memory_mb = luigi.FloatParameter(default=4096)
    sh_config = luigi.DictParameter(default=dict())
    priority = luigi.IntParameter(default=sys.maxsize)

    def output(self):
        input_cram = Path(self.input()[0][0].path)
        qc_dir = Path(self.cf['qc_dir_path'])
        return (
            [
                luigi.LocalTarget(
                    input_cram.parent.joinpath(
                        f'{input_cram.stem}.dedup.cram{s}'
                    )
                ) for s in ['', '.crai']
            ] + [
                luigi.LocalTarget(
                    qc_dir.joinpath('fastqc').joinpath(
                        self.sample_name
                    ).joinpath(
                        re.sub(r'\.(fq|fastq)$', '', s) + '_fastqc.html'
                    )
                ) for s in (
                    [Path(i.path).stem for i in self.input()[2]]
                    if 'fastqc' in self.cf['metrics_collectors'] else list()
                )
            ] + [
                luigi.LocalTarget(
                    qc_dir.joinpath('picard').joinpath(
                        self.sample_name
                    ).joinpath(f'{input_cram.stem}.{c}.txt')
                ) for c in (
                    self.picard_qc_commands
                    if 'picard' in self.cf['metrics_collectors'] else list()
                )
            ] + [
                luigi.LocalTarget(
                    qc_dir.joinpath('samtools').joinpath(
                        self.sample_name
                    ).joinpath(f'{input_cram.stem}.{c}.txt')
                ) for c in (
                    self.samtools_qc_commands
                    if 'samtools' in self.cf['metrics_collectors'] else list()
                )
            ]
        )

    def run(self):
        input_sam_path = self.input()[0][0].path
        fa_path = self.input()[1][0].path
        yield ValidateSamFile(
            input_sam_path=input_sam_path, fa_path=fa_path,
            dest_dir_path=str(Path(input_sam_path).parent),
            picard=self.cf['gatk'], n_cpu=self.n_cpu, memory_mb=self.memory_mb,
            sh_config=self.sh_config
        )
        yield SamtoolsView(
            input_sam_path=input_sam_path,
            output_sam_path=self.output()[0].path, fa_path=fa_path,
            samtools=self.cf['samtools'], n_cpu=self.n_cpu, add_args='-F 1024',
            message='Remove duplicates', remove_input=False, index_sam=True,
            sh_config=self.sh_config
        )
        qc_dir = Path(self.cf['qc_dir_path'])
        if 'fastqc' in self.cf['metrics_collectors']:
            yield CollectFqMetricsWithFastqc(
                input_fq_paths=[i.path for i in self.input()[2]],
                dest_dir_path=str(
                    qc_dir.joinpath('fastqc').joinpath(self.sample_name)
                ),
                fastqc=self.cf['fastqc'], n_cpu=self.n_cpu,
                memory_mb=self.memory_mb, sh_config=self.sh_config
            )
        if {'picard', 'samtools'} & set(self.cf['metrics_collectors']):
            yield [
                CollectMultipleSamMetrics(
                    input_sam_path=input_sam_path, fa_path=fa_path,
                    dest_dir_path=str(
                        qc_dir.joinpath(m).joinpath(self.sample_name)
                    ),
                    metrics_collectors=[m],
                    picard_qc_commands=self.picard_qc_commands,
                    samtools_qc_commands=self.samtools_qc_commands,
                    samtools=self.cf['samtools'],
                    pigz=self.cf['pigz'], picard=self.cf['gatk'],
                    n_cpu=self.n_cpu, memory_mb=self.memory_mb,
                    sh_config=self.sh_config
                ) for m in self.cf['metrics_collectors']
            ]


class CollectMultipleSamMetrics(luigi.WrapperTask):
    input_sam_path = luigi.Parameter()
    fa_path = luigi.Parameter()
    dest_dir_path = luigi.Parameter(default='.')
    metrics_collectors = luigi.ListParameter(default=['picard', 'samtools'])
    picard_qc_commands = luigi.ListParameter(
        default=[
            'CollectRawWgsMetrics', 'CollectAlignmentSummaryMetrics',
            'CollectInsertSizeMetrics', 'QualityScoreDistribution',
            'MeanQualityByCycle', 'CollectBaseDistributionByCycle',
            'CollectGcBiasMetrics'
        ]
    )
    samtools_qc_commands = luigi.ListParameter(
        default=['coverage', 'flagstat', 'idxstats', 'stats']
    )
    samtools = luigi.Parameter(default='samtools')
    pigz = luigi.Parameter(default='pigz')
    picard = luigi.Parameter(default='picard')
    n_cpu = luigi.IntParameter(default=1)
    memory_mb = luigi.FloatParameter(default=4096)
    sh_config = luigi.DictParameter(default=dict())
    priority = 10

    def requires(self):
        return (
            [
                CollectSamMetricsWithPicard(
                    input_sam_path=self.input_sam_path, fa_path=self.fa_path,
                    dest_dir_path=self.dest_dir_path, picard_commands=[c],
                    picard=self.picard, n_cpu=self.n_cpu,
                    memory_mb=self.memory_mb, sh_config=self.sh_config
                ) for c in (
                    self.picard_qc_commands
                    if 'picard' in self.metrics_collectors else list()
                )
            ] + [
                CollectSamMetricsWithSamtools(
                    input_sam_path=self.input_sam_path, fa_path=self.fa_path,
                    dest_dir_path=self.dest_dir_path, samtools_commands=[c],
                    samtools=self.samtools, pigz=self.pigz, n_cpu=self.n_cpu,
                    sh_config=self.sh_config
                ) for c in (
                    self.samtools_qc_commands
                    if 'samtools' in self.metrics_collectors else list()
                )
            ]
        )

    def output(self):
        return self.input()


if __name__ == '__main__':
    luigi.run()
