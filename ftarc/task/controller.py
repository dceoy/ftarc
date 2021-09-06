#!/usr/bin/env python

import re
import sys
from pathlib import Path
from socket import gethostname

import luigi
from luigi.util import requires

from .bwa import AlignReads
from .core import FtarcTask
from .fastqc import CollectFqMetricsWithFastqc
from .gatk import DeduplicateReads, MarkDuplicates
from .picard import CollectSamMetricsWithPicard, ValidateSamFile
from .resource import (CreateSequenceDictionary, FetchKnownSitesVcfs,
                       FetchReferenceFasta)
from .samtools import CollectSamMetricsWithSamtools
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


@requires(PrepareFastqs, FetchReferenceFasta, CreateSequenceDictionary,
          FetchKnownSitesVcfs)
class RunPreprocessingPipeline(luigi.Task):
    sample_name = luigi.Parameter()
    read_group = luigi.DictParameter()
    cf = luigi.DictParameter()
    n_cpu = luigi.IntParameter(default=1)
    memory_mb = luigi.FloatParameter(default=4096)
    sh_config = luigi.DictParameter(default=dict())
    priority = 70

    def output(self):
        dest_dir = Path(self.cf['align_dir_path']).resolve().joinpath(
            self.sample_name
        )
        output_stem = (
            self.sample_name
            + ('.trim.' if self.cf['adapter_removal'] else '.')
            + (self.cf['reference_name'] or Path(self.input()[1][0].path).stem)
            + '.markdup.bqsr.cram'
        )
        return [
            luigi.LocalTarget(dest_dir.joinpath(f'{output_stem}.{s}'))
            for s in ['cram', 'cram.crai', 'dedup.cram', 'dedup.cram.crai']
        ]

    def run(self):
        fa_path = self.input()[1][0].path
        dest_dir_path = str(Path(self.output()[0].path).parent)
        align_target = yield AlignReads(
            fq_paths=[i.path for i in self.input()[0]], fa_path=fa_path,
            dest_dir_path=dest_dir_path, sample_name=self.sample_name,
            read_group=self.read_group,
            output_stem=(
                self.sample_name
                + ('.trim.' if self.cf['adapter_removal'] else '.')
                + (self.cf['reference_name'] or Path(fa_path).stem)
            ),
            bwa=self.cf['bwa'], samtools=self.cf['samtools'],
            use_bwa_mem2=self.cf['use_bwa_mem2'], n_cpu=self.n_cpu,
            memory_mb=self.memory_mb, sh_config=self.sh_config
        )
        markdup_target = yield MarkDuplicates(
            input_sam_path=align_target[0].path, fa_path=fa_path,
            dest_dir_path=dest_dir_path,
            gatk=self.cf['gatk'], samtools=self.cf['samtools'],
            use_spark=self.cf['use_spark'], save_memory=self.cf['save_memory'],
            n_cpu=self.n_cpu, memory_mb=self.memory_mb,
            sh_config=self.sh_config
        )
        yield DeduplicateReads(
            input_sam_path=markdup_target[0].path, fa_path=fa_path,
            known_sites_vcf_paths=[i[0].path for i in self.input()[3]],
            dest_dir_path=dest_dir_path, gatk=self.cf['gatk'],
            samtools=self.cf['samtools'], use_spark=self.cf['use_spark'],
            save_memory=self.cf['save_memory'], n_cpu=self.n_cpu,
            memory_mb=self.memory_mb, sh_config=self.sh_config
        )


@requires(RunPreprocessingPipeline, FetchReferenceFasta,
          PrepareFastqs)
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
        qc_dir = Path(self.cf['qc_dir_path'])
        if 'fastqc' in self.cf['metrics_collectors']:
            yield CollectFqMetricsWithFastqc(
                fq_paths=[i.path for i in self.input()[2]],
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
                    picard=self.cf['gatk'], samtools=self.cf['samtools'],
                    plot_bamstats=self.cf['plot_bamstats'],
                    gnuplot=self.cf['gnuplot'], n_cpu=self.n_cpu,
                    memory_mb=self.memory_mb, sh_config=self.sh_config
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
    picard = luigi.Parameter(default='picard')
    samtools = luigi.Parameter(default='samtools')
    plot_bamstats = luigi.Parameter(default='plot-bamstats')
    gnuplot = luigi.Parameter(default='gnuplot')
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
                    samtools=self.samtools, plot_bamstats=self.plot_bamstats,
                    gnuplot=self.gnuplot, n_cpu=self.n_cpu,
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
