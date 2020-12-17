#!/usr/bin/env python

import sys
from pathlib import Path

import luigi
from luigi.util import requires

from .base import ShellTask
from .bwa import AlignReads
from .resource import FetchReferenceFASTA
from .samtools import samtools_index, samtools_view_and_index


@requires(FetchReferenceFASTA)
class CreateSequenceDictionary(ShellTask):
    cf = luigi.DictParameter()
    priority = 70

    def output(self):
        fa = Path(self.input()[0].path)
        return luigi.LocalTarget(fa.parent.joinpath(f'{fa.stem}.dict'))

    def run(self):
        fa = Path(self.input()[0].path)
        run_id = fa.stem
        self.print_log(f'Create a sequence dictionary:\t{run_id}')
        gatk = self.cf.get('gatk') or self.cf['picard']
        seq_dict_path = self.output().path
        self.setup_shell(
            run_id=run_id, log_dir_path=self.cf['log_dir_path'], commands=gatk,
            cwd=fa.parent, remove_if_failed=self.cf['remove_if_failed'],
            quiet=self.cf['quiet'],
            env={
                'JAVA_TOOL_OPTIONS': generate_gatk_java_options(
                    n_cpu=self.cf['n_cpu_per_worker'],
                    memory_mb=self.cf['memory_mb_per_worker']
                )
            }
        )
        self.run_shell(
            args=(
                f'set -e && {gatk} CreateSequenceDictionary'
                + f' --REFERENCE {fa} --OUTPUT {seq_dict_path}'
            ),
            input_files_or_dirs=fa, output_files_or_dirs=seq_dict_path
        )


@requires(AlignReads, FetchReferenceFASTA, CreateSequenceDictionary)
class MarkDuplicates(ShellTask):
    cf = luigi.DictParameter()
    set_nm_md_uq = luigi.BoolParameter(default=False)
    priority = 70

    def output(self):
        input_cram = Path(self.input()[0][0].path)
        return [
            luigi.LocalTarget(
                input_cram.parent.joinpath(f'{input_cram.stem}.markdup.{s}')
            ) for s in ['cram', 'cram.crai', 'metrics.txt']
        ]

    def run(self):
        input_cram = Path(self.input()[0][0].path)
        run_id = input_cram.stem
        self.print_log(f'Mark duplicates:\t{run_id}')
        gatk = self.cf.get('gatk') or self.cf['picard']
        samtools = self.cf['samtools']
        n_cpu = self.cf['n_cpu_per_worker']
        memory_mb_per_thread = int(self.cf['memory_mb_per_worker'] / n_cpu / 8)
        fa = Path(self.input()[1][0].path)
        fa_dict = fa.parent.joinpath(f'{fa.stem}.dict')
        output_cram = Path(self.output()[0].path)
        markdup_metrics_txt = Path(self.output()[2].path)
        tmp_bams = [
            output_cram.parent.joinpath(f'{output_cram.stem}{s}.bam')
            for s in ['.unsorted', '']
        ]
        self.setup_shell(
            run_id=run_id, log_dir_path=self.cf['log_dir_path'],
            commands=[gatk, samtools], cwd=input_cram.parent,
            remove_if_failed=self.cf['remove_if_failed'],
            quiet=self.cf['quiet'],
            env={
                'REF_CACHE': '.ref_cache',
                'JAVA_TOOL_OPTIONS': generate_gatk_java_options(
                    n_cpu=self.cf['n_cpu_per_worker'],
                    memory_mb=self.cf['memory_mb_per_worker']
                )
            }
        )
        self.run_shell(
            args=(
                f'set -e && {gatk} MarkDuplicates'
                + f' --INPUT {input_cram}'
                + f' --REFERENCE_SEQUENCE {fa}'
                + f' --METRICS_FILE {markdup_metrics_txt}'
                + f' --OUTPUT {tmp_bams[0]}'
                + ' --ASSUME_SORT_ORDER coordinate'
            ),
            input_files_or_dirs=[input_cram, fa, fa_dict],
            output_files_or_dirs=[tmp_bams[0], markdup_metrics_txt]
        )
        if self.set_nm_md_uq:
            self.run_shell(
                args=(
                    f'set -eo pipefail && {samtools} sort -@ {n_cpu}'
                    + f' -m {memory_mb_per_thread}M -O BAM -l 0'
                    + f' -T {output_cram}.sort {tmp_bams[0]}'
                    + f' | {gatk} SetNmMdAndUqTags'
                    + ' --INPUT /dev/stdin'
                    + f' --OUTPUT {tmp_bams[1]}'
                    + f' --REFERENCE_SEQUENCE {fa}'
                ),
                input_files_or_dirs=[tmp_bams[0], fa, fa_dict],
                output_files_or_dirs=tmp_bams[1]
            )
            samtools_view_and_index(
                shelltask=self, samtools=samtools,
                input_sam_path=str(tmp_bams[1]),
                fa_path=str(fa), output_sam_path=str(output_cram), n_cpu=n_cpu
            )
        else:
            self.run_shell(
                args=(
                    f'set -eo pipefail && {samtools} sort -@ {n_cpu}'
                    + f' -m {memory_mb_per_thread}M -O BAM -l 0'
                    + f' -T {output_cram}.sort {tmp_bams[0]}'
                    + f' | {samtools} view -@ {n_cpu} -T {fa} -CS'
                    + f' -o {output_cram} -'
                ),
                input_files_or_dirs=[tmp_bams[0], fa],
                output_files_or_dirs=output_cram
            )
            samtools_index(
                shelltask=self, samtools=samtools, sam_path=str(output_cram),
                n_cpu=n_cpu
            )
        for b in tmp_bams[0:(1 + int(self.set_nm_md_uq))]:
            self.run_shell(args=f'rm -f {b}', input_files_or_dirs=b)


class CollectSamMetricsWithPicard(ShellTask):
    input_sam_path = luigi.Parameter()
    fa_path = luigi.Parameter()
    dest_dir_path = luigi.Parameter(default='.')
    picard = luigi.Parameter(default='picard')
    n_cpu = luigi.IntParameter(default=1)
    memory_mb = luigi.FloatParameter(default=4096)
    picard_commands = luigi.ListParameter(
        default=[
            'CollectRawWgsMetrics', 'CollectAlignmentSummaryMetrics',
            'CollectInsertSizeMetrics', 'QualityScoreDistribution',
            'MeanQualityByCycle', 'CollectBaseDistributionByCycle',
            'CollectGcBiasMetrics'
        ]
    )
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
            luigi.LocalTarget(f'{output_path_prefix}.{c}.txt')
            for c in self.picard_commands
        ]

    def run(self):
        input_sam = Path(self.input_sam_path).resolve()
        run_id = input_sam.stem
        self.print_log(f'Collect SAM metrics using Picard:\t{run_id}')
        fa = Path(self.fa_path).resolve()
        fa_dict = fa.parent.joinpath(f'{fa.stem}.dict')
        dest_dir = Path(self.dest_dir_path).resolve()
        self.setup_shell(
            run_id=run_id, log_dir_path=self.log_dir_path,
            commands=self.picard, cwd=dest_dir,
            remove_if_failed=self.remove_if_failed, quiet=self.quiet,
            env={
                'JAVA_TOOL_OPTIONS': generate_gatk_java_options(
                    n_cpu=self.n_cpu, memory_mb=self.memory_mb
                )
            }
        )
        for c in self.picard_commands:
            prefix = str(dest_dir.joinpath(f'{input_sam.stem}.{c}'))
            if c == 'CollectRawWgsMetrics':
                add_args = {'INCLUDE_BQ_HISTOGRAM': 'true'}
            elif c in {'MeanQualityByCycle', 'QualityScoreDistribution',
                       'CollectBaseDistributionByCycle'}:
                add_args = {'CHART_OUTPUT': f'{prefix}.pdf'}
            elif c == 'CollectInsertSizeMetrics':
                add_args = {'Histogram_FILE': f'{prefix}.histogram.pdf'}
            elif c == 'CollectGcBiasMetrics':
                add_args = {
                    'CHART_OUTPUT': f'{prefix}.pdf',
                    'SUMMARY_OUTPUT': f'{prefix}.summary.txt'
                }
            else:
                add_args = dict()
            output_args = {'OUTPUT': f'{prefix}.txt', **add_args}
            self.run_shell(
                args=(
                    f'set -e && {self.picard} {c}'
                    + f' --INPUT {input_sam} --REFERENCE_SEQUENCE {fa}'
                    + ''.join([f' --{k} {v}' for k, v in output_args.items()])
                ),
                input_files_or_dirs=[input_sam, fa, fa_dict],
                output_files_or_dirs=[
                    v for v in output_args.values()
                    if v.endswith(('.txt', '.pdf'))
                ]
            )


class ValidateSamFile(ShellTask):
    input_sam_paths = luigi.ListParameter()
    fa_path = luigi.Parameter()
    picard = luigi.Parameter(default='picard')
    mode_of_output = luigi.Parameter(default='VERBOSE')
    n_cpu = luigi.IntParameter(default=1)
    memory_mb = luigi.FloatParameter(default=4096)
    log_dir_path = luigi.Parameter(default='')
    quiet = luigi.BoolParameter(default=False)
    priority = luigi.IntParameter(default=sys.maxsize)
    __is_completed = False

    def complete(self):
        return self.__is_completed

    def run(self):
        input_sam = Path(self.input_sam_path).resolve()
        run_id = input_sam.stem
        self.print_log(f'Validate a SAM file:\t{run_id}')
        fa = Path(self.fa_path).resolve()
        fa_dict = fa.parent.joinpath(f'{fa.stem}.dict')
        self.setup_shell(
            run_id=run_id, log_dir_path=self.log_dir_path,
            commands=self.picard, quiet=self.quiet,
            env={
                'JAVA_TOOL_OPTIONS': generate_gatk_java_options(
                    n_cpu=self.n_cpu, memory_mb=self.memory_mb
                )
            }
        )
        self.run_shell(
            args=(
                f'set -e && {self.picard} ValidateSamFile'
                + f' --INPUT {input_sam} --REFERENCE_SEQUENCE {fa}'
                + f' --MODE {self.mode_of_output}'
            ),
            input_files_or_dirs=[input_sam, fa, fa_dict]
        )
        self.__is_completed = True


def generate_gatk_java_options(n_cpu=1, memory_mb=4096):
    return ' '.join([
        '-Dsamjdk.compression_level=5',
        '-Dsamjdk.use_async_io_read_samtools=true',
        '-Dsamjdk.use_async_io_write_samtools=true',
        '-Dsamjdk.use_async_io_write_tribble=false',
        '-Xmx{}m'.format(int(memory_mb)), '-XX:+UseParallelGC',
        '-XX:ParallelGCThreads={}'.format(int(n_cpu))
    ])


if __name__ == '__main__':
    luigi.run()
