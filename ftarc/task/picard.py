#!/usr/bin/env python

from pathlib import Path

import luigi
from luigi.util import requires

from .bwa import AlignReads
from .core import FtarcTask
from .resource import FetchReferenceFasta


@requires(FetchReferenceFasta)
class CreateSequenceDictionary(FtarcTask):
    cf = luigi.DictParameter()
    n_cpu = luigi.IntParameter(default=1)
    memory_mb = luigi.FloatParameter(default=4096)
    sh_config = luigi.DictParameter(default=dict())
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
            run_id=run_id, commands=gatk, cwd=fa.parent, **self.sh_config,
            env={
                'JAVA_TOOL_OPTIONS': self.generate_gatk_java_options(
                    n_cpu=self.n_cpu, memory_mb=self.memory_mb
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


@requires(AlignReads, FetchReferenceFasta, CreateSequenceDictionary)
class MarkDuplicates(FtarcTask):
    cf = luigi.DictParameter()
    set_nm_md_uq = luigi.BoolParameter(default=True)
    n_cpu = luigi.IntParameter(default=1)
    memory_mb = luigi.FloatParameter(default=4096)
    sh_config = luigi.DictParameter(default=dict())
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
        memory_mb_per_thread = int(self.memory_mb / self.n_cpu / 8)
        fa = Path(self.input()[1][0].path)
        fa_dict = fa.parent.joinpath(f'{fa.stem}.dict')
        output_cram = Path(self.output()[0].path)
        markdup_metrics_txt = Path(self.output()[2].path)
        tmp_bams = [
            output_cram.parent.joinpath(f'{output_cram.stem}{s}.bam')
            for s in ['.unsorted', '']
        ]
        self.setup_shell(
            run_id=run_id, commands=[gatk, samtools], cwd=input_cram.parent,
            **self.sh_config,
            env={
                'REF_CACHE': '.ref_cache',
                'JAVA_TOOL_OPTIONS': self.generate_gatk_java_options(
                    n_cpu=self.n_cpu, memory_mb=self.memory_mb
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
                    f'set -eo pipefail && {samtools} sort'
                    + f' -@ {self.n_cpu} -m {memory_mb_per_thread}M'
                    + f' -O BAM -l 0 -T {output_cram}.sort {tmp_bams[0]}'
                    + f' | {gatk} SetNmMdAndUqTags'
                    + ' --INPUT /dev/stdin'
                    + f' --OUTPUT {tmp_bams[1]}'
                    + f' --REFERENCE_SEQUENCE {fa}'
                ),
                input_files_or_dirs=[tmp_bams[0], fa, fa_dict],
                output_files_or_dirs=tmp_bams[1]
            )
            self.remove_files_and_dirs(tmp_bams[0])
            self.samtools_view(
                input_sam_path=tmp_bams[1], fa_path=fa,
                output_sam_path=output_cram, samtools=samtools,
                n_cpu=self.n_cpu, index_sam=True, remove_input=True
            )
        else:
            self.run_shell(
                args=(
                    f'set -eo pipefail && {samtools} sort'
                    + f' -@ {self.n_cpu} -m {memory_mb_per_thread}M'
                    + f' -O BAM -l 0 -T {output_cram}.sort {tmp_bams[0]}'
                    + f' | {samtools} view -@ {self.n_cpu} -T {fa} -CS'
                    + f' -o {output_cram} -'
                ),
                input_files_or_dirs=[tmp_bams[0], fa],
                output_files_or_dirs=output_cram
            )
            self.remove_files_and_dirs(tmp_bams[0])
            self.samtools_index(
                sam_path=output_cram, samtools=samtools, n_cpu=self.n_cpu
            )


class CollectSamMetricsWithPicard(FtarcTask):
    input_sam_path = luigi.Parameter()
    fa_path = luigi.Parameter()
    dest_dir_path = luigi.Parameter(default='.')
    picard_commands = luigi.ListParameter(
        default=[
            'CollectRawWgsMetrics', 'CollectAlignmentSummaryMetrics',
            'CollectInsertSizeMetrics', 'QualityScoreDistribution',
            'MeanQualityByCycle', 'CollectBaseDistributionByCycle',
            'CollectGcBiasMetrics'
        ]
    )
    picard = luigi.Parameter(default='picard')
    n_cpu = luigi.IntParameter(default=1)
    memory_mb = luigi.FloatParameter(default=4096)
    sh_config = luigi.DictParameter(default=dict())
    priority = 100

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
        target_sam = Path(self.input_sam_path)
        run_id = target_sam.stem
        self.print_log(f'Collect SAM metrics using Picard:\t{run_id}')
        input_sam = target_sam.resolve()
        fa = Path(self.fa_path).resolve()
        fa_dict = fa.parent.joinpath(f'{fa.stem}.dict')
        dest_dir = Path(self.dest_dir_path).resolve()
        for c in self.picard_commands:
            self.setup_shell(
                run_id=f'{run_id}.{c}', commands=f'{self.picard} {c}',
                cwd=dest_dir, **self.sh_config,
                env={
                    'JAVA_TOOL_OPTIONS': self.generate_gatk_java_options(
                        n_cpu=self.n_cpu, memory_mb=self.memory_mb
                    )
                }
            )
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
                    + ''.join(f' --{k} {v}' for k, v in output_args.items())
                ),
                input_files_or_dirs=[input_sam, fa, fa_dict],
                output_files_or_dirs=[
                    v for v in output_args.values()
                    if v.endswith(('.txt', '.pdf'))
                ]
            )


class ValidateSamFile(FtarcTask):
    input_sam_path = luigi.Parameter()
    fa_path = luigi.Parameter()
    dest_dir_path = luigi.Parameter(default='.')
    picard = luigi.Parameter(default='picard')
    mode_of_output = luigi.Parameter(default='VERBOSE')
    ignored_warnings = luigi.ListParameter(default=['MISSING_TAG_NM'])
    n_cpu = luigi.IntParameter(default=1)
    memory_mb = luigi.FloatParameter(default=4096)
    sh_config = luigi.DictParameter(default=dict())
    priority = luigi.IntParameter(default=100)

    def output(self):
        return luigi.LocalTarget(
            Path(self.dest_dir_path).resolve().joinpath(
                Path(self.input_sam_path).stem + '.ValidateSamFile.txt'
            )
        )

    def run(self):
        target_sam = Path(self.input_sam_path)
        run_id = target_sam.stem
        self.print_log(f'Validate a SAM file:\t{run_id}')
        input_sam = target_sam.resolve()
        fa = Path(self.fa_path).resolve()
        fa_dict = fa.parent.joinpath(f'{fa.stem}.dict')
        dest_dir = Path(self.dest_dir_path).resolve()
        output_txt = Path(self.output().path)
        self.setup_shell(
            run_id=run_id, commands=self.picard, cwd=dest_dir,
            **self.sh_config,
            env={
                'JAVA_TOOL_OPTIONS': self.generate_gatk_java_options(
                    n_cpu=self.n_cpu, memory_mb=self.memory_mb
                )
            }
        )
        self.run_shell(
            args=(
                f'set -e && {self.picard} ValidateSamFile'
                + f' --INPUT {input_sam} --REFERENCE_SEQUENCE {fa}'
                + f' --OUTPUT {output_txt} --MODE {self.mode_of_output}'
                + ''.join(f' --IGNORE {w}' for w in self.ignored_warnings)
            ),
            input_files_or_dirs=[input_sam, fa, fa_dict],
            output_files_or_dirs=output_txt
        )


if __name__ == '__main__':
    luigi.run()
