#!/usr/bin/env python

from pathlib import Path

import luigi

from .core import FtarcTask


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
        sam_name = Path(self.input_sam_path).name
        dest_dir = Path(self.dest_dir_path).resolve()
        return [
            luigi.LocalTarget(dest_dir.joinpath(f'{sam_name}.{c}.txt'))
            for c in self.picard_commands
        ]

    def run(self):
        target_sam = Path(self.input_sam_path)
        run_id = target_sam.name
        self.print_log(f'Collect SAM metrics using Picard:\t{run_id}')
        input_sam = target_sam.resolve()
        fa = Path(self.fa_path).resolve()
        fa_dict = fa.parent.joinpath(f'{fa.stem}.dict')
        for c, o in zip(self.picard_commands, self.output()):
            output_txt = Path(o.path)
            self.setup_shell(
                run_id=f'{run_id}.{c}', commands=f'{self.picard} {c}',
                cwd=output_txt.parent, **self.sh_config,
                env={
                    'JAVA_TOOL_OPTIONS': self.generate_gatk_java_options(
                        n_cpu=self.n_cpu, memory_mb=self.memory_mb
                    )
                }
            )
            prefix = str(output_txt.parent.joinpath(output_txt.stem))
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
                Path(self.input_sam_path).name + '.ValidateSamFile.txt'
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
