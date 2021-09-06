#!/usr/bin/env python

from pathlib import Path

import luigi
from luigi.util import requires

from .core import FtarcTask
from .picard import CreateSequenceDictionary
from .samtools import SamtoolsFaidx


@requires(SamtoolsFaidx, CreateSequenceDictionary)
class MarkDuplicates(FtarcTask):
    input_sam_path = luigi.Parameter()
    fa_path = luigi.Parameter()
    dest_dir_path = luigi.Parameter(default='.')
    gatk = luigi.Parameter(default='gatk')
    samtools = luigi.Parameter(default='samtools')
    use_spark = luigi.BoolParameter(default=False)
    save_memory = luigi.BoolParameter(default=False)
    n_cpu = luigi.IntParameter(default=1)
    memory_mb = luigi.FloatParameter(default=4096)
    sh_config = luigi.DictParameter(default=dict())
    priority = 70

    def output(self):
        dest_dir = Path(self.dest_dir_path).resolve()
        output_stem = Path(self.input_sam_path).stem + '.markdup'
        return [
            luigi.LocalTarget(dest_dir.joinpath(f'{output_stem}.{s}'))
            for s in ['cram', 'cram.crai', 'metrics.txt']
        ]

    def run(self):
        target_sam = Path(self.input_sam_path)
        run_id = target_sam.stem
        self.print_log(f'Mark duplicates:\t{run_id}')
        input_sam = target_sam.resolve()
        fa = Path(self.fa_path).resolve()
        fa_dict = fa.parent.joinpath(f'{fa.stem}.dict')
        output_cram = Path(self.output()[0].path)
        markdup_metrics_txt = Path(self.output()[2].path)
        dest_dir = output_cram.parent
        tmp_bams = [
            dest_dir.joinpath(f'{output_cram.stem}{s}.bam')
            for s in ['.unfixed', '']
        ]
        memory_mb_per_thread = int(self.memory_mb / self.n_cpu / 8)
        self.setup_shell(
            run_id=run_id, commands=[self.gatk, self.samtools], cwd=dest_dir,
            **self.sh_config,
            env={
                'REF_CACHE': str(dest_dir.joinpath('.ref_cache')),
                'JAVA_TOOL_OPTIONS': self.generate_gatk_java_options(
                    n_cpu=self.n_cpu, memory_mb=self.memory_mb
                )
            }
        )
        if self.use_spark:
            self.run_shell(
                args=(
                    f'set -e && {self.gatk} MarkDuplicatesSpark'
                    + f' --spark-master local[{self.n_cpu}]'
                    + f' --input {input_sam}'
                    + f' --reference {fa}'
                    + f' --metrics-file {markdup_metrics_txt}'
                    + f' --output {tmp_bams[0]}'
                    + ' --create-output-bam-index false'
                    + ' --create-output-bam-splitting-index false'
                ),
                input_files_or_dirs=[input_sam, fa, fa_dict],
                output_files_or_dirs=[tmp_bams[0], markdup_metrics_txt]
            )
            self.run_shell(
                args=(
                    f'set -e && {self.gatk} SetNmMdAndUqTags'
                    + f' --INPUT {tmp_bams[0]}'
                    + f' --OUTPUT {tmp_bams[1]}'
                    + f' --REFERENCE_SEQUENCE {fa}'
                ),
                input_files_or_dirs=[tmp_bams[0], fa, fa_dict],
                output_files_or_dirs=tmp_bams[1]
            )
        else:
            self.run_shell(
                args=(
                    f'set -e && {self.gatk} MarkDuplicates'
                    + f' --INPUT {input_sam}'
                    + f' --REFERENCE_SEQUENCE {fa}'
                    + f' --METRICS_FILE {markdup_metrics_txt}'
                    + f' --OUTPUT {tmp_bams[0]}'
                    + ' --ASSUME_SORT_ORDER coordinate'
                ),
                input_files_or_dirs=[input_sam, fa, fa_dict],
                output_files_or_dirs=[tmp_bams[0], markdup_metrics_txt]
            )
            self.run_shell(
                args=(
                    f'set -eo pipefail && {self.samtools} sort -@ {self.n_cpu}'
                    + f' -m {memory_mb_per_thread}M -O BAM -l 0'
                    + f' -T {output_cram}.sort {tmp_bams[0]}'
                    + f' | {self.gatk} SetNmMdAndUqTags'
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
            output_sam_path=output_cram, samtools=self.samtools,
            n_cpu=self.n_cpu, index_sam=True, remove_input=True
        )


@requires(SamtoolsFaidx, CreateSequenceDictionary)
class ApplyBqsr(FtarcTask):
    input_sam_path = luigi.Parameter()
    fa_path = luigi.Parameter()
    known_sites_vcf_paths = luigi.ListParameter()
    dest_dir_path = luigi.Parameter(default='.')
    static_quantized_quals = luigi.ListParameter(default=[10, 20, 30])
    gatk = luigi.Parameter(default='gatk')
    samtools = luigi.Parameter(default='samtools')
    use_spark = luigi.BoolParameter(default=False)
    save_memory = luigi.BoolParameter(default=False)
    n_cpu = luigi.IntParameter(default=1)
    memory_mb = luigi.FloatParameter(default=4096)
    sh_config = luigi.DictParameter(default=dict())
    priority = 70

    def output(self):
        dest_dir = Path(self.dest_dir_path).resolve()
        output_stem = Path(self.input_sam_path).stem + '.bqsr'
        return [
            luigi.LocalTarget(dest_dir.joinpath(f'{output_stem}.{s}'))
            for s in ['cram', 'cram.crai']
        ]

    def run(self):
        target_sam = Path(self.input_sam_path)
        run_id = target_sam.stem
        self.print_log(f'Apply base quality score recalibration:\t{run_id}')
        input_sam = target_sam.resolve()
        fa = Path(self.fa_path).resolve()
        fa_dict = fa.parent.joinpath(f'{fa.stem}.dict')
        known_sites_vcfs = [
            Path(p).resolve() for p in self.known_sites_vcf_paths
        ]
        output_cram = Path(self.output()[0].path)
        dest_dir = output_cram.parent
        tmp_bam = dest_dir.joinpath(f'{output_cram.stem}.bam')
        self.setup_shell(
            run_id=run_id, commands=[self.gatk, self.samtools],
            cwd=dest_dir, **self.sh_config,
            env={
                'REF_CACHE': str(dest_dir.joinpath('.ref_cache')),
                'JAVA_TOOL_OPTIONS': self.generate_gatk_java_options(
                    n_cpu=self.n_cpu, memory_mb=self.memory_mb
                )
            }
        )
        if self.use_spark:
            self.run_shell(
                args=(
                    f'set -e && {self.gatk} BQSRPipelineSpark'
                    + f' --spark-master local[{self.n_cpu}]'
                    + f' --input {input_sam}'
                    + f' --reference {fa}'
                    + ''.join(f' --known-sites {p}' for p in known_sites_vcfs)
                    + f' --output {tmp_bam}'
                    + ''.join(
                        f' --static-quantized-quals {i}'
                        for i in self.static_quantized_quals
                    ) + ' --use-original-qualities true'
                    + ' --create-output-bam-index false'
                    + ' --create-output-bam-splitting-index false'
                ),
                input_files_or_dirs=[
                    input_sam, fa, fa_dict, *known_sites_vcfs
                ],
                output_files_or_dirs=tmp_bam
            )
        else:
            bqsr_txt = output_cram.parent.joinpath(
                f'{output_cram.stem}.data.txt'
            )
            self.run_shell(
                args=(
                    f'set -e && {self.gatk} BaseRecalibrator'
                    + f' --input {input_sam}'
                    + f' --reference {fa}'
                    + f' --output {bqsr_txt}'
                    + ' --use-original-qualities true'
                    + ''.join(f' --known-sites {p}' for p in known_sites_vcfs)
                    + ' --disable-bam-index-caching '
                    + str(self.save_memory).lower()
                ),
                input_files_or_dirs=[
                    input_sam, fa, fa_dict, *known_sites_vcfs
                ],
                output_files_or_dirs=bqsr_txt
            )
            self.run_shell(
                args=(
                    f'set -e && {self.gatk} ApplyBQSR'
                    + f' --input {input_sam}'
                    + f' --reference {fa}'
                    + f' --bqsr-recal-file {bqsr_txt}'
                    + f' --output {tmp_bam}'
                    + ''.join(
                        f' --static-quantized-quals {i}'
                        for i in self.static_quantized_quals
                    ) + ' --add-output-sam-program-record'
                    + ' --use-original-qualities true'
                    + ' --create-output-bam-index false'
                    + ' --disable-bam-index-caching '
                    + str(self.save_memory).lower()
                ),
                input_files_or_dirs=[input_sam, fa, fa_dict, bqsr_txt],
                output_files_or_dirs=tmp_bam
            )
        self.samtools_view(
            input_sam_path=tmp_bam, fa_path=fa, output_sam_path=output_cram,
            samtools=self.samtools, n_cpu=self.n_cpu, index_sam=True,
            remove_input=True
        )


if __name__ == '__main__':
    luigi.run()
