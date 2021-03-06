#!/usr/bin/env python

from pathlib import Path

import luigi
from luigi.util import requires

from .core import FtarcTask
from .picard import CreateSequenceDictionary, MarkDuplicates
from .resource import FetchKnownSitesVcfs, FetchReferenceFasta
from .samtools import RemoveDuplicates


@requires(MarkDuplicates, FetchReferenceFasta, CreateSequenceDictionary,
          FetchKnownSitesVcfs)
class RecalibrateBaseQualityScoresAndDeduplicateReads(luigi.Task):
    cf = luigi.DictParameter()
    n_cpu = luigi.IntParameter(default=1)
    memory_mb = luigi.FloatParameter(default=4096)
    sh_config = luigi.DictParameter(default=dict())
    priority = 70

    def output(self):
        input_cram = Path(self.input()[0][0].path)
        return [
            luigi.LocalTarget(
                input_cram.parent.joinpath(f'{input_cram.stem}.bqsr.{s}')
            ) for s in ['cram', 'cram.crai', 'dedup.cram', 'dedup.cram.crai']
        ]

    def run(self):
        yield DeduplicateReads(
            input_sam_path=self.input()[0][0].path,
            fa_path=self.input()[1][0].path,
            known_sites_vcf_paths=[i[0].path for i in self.input()[3]],
            dest_dir_path=str(Path(self.output()[0].path).parent),
            gatk=self.cf['gatk'], samtools=self.cf['samtools'],
            use_spark=self.cf['use_spark'], save_memory=self.cf['save_memory'],
            n_cpu=self.n_cpu, memory_mb=self.memory_mb,
            sh_config=self.sh_config
        )


class ApplyBQSR(FtarcTask):
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
        output_path_prefix = str(
            Path(self.dest_dir_path).resolve().joinpath(
                Path(self.input_sam_path).stem
            )
        )
        return [
            luigi.LocalTarget(f'{output_path_prefix}.bqsr.{s}')
            for s in ['cram', 'cram.crai']
        ]

    def run(self):
        target_sam = Path(self.input_sam_path)
        run_id = target_sam.stem
        self.print_log(f'Apply base quality score recalibration:\t{run_id}')
        input_sam = target_sam.resolve()
        output_cram = Path(self.output()[0].path)
        fa = Path(self.fa_path).resolve()
        fa_dict = fa.parent.joinpath(f'{fa.stem}.dict')
        known_sites_vcfs = [
            Path(p).resolve() for p in self.known_sites_vcf_paths
        ]
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


@requires(ApplyBQSR)
class DeduplicateReads(FtarcTask):
    fa_path = luigi.Parameter()
    samtools = luigi.Parameter(default='samtools')
    n_cpu = luigi.IntParameter(default=1)
    sh_config = luigi.DictParameter(default=dict())
    priority = 70

    def output(self):
        input_cram = Path(self.input()[0].path)
        return [
            luigi.LocalTarget(
                input_cram.parent.joinpath(f'{input_cram.stem}.dedup.cram{s}')
            ) for s in ['', '.crai']
        ]

    def run(self):
        input_cram = Path(self.input()[0].path)
        yield RemoveDuplicates(
            input_sam_path=str(input_cram), fa_path=self.fa_path,
            dest_dir_path=str(input_cram.parent), samtools=self.samtools,
            n_cpu=self.n_cpu, remove_input=False, index_sam=True,
            sh_config=self.sh_config
        )


if __name__ == '__main__':
    luigi.run()
