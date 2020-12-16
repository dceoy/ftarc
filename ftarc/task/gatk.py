#!/usr/bin/env python

import re
from pathlib import Path

import luigi
from luigi.util import requires

from .base import ShellTask
from .picard import (CreateSequenceDictionary, MarkDuplicates,
                     generate_gatk_java_options)
from .resource import (FetchDbsnpVCF, FetchKnownIndelVCF, FetchMillsIndelVCF,
                       FetchReferenceFASTA)
from .samtools import samtools_view_and_index


@requires(MarkDuplicates, FetchReferenceFASTA, CreateSequenceDictionary,
          FetchDbsnpVCF, FetchMillsIndelVCF, FetchKnownIndelVCF)
class ApplyBQSR(ShellTask):
    cf = luigi.DictParameter()
    priority = 70

    def output(self):
        input_cram = Path(self.input()[0][0].path)
        return [
            luigi.LocalTarget(
                input_cram.parent.joinpath(f'{input_cram.stem}.bqsr.{s}')
            ) for s in ['cram', 'cram.crai', 'data.csv']
        ]

    def run(self):
        input_cram = Path(self.input()[0][0].path)
        run_id = input_cram.stem
        self.print_log(f'Apply Base Quality Score Recalibration:\t{run_id}')
        gatk = self.cf['gatk']
        samtools = self.cf['samtools']
        n_cpu = self.cf['n_cpu_per_worker']
        save_memory = str(self.cf['save_memory']).lower()
        output_cram_path = self.output()[0].path
        fa_path = self.input()[1][0].path
        fa_dict_path = self.input()[2].path
        known_site_vcf_gz_paths = [i[0].path for i in self.input()[3:6]]
        bqsr_csv_path = self.output()[2].path
        tmp_bam_path = re.sub(r'\.cram', '.bam', output_cram_path)
        self.setup_shell(
            run_id=run_id, log_dir_path=self.cf['log_dir_path'],
            commands=[gatk, samtools], cwd=input_cram.parent,
            remove_if_failed=self.cf['remove_if_failed'],
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
                f'set -e && {gatk} BaseRecalibrator'
                + f' --input {input_cram}'
                + f' --reference {fa_path}'
                + f' --output {bqsr_csv_path}'
                + ' --use-original-qualities'
                + ''.join([
                    f' --known-sites {p}' for p in known_site_vcf_gz_paths
                ])
            ),
            input_files_or_dirs=[
                input_cram, fa_path, fa_dict_path,
                *known_site_vcf_gz_paths
            ],
            output_files_or_dirs=bqsr_csv_path
        )
        self.run_shell(
            args=(
                f'set -e && {gatk} ApplyBQSR'
                + f' --input {input_cram}'
                + f' --reference {fa_path}'
                + f' --bqsr-recal-file {bqsr_csv_path}'
                + f' --output {tmp_bam_path}'
                + ' --static-quantized-quals 10'
                + ' --static-quantized-quals 20'
                + ' --static-quantized-quals 30'
                + ' --add-output-sam-program-record'
                + ' --use-original-qualities'
                + ' --create-output-bam-index false'
                + f' --disable-bam-index-caching {save_memory}'
            ),
            input_files_or_dirs=[input_cram, fa_path, bqsr_csv_path],
            output_files_or_dirs=tmp_bam_path
        )
        samtools_view_and_index(
            shelltask=self, samtools=samtools, input_sam_path=tmp_bam_path,
            fa_path=fa_path, output_sam_path=output_cram_path, n_cpu=n_cpu
        )
        self.run_shell(
            args=f'rm -f {tmp_bam_path}', input_files_or_dirs=tmp_bam_path
        )


if __name__ == '__main__':
    luigi.run()
