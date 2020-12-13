#!/usr/bin/env python

import re
from pathlib import Path

import luigi
from luigi.util import requires

from .base import ShellTask
from .bwa import AlignReads
from .resource import FetchReferenceFASTA
from .samtools import samtools_view_and_index


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
            env={'JAVA_TOOL_OPTIONS': self.cf['gatk_java_options']}
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
        fa_path = self.input()[1][0].path
        output_cram_path = self.output()[0].path
        tmp_bam_paths = [
            re.sub(r'\.cram', f'{s}.bam', output_cram_path)
            for s in ['.unfixed.unsorted', '']
        ]
        markdup_metrics_txt_path = self.output()[2].path
        self.setup_shell(
            run_id=run_id, log_dir_path=self.cf['log_dir_path'],
            commands=[gatk, samtools], cwd=input_cram.parent,
            remove_if_failed=self.cf['remove_if_failed'],
            quiet=self.cf['quiet'],
            env={
                'REF_CACHE': '.ref_cache',
                'JAVA_TOOL_OPTIONS': self.cf['gatk_java_options']
            }
        )
        self.run_shell(
            args=(
                f'set -e && {gatk} MarkDuplicates'
                + f' --INPUT {input_cram}'
                + f' --REFERENCE_SEQUENCE {fa_path}'
                + f' --METRICS_FILE {markdup_metrics_txt_path}'
                + f' --OUTPUT {tmp_bam_paths[0]}'
                + ' --ASSUME_SORT_ORDER coordinate'
            ),
            input_files_or_dirs=[input_cram, fa_path],
            output_files_or_dirs=tmp_bam_paths[0]
        )
        self.run_shell(
            args=(
                f'set -eo pipefail && {samtools} sort -@ {n_cpu}'
                + f' -m {memory_mb_per_thread}M -O bam -l 0'
                + f' -T {output_cram_path}.sort {tmp_bam_paths[0]}'
                + f' | {gatk} SetNmMdAndUqTags'
                + ' --INPUT /dev/stdin'
                + f' --OUTPUT {tmp_bam_paths[1]}'
                + f' --REFERENCE_SEQUENCE {fa_path}'
            ),
            input_files_or_dirs=[tmp_bam_paths[0], fa_path],
            output_files_or_dirs=tmp_bam_paths[1]
        )
        samtools_view_and_index(
            shelltask=self, samtools=samtools, input_sam_path=tmp_bam_paths[1],
            fa_path=fa_path, output_sam_path=output_cram_path, n_cpu=n_cpu
        )
        self.run_shell(
            args='rm -f {0} {1}'.format(*tmp_bam_paths),
            input_files_or_dirs=tmp_bam_paths
        )


if __name__ == '__main__':
    luigi.run()
