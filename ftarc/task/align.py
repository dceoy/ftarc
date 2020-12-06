#!/usr/bin/env python

import re
from pathlib import Path

import luigi
from luigi.util import requires

from .base import ShellTask
from .ref import (CreateBWAIndices, CreateSequenceDictionary, FetchDbsnpVCF,
                  FetchKnownIndelVCF, FetchMillsIndelVCF, FetchReferenceFASTA)
from .samtools import SamtoolsView, samtools_index, samtools_view_and_index
from .trim import PrepareFASTQs


@requires(PrepareFASTQs, FetchReferenceFASTA, CreateBWAIndices)
class AlignReads(ShellTask):
    sample_name = luigi.Parameter()
    read_group = luigi.DictParameter()
    cf = luigi.DictParameter()
    priority = 70

    def output(self):
        output_cram = Path(self.cf['align_dir_path']).joinpath(
            '{0}/{0}{1}.{2}.cram'.format(
                self.sample_name,
                ('.trim' if self.cf['adapter_removal'] else ''),
                (
                    self.cf['reference_name']
                    or Path(self.input()[1][0].path).stem
                )
            )
        )
        return [luigi.LocalTarget(f'{output_cram}{s}') for s in ['', '.crai']]

    def run(self):
        output_cram = Path(self.output()[0].path)
        run_id = output_cram.stem
        self.print_log(f'Align reads:\t{run_id}')
        bwa = self.cf['bwa']
        samtools = self.cf['samtools']
        n_cpu = self.cf['n_cpu_per_worker']
        memory_mb_per_thread = int(self.cf['memory_mb_per_worker'] / n_cpu / 8)
        fq_paths = [i.path for i in self.input()[0]]
        rg = '\\t'.join(
            [
                '@RG',
                'ID:{}'.format(self.read_group.get('ID') or 1),
                'PU:{}'.format(self.read_group.get('PU') or 'UNIT-1'),
                'SM:{}'.format(self.read_group.get('SM') or self.sample_name),
                'PL:{}'.format(self.read_group.get('PL') or 'ILLUMINA'),
                'LB:{}'.format(self.read_group.get('LB') or 'LIBRARY-1')
            ] + [
                f'{k}:{v}' for k, v in self.read_group.items()
                if k not in ['ID', 'PU', 'SM', 'PL', 'LB']
            ]
        )
        fa_path = self.input()[1][0].path
        index_paths = [o.path for o in self.input()[2]]
        self.setup_shell(
            run_id=run_id, log_dir_path=self.cf['log_dir_path'],
            commands=[bwa, samtools], cwd=output_cram.parent,
            remove_if_failed=self.cf['remove_if_failed'],
            quiet=self.cf['quiet'], env={'REF_CACHE': '.ref_cache'}
        )
        self.run_shell(
            args=(
                'set -eo pipefail && '
                + f'{bwa} mem -t {n_cpu} -R \'{rg}\' -T 0 -P {fa_path}'
                + ''.join([f' {a}' for a in fq_paths])
                + f' | {samtools} sort -@ {n_cpu} -m {memory_mb_per_thread}M'
                + f' -O bam -l 0 -T {output_cram}.sort -'
                + f' | {samtools} view -@ {n_cpu} -T {fa_path} -CS'
                + f' -o {output_cram} -'
            ),
            input_files_or_dirs=[fa_path, *index_paths, *fq_paths],
            output_files_or_dirs=[output_cram, output_cram.parent]
        )
        samtools_index(
            shelltask=self, samtools=samtools, sam_path=output_cram,
            n_cpu=n_cpu
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
        gatk = self.cf['gatk']
        gatk_opts = ' --java-options "{}"'.format(self.cf['gatk_java_options'])
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
            quiet=self.cf['quiet'], env={'REF_CACHE': '.ref_cache'}
        )
        self.run_shell(
            args=(
                f'set -e && {gatk}{gatk_opts} MarkDuplicates'
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
                + f' | {gatk}{gatk_opts} SetNmMdAndUqTags'
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
        gatk_opts = ' --java-options "{}"'.format(self.cf['gatk_java_options'])
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
            quiet=self.cf['quiet']
        )
        self.run_shell(
            args=(
                f'set -e && {gatk}{gatk_opts} BaseRecalibrator'
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
                f'set -e && {gatk}{gatk_opts} ApplyBQSR'
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


@requires(ApplyBQSR, FetchReferenceFASTA)
class RemoveDuplicates(luigi.Task):
    sample_name = luigi.Parameter()
    cf = luigi.DictParameter()
    priority = 70

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


if __name__ == '__main__':
    luigi.run()
