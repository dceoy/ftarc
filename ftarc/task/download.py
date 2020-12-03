#!/usr/bin/env python

from pathlib import Path
from urllib.parse import urlparse

import luigi

from .base import ShellTask


class DownloadResourceFilesRecursively(ShellTask):
    ftp_src_url = luigi.Parameter()
    dest_path = luigi.Parameter()
    wget = luigi.Parameter(default='wget')

    def output(self):
        return luigi.LocalTarget(self.dest_path)

    def run(self):
        dest = Path(self.output().path)
        uo = urlparse(self.ftp_src_url)
        tmp_root_dir = dest.parent.joinpath(uo.netloc)
        tmp_dest = dest.parent.joinpath(uo.netloc + uo.path)
        self.print_log(f'Download resource files recursively:\t{dest}')
        self.setup_shell(commands=self.wget, cwd=dest.parent, quiet=False)
        self.run_shell(
            args=[
                (
                    'set -e && {self.wget} -qSL --recursive --convert-links'
                    f' --no-clobber --random-wait --no-parent {uo.geturl()}'
                ),
                f'mv {tmp_dest} {dest}',
                f'rm -rf {tmp_root_dir}'
            ],
            output_files_or_dirs=dest
        )


if __name__ == '__main__':
    luigi.run()
