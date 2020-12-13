#!/usr/bin/env python

import sys
from pathlib import Path

import luigi
from luigi.util import requires

from .base import ShellTask
from .gatk import RemoveDuplicates


class PrintEnvVersions(ShellTask):
    log_dir_path = luigi.Parameter()
    command_paths = luigi.ListParameter(default=list())
    run_id = luigi.Parameter(default='env')
    quiet = luigi.BoolParameter(default=False)
    priority = luigi.IntParameter(default=sys.maxsize)
    __is_completed = False

    def complete(self):
        return self.__is_completed

    def run(self):
        python = sys.executable
        self.print_log(f'Print environment versions: {python}')
        version_files = [
            Path('/proc/version'),
            *[
                o for o in Path('/etc').iterdir()
                if o.name.endswith(('-release', '_version'))
            ]
        ]
        self.setup_shell(
            run_id=self.run_id, log_dir_path=self.log_dir_path,
            commands=[python, *self.command_paths], quiet=self.quiet
        )
        self.run_shell(
            args=[
                f'{python} -m pip --version',
                f'{python} -m pip freeze --no-cache-dir'
            ]
        )
        self.run_shell(
            args=[
                'uname -a',
                *[f'cat {o}' for o in version_files if o.is_file()]
            ]
        )
        self.__is_completed = True


@requires(RemoveDuplicates)
class PrepareAnalysisReadyCRAM(luigi.WrapperTask):
    priority = luigi.IntParameter(default=sys.maxsize)

    def output(self):
        return self.input()
