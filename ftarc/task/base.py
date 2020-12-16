#!/usr/bin/env python

import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

import luigi
from shoper.shelloperator import ShellOperator


class BaseTask(luigi.Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @luigi.Task.event_handler(luigi.Event.PROCESSING_TIME)
    def print_execution_time(self, processing_time):
        logger = logging.getLogger('task-timer')
        message = '{0}.{1} - total elapsed time:\t{2}'.format(
            self.__class__.__module__, self.__class__.__name__,
            timedelta(seconds=processing_time)
        )
        logger.info(message)
        print(message, flush=True)

    @classmethod
    def print_log(cls, message, new_line=True):
        logger = logging.getLogger(cls.__name__)
        logger.info(message)
        print((os.linesep if new_line else '') + f'>>\t{message}', flush=True)


class ShellTask(BaseTask):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__log_txt_path = None
        self.__sh = None
        self.__run_kwargs = None

    @classmethod
    def setup_shell(cls, run_id=None, log_dir_path=None, commands=None,
                    cwd=None, remove_if_failed=True, clear_log_txt=False,
                    print_command=True, quiet=True, executable='/bin/bash',
                    **kwargs):
        cls.__log_txt_path = (
            str(
                Path(log_dir_path).joinpath(
                    f'{cls.__module__}.{cls.__name__}.{run_id}.sh.log.txt'
                ).resolve()
            ) if log_dir_path and run_id else None
        )
        cls.__sh = ShellOperator(
            log_txt=cls.__log_txt_path, quiet=quiet,
            clear_log_txt=clear_log_txt,
            logger=logging.getLogger(cls.__name__),
            print_command=print_command, executable=executable
        )
        cls.__run_kwargs = {
            'cwd': cwd, 'remove_if_failed': remove_if_failed, **kwargs
        }
        for p in [log_dir_path, cwd]:
            if p:
                d = Path(p).resolve()
                if not d.is_dir():
                    cls.print_log(f'Make a directory:\t{d}', new_line=False)
                    d.mkdir(parents=True, exist_ok=True)
        if commands:
            cls.run_shell(args=list(cls._generate_version_commands(commands)))

    @classmethod
    def run_shell(cls, *args, **kwargs):
        logger = logging.getLogger(cls.__name__)
        start_datetime = datetime.now()
        cls.__sh.run(
            *args, **kwargs,
            **{k: v for k, v in cls.__run_kwargs.items() if k not in kwargs}
        )
        if 'asynchronous' in kwargs:
            cls.__sh.wait()
        elapsed_timedelta = datetime.now() - start_datetime
        message = f'shell elapsed time:\t{elapsed_timedelta}'
        logger.info(message)
        if cls.__log_txt_path:
            with open(cls.__log_txt_path, 'a') as f:
                f.write(f'### {message}{os.linesep}')

    @staticmethod
    def _generate_version_commands(commands):
        for c in ([commands] if isinstance(commands, str) else commands):
            n = Path(c).name
            if n in {'java', 'snpEff'} or n.endswith('.jar'):
                yield f'{c} -version'
            elif n in {'bwa', 'msisensor'}:
                yield f'{c} 2>&1 | grep -e "Program:" -e "Version:"'
            elif n == 'wget':
                yield f'{c} --version | head -1'
            elif n == 'bwa-mem2':
                yield f'{c} version'
            elif n == 'dotnet':
                yield f'{c} --info'
            elif n == 'picard':
                yield f'{c} CreateSequenceDictionary --version'
            else:
                yield f'{c} --version'
