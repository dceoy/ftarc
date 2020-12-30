#!/usr/bin/env python

import logging
import os
import re
import shutil
from pathlib import Path
from pprint import pformat

import luigi
import yaml
from jinja2 import Environment, FileSystemLoader


def write_config_yml(path, src_yml='example_ftarc.yml'):
    if Path(path).is_file():
        print_log(f'The file exists:\t{path}')
    else:
        print_log(f'Create a config YAML:\t{path}')
        shutil.copyfile(
            str(Path(__file__).parent.joinpath('../static').joinpath(src_yml)),
            Path(path).resolve()
        )


def print_log(message):
    logger = logging.getLogger(__name__)
    logger.debug(message)
    print(f'>>\t{message}', flush=True)


def fetch_executable(cmd, ignore_errors=False):
    executables = [
        cp for cp in [
            str(Path(p).joinpath(cmd))
            for p in os.environ['PATH'].split(os.pathsep)
        ] if os.access(cp, os.X_OK)
    ]
    if executables:
        return executables[0]
    elif ignore_errors:
        return None
    else:
        raise RuntimeError(f'command not found: {cmd}')


def read_yml(path):
    logger = logging.getLogger(__name__)
    with open(str(path), 'r') as f:
        d = yaml.load(f, Loader=yaml.FullLoader)
    logger.debug('YAML data:' + os.linesep + pformat(d))
    return d


def render_template(template, data, output_path):
    po = (Path(output_path) if isinstance(output_path, str) else output_path)
    print_log(('Overwrite' if po.exists() else 'Render') + f' a file:\t{po}')
    with po.open(mode='w') as f:
        f.write(
            Environment(
                loader=FileSystemLoader(
                    str(Path(__file__).parent.joinpath('../template')),
                    encoding='utf8'
                )
            ).get_template(template).render(data) + os.linesep
        )


def load_default_dict(stem):
    return read_yml(
        path=Path(__file__).parent.parent.joinpath(f'static/{stem}.yml')
    )


def build_luigi_tasks(*args, **kwargs):
    r = luigi.build(
        *args,
        **{
            k: v for k, v in kwargs.items() if (
                k not in {'logging_conf_file', 'hide_summary'}
                or (k == 'logging_conf_file' and v)
            )
        },
        local_scheduler=True, detailed_summary=True
    )
    if not kwargs.get('hide_summary'):
        print(
            os.linesep
            + os.linesep.join(['Execution summary:', r.summary_text, str(r)])
        )


def parse_fq_id(fq_path):
    fq_stem = Path(fq_path).name
    for _ in range(3):
        if fq_stem.endswith(('fq', 'fastq')):
            fq_stem = Path(fq_stem).stem
            break
        else:
            fq_stem = Path(fq_stem).stem
    return (
        re.sub(
            r'[\._](read[12]|r[12]|[12]|[a-z0-9]+_val_[12]|r[12]_[0-9]+)$', '',
            fq_stem, flags=re.IGNORECASE
        ) or fq_stem
    )
