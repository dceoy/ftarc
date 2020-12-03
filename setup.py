#!/usr/bin/env python

from setuptools import find_packages, setup

from ftarc import __version__

with open('README.md', 'r') as f:
    long_description = f.read()

setup(
    name='ftarc',
    version=__version__,
    author='Daichi Narushima',
    author_email='dnarsil+github@gmail.com',
    description='Preprocessor for Human Genome Sequencing Data',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='git@github.com:dceoy/ftarc.git',
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'docopt', 'jinja2', 'luigi', 'pip', 'psutil', 'pyyaml', 'shoper'
    ],
    entry_points={
        'console_scripts': ['ftarc=ftarc.cli.main:main']
    },
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3',
        'Topic :: Software Development'
    ],
    python_requires='>=3.6',
)
