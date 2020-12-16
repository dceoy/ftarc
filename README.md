ftarc
=====

FASTQ-to-analysis-ready-CRAM Workflow Executor for Human Genome Sequencing

[![wercker status](https://app.wercker.com/status/5009106bfe21f2c24d5084a3ba336463/s/main "wercker status")](https://app.wercker.com/project/byKey/5009106bfe21f2c24d5084a3ba336463)

- Input:
  - read1/read2 FASTQ files from Illumina DNA sequencers
- Workflow:
  - Trim adapters
  - Map reads to a human reference genome
  - Mark duplicates
  - Apply BQSR (Base Quality Score Recalibration)
- Output:
  - analysis-ready CRAM files

Installation
------------

```sh
$ pip install -U https://github.com/dceoy/ftarc/archive/main.tar.gz
```

Dependent commands:

- pigz
- pbzip2
- bgzip
- tabix
- samtools
- java
- gatk
- cutadapt
- fastqc
- trim_galore
- bwa
- bwa-mem2 (optional)

Docker image
------------

Pull the image from [Docker Hub](https://hub.docker.com/r/dceoy/ftarc/).

```sh
$ docker image pull dceoy/ftarc
```

Usage
-----

1.  Download hg38 resource data.

    ```sh
    $ ftarc download --dest-dir=/path/to/download/dir
    ```

2.  Write input file paths and configurations into `ftarc.yml`.

    ```sh
    $ ftarc init
    $ vi ftarc.yml  # => edit
    ```

3.  Create analysis-ready CRAM files from FASTQ files

    ```sh
    $ ftarc run --workers=2
    ```

Run `ftarc --help` for more information.
