ftarc
=====

FASTQ-to-analysis-ready-CRAM Workflow Executor for Human Genome Sequencing

[![Test](https://github.com/dceoy/ftarc/actions/workflows/test.yml/badge.svg)](https://github.com/dceoy/ftarc/actions/workflows/test.yml)
[![Upload Python Package](https://github.com/dceoy/ftarc/actions/workflows/python-publish.yml/badge.svg)](https://github.com/dceoy/ftarc/actions/workflows/python-publish.yml)
[![CI to Docker Hub](https://github.com/dceoy/ftarc/actions/workflows/docker-publish.yml/badge.svg)](https://github.com/dceoy/ftarc/actions/workflows/docker-publish.yml)

Installation
------------

```sh
$ pip install -U ftarc
```

Dependent commands:

- `pigz`
- `pbzip2`
- `bgzip`
- `tabix`
- `samtools` (and `plot-bamstats`)
- `gnuplot`
- `java`
- `gatk`
- `cutadapt`
- `fastqc`
- `trim_galore`
- `bwa` or `bwa-mem2`

Docker image
------------

Pull the image from [Docker Hub](https://hub.docker.com/r/dceoy/ftarc/).

```sh
$ docker image pull dceoy/ftarc
```

Usage
-----

#### Create analysis-ready CRAM files from FASTQ files

| input files                   | output files        |
|:-----------------------------:|:-------------------:|
| read1/read2 FASTQ (Illumina)  | analysis-ready CRAM |

1.  Download hg38 resource data.

    ```sh
    $ ftarc download --dest-dir=/path/to/download/dir
    ```

2.  Write input file paths and configurations into `ftarc.yml`.

    ```sh
    $ ftarc init
    $ vi ftarc.yml  # => edit
    ```

    Example of `ftarc.yml`:

    ```yaml
    ---
    reference_name: hs38DH
    adapter_removal: true
    metrics_collectors:
      fastqc: true
      picard: true
      samtools: true
    resources:
      reference_fa: /path/to/GRCh38_full_analysis_set_plus_decoy_hla.fa
      known_sites_vcf:
        - /path/to/Homo_sapiens_assembly38.dbsnp138.vcf.gz
        - /path/to/Mills_and_1000G_gold_standard.indels.hg38.vcf.gz
        - /path/to/Homo_sapiens_assembly38.known_indels.vcf.gz
    runs:
      - fq:
          - /path/to/sample01.WGS.R1.fq.gz
          - /path/to/sample01.WGS.R2.fq.gz
      - fq:
          - /path/to/sample02.WGS.R1.fq.gz
          - /path/to/sample02.WGS.R2.fq.gz
      - fq:
          - /path/to/sample03.WGS.R1.fq.gz
          - /path/to/sample03.WGS.R2.fq.gz
        read_group:
          ID: FLOWCELL-1
          PU: UNIT-1
          SM: sample03
          PL: ILLUMINA
          LB: LIBRARY-1
    ```

3.  Create analysis-ready CRAM files from FASTQ files

    ```sh
    $ ftarc pipeline --yml=ftarc.yml --workers=2
    ```

    Standard workflow:
    1.  Trim adapters
        - `trim_galore`
    2.  Map reads to a human reference genome
        - `bwa mem` (or `bwa-mem2 mem`)
    3.  Mark duplicates
        - `gatk MarkDuplicates`
        - `gatk SetNmMdAndUqTags`
    4.  Apply BQSR (Base Quality Score Recalibration)
        - `gatk BaseRecalibrator`
        - `gatk ApplyBQSR`
    5.  Remove duplicates
        - `samtools view`
    6.  Validate output CRAM files
        - `gatk ValidateSamFile`
    7.  Collect QC metrics
        - `fastqc`
        - `samtools`
        - `gatk`

#### Preprocessing and QC-check

- Validate BAM or CRAM files using Picard

  ```sh
  $ ftarc validate /path/to/genome.fa /path/to/aligned.cram
  ```

- Collect metrics from FASTQ files using FastQC

  ```sh
  $ ftarc fastqc read1.fq.gz read2.fq.gz
  ```

- Collect metrics from FASTQ files using FastQC

  ```sh
  $ ftarc samqc /path/to/genome.fa /path/to/aligned.cram
  ```

- Apply BQSR to BAM or CRAM files using GATK

  ```sh
  $ ftarc bqsr \
      --known-sites-vcf=/path/to/Homo_sapiens_assembly38.dbsnp138.vcf.gz \
      --known-sites-vcf=/path/to/Mills_and_1000G_gold_standard.indels.hg38.vcf.gz \
      --known-sites-vcf=/path/to/Homo_sapiens_assembly38.known_indels.vcf.gz \
      /path/to/genome.fa /path/to/markdup.cram
  ```

- Remove duplicates in marked BAM or CRAM files

  ```sh
  $ ftarc dedup /path/to/genome.fa /path/to/markdup.cram
  ```

Run `ftarc --help` for more information.
