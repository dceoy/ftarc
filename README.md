ftarc
=====

FASTQ-to-analysis-ready-CRAM Workflow Executor for Human Genome Sequencing

[![wercker status](https://app.wercker.com/status/5009106bfe21f2c24d5084a3ba336463/s/main "wercker status")](https://app.wercker.com/project/byKey/5009106bfe21f2c24d5084a3ba336463)
![Upload Python Package](https://github.com/dceoy/ftarc/workflows/Upload%20Python%20Package/badge.svg)

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
$ pip install -U ftarc
```

Dependent commands:

- `pigz`
- `pbzip2`
- `bgzip`
- `tabix`
- `samtools`
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
      ref_fa: /path/to/GRCh38_full_analysis_set_plus_decoy_hla.fa
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
    $ ftarc run --yml=ftarc.yml --workers=2
    ```

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
      --known-sites=/path/to/Homo_sapiens_assembly38.dbsnp138.vcf.gz \
      --known-sites=/path/to/Mills_and_1000G_gold_standard.indels.hg38.vcf.gz \
      --known-sites=/path/to/Homo_sapiens_assembly38.known_indels.vcf.gz \
      /path/to/genome.fa /path/to/markdup.cram
  ```

- Remove duplicates in marked BAM or CRAM files

  ```sh
  $ ftarc dedup /path/to/genome.fa /path/to/markdup.cram
  ```

Run `ftarc --help` for more information.
