# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

FTARC (FASTQ-to-analysis-ready-CRAM) is a Luigi-based workflow executor for human genome sequencing. It provides a comprehensive pipeline system for processing genomic sequencing data through various stages including adapter trimming, read alignment, duplicate marking, base quality score recalibration (BQSR), and quality control metrics collection.

## Development Commands

### Environment Setup

```sh
uv sync
```

### Testing

```sh
# Test basic functionality
ftarc init
ftarc --help
```

### Code Quality

```sh
# Run linting
uv run ruff check .

# Run linting with auto-fix
uv run ruff check --fix .

# Run type checking
uv run pyright .
```

### Building and Packaging

```sh
uv build
```

## Architecture

### Core Components

1. **CLI Interface (`ftarc/cli/`)**: Command-line interface using docopt
   - `main.py`: Main entry point with command parsing
   - `pipeline.py`: Pipeline orchestration and configuration management
   - `util.py`: Shared utilities for Luigi task building and executable fetching

2. **Task Layer (`ftarc/task/`)**: Luigi-based task definitions
   - `controller.py`: High-level pipeline coordination tasks
   - `core.py`: Base task classes and shared functionality
   - Tool-specific modules: `bwa.py`, `gatk.py`, `picard.py`, `samtools.py`, `trimgalore.py`, `fastqc.py`
   - `downloader.py`: Resource file downloading and processing
   - `resource.py`: Resource management utilities

3. **Configuration (`ftarc/static/`)**: Static configuration files
   - `example_ftarc.yml`: Template configuration file
   - `urls.yml`: Resource download URLs

### Data Flow

1. User provides FASTQ files and configuration
2. CLI parses arguments and initializes Luigi pipeline
3. Pipeline executes tasks in dependency order:
   - Adapter trimming (trim_galore)
   - Read alignment (bwa mem or bwa-mem2)
   - Duplicate marking (GATK MarkDuplicates)
   - BQSR (GATK BaseRecalibrator + ApplyBQSR)
   - Duplicate removal (samtools view)
   - Validation (GATK ValidateSamFile)
   - QC metrics collection (FastQC, Samtools, GATK)
4. Final CRAM files and QC reports are generated

### Key Design Patterns

- **Task Graph Pattern**: Luigi manages complex task dependencies
- **Command Pattern**: Each processing step encapsulated as a Luigi task
- **Factory Pattern**: Dynamic task creation based on configuration
- **Template Pattern**: Base task classes provide common functionality

## CLI Commands

### Resource Management

```sh
# Download reference genome resources
ftarc download --dest-dir=/path/to/resources
```

### Pipeline Execution

```sh
# Initialize configuration
ftarc init  # Creates ftarc.yml template

# Run full pipeline
ftarc pipeline --yml=ftarc.yml --workers=2

# Individual pipeline steps
ftarc trim <fq_path_prefix>...
ftarc align <fa_path> <fq_path_prefix>...
ftarc markdup <fa_path> <sam_path>...
ftarc bqsr --known-sites-vcf=<path> <fa_path> <sam_path>...
ftarc dedup <fa_path> <sam_path>...
ftarc validate <fa_path> <sam_path>...
ftarc fastqc <fq_path>...
ftarc samqc <fa_path> <sam_path>...
ftarc depth <sam_path>...
```

## Dependencies

### External Tools (must be in PATH)

- Compression: `pigz`, `pbzip2`, `bgzip`, `tabix`
- Alignment: `bwa` or `bwa-mem2`
- SAM/BAM processing: `samtools`, `plot-bamstats`
- Variant calling: `java`, `gatk`
- QC: `cutadapt`, `fastqc`, `trim_galore`
- Visualization: `gnuplot` (for plot-bamstats)

### Python Dependencies

- Core: `docopt`, `jinja2`, `luigi`, `pip`, `psutil`, `pyyaml`, `shoper`

## Configuration File Structure

The `ftarc.yml` configuration includes:

```yaml
reference_name: GRCh38  # Reference genome identifier
adapter_removal: true  # Boolean for adapter trimming
metrics_collectors:  # QC tools to run
  fastqc: true
  samtools: true
  gatk: true
resources:  # Paths to reference files
  ref_fa: /path/to/reference.fa
  known_sites_vcf:
    - /path/to/dbsnp.vcf.gz
    - /path/to/mills.vcf.gz
runs:  # Sample information
  - fq1: /path/to/sample_1.fq.gz
    fq2: /path/to/sample_2.fq.gz
    read_group: "@RG\\tID:1\\tSM:sample\\tPL:ILLUMINA"
```

## Web Search Instructions

For tasks requiring web search, always use Gemini CLI (`gemini` command) instead of the built-in web search tools (WebFetch and WebSearch).
Gemini CLI is an AI workflow tool that provides reliable web search capabilities.

### Usage

```sh
# Basic search query
gemini --sandbox --prompt "WebSearch: <query>"

# Example: Search for latest news
gemini --sandbox --prompt "WebSearch: What are the latest developments in AI?"
```

### Policy

When users request information that requires web search:

1. Use `gemini --sandbox --prompt` command via terminal
2. Parse and present the Gemini response appropriately

This ensures consistent and reliable web search results through the Gemini API.

## Code Design Principles

Follow Robert C. Martin's SOLID and Clean Code principles:

### SOLID Principles

1. **SRP (Single Responsibility)**: One reason to change per class; separate concerns (e.g., storage vs formatting vs calculation)
2. **OCP (Open/Closed)**: Open for extension, closed for modification; use polymorphism over if/else chains
3. **LSP (Liskov Substitution)**: Subtypes must be substitutable for base types without breaking expectations
4. **ISP (Interface Segregation)**: Many specific interfaces over one general; no forced unused dependencies
5. **DIP (Dependency Inversion)**: Depend on abstractions, not concretions; inject dependencies

### Clean Code Practices

- **Naming**: Intention-revealing, pronounceable, searchable names (`daysSinceLastUpdate` not `d`)
- **Functions**: Small, single-task, verb names, 0-3 args, extract complex logic
- **Classes**: Follow SRP, high cohesion, descriptive names
- **Error Handling**: Exceptions over error codes, no null returns, provide context, try-catch-finally first
- **Testing**: TDD, one assertion/test, FIRST principles (Fast, Independent, Repeatable, Self-validating, Timely), Arrange-Act-Assert pattern
- **Code Organization**: Variables near usage, instance vars at top, public then private functions, conceptual affinity
- **Comments**: Self-documenting code preferred, explain "why" not "what", delete commented code
- **Formatting**: Consistent, vertical separation, 88-char limit, team rules override preferences
- **General**: DRY, KISS, YAGNI, Boy Scout Rule, fail fast

## Development Methodology

Follow Martin Fowler's Refactoring, Kent Beck's Tidy Code, and t_wada's TDD principles:

### Core Philosophy

- **Small, safe changes**: Tiny, reversible, testable modifications
- **Separate concerns**: Never mix features with refactoring
- **Test-driven**: Tests provide safety and drive design
- **Economic**: Only refactor when it aids immediate work

### TDD Cycle

1. **Red** → Write failing test
2. **Green** → Minimum code to pass
3. **Refactor** → Clean without changing behavior
4. **Commit** → Separate commits for features vs refactoring

### Practices

- **Before**: Create TODOs, ensure coverage, identify code smells
- **During**: Test-first, small steps, frequent tests, two hats rule
- **Refactoring**: Extract function/variable, rename, guard clauses, remove dead code, normalize symmetries
- **TDD Strategies**: Fake it, obvious implementation, triangulation

### When to Apply

- Rule of Three (3rd duplication)
- Preparatory (before features)
- Comprehension (as understanding grows)
- Opportunistic (daily improvements)

### Key Rules

- One assertion per test
- Separate refactoring commits
- Delete redundant tests
- Human-readable code first

> "Make the change easy, then make the easy change." - Kent Beck
