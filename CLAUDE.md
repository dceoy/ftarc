# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

ftarc is a FASTQ-to-analysis-ready-CRAM workflow executor for human genome sequencing. It provides a Luigi-based pipeline system for processing genomic sequencing data through various stages including adapter trimming, read alignment, duplicate marking, base quality score recalibration (BQSR), and quality control metrics collection.

## Commands

### Development and Testing

```bash
# Install the package in development mode
pip install -e .

# Run linting (when linting tools are installed)
pip install -U autopep8 flake8 flake8-bugbear flake8-isort pep8-naming
find . -name '*.py' | xargs flake8

# Test basic functionality
ftarc init
ftarc --help
```

### Running the Pipeline

```bash
# Download reference genome resources
ftarc download --dest-dir=/path/to/resources

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

## Architecture

### Core Components

1. **CLI Layer** (`ftarc/cli/`)
   - `main.py`: Main entry point using docopt for command parsing
   - `pipeline.py`: Pipeline orchestration and configuration management
   - `util.py`: Shared utilities for Luigi task building and executable fetching

2. **Task Layer** (`ftarc/task/`)
   - Luigi-based task definitions for each processing step
   - `controller.py`: High-level pipeline coordination tasks
   - `core.py`: Base task classes and shared functionality
   - Tool-specific modules: `bwa.py`, `gatk.py`, `picard.py`, `samtools.py`, `trimgalore.py`, `fastqc.py`
   - `downloader.py`: Resource file downloading and processing
   - `resource.py`: Resource management utilities

3. **Configuration** (`ftarc/static/`)
   - `example_ftarc.yml`: Template configuration file
   - `urls.yml`: Resource download URLs

### Workflow Pipeline

The standard workflow (`RunPreprocessingPipeline`) executes:
1. Adapter trimming (trim_galore)
2. Read alignment (bwa mem or bwa-mem2)
3. Duplicate marking (GATK MarkDuplicates)
4. BQSR (GATK BaseRecalibrator + ApplyBQSR)
5. Duplicate removal (samtools view)
6. Validation (GATK ValidateSamFile)
7. QC metrics collection (FastQC, Samtools, GATK)

### Key Design Patterns

- **Luigi Task System**: All processing steps are Luigi tasks with proper dependency management
- **Shell Command Execution**: Tasks use subprocess calls with configurable logging and error handling
- **Resource Management**: Automatic CPU and memory allocation based on system resources
- **Parallel Processing**: Support for multiple workers to process samples concurrently
- **Spark Support**: Optional Spark-enabled GATK tools for large-scale processing

### Dependencies

Required external tools (must be in PATH):
- `pigz`, `pbzip2`, `bgzip`, `tabix`
- `samtools`, `plot-bamstats`
- `java`, `gatk`
- `cutadapt`, `fastqc`, `trim_galore`
- `bwa` or `bwa-mem2`
- `gnuplot` (for plot-bamstats)

Python dependencies:
- `docopt`, `jinja2`, `luigi`, `pip`, `psutil`, `pyyaml`, `shoper`

### Configuration File Structure

The `ftarc.yml` configuration includes:
- `reference_name`: Reference genome identifier
- `adapter_removal`: Boolean for adapter trimming
- `metrics_collectors`: Dictionary of QC tools to run
- `resources`: Paths to reference files and known variant sites
- `runs`: List of sample runs with FASTQ paths and read group information

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
