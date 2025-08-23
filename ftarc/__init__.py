"""FASTQ-to-analysis-ready-CRAM workflow executor for human genome sequencing.

ftarc provides a Luigi-based pipeline system for processing genomic sequencing data through
various stages including adapter trimming, read alignment, duplicate marking, base quality score
recalibration (BQSR), and quality control metrics collection.
"""

from importlib.metadata import version

__version__ = version(__package__) if __package__ else None

__all__ = ["__version__"]
