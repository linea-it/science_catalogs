"""Command-line entrypoints for the science catalogs package."""

import argparse
import logging
import sys
from pathlib import Path

from science_catalogs import build_catalog


class _MaxLevelFilter(logging.Filter):
    """Allow records up to and including the configured level."""

    def __init__(self, max_level: int):
        super().__init__()
        self.max_level = max_level

    def filter(self, record: logging.LogRecord) -> bool:
        return record.levelno <= self.max_level


def _configure_logging():
    """Route progress logs to stdout and warnings/errors to stderr."""
    formatter = logging.Formatter("%(levelname)s:%(name)s:%(message)s")

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(logging.INFO)
    stdout_handler.addFilter(_MaxLevelFilter(logging.INFO))
    stdout_handler.setFormatter(formatter)

    stderr_handler = logging.StreamHandler(sys.stderr)
    stderr_handler.setLevel(logging.WARNING)
    stderr_handler.setFormatter(formatter)

    logging.basicConfig(
        level=logging.INFO,
        handlers=[stdout_handler, stderr_handler],
        force=True,
    )
    logging.captureWarnings(True)


def main():
    """Run the catalog-building flow from the command line."""
    _configure_logging()

    parser = argparse.ArgumentParser(description="Science Catalogs catalog builder")
    parser.add_argument("config_path", help="path to config.yaml")
    parser.add_argument(
        "output_dir",
        nargs="?",
        default=None,
        help="directory where processed files should be written",
    )
    parser.add_argument(
        "--output-format",
        choices=("parquet", "hats"),
        default=None,
        help="override the configured on-disk output format",
    )
    args = parser.parse_args()

    if args.output_dir is not None:
        Path(args.output_dir).mkdir(parents=True, exist_ok=True)

    result = build_catalog(
        args.config_path,
        output_dir=args.output_dir,
        output_format=args.output_format,
    )

    if isinstance(result, tuple):
        print(f"Wrote {len(result)} partition files")
    else:
        print(f"Wrote artifact to {result}")


__all__ = ["main"]
