"""Command-line entrypoints for the science catalogs package."""

import argparse
from pathlib import Path

from science_catalogs import build_catalog


def main():
    """Run the catalog-building flow from the command line."""
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
