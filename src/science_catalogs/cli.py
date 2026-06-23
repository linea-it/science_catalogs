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
        help="directory where processed files should be written when using disk or lsdb mode",
    )
    parser.add_argument(
        "--output-mode",
        choices=("disk", "memory", "lsdb"),
        default="disk",
        help="whether to return the processed catalog in memory, write it to disk, or reopen it as LSDB",
    )
    args = parser.parse_args()

    if args.output_mode in {"disk", "lsdb"} and args.output_dir is not None:
        Path(args.output_dir).mkdir(parents=True, exist_ok=True)

    result = build_catalog(
        args.config_path,
        output_mode=args.output_mode,
        output_dir=args.output_dir,
    )

    if result.output_mode == "memory" and result.data is not None:
        print(f"Computed dataframe with {len(result.data)} rows")
    elif result.output_mode == "disk":
        print(f"Wrote {len(result.written_paths)} partition files")
    elif result.output_mode == "lsdb" and result.catalog is not None:
        print("Opened LSDB catalog")


__all__ = ["main"]
