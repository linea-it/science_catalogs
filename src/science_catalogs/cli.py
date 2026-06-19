"""Command-line entrypoints for the science catalogs core package."""

import argparse
from pathlib import Path

from science_catalogs import run_pipeline


def main():
    """Run the pipeline from the command line."""
    parser = argparse.ArgumentParser(description="Science Catalogs pipeline runner")
    parser.add_argument("config_path", help="path to config.yaml")
    parser.add_argument(
        "output_dir",
        nargs="?",
        default=None,
        help="directory where processed files should be written when using disk mode",
    )
    parser.add_argument(
        "--output-mode",
        choices=("disk", "memory"),
        default="disk",
        help="whether to write processed files to disk or return the processed catalog in memory",
    )
    args = parser.parse_args()

    if args.output_mode == "disk" and args.output_dir is not None:
        Path(args.output_dir).mkdir(parents=True, exist_ok=True)

    result = run_pipeline(
        args.config_path,
        output_mode=args.output_mode,
        output_dir=args.output_dir,
    )

    if result.output_mode == "memory" and result.data is not None:
        print(f"Computed dataframe with {len(result.data)} rows")
    elif result.output_mode == "disk":
        print(f"Wrote {len(result.written_paths)} partition files")


__all__ = ["main"]
