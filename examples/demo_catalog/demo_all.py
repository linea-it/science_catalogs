from __future__ import annotations

import os
import shutil
from pathlib import Path

from science_catalogs import (
    build_catalog,
    materialize_catalog,
    materialize_lsdb_catalog,
    open_lsdb_catalog,
    prepare_catalog,
    write_catalog,
)


REPO_ROOT = Path(__file__).resolve().parents[2]
DEMO_ROOT = Path(__file__).resolve().parent
PARQUET_CFG = DEMO_ROOT / "configs" / "demo_parquet.yml"
HATS_CFG = DEMO_ROOT / "configs" / "demo_hats.yml"
OUTPUT_ROOT = DEMO_ROOT / "output"


def reset_output_dir(name: str) -> Path:
    path = OUTPUT_ROOT / name
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)
    return path


def print_header(title: str) -> None:
    print(f"\n=== {title} ===")


def main() -> None:
    os.chdir(REPO_ROOT)

    print_header("prepare_catalog + materialize_catalog")
    prepared = prepare_catalog(str(PARQUET_CFG))
    print("suffix:", prepared.suffix)
    print("input files:", prepared.input_files)
    frame = materialize_catalog(prepared)
    print(frame[["object_id", "tile", "mag_g", "mag_r"]])

    print_header("write_catalog parquet")
    parquet_out = reset_output_dir("manual_parquet")
    written_parquet = write_catalog(prepared, str(parquet_out))
    print("written parquet:", written_parquet)

    print_header("materialize_lsdb_catalog + open_lsdb_catalog")
    prepared_hats = prepare_catalog(str(HATS_CFG))
    hats_out = reset_output_dir("manual_hats")
    catalog = materialize_lsdb_catalog(prepared_hats, str(hats_out))
    print("lsdb catalog:", catalog)
    reopened = open_lsdb_catalog(hats_out / "demo_hats_catalog")
    print("reopened catalog:", reopened)

    print_header("build_catalog convenience wrapper")
    result_memory = build_catalog(str(PARQUET_CFG), output_mode="memory")
    print("memory rows:", len(result_memory.data))

    build_parquet_out = reset_output_dir("build_parquet")
    result_disk = build_catalog(
        str(PARQUET_CFG),
        output_mode="disk",
        output_dir=str(build_parquet_out),
    )
    print("disk outputs:", result_disk.written_paths)

    build_hats_out = reset_output_dir("build_hats")
    result_lsdb = build_catalog(
        str(HATS_CFG),
        output_mode="lsdb",
        output_dir=str(build_hats_out),
    )
    print("built lsdb catalog:", result_lsdb.catalog)


if __name__ == "__main__":
    main()
