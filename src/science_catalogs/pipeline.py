"""Core catalog preparation and materialization helpers."""

from __future__ import annotations

import gc
import glob
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd
from dask import delayed
from dask import dataframe as dd
from dask.distributed import Client, wait

from science_catalogs.executor import get_executor
from science_catalogs.processing import process_file_df
from science_catalogs.utils.config import decide_suffix_and_flags
from science_catalogs.utils.dust import configure_dustmaps_path
from science_catalogs.utils.partitioning import reorder_and_rechunk
from science_catalogs.utils.writers import write_hats_catalog, write_partitions


@dataclass(slots=True)
class PreparedCatalog:
    """Lazy processed catalog plus the metadata needed to materialize it."""

    config: dict[str, Any]
    ddf: dd.DataFrame
    suffix: str
    output_cfg: dict[str, Any]
    ra_col: str | None
    dec_col: str | None
    input_files: list[str] = field(default_factory=list)


@dataclass(slots=True)
class PipelineResult:
    """Materialized result returned by the convenience wrapper."""

    output_mode: str
    suffix: str
    data: pd.DataFrame | None = None
    catalog: Any | None = None
    written_paths: tuple[str, ...] = ()


def load_pipeline_config(config_path: str) -> dict[str, Any]:
    """Load a pipeline YAML config from disk."""
    import yaml

    with open(config_path, "r", encoding="utf-8") as _file:
        return yaml.safe_load(_file) or {}


def prepare_catalog(config_path: str, config: dict[str, Any] | None = None) -> PreparedCatalog:
    """Build the lazy processed catalog without creating pipeline artifacts."""
    cfg = config if config is not None else load_pipeline_config(config_path)

    inputs = cfg.get("input", {})
    dust = cfg.get("dust", {})
    output_cfg = cfg.get("output", {})

    configure_dustmaps_path(dust)

    suffix, will_mag, will_dered_flux, will_dered_mag = decide_suffix_and_flags(
        inputs,
        inputs.get("compute_magnitude", True),
        inputs.get("compute_dereddening", True),
    )

    input_files = [
        f
        for f in glob.glob(
            Path(inputs.get("catalog_folder", "")).expanduser().as_posix()
            + "/"
            + inputs.get("catalog_pattern", "*.parquet")
        )
    ]
    if not input_files:
        raise FileNotFoundError("No input files found for catalog_pattern")

    delayed_dfs = [
        delayed(process_file_df)(
            p,
            cfg_path=config_path,
            will_mag=will_mag,
            will_dered_flux=will_dered_flux,
            will_dered_mag=will_dered_mag,
        )
        for p in input_files
    ]
    ddf = dd.from_delayed(delayed_dfs)
    ddf_out = reorder_and_rechunk(ddf, output_cfg)

    return PreparedCatalog(
        config=cfg,
        ddf=ddf_out,
        suffix=suffix,
        output_cfg=output_cfg,
        ra_col=inputs.get("ra_col"),
        dec_col=inputs.get("dec_col"),
        input_files=input_files,
    )


def materialize_catalog(prepared: PreparedCatalog) -> pd.DataFrame:
    """Compute the processed catalog into memory as a pandas dataframe."""
    return prepared.ddf.compute()


def open_lsdb_catalog(catalog_path: str | Path, **kwargs):
    """Open an LSDB catalog from a HATS path on disk."""
    import lsdb

    return lsdb.open_catalog(str(catalog_path), **kwargs)


def materialize_lsdb_catalog(
    prepared: PreparedCatalog,
    output_dir: str,
    client=None,
    **kwargs,
):
    """
    Write the prepared catalog as HATS and open it back as an LSDB Catalog.

    This avoids the in-memory `lsdb.from_dataframe(...)` path and relies on
    LSDB's HATS loader instead.
    """
    if prepared.output_cfg.get("save_as", "parquet") != "hats":
        raise ValueError("materialize_lsdb_catalog requires output.save_as == 'hats'")

    written_paths = write_catalog(prepared, output_dir, client=client)
    if not written_paths:
        raise ValueError("No HATS catalog path was produced")
    return open_lsdb_catalog(written_paths[0], **kwargs)


def write_catalog(prepared: PreparedCatalog, output_dir: str, client=None) -> tuple[str, ...]:
    """Write the processed catalog partitions or HATS output to disk."""
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    if prepared.output_cfg.get("save_as", "parquet") == "hats":
        return write_hats_catalog(
            prepared.ddf,
            prepared.output_cfg,
            prepared.config.get("collection", {}),
            str(output_path),
            prepared.suffix,
            prepared.ra_col,
            prepared.dec_col,
            client=client,
        )
    return write_partitions(prepared.ddf, prepared.output_cfg, str(output_path), prepared.suffix)


def _resolve_output_dir(
    prepared: PreparedCatalog,
    cwd: str | None,
    output_dir: str | None,
) -> str:
    if output_dir is not None:
        return str(output_dir)
    if cwd is not None:
        return str(Path(cwd) / "data")

    base_path = prepared.output_cfg.get("base_path")
    if base_path:
        return str(base_path)

    raise ValueError("output_dir is required for output_mode='disk' when cwd/base_path are not available")


def run_pipeline(
    config_path: str,
    cwd: str | None = None,
    *,
    output_mode: str = "disk",
    output_dir: str | None = None,
) -> PipelineResult:
    """
    Convenience wrapper around the package core.

    `output_mode="memory"` returns the processed catalog as a pandas dataframe.
    `output_mode="disk"` writes the processed catalog to `output_dir` and returns written paths.
    `output_mode="lsdb"` writes HATS output and reopens it as an `lsdb.Catalog`.
    """
    cfg = load_pipeline_config(config_path)
    cluster = get_executor(cfg.get("cluster", {}))
    client = Client(cluster)
    cluster_ref = client.cluster
    cluster_comm = getattr(cluster_ref, "comm", None)

    try:
        if cluster_comm:
            wait(cluster_comm)
        client.run(lambda: gc.collect())

        prepared = prepare_catalog(config_path, config=cfg)

        if output_mode == "memory":
            data = materialize_catalog(prepared)
            return PipelineResult(output_mode="memory", suffix=prepared.suffix, data=data)

        if output_mode == "disk":
            resolved_output_dir = _resolve_output_dir(prepared, cwd, output_dir)
            written_paths = write_catalog(prepared, resolved_output_dir, client=client)
            return PipelineResult(
                output_mode="disk",
                suffix=prepared.suffix,
                written_paths=tuple(written_paths),
            )

        if output_mode == "lsdb":
            resolved_output_dir = _resolve_output_dir(prepared, cwd, output_dir)
            catalog = materialize_lsdb_catalog(prepared, resolved_output_dir, client=client)
            return PipelineResult(
                output_mode="lsdb",
                suffix=prepared.suffix,
                catalog=catalog,
            )

        raise ValueError("output_mode must be 'memory', 'disk', or 'lsdb'")
    finally:
        client.close()
        cluster.close()


__all__ = [
    "PreparedCatalog",
    "PipelineResult",
    "load_pipeline_config",
    "prepare_catalog",
    "materialize_catalog",
    "materialize_lsdb_catalog",
    "open_lsdb_catalog",
    "write_catalog",
    "run_pipeline",
]
