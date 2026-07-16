"""Core catalog preparation and materialization helpers."""

from __future__ import annotations

import gc
import glob
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any

from dask import dataframe as dd
from dask import delayed
from dask.distributed import Client, wait

from science_catalogs.executor import get_executor
from science_catalogs.processing import process_dataframe, process_file_df
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


def load_catalog_config(config_path: str) -> dict[str, Any]:
    """Load a catalog-processing YAML config from disk."""
    import yaml

    with open(config_path, "r", encoding="utf-8") as _file:
        return yaml.safe_load(_file) or {}


def _is_hats_catalog_path(path: Path) -> bool:
    """Check whether a directory already contains a valid HATS catalog."""
    if not path.is_dir():
        return False

    from hats.io.validation import is_valid_catalog

    return bool(is_valid_catalog(path))


def _glob_input_files(base_path: Path, pattern: str) -> list[str]:
    """Collect matching input files below a directory."""
    return sorted(glob.glob((base_path / pattern).as_posix()))


def _resolve_input_source(inputs: dict[str, Any]) -> dict[str, Any]:
    """Resolve a single input path as HATS, a single file, or a directory of files."""
    raw_catalog_path = inputs.get("catalog_path")
    raw_catalog_folder = inputs.get("catalog_folder")
    pattern = inputs.get("catalog_pattern", "*.parquet")

    if raw_catalog_path:
        catalog_path = Path(raw_catalog_path).expanduser()
    elif raw_catalog_folder:
        catalog_path = Path(raw_catalog_folder).expanduser()
    else:
        raise ValueError("input.catalog_path is required")

    if not catalog_path.exists():
        raise FileNotFoundError(f"Input path does not exist: {catalog_path}")

    if _is_hats_catalog_path(catalog_path):
        return {"source": "hats", "catalog_path": str(catalog_path)}

    if catalog_path.is_file():
        return {"source": "files", "input_files": [str(catalog_path)]}

    if catalog_path.is_dir():
        input_files = _glob_input_files(catalog_path, pattern)
        if not input_files:
            raise FileNotFoundError(f"No input files found under {catalog_path} matching pattern {pattern!r}")
        return {"source": "files", "input_files": input_files}

    raise ValueError(f"Unsupported input path type: {catalog_path}")


def _build_processed_meta(
    ddf: dd.DataFrame,
    cfg: dict[str, Any],
    *,
    will_mag: bool,
    will_dered_flux: bool,
    will_dered_mag: bool,
):
    """Infer the processed partition schema for Dask map_partitions."""
    meta_input = ddf._meta.copy()
    meta_output = process_dataframe(
        meta_input,
        cfg,
        will_mag=will_mag,
        will_dered_flux=will_dered_flux,
        will_dered_mag=will_dered_mag,
        source_name="<meta>",
    )
    return meta_output.iloc[:0]


def prepare_catalog(
    config_path: str,
    config: dict[str, Any] | None = None,
    *,
    client=None,
) -> PreparedCatalog:
    """Build the lazy processed catalog from file inputs or an existing HATS catalog."""
    cfg = config if config is not None else load_catalog_config(config_path)

    inputs = cfg.get("input", {})
    dust = cfg.get("dust", {})
    output_cfg = cfg.get("output", {})

    configure_dustmaps_path(dust)

    suffix, will_mag, will_dered_flux, will_dered_mag = decide_suffix_and_flags(
        inputs,
        inputs.get("compute_magnitude", True),
        inputs.get("compute_dereddening", True),
    )

    input_source = _resolve_input_source(inputs)

    if input_source["source"] == "files":
        input_files = input_source["input_files"]
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
    else:
        input_files = [input_source["catalog_path"]]
        selected_columns = list(inputs.get("user_selected_cols", []) or []) or None
        hats_catalog = open_lsdb_catalog(
            input_source["catalog_path"],
            client=client,
            columns=selected_columns,
        )
        ddf = hats_catalog.to_dask_dataframe()
        ddf = ddf.map_partitions(
            process_dataframe,
            cfg,
            will_mag=will_mag,
            will_dered_flux=will_dered_flux,
            will_dered_mag=will_dered_mag,
            source_name=input_source["catalog_path"],
            meta=_build_processed_meta(
                ddf,
                cfg,
                will_mag=will_mag,
                will_dered_flux=will_dered_flux,
                will_dered_mag=will_dered_mag,
            ),
        )
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


def _resolve_output_cfg(output_cfg: dict[str, Any], output_format: str | None) -> dict[str, Any]:
    resolved_cfg = dict(output_cfg)
    if output_format is None:
        return resolved_cfg
    if output_format not in {"parquet", "hats"}:
        raise ValueError("output_format must be 'parquet' or 'hats'")
    resolved_cfg["save_as"] = output_format
    return resolved_cfg


def _normalize_written_path(written_paths: tuple[str, ...]) -> str | tuple[str, ...]:
    if len(written_paths) == 1:
        return written_paths[0]
    return written_paths


def _persist_prepared(prepared: PreparedCatalog, client=None) -> PreparedCatalog:
    if client is None or not hasattr(client, "persist"):
        return prepared

    persisted_ddf = client.persist(prepared.ddf)
    wait(persisted_ddf)
    return replace(prepared, ddf=persisted_ddf)


def materialize_catalog(
    prepared: PreparedCatalog,
    output_dir: str | None = None,
    client=None,
    *,
    output_format: str | None = None,
) -> dict[str, Any]:
    """Compute the catalog in memory and also persist the written artifact paths."""
    resolved_output_dir = _resolve_output_dir(prepared, output_dir)
    prepared_for_materialization = _persist_prepared(prepared, client=client)
    data = prepared_for_materialization.ddf.compute()
    written_paths = write_catalog(
        prepared_for_materialization,
        resolved_output_dir,
        client=client,
        output_format=output_format,
    )
    return {"data": data, "path": _normalize_written_path(tuple(written_paths))}


def open_lsdb_catalog(catalog_path: str | Path, client=None, **kwargs):
    """Open an LSDB catalog from a HATS path on disk."""
    import lsdb

    if client is not None:
        kwargs["client"] = client
    return lsdb.open_catalog(str(catalog_path), **kwargs)


def materialize_lsdb_catalog(
    prepared: PreparedCatalog,
    output_dir: str | None = None,
    client=None,
    **kwargs,
):
    """
    Write the prepared catalog as HATS and open it back as an LSDB Catalog.

    This avoids the in-memory `lsdb.from_dataframe(...)` path and relies on
    LSDB's HATS loader instead.
    """
    resolved_output_dir = _resolve_output_dir(prepared, output_dir)
    prepared_for_materialization = _persist_prepared(prepared, client=client)
    written_paths = write_catalog(
        prepared_for_materialization,
        resolved_output_dir,
        client=client,
        output_format="hats",
    )
    if not written_paths:
        raise ValueError("No HATS catalog path was produced")
    return {
        "data": open_lsdb_catalog(written_paths[0], client=client, **kwargs),
        "path": written_paths[0],
    }


def write_catalog(
    prepared: PreparedCatalog,
    output_dir: str,
    client=None,
    *,
    output_format: str | None = None,
) -> tuple[str, ...]:
    """Write the processed catalog partitions or HATS output to disk."""
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    output_cfg = _resolve_output_cfg(prepared.output_cfg, output_format)
    if output_cfg.get("save_as", "parquet") == "hats":
        return write_hats_catalog(
            prepared.ddf,
            output_cfg,
            prepared.config.get("collection", {}),
            str(output_path),
            prepared.suffix,
            prepared.ra_col,
            prepared.dec_col,
            client=client,
        )
    return write_partitions(prepared.ddf, output_cfg, str(output_path), prepared.suffix)


def _resolve_output_dir(
    prepared: PreparedCatalog,
    output_dir: str | None,
) -> str:
    if output_dir is not None:
        return str(output_dir)

    base_path = prepared.output_cfg.get("base_path")
    if base_path:
        return str(base_path)

    return str(Path.cwd() / "data")


def build_catalog(
    config_path: str,
    *,
    output_dir: str | None = None,
    output_format: str | None = None,
) -> str | tuple[str, ...]:
    """
    Execute the full catalog-building flow and persist the result to disk.

    When `output_dir` is omitted, the default output directory is `./data`.
    The returned value is the written parquet partition paths, or the HATS
    artifact directory when `output_format="hats"`.
    """
    cfg = load_catalog_config(config_path)
    cluster = get_executor(cfg.get("cluster", {}))
    client = Client(cluster)
    cluster_ref = client.cluster
    cluster_comm = getattr(cluster_ref, "comm", None)

    try:
        if cluster_comm:
            wait(cluster_comm)
        client.run(lambda: gc.collect())

        prepared = prepare_catalog(config_path, config=cfg, client=client)
        resolved_output_dir = _resolve_output_dir(prepared, output_dir)
        written_paths = write_catalog(
            prepared,
            resolved_output_dir,
            client=client,
            output_format=output_format,
        )
        return _normalize_written_path(tuple(written_paths))
    finally:
        client.close()
        cluster.close()


__all__ = [
    "PreparedCatalog",
    "load_catalog_config",
    "prepare_catalog",
    "materialize_catalog",
    "materialize_lsdb_catalog",
    "open_lsdb_catalog",
    "write_catalog",
    "build_catalog",
]
