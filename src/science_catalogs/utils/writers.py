"""Output helpers (file naming and writing)."""

import hashlib
import os
import re
import shutil
import tempfile
import warnings
from pathlib import Path
from typing import Any

import dask
import pandas as pd
from dask import dataframe as dd
from dask import delayed

_fname_safe_re = re.compile(r"[^A-Za-z0-9._-]+")


def _sanitize_token(token: str) -> str:
    token = str(token).strip()
    if token == "":
        return "empty"
    return _fname_safe_re.sub("_", token)


def _build_values_tag(values, col_name: str, max_list: int = 12) -> str:
    vals = [v for v in values if v is not None and not (isinstance(v, float) and pd.isna(v))]
    if not vals:
        return f"{col_name}_none"
    vals = sorted(map(_sanitize_token, vals))
    if len(vals) <= max_list:
        return f"{col_name}_" + "_".join(vals)
    head = "_".join(vals[:max_list])
    digest = hashlib.sha1(",".join(vals).encode("utf-8")).hexdigest()[:8]
    return f"{col_name}_{head}_plus{len(vals) - max_list}_{digest}"


def _resolve_materialization_format(output_cfg: dict[str, Any]) -> str:
    save_as = output_cfg.get("save_as", "parquet")
    if save_as == "hats":
        source_format = output_cfg.get("hats_source_save_as", "parquet")
        if source_format not in {"parquet", "csv"}:
            raise ValueError("HATS output supports only parquet or csv as staging formats")
        return source_format
    return save_as


@delayed
def _write_part(
    pdf: pd.DataFrame,
    base_dir: str,
    base_suffix: str,
    i: int,
    ext: str,
    col_for_name: str | None,
):
    tag = None
    if col_for_name is not None and col_for_name in pdf.columns:
        uniques = pd.unique(pdf[col_for_name])
        tag = _build_values_tag(uniques, _sanitize_token(col_for_name))

    fname = f"{base_suffix}_part{i}.{ext}" if not tag else f"{base_suffix}_part{i}_{tag}.{ext}"
    out_path = os.path.join(base_dir, fname)

    if ext == "parquet":
        pdf.to_parquet(out_path, index=False)
    elif ext == "csv":
        pdf.to_csv(out_path, index=False)
    elif ext == "h5":
        import tables_io

        pdf = pdf.reset_index(drop=True)
        tables_io.write(pdf, out_path)
    else:
        raise ValueError(f"Unsupported extension {ext}")

    return out_path


def write_partitions(ddf_out: dd.DataFrame, output_cfg: dict[str, Any], data_dir: str, suffix: str):
    """Write each partition to disk using the configured output format."""
    ext_map = {"parquet": "parquet", "csv": "csv", "hdf5": "h5"}
    ext = ext_map[_resolve_materialization_format(output_cfg)]

    col_for_name = output_cfg.get("col_for_filename")
    if col_for_name is not None and col_for_name not in ddf_out.columns:
        warnings.warn(f"col_for_filename='{col_for_name}' not found; ignoring")
        col_for_name = None

    delayed_parts = ddf_out.to_delayed()
    tasks = [
        _write_part(part, data_dir, suffix, i, ext, col_for_name) for i, part in enumerate(delayed_parts)
    ]
    written_paths = dask.compute(*tasks)
    return written_paths


def write_hats_catalog(
    ddf_out: dd.DataFrame,
    output_cfg: dict[str, Any],
    collection_cfg: dict[str, Any],
    output_dir: str,
    suffix: str,
    ra_col: str | None,
    dec_col: str | None,
    client=None,
    *,
    force_recreate: bool = False,
):
    """Materialize staging files and import them as a HATS collection."""
    if not ra_col or not dec_col:
        raise ValueError("HATS output requires both ra_col and dec_col")

    try:
        from hats.io.validation import is_valid_catalog
        from hats_import.catalog.file_readers import CsvReader, ParquetReader
        from hats_import.collection.arguments import CollectionArguments
        from hats_import.collection.run_import import run
    except Exception as exc:  # pragma: no cover
        raise RuntimeError("hats-import not available in the environment") from exc

    source_format = output_cfg.get("hats_source_save_as", "parquet") or "parquet"
    artifact_name = output_cfg.get("hats_artifact_name") or f"{suffix}_collection"
    margin_threshold = output_cfg.get("hats_margin_threshold")
    if margin_threshold is None:
        margin_threshold = collection_cfg.get("margin_threshold", 10.0)

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    artifact_path = output_path / artifact_name

    if not force_recreate and is_valid_catalog(artifact_path):
        return (str(artifact_path),)

    if force_recreate and artifact_path.exists():
        if artifact_path.is_dir():
            shutil.rmtree(artifact_path)
        else:
            artifact_path.unlink()

    with tempfile.TemporaryDirectory(prefix="science_catalogs_hats_") as temp_dir:
        staging_dir = Path(temp_dir) / "staging"
        staging_dir.mkdir(parents=True, exist_ok=True)

        staging_cfg = dict(output_cfg)
        staging_cfg["save_as"] = source_format
        written_paths = write_partitions(ddf_out, staging_cfg, str(staging_dir), suffix)

        if source_format == "parquet":
            file_reader = ParquetReader()
        elif source_format == "csv":
            file_reader = CsvReader()
        else:  # pragma: no cover
            raise ValueError("HATS output supports only parquet or csv as staging formats")

        args = (
            CollectionArguments(
                output_artifact_name=artifact_name,
                output_path=str(output_path),
                progress_bar=True,
            )
            .catalog(
                output_artifact_name="catalog",
                ra_column=ra_col,
                dec_column=dec_col,
                input_file_list=[Path(path) for path in written_paths],
                file_reader=file_reader,
            )
            .add_margin(
                output_artifact_name=f"margin_{str(margin_threshold).rstrip('0').rstrip('.')}arcs",
                margin_threshold=margin_threshold,
            )
        )

        created_local_client = None
        hats_client = client
        if hats_client is None:
            from dask.distributed import Client

            created_local_client = Client(processes=False)
            hats_client = created_local_client

        try:
            run(args, hats_client)
        finally:
            if created_local_client is not None:
                created_local_client.close()

    return (str(artifact_path),)


__all__ = ["write_partitions", "write_hats_catalog"]
