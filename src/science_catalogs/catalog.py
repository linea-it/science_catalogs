"""Core catalog preparation and materialization helpers."""

from __future__ import annotations

import gc
import glob
import logging
import re
import warnings
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any

from dask import dataframe as dd
from dask import delayed
from dask.distributed import Client, wait

from science_catalogs.executor import get_executor
from science_catalogs.processing import process_dataframe, process_files_df
from science_catalogs.utils.config import (
    decide_photometry_suffix,
    decide_suffix_and_flags,
    normalize_catalog_config,
)
from science_catalogs.utils.dust import configure_dustmaps_path
from science_catalogs.utils.io_readers import detect_and_read
from science_catalogs.utils.partitioning import reorder_and_rechunk
from science_catalogs.utils.writers import write_hats_catalog, write_partitions

logger = logging.getLogger(__name__)
DEFAULT_FILES_PER_PARTITION = 1
DEFAULT_PARQUET_METADATA_WORKERS = 8


@contextmanager
def _suppress_dask_large_graph_warning():
    """Hide Dask's advisory large-graph warning while preserving all others."""
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message=re.escape("Sending large graph of size"),
            category=UserWarning,
            module=r"distributed\.client",
        )
        yield


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
class CatalogBuildSpec:
    """One named catalog configuration in an executable build plan."""

    name: str | None
    config: dict[str, Any]


@dataclass(slots=True)
class CatalogBuildPlan:
    """Validated single-catalog or batch build plan."""

    catalogs: list[CatalogBuildSpec]
    execution_cfg: dict[str, Any]
    is_batch: bool = False


def _load_yaml_config(config_path: str) -> dict[str, Any]:
    """Load a YAML mapping without assuming the single-catalog schema."""
    import yaml

    with open(config_path, "r", encoding="utf-8") as _file:
        config = yaml.safe_load(_file) or {}
    if not isinstance(config, dict):
        raise ValueError("Configuration root must be a mapping")
    return config


def load_catalog_config(config_path: str) -> dict[str, Any]:
    """Load a catalog-processing YAML config from disk."""
    return normalize_catalog_config(_load_yaml_config(config_path))


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """Recursively merge mappings while replacing all non-mapping values."""
    merged = deepcopy(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = deepcopy(value)
    return merged


def load_build_plan(config_path: str) -> CatalogBuildPlan:
    """Load and validate either a legacy single catalog or a batch configuration."""
    raw = _load_yaml_config(config_path)
    if "batch" not in raw:
        config = normalize_catalog_config(raw)
        return CatalogBuildPlan(
            catalogs=[CatalogBuildSpec(name=None, config=config)],
            execution_cfg=config.get("execution", {}),
        )

    if set(raw) != {"batch"}:
        extra = sorted(set(raw) - {"batch"})
        raise ValueError(f"Batch configuration cannot contain sibling root keys: {extra}")
    batch = raw["batch"]
    if not isinstance(batch, dict):
        raise ValueError("batch must be a mapping")
    allowed_batch_keys = {"execution", "defaults", "catalogs", "on_existing"}
    unknown = sorted(set(batch) - allowed_batch_keys)
    if unknown:
        raise ValueError(f"Unknown configuration key(s) in batch: {unknown}")

    execution_cfg = batch.get("execution", {}) or {}
    defaults = batch.get("defaults", {}) or {}
    catalog_entries = batch.get("catalogs", []) or []
    on_existing = batch.get("on_existing", "error")
    if not isinstance(execution_cfg, dict):
        raise ValueError("batch.execution must be a mapping")
    if not isinstance(defaults, dict):
        raise ValueError("batch.defaults must be a mapping")
    if "execution" in defaults or "cluster" in defaults:
        raise ValueError("Configure execution only in batch.execution")
    if not isinstance(catalog_entries, list) or not catalog_entries:
        raise ValueError("batch.catalogs must be a non-empty list")
    if on_existing not in {"reuse", "error", "replace"}:
        raise ValueError("batch.on_existing must be 'reuse', 'error', or 'replace'")

    specs = []
    names = set()
    hats_artifacts = set()
    for index, entry in enumerate(catalog_entries):
        if not isinstance(entry, dict):
            raise ValueError(f"batch.catalogs[{index}] must be a mapping")
        name = entry.get("name")
        if not isinstance(name, str) or not name.strip():
            raise ValueError(f"batch.catalogs[{index}].name must be a non-empty string")
        name = name.strip()
        if name in names:
            raise ValueError(f"Duplicate batch catalog name: {name}")
        names.add(name)
        if "execution" in entry or "cluster" in entry:
            raise ValueError(f"Configure execution only in batch.execution, not catalog '{name}'")

        catalog_override = {key: value for key, value in entry.items() if key != "name"}
        config = _deep_merge(defaults, catalog_override)
        config["execution"] = deepcopy(execution_cfg)
        output_cfg = config.setdefault("output", {})
        if not isinstance(output_cfg, dict):
            raise ValueError(f"batch catalog '{name}' output must be a mapping")
        output_cfg.setdefault("on_existing", on_existing)
        config = normalize_catalog_config(config)

        if config.get("output", {}).get("save_as", "parquet") == "hats":
            artifact_name = config["output"].get("hats_artifact_name")
            if not isinstance(artifact_name, str) or not artifact_name.strip():
                raise ValueError(f"Batch HATS catalog '{name}' requires output.hats_artifact_name")
            if artifact_name in hats_artifacts:
                raise ValueError(f"Duplicate batch HATS artifact name: {artifact_name}")
            hats_artifacts.add(artifact_name)
        specs.append(CatalogBuildSpec(name=name, config=config))

    return CatalogBuildPlan(catalogs=specs, execution_cfg=execution_cfg, is_batch=True)


def _is_hats_catalog_path(path: Path) -> bool:
    """Check whether a directory already contains a valid HATS catalog."""
    if not path.is_dir():
        return False

    from hats.io.validation import is_valid_catalog, is_valid_collection

    return bool(is_valid_collection(path) or is_valid_catalog(path))


def _glob_input_files(base_path: Path, pattern: str) -> list[str]:
    """Collect matching input files below a directory."""
    return sorted(glob.glob((base_path / pattern).as_posix(), recursive=True))


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


def _chunk_files(input_files: list[str], files_per_partition: int) -> list[tuple[str, ...]]:
    """Group source files so the task graph does not contain one task per small file."""
    return [
        tuple(input_files[offset : offset + files_per_partition])
        for offset in range(0, len(input_files), files_per_partition)
    ]


def _processing_preserves_row_count(input_cfg: dict[str, Any]) -> bool:
    """Return whether configured processing can use exact source-file row counts."""
    return not input_cfg.get("filter", {}).get("enabled") and not input_cfg.get("initial_cut", {}).get(
        "enabled"
    )


def _parquet_batch_row_count(paths: tuple[str, ...]) -> int:
    """Read only Parquet footers and return the total rows for one file batch."""
    import pyarrow.parquet as pq

    return sum(pq.read_metadata(path).num_rows for path in paths)


def _preflight_input_source(cfg: dict[str, Any]) -> dict[str, Any]:
    """Discover, batch, and inspect inputs before a distributed scheduler exists."""
    input_cfg = cfg.get("input", {})
    source = _resolve_input_source(input_cfg)
    if source["source"] != "files":
        return source

    input_files = source["input_files"]
    files_per_partition = int(
        input_cfg.get("files_per_partition", DEFAULT_FILES_PER_PARTITION) or DEFAULT_FILES_PER_PARTITION
    )
    file_batches = _chunk_files(input_files, files_per_partition)
    source["file_batches"] = file_batches
    source["partition_row_counts"] = None

    output_cfg = cfg.get("output", {})
    needs_counts = output_cfg.get("target_rows_per_part") not in (None, False)
    parquet_suffixes = {".parquet", ".pq", ".parq"}
    all_parquet = all(Path(path).suffix.lower() in parquet_suffixes for path in input_files)
    if needs_counts and all_parquet and _processing_preserves_row_count(input_cfg):
        metadata_workers = int(
            input_cfg.get("parquet_metadata_workers", DEFAULT_PARQUET_METADATA_WORKERS)
            or DEFAULT_PARQUET_METADATA_WORKERS
        )
        logger.info(
            "Reading Parquet row counts for %d files in %d batches (%d metadata threads)",
            len(input_files),
            len(file_batches),
            metadata_workers,
        )
        try:
            with ThreadPoolExecutor(max_workers=metadata_workers) as executor:
                source["partition_row_counts"] = list(executor.map(_parquet_batch_row_count, file_batches))
            total_rows = sum(source["partition_row_counts"])
            logger.info(
                "Read %s total rows from Parquet metadata without scanning table data",
                f"{total_rows:,}",
            )
        except Exception:
            logger.warning(
                "Could not read all Parquet row counts from metadata; falling back to a data scan",
                exc_info=True,
            )
            source["partition_row_counts"] = None

    logger.info(
        "Discovered %d input files grouped into %d Dask partitions",
        len(input_files),
        len(file_batches),
    )
    return source


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


def _build_file_processed_meta(
    path: str,
    cfg: dict[str, Any],
    *,
    will_mag: bool,
    will_dered_flux: bool,
    will_dered_mag: bool,
):
    """Infer processed schema for file inputs from a representative input file."""
    input_cfg = cfg.get("input", {})
    selected_columns = list(input_cfg.get("user_selected_cols", []) or [])
    meta_input = detect_and_read(path, selected_columns).iloc[:0]
    return process_dataframe(
        meta_input,
        cfg,
        will_mag=will_mag,
        will_dered_flux=will_dered_flux,
        will_dered_mag=will_dered_mag,
        source_name="<meta>",
    ).iloc[:0]


def prepare_catalog(
    config_path: str,
    config: dict[str, Any] | None = None,
    *,
    client=None,
    input_source: dict[str, Any] | None = None,
) -> PreparedCatalog:
    """Build the lazy processed catalog from file inputs or an existing HATS catalog."""
    cfg = normalize_catalog_config(config) if config is not None else load_catalog_config(config_path)

    inputs = cfg.get("input", {})
    dust = cfg.get("dust", {})
    output_cfg = cfg.get("output", {})

    if not list(inputs.get("user_selected_cols", []) or []):
        logger.warning(
            "No input.user_selected_cols was configured; the pipeline will read every "
            "column. This can cause very high memory use for wide catalogs."
        )

    configure_dustmaps_path(dust, client=client)

    if "photometry" in cfg:
        suffix, flags = decide_photometry_suffix(cfg)
        will_mag, will_dered_flux, will_dered_mag = flags
    else:
        suffix, will_mag, will_dered_flux, will_dered_mag = decide_suffix_and_flags(
            inputs,
            inputs.get("compute_magnitude", False),
            inputs.get("compute_dereddening", False),
            dust,
        )

    input_source = input_source or _preflight_input_source(cfg)

    if input_source["source"] == "files":
        input_files = input_source["input_files"]
        file_batches = input_source.get("file_batches") or _chunk_files(
            input_files,
            int(
                inputs.get("files_per_partition", DEFAULT_FILES_PER_PARTITION) or DEFAULT_FILES_PER_PARTITION
            ),
        )
        processed_meta = _build_file_processed_meta(
            input_files[0],
            cfg,
            will_mag=will_mag,
            will_dered_flux=will_dered_flux,
            will_dered_mag=will_dered_mag,
        )
        delayed_dfs = [
            delayed(process_files_df)(
                paths,
                cfg_path=cfg,
                will_mag=will_mag,
                will_dered_flux=will_dered_flux,
                will_dered_mag=will_dered_mag,
                output_columns=tuple(processed_meta.columns),
                output_dtypes=processed_meta.dtypes.to_dict(),
            )
            for paths in file_batches
        ]
        ddf = dd.from_delayed(delayed_dfs, meta=processed_meta)
    else:
        input_files = [input_source["catalog_path"]]
        selected_columns = list(inputs.get("user_selected_cols", []) or []) or "all"
        hats_catalog = open_lsdb_catalog(
            input_source["catalog_path"],
            client=client,
            columns=selected_columns,
        )
        processed_catalog = hats_catalog.map_partitions(
            process_dataframe,
            cfg,
            will_mag=will_mag,
            will_dered_flux=will_dered_flux,
            will_dered_mag=will_dered_mag,
            source_name=input_source["catalog_path"],
            meta=_build_processed_meta(
                hats_catalog.to_dask_dataframe(),
                cfg,
                will_mag=will_mag,
                will_dered_flux=will_dered_flux,
                will_dered_mag=will_dered_mag,
            ),
        )
        ddf = processed_catalog.to_dask_dataframe().clear_divisions()
    ddf_out = reorder_and_rechunk(
        ddf,
        output_cfg,
        partition_row_counts=input_source.get("partition_row_counts"),
    )

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
            execution_cfg=prepared.config.get("execution", {}),
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


def _expected_worker_count(execution_cfg: dict[str, Any], cluster) -> int:
    """Resolve how many worker processes must register before preparation starts."""
    executor_name = execution_cfg.get("executor", "local")
    if executor_name == "slurm":
        slurm_cfg = execution_cfg.get("slurm", {})
        jobs = int(slurm_cfg.get("dask_scale_number", 1) or 1)
        dummy_job = getattr(cluster, "_dummy_job", None)
        processes = getattr(dummy_job, "worker_processes", None)
        if processes is None:
            processes = int(slurm_cfg.get("processes", 1) or 1)
        return jobs * int(processes)

    local_cfg = execution_cfg.get("local", {})
    configured = local_cfg.get("n_workers")
    if configured is not None:
        return int(configured)
    worker_spec = getattr(cluster, "worker_spec", None)
    return len(worker_spec) if worker_spec else 1


def build_catalog(
    config_path: str,
    *,
    output_dir: str | None = None,
    output_format: str | None = None,
    require_output_path: bool = False,
) -> str | tuple[str, ...] | dict[str, str | tuple[str, ...]]:
    """
    Execute the full catalog-building flow and persist the result to disk.

    When `output_dir` is omitted, the Python API defaults to `./data`. Set
    `require_output_path=True` to require either `output_dir` or
    `output.base_path`, as the command-line interface does.
    The returned value is the written parquet partition paths, or the HATS
    artifact directory when `output_format="hats"`.
    """
    logger.info("[1/5] Loading configuration: %s", config_path)
    plan = load_build_plan(config_path)
    if require_output_path and output_dir is None:
        missing = [
            spec.name or "catalog"
            for spec in plan.catalogs
            if not spec.config.get("output", {}).get("base_path")
        ]
        if missing:
            raise ValueError(
                "An explicit output destination is required. Pass OUTPUT_DIR or configure "
                "output.base_path for: " + ", ".join(missing)
            )
    resolved_sources = []
    for index, spec in enumerate(plan.catalogs, start=1):
        label = spec.name or "catalog"
        logger.info(
            "Preflighting input catalog %d/%d before scheduler startup: %s",
            index,
            len(plan.catalogs),
            label,
        )
        resolved_sources.append(_preflight_input_source(spec.config))

    execution_cfg = plan.execution_cfg
    executor_name = execution_cfg.get("executor", "local")
    if executor_name == "slurm":
        job_count = int(execution_cfg.get("slurm", {}).get("dask_scale_number", 1) or 1)
        logger.info("[2/5] Starting Dask executor: SLURM (%d jobs)", job_count)
    else:
        logger.info("[2/5] Starting Dask executor: %s", executor_name)

    cluster = get_executor(execution_cfg)
    client = Client(cluster)

    try:
        expected_workers = _expected_worker_count(execution_cfg, cluster)
        worker_wait_timeout = execution_cfg.get("worker_wait_timeout", 900)
        logger.info("Waiting for %d Dask worker(s) to become ready", expected_workers)
        client.wait_for_workers(expected_workers, timeout=worker_wait_timeout)
        logger.info("Dask executor is ready with %d worker(s)", expected_workers)
        client.run(lambda: gc.collect())

        results = {}
        single_result = None
        with _suppress_dask_large_graph_warning():
            for index, (spec, input_source) in enumerate(
                zip(plan.catalogs, resolved_sources, strict=True), start=1
            ):
                label = spec.name or "catalog"
                logger.info(
                    "[3/5] Preparing input catalog %d/%d: %s",
                    index,
                    len(plan.catalogs),
                    label,
                )
                prepared = None
                try:
                    prepared = prepare_catalog(
                        config_path,
                        config=spec.config,
                        client=client,
                        input_source=input_source,
                    )
                    logger.info(
                        "Prepared %d input files in %d partitions",
                        len(prepared.input_files),
                        prepared.ddf.npartitions,
                    )

                    resolved_output_dir = _resolve_output_dir(prepared, output_dir)
                    resolved_format = output_format or prepared.output_cfg.get("save_as", "parquet")
                    if plan.is_batch and resolved_format != "hats":
                        resolved_output_dir = str(Path(resolved_output_dir) / label)
                    logger.info(
                        "[4/5] Writing %s output for %s to: %s",
                        str(resolved_format).upper(),
                        label,
                        resolved_output_dir,
                    )
                    written_paths = write_catalog(
                        prepared,
                        resolved_output_dir,
                        client=client,
                        output_format=output_format,
                    )
                    result = _normalize_written_path(tuple(written_paths))
                    if plan.is_batch:
                        results[label] = result
                    else:
                        single_result = result
                finally:
                    prepared = None
                    gc.collect()
                    client.run(lambda: gc.collect())

        logger.info("[5/5] Catalog build completed successfully")
        return results if plan.is_batch else single_result
    finally:
        client.close()
        cluster.close()


__all__ = [
    "PreparedCatalog",
    "CatalogBuildPlan",
    "CatalogBuildSpec",
    "load_catalog_config",
    "load_build_plan",
    "prepare_catalog",
    "materialize_catalog",
    "materialize_lsdb_catalog",
    "open_lsdb_catalog",
    "write_catalog",
    "build_catalog",
]
