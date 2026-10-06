"""Tests for prepare_catalog input resolution."""

from pathlib import Path

import dask.dataframe as dd
import pandas as pd
import pytest

from science_catalogs.catalog import _preflight_input_source, _resolve_input_source, prepare_catalog


def _base_cfg():
    return {
        "input": {
            "ra_col": "ra",
            "dec_col": "dec",
            "user_selected_cols": ["object_id", "ra", "dec", "MAG_G_DERED", "MAGERR_G"],
            "col_pattern": "MAG_BAND_DERED",
            "err_pattern": "MAGERR_BAND",
            "selected_bands": ["G"],
            "band_case": "lower_case",
            "keep_input_columns_after_filters_or_transformations": False,
        },
        "output": {
            "col_final_pattern": "mag_BAND",
            "err_final_pattern": "magerr_BAND",
            "band_case": "lower_case",
        },
    }


def test_resolve_input_source_defaults_to_files(tmp_path):
    """Treat a regular directory catalog_path as a file collection."""
    first = tmp_path / "part1.csv"
    second = tmp_path / "part2.csv"
    first.write_text("id\n1\n", encoding="utf-8")
    second.write_text("id\n2\n", encoding="utf-8")

    resolved = _resolve_input_source(
        {
            "catalog_path": str(tmp_path),
            "catalog_pattern": "*.csv",
        }
    )

    assert resolved["source"] == "files"
    assert set(resolved["input_files"]) == {str(first), str(second)}


def test_resolve_input_source_accepts_single_file(tmp_path):
    """Treat a single file catalog_path as a one-file catalog input."""
    input_file = tmp_path / "part1.parquet"
    input_file.write_text("placeholder", encoding="utf-8")

    resolved = _resolve_input_source({"catalog_path": str(input_file)})

    assert resolved["source"] == "files"
    assert resolved["input_files"] == [str(input_file)]


def test_resolve_input_source_detects_hats(monkeypatch, tmp_path):
    """Treat a valid HATS directory as an LSDB-opened input."""
    monkeypatch.setattr("science_catalogs.catalog._is_hats_catalog_path", lambda path: True)

    resolved = _resolve_input_source({"catalog_path": str(tmp_path)})

    assert resolved["source"] == "hats"
    assert resolved["catalog_path"] == str(tmp_path)


def test_resolve_input_source_requires_catalog_path():
    """Reject configs without the unified input path."""
    with pytest.raises(ValueError, match="input.catalog_path is required"):
        _resolve_input_source({})


def test_preflight_batches_files_and_reads_parquet_footer_counts(tmp_path):
    """Build a compact graph and exact partition sizes without reading table data."""
    for index in range(5):
        pd.DataFrame({"value": [index, index + 1]}).to_parquet(tmp_path / f"part{index}.parq")

    source = _preflight_input_source(
        {
            "input": {
                "catalog_path": str(tmp_path),
                "catalog_pattern": "*.parq",
                "files_per_partition": 2,
                "parquet_metadata_workers": 2,
            },
            "output": {"target_rows_per_part": 3},
        }
    )

    assert [len(batch) for batch in source["file_batches"]] == [2, 2, 1]
    assert source["partition_row_counts"] == [4, 4, 2]


def test_preflight_does_not_use_source_counts_when_filter_removes_rows(tmp_path, monkeypatch):
    """Fall back to computed processed lengths when filters invalidate footer counts."""
    pd.DataFrame({"keep": [True, False]}).to_parquet(tmp_path / "part.parq")
    monkeypatch.setattr(
        "science_catalogs.catalog._parquet_batch_row_count",
        lambda paths: (_ for _ in ()).throw(AssertionError("footer count should not be read")),
    )

    source = _preflight_input_source(
        {
            "input": {
                "catalog_path": str(tmp_path),
                "catalog_pattern": "*.parq",
                "filter": {"enabled": True, "column": "keep", "value": True},
            },
            "output": {"target_rows_per_part": 1},
        }
    )

    assert source["partition_row_counts"] is None


def test_prepare_catalog_keeps_file_mode_behavior(monkeypatch, tmp_path):
    """Prepare file-glob inputs through the existing per-file path."""
    first = tmp_path / "part1.csv"
    second = tmp_path / "part2.csv"
    first.write_text("", encoding="utf-8")
    second.write_text("", encoding="utf-8")

    cfg = _base_cfg()
    cfg["input"].update(
        {
            "catalog_path": str(tmp_path),
            "catalog_pattern": "*.csv",
        }
    )

    seen = []

    monkeypatch.setattr("science_catalogs.catalog.configure_dustmaps_path", lambda dust, client=None: None)
    monkeypatch.setattr(
        "science_catalogs.catalog.decide_suffix_and_flags",
        lambda *args, **kwargs: ("_demo", False, False, False),
    )
    monkeypatch.setattr("science_catalogs.catalog.reorder_and_rechunk", lambda ddf, output_cfg, **kwargs: ddf)
    monkeypatch.setattr(
        "science_catalogs.catalog._build_file_processed_meta",
        lambda *args, **kwargs: pd.DataFrame(
            {
                "ra": pd.Series(dtype="float64"),
                "dec": pd.Series(dtype="float64"),
                "mag_g": pd.Series(dtype="float64"),
                "magerr_g": pd.Series(dtype="float64"),
            }
        ),
    )

    def fake_process_files_df(
        paths,
        cfg_path,
        will_mag,
        will_dered_flux,
        will_dered_mag,
        output_columns=None,
        output_dtypes=None,
    ):
        seen.extend(Path(path).name for path in paths)
        df = pd.DataFrame(
            {
                "ra": [1.0] * len(paths),
                "dec": [2.0] * len(paths),
                "mag_g": [22.5] * len(paths),
                "magerr_g": [0.1] * len(paths),
            }
        )
        if output_columns is not None:
            df = df.loc[:, list(output_columns)]
        if output_dtypes is not None:
            df = df.astype(output_dtypes)
        return df

    monkeypatch.setattr("science_catalogs.catalog.process_files_df", fake_process_files_df)

    prepared = prepare_catalog("unused.yml", config=cfg)
    result = prepared.ddf.compute()

    assert set(prepared.input_files) == {str(first), str(second)}
    assert {"part1.csv", "part2.csv"}.issubset(set(seen))
    assert list(result.columns) == ["ra", "dec", "mag_g", "magerr_g"]
    assert len(result) == 2


def test_prepare_catalog_aligns_file_partition_columns_to_meta(monkeypatch, tmp_path):
    """Normalize file-input partition column order before Dask validates metadata."""
    first = tmp_path / "part1.parq"
    second = tmp_path / "part2.parq"
    first.write_text("", encoding="utf-8")
    second.write_text("", encoding="utf-8")

    cfg = {
        "input": {
            "catalog_path": str(tmp_path),
            "catalog_pattern": "*.parq",
            "ra_col": "ra",
            "dec_col": "dec",
            "selected_bands": [],
        },
        "output": {},
    }

    monkeypatch.setattr("science_catalogs.catalog.configure_dustmaps_path", lambda dust, client=None: None)
    monkeypatch.setattr(
        "science_catalogs.catalog.decide_suffix_and_flags",
        lambda *args, **kwargs: ("_demo", False, False, False),
    )
    monkeypatch.setattr("science_catalogs.catalog.reorder_and_rechunk", lambda ddf, output_cfg, **kwargs: ddf)
    monkeypatch.setattr(
        "science_catalogs.catalog._build_file_processed_meta",
        lambda *args, **kwargs: pd.DataFrame(
            {
                "ra": pd.Series(dtype="float64"),
                "dec": pd.Series(dtype="float64"),
                "tract": pd.Series(dtype="int64"),
                "patch": pd.Series(dtype="int64"),
            }
        ),
    )

    def fake_process_files_df(
        paths,
        cfg_path,
        will_mag,
        will_dered_flux,
        will_dered_mag,
        output_columns=None,
        output_dtypes=None,
    ):
        df = pd.DataFrame(
            {
                "tract": [1] * len(paths),
                "patch": [2] * len(paths),
                "ra": [10.0] * len(paths),
                "dec": [-20.0] * len(paths),
            }
        )
        if output_columns is not None:
            df = df.loc[:, list(output_columns)]
        if output_dtypes is not None:
            df = df.astype(output_dtypes)
        return df

    monkeypatch.setattr("science_catalogs.catalog.process_files_df", fake_process_files_df)

    prepared = prepare_catalog("unused.yml", config=cfg)
    result = prepared.ddf.compute()

    assert list(result.columns) == ["ra", "dec", "tract", "patch"]
    assert len(result) == 2


def test_prepare_catalog_aligns_file_partition_dtypes_to_meta(monkeypatch, tmp_path):
    """Normalize file-input dtypes before Dask validates partition metadata."""
    first = tmp_path / "part1.parq"
    second = tmp_path / "part2.parq"
    first.write_text("", encoding="utf-8")
    second.write_text("", encoding="utf-8")

    cfg = {
        "input": {
            "catalog_path": str(tmp_path),
            "catalog_pattern": "*.parq",
            "ra_col": "ra",
            "dec_col": "dec",
        },
        "photometry": {"enabled": False},
        "output": {},
    }
    meta_input = pd.DataFrame(
        {
            "ra": pd.Series(dtype="float64"),
            "dec": pd.Series(dtype="float64"),
            "band": pd.Series(dtype="str"),
        }
    )

    monkeypatch.setattr("science_catalogs.catalog.configure_dustmaps_path", lambda dust, client=None: None)
    monkeypatch.setattr("science_catalogs.catalog.reorder_and_rechunk", lambda ddf, output_cfg, **kwargs: ddf)
    monkeypatch.setattr("science_catalogs.catalog.detect_and_read", lambda *args, **kwargs: meta_input.copy())
    monkeypatch.setattr(
        "science_catalogs.processing.detect_and_read",
        lambda *args, **kwargs: pd.DataFrame(
            {
                "ra": [10.0],
                "dec": [-20.0],
                "band": pd.Series(["g"], dtype=object),
            }
        ),
    )

    prepared = prepare_catalog("unused.yml", config=cfg)
    result = prepared.ddf.compute()

    assert result["band"].dtype == meta_input["band"].dtype
    assert result["band"].tolist() == ["g", "g"]


def test_prepare_catalog_reads_hats_input(monkeypatch, tmp_path):
    """Open an existing HATS catalog and process it lazily per partition."""
    hats_path = tmp_path / "demo_hats_catalog"
    hats_path.mkdir()

    cfg = _base_cfg()
    cfg["input"].update(
        {
            "catalog_path": str(hats_path),
        }
    )

    source_df = pd.DataFrame(
        {
            "object_id": [1, 2],
            "ra": [10.0, 11.0],
            "dec": [-20.0, -21.0],
            "MAG_G_DERED": [22.5, 23.0],
            "MAGERR_G": [0.1, 0.2],
        }
    )
    calls = {}
    fake_client = object()

    class _FakeCatalog:
        def __init__(self, ddf):
            self._ddf = ddf

        def to_dask_dataframe(self):
            return self._ddf

        def map_partitions(self, func, *args, **kwargs):
            calls["map_partitions_kwargs"] = dict(kwargs)
            meta = kwargs.pop("meta")
            mapped = self._ddf.map_partitions(func, *args, meta=meta, **kwargs)
            return _FakeCatalog(mapped)

    monkeypatch.setattr("science_catalogs.catalog.configure_dustmaps_path", lambda dust, client=None: None)
    monkeypatch.setattr(
        "science_catalogs.catalog.decide_suffix_and_flags",
        lambda *args, **kwargs: ("_demo", False, False, False),
    )
    monkeypatch.setattr("science_catalogs.catalog._is_hats_catalog_path", lambda path: True)
    monkeypatch.setattr("science_catalogs.catalog.reorder_and_rechunk", lambda ddf, output_cfg, **kwargs: ddf)

    def fake_open_lsdb_catalog(path, client=None, **kwargs):
        calls["path"] = path
        calls["client"] = client
        calls["columns"] = kwargs.get("columns")
        return _FakeCatalog(dd.from_pandas(source_df, npartitions=2))

    monkeypatch.setattr("science_catalogs.catalog.open_lsdb_catalog", fake_open_lsdb_catalog)

    prepared = prepare_catalog("unused.yml", config=cfg, client=fake_client)
    result = prepared.ddf.compute()

    assert prepared.input_files == [str(hats_path)]
    assert calls["path"] == str(hats_path)
    assert calls["client"] is fake_client
    assert calls["columns"] == ["object_id", "ra", "dec", "MAG_G_DERED", "MAGERR_G"]
    assert "transform_divisions" not in calls["map_partitions_kwargs"]
    assert "clear_divisions" not in calls["map_partitions_kwargs"]
    assert list(result.columns) == ["object_id", "ra", "dec", "mag_g", "magerr_g"]
    assert len(result) == 2


def test_prepare_catalog_reads_all_hats_columns_by_default(monkeypatch, tmp_path):
    """Request all HATS columns when no explicit column selection is configured."""
    hats_path = tmp_path / "demo_hats_catalog"
    hats_path.mkdir()

    cfg = {
        "input": {
            "catalog_path": str(hats_path),
            "ra_col": "ra",
            "dec_col": "dec",
            "selected_bands": [],
        },
        "output": {},
    }
    source_df = pd.DataFrame({"ra": [10.0], "dec": [-20.0], "value": [1]})
    calls = {}

    class _FakeCatalog:
        def __init__(self, ddf):
            self._ddf = ddf

        def to_dask_dataframe(self):
            return self._ddf

        def map_partitions(self, func, *args, **kwargs):
            meta = kwargs.pop("meta")
            mapped = self._ddf.map_partitions(func, *args, meta=meta, **kwargs)
            return _FakeCatalog(mapped)

    monkeypatch.setattr("science_catalogs.catalog.configure_dustmaps_path", lambda dust, client=None: None)
    monkeypatch.setattr(
        "science_catalogs.catalog.decide_suffix_and_flags",
        lambda *args, **kwargs: ("_demo", False, False, False),
    )
    monkeypatch.setattr("science_catalogs.catalog._is_hats_catalog_path", lambda path: True)
    monkeypatch.setattr("science_catalogs.catalog.reorder_and_rechunk", lambda ddf, output_cfg, **kwargs: ddf)

    def fake_open_lsdb_catalog(path, client=None, **kwargs):
        calls["columns"] = kwargs.get("columns")
        return _FakeCatalog(dd.from_pandas(source_df, npartitions=1))

    monkeypatch.setattr("science_catalogs.catalog.open_lsdb_catalog", fake_open_lsdb_catalog)

    prepared = prepare_catalog("unused.yml", config=cfg)
    result = prepared.ddf.compute()

    assert calls["columns"] == "all"
    assert list(result.columns) == ["ra", "dec", "value"]


def test_prepare_catalog_uses_programmatic_config_on_file_workers(tmp_path):
    """Use one config for metadata inference and delayed file processing."""
    input_path = tmp_path / "input.csv"
    input_path.write_text("id,ra,dec\n1,10.0,-20.0\n", encoding="utf-8")
    config_path = tmp_path / "different-on-disk.yml"
    config_path.write_text("unexpected_root_key: true\n", encoding="utf-8")
    cfg = {
        "metadata": {"release": "TEST"},
        "input": {
            "catalog_path": str(input_path),
            "user_selected_cols": ["id", "ra", "dec"],
            "ra_col": "ra",
            "dec_col": "dec",
        },
        "photometry": {"enabled": False},
        "output": {},
    }

    prepared = prepare_catalog(str(config_path), config=cfg)
    result = prepared.ddf.compute()

    assert result.to_dict("records") == [{"id": 1, "ra": 10.0, "dec": -20.0}]
