"""Tests for prepare_catalog input resolution."""

from pathlib import Path

import dask.dataframe as dd
import pandas as pd
import pytest
from science_catalogs.catalog import _resolve_input_source, prepare_catalog


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

    monkeypatch.setattr("science_catalogs.catalog.configure_dustmaps_path", lambda dust: None)
    monkeypatch.setattr(
        "science_catalogs.catalog.decide_suffix_and_flags",
        lambda *args, **kwargs: ("_demo", False, False, False),
    )
    monkeypatch.setattr("science_catalogs.catalog.reorder_and_rechunk", lambda ddf, output_cfg: ddf)

    def fake_process_file_df(path, cfg_path, will_mag, will_dered_flux, will_dered_mag):
        seen.append(Path(path).name)
        return pd.DataFrame({"ra": [1.0], "dec": [2.0], "mag_g": [22.5], "magerr_g": [0.1]})

    monkeypatch.setattr("science_catalogs.catalog.process_file_df", fake_process_file_df)

    prepared = prepare_catalog("unused.yml", config=cfg)
    result = prepared.ddf.compute()

    assert set(prepared.input_files) == {str(first), str(second)}
    assert {"part1.csv", "part2.csv"}.issubset(set(seen))
    assert list(result.columns) == ["ra", "dec", "mag_g", "magerr_g"]
    assert len(result) == 2


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
            meta = kwargs.pop("meta")
            mapped = self._ddf.map_partitions(func, *args, meta=meta, **kwargs)
            return _FakeCatalog(mapped)

    monkeypatch.setattr("science_catalogs.catalog.configure_dustmaps_path", lambda dust: None)
    monkeypatch.setattr(
        "science_catalogs.catalog.decide_suffix_and_flags",
        lambda *args, **kwargs: ("_demo", False, False, False),
    )
    monkeypatch.setattr("science_catalogs.catalog._is_hats_catalog_path", lambda path: True)
    monkeypatch.setattr("science_catalogs.catalog.reorder_and_rechunk", lambda ddf, output_cfg: ddf)

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
    assert list(result.columns) == ["object_id", "ra", "dec", "mag_g", "magerr_g"]
    assert len(result) == 2
