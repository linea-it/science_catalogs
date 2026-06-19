"""Tests for final materialization helpers."""

from science_catalogs.pipeline import (
    PreparedCatalog,
    materialize_catalog,
    materialize_lsdb_catalog,
    open_lsdb_catalog,
)


class _FakeDdf:
    def __init__(self, dataframe):
        self._dataframe = dataframe

    def compute(self):
        return self._dataframe


def test_prepared_catalog_to_dataframe_uses_ddf_compute():
    import pandas as pd

    dataframe = pd.DataFrame({"ra": [1.0], "dec": [2.0], "value": [3]})
    prepared = PreparedCatalog(
        config={},
        ddf=_FakeDdf(dataframe),
        suffix="_demo",
        output_cfg={},
        ra_col="ra",
        dec_col="dec",
    )

    result = materialize_catalog(prepared)

    assert result is dataframe
    assert materialize_catalog(prepared) is dataframe


def test_open_lsdb_catalog(monkeypatch):
    import sys
    import types

    calls = {}

    fake_lsdb = types.SimpleNamespace()

    def fake_open_catalog(path, **kwargs):
        calls["path"] = path
        calls["kwargs"] = kwargs
        return "fake_catalog"

    fake_lsdb.open_catalog = fake_open_catalog
    monkeypatch.setitem(sys.modules, "lsdb", fake_lsdb)

    result = open_lsdb_catalog("/tmp/demo_collection", columns=["ra", "dec"])

    assert result == "fake_catalog"
    assert calls["path"] == "/tmp/demo_collection"
    assert calls["kwargs"]["columns"] == ["ra", "dec"]


def test_materialize_lsdb_catalog_uses_hats_output(monkeypatch):
    import sys
    import types

    dataframe = object()
    fake_lsdb = types.SimpleNamespace()
    calls = {}

    def fake_open_catalog(path, **kwargs):
        calls["path"] = path
        calls["kwargs"] = kwargs
        return "fake_catalog"

    fake_lsdb.open_catalog = fake_open_catalog
    monkeypatch.setitem(sys.modules, "lsdb", fake_lsdb)

    def fake_write_catalog(prepared, output_dir, client=None):
        calls["output_dir"] = output_dir
        calls["client"] = client
        return (f"{output_dir}/demo_collection",)

    monkeypatch.setattr("science_catalogs.pipeline.write_catalog", fake_write_catalog)

    prepared = PreparedCatalog(
        config={},
        ddf=_FakeDdf(dataframe),
        suffix="_demo",
        output_cfg={"save_as": "hats"},
        ra_col="ra",
        dec_col="dec",
    )

    result = materialize_lsdb_catalog(prepared, "/tmp/out", highest_order=5)

    assert result == "fake_catalog"
    assert calls["output_dir"] == "/tmp/out"
    assert calls["path"] == "/tmp/out/demo_collection"
    assert calls["kwargs"]["highest_order"] == 5
