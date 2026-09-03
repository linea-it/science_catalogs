"""Tests for catalog output writers."""

import sys
import types

import pandas as pd
from science_catalogs.utils import writers


def test_write_hats_catalog_marks_margin_as_default(monkeypatch, tmp_path):
    """Pass is_default=True when creating the HATS margin catalog."""
    captured = {}

    class _FakeCollectionArguments:
        def __init__(self, **kwargs):
            captured["collection"] = kwargs

        def catalog(self, **kwargs):
            captured["catalog"] = kwargs
            return self

        def add_margin(self, **kwargs):
            captured["margin"] = kwargs
            return self

    fake_validation = types.SimpleNamespace(is_valid_catalog=lambda path: False)
    fake_readers = types.SimpleNamespace(
        CsvReader=lambda: "csv_reader",
        ParquetPyarrowReader=lambda: "parquet_reader",
    )
    fake_arguments = types.SimpleNamespace(CollectionArguments=_FakeCollectionArguments)
    fake_run_import = types.SimpleNamespace(run=lambda args, client: captured.update(run_client=client))

    monkeypatch.setitem(sys.modules, "hats.io.validation", fake_validation)
    monkeypatch.setitem(sys.modules, "hats_import.catalog.file_readers", fake_readers)
    monkeypatch.setitem(sys.modules, "hats_import.collection.arguments", fake_arguments)
    monkeypatch.setitem(sys.modules, "hats_import.collection.run_import", fake_run_import)
    monkeypatch.setattr(
        writers, "write_partitions", lambda *args, **kwargs: [str(tmp_path / "part0.parquet")]
    )

    writers.write_hats_catalog(
        pd.DataFrame({"ra": [1.0], "dec": [2.0]}),
        {"save_as": "hats", "hats_artifact_name": "demo"},
        {"margin_threshold": 5.0},
        str(tmp_path),
        "_demo",
        "ra",
        "dec",
        client="fake_client",
    )

    assert captured["margin"]["margin_threshold"] == 5.0
    assert captured["margin"]["is_default"] is True
