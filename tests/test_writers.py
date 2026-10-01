"""Tests for catalog output writers."""

import logging
import sys
import types
import warnings

import pandas as pd
from science_catalogs.utils import writers


def test_write_hats_catalog_marks_margin_as_default(monkeypatch, tmp_path):
    """Pass is_default=True when creating the HATS margin catalog."""
    captured = {}

    class _FakeCollectionArguments:
        def __init__(self, **kwargs):
            captured["collection"] = kwargs
            self.tqdm_kwargs = kwargs.get("tqdm_kwargs") or {}

        def catalog(self, **kwargs):
            captured["catalog"] = kwargs
            return self

        def add_margin(self, **kwargs):
            captured["margin"] = kwargs
            return self

    fake_validation = types.SimpleNamespace(is_valid_collection=lambda path: False)
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


def test_write_hats_catalog_routes_tqdm_progress_to_stdout(monkeypatch, tmp_path, capsys):
    """Route HATS import progress bars to stdout without moving warnings."""
    captured = {}

    class _FakeCollectionArguments:
        def __init__(self, **kwargs):
            captured["collection"] = kwargs
            self.tqdm_kwargs = kwargs.get("tqdm_kwargs") or {}

        def catalog(self, **kwargs):
            captured["catalog"] = kwargs
            return self

        def add_margin(self, **kwargs):
            captured["margin"] = kwargs
            return self

    def fake_run(args, client):
        from tqdm import tqdm

        for _ in tqdm(range(1), desc="Catalog: Planning", **args.tqdm_kwargs):
            pass
        sys.stderr.write("hats-import warning\n")

    fake_validation = types.SimpleNamespace(is_valid_collection=lambda path: False)
    fake_readers = types.SimpleNamespace(
        CsvReader=lambda: "csv_reader",
        ParquetPyarrowReader=lambda: "parquet_reader",
    )
    fake_arguments = types.SimpleNamespace(CollectionArguments=_FakeCollectionArguments)
    fake_run_import = types.SimpleNamespace(run=fake_run)

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

    captured_output = capsys.readouterr()
    assert "Catalog: Planning" in captured_output.out
    assert "Catalog: Planning" not in captured_output.err
    assert "hats-import warning" in captured_output.err
    assert captured["collection"]["tqdm_kwargs"]["file"] is sys.stdout


def test_write_hats_catalog_reuses_existing_collection(monkeypatch, tmp_path):
    """Detect existing HATS collections without invoking staging or import."""
    monkeypatch.setitem(
        sys.modules,
        "hats.io.validation",
        types.SimpleNamespace(is_valid_collection=lambda path: True),
    )

    def fail_write_partitions(*args, **kwargs):
        raise AssertionError("write_partitions should not run for an existing collection")

    monkeypatch.setattr(writers, "write_partitions", fail_write_partitions)

    result = writers.write_hats_catalog(
        pd.DataFrame({"ra": [1.0], "dec": [2.0]}),
        {"save_as": "hats", "hats_artifact_name": "demo"},
        {"margin_threshold": 5.0},
        str(tmp_path),
        "_demo",
        "ra",
        "dec",
        client="fake_client",
    )

    assert result == (str(tmp_path / "demo"),)


def test_suppress_hats_collection_validation_warning(caplog):
    """Suppress only the noisy HATS finalization messages."""
    with caplog.at_level(logging.WARNING):
        with writers._suppress_hats_collection_validation_warning():
            logging.warning("Looking for catalog - found collection.")
            logging.warning("another warning")
            with warnings.catch_warnings(record=True) as captured_warnings:
                warnings.simplefilter("always", append=True)
                warnings.warn(
                    "Computing partitions from catalog parquet files. This may be slow.",
                    UserWarning,
                )
                warnings.warn("another user warning", UserWarning)

    assert "Looking for catalog - found collection." not in caplog.text
    assert "another warning" in caplog.text
    assert [str(warning.message) for warning in captured_warnings] == ["another user warning"]
