"""Tests for catalog output writers."""

import logging
import shutil
import sys
import types
import warnings

import pandas as pd
import pytest

from science_catalogs.utils import writers


@pytest.fixture
def isolated_hats_client(monkeypatch):
    """Avoid opening a real distributed client in writer unit tests."""

    class _FakeClient:
        def __init__(self):
            self.closed = False

        def close(self, timeout=None):
            self.closed = True

    fake_client = _FakeClient()
    monkeypatch.setattr(writers, "_create_hats_client", lambda client: fake_client)
    return fake_client


def test_write_hats_catalog_marks_margin_as_default(monkeypatch, tmp_path, isolated_hats_client):
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

        def add_index(self, **kwargs):
            captured.setdefault("indexes", []).append(kwargs)
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
        {},
        str(tmp_path),
        "_demo",
        "ra",
        "dec",
        client="fake_client",
    )

    assert captured["margin"]["margin_threshold"] == 5.0
    assert captured["margin"]["is_default"] is True
    assert captured["run_client"] is isolated_hats_client
    assert isolated_hats_client.closed is True


def test_write_hats_catalog_adds_configured_indexes(monkeypatch, tmp_path, isolated_hats_client):
    """Create collection index catalogs from the canonical collection config."""
    captured = {}

    class _FakeCollectionArguments:
        def __init__(self, **kwargs):
            self.tqdm_kwargs = kwargs.get("tqdm_kwargs") or {}

        def catalog(self, **kwargs):
            captured["catalog"] = kwargs
            return self

        def add_margin(self, **kwargs):
            return self

        def add_index(self, **kwargs):
            captured.setdefault("indexes", []).append(kwargs)
            return self

    monkeypatch.setitem(
        sys.modules,
        "hats.io.validation",
        types.SimpleNamespace(is_valid_collection=lambda path: False),
    )
    monkeypatch.setitem(
        sys.modules,
        "hats_import.catalog.file_readers",
        types.SimpleNamespace(CsvReader=lambda: "csv", ParquetPyarrowReader=lambda: "parquet"),
    )
    monkeypatch.setitem(
        sys.modules,
        "hats_import.collection.arguments",
        types.SimpleNamespace(CollectionArguments=_FakeCollectionArguments),
    )
    monkeypatch.setitem(
        sys.modules,
        "hats_import.collection.run_import",
        types.SimpleNamespace(run=lambda args, client: None),
    )
    monkeypatch.setattr(
        writers, "write_partitions", lambda *args, **kwargs: [str(tmp_path / "part0.parquet")]
    )

    writers.write_hats_catalog(
        pd.DataFrame({"objectId": [1], "ra": [1.0], "dec": [2.0]}),
        {"save_as": "hats", "hats_artifact_name": "demo"},
        {
            "catalog": {
                "artifact_name": "object_lc",
                "pixel_threshold": 2_000_000,
                "highest_healpix_order": 12,
            },
            "margin": {"threshold_arcsec": 5.0},
            "indexes": [{"column": "objectId", "drop_duplicates": False}],
        },
        str(tmp_path),
        "_demo",
        "ra",
        "dec",
        client="fake_client",
    )

    assert captured["catalog"]["output_artifact_name"] == "object_lc"
    assert captured["catalog"]["pixel_threshold"] == 2_000_000
    assert captured["catalog"]["highest_healpix_order"] == 12
    assert captured["indexes"] == [
        {
            "indexing_column": "objectId",
            "drop_duplicates": False,
            "include_healpix_29": True,
            "include_order_pixel": True,
            "include_radec": False,
        }
    ]


def test_write_hats_catalog_routes_tqdm_progress_to_stdout(
    monkeypatch, tmp_path, capsys, isolated_hats_client
):
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


def test_create_hats_client_connects_to_existing_scheduler(monkeypatch):
    """Use a separate client identity while sharing the existing scheduler."""
    captured = {}

    class _ExternalClient:
        def scheduler_info(self):
            return {"address": "tcp://scheduler:8786"}

    def fake_client(*args, **kwargs):
        captured["args"] = args
        captured["kwargs"] = kwargs
        return "isolated-client"

    monkeypatch.setitem(sys.modules, "distributed", types.SimpleNamespace(Client=fake_client))

    result = writers._create_hats_client(_ExternalClient())

    assert result == "isolated-client"
    assert captured == {
        "args": ("tcp://scheduler:8786",),
        "kwargs": {"set_as_default": False},
    }


def test_write_hats_catalog_preserves_staging_when_client_cannot_close(monkeypatch, tmp_path, caplog):
    """Never remove staging while HATS tasks may still be using it."""

    class _FakeCollectionArguments:
        def __init__(self, **kwargs):
            self.tqdm_kwargs = kwargs.get("tqdm_kwargs") or {}

        def catalog(self, **kwargs):
            return self

        def add_margin(self, **kwargs):
            return self

    class _UnclosableClient:
        def close(self, timeout=None):
            raise TimeoutError("scheduler did not acknowledge client close")

    def fail_import(args, client):
        raise RuntimeError("import failed")

    monkeypatch.setitem(
        sys.modules,
        "hats.io.validation",
        types.SimpleNamespace(is_valid_collection=lambda path: False),
    )
    monkeypatch.setitem(
        sys.modules,
        "hats_import.catalog.file_readers",
        types.SimpleNamespace(CsvReader=lambda: "csv", ParquetPyarrowReader=lambda: "parquet"),
    )
    monkeypatch.setitem(
        sys.modules,
        "hats_import.collection.arguments",
        types.SimpleNamespace(CollectionArguments=_FakeCollectionArguments),
    )
    monkeypatch.setitem(
        sys.modules,
        "hats_import.collection.run_import",
        types.SimpleNamespace(run=fail_import),
    )
    monkeypatch.setattr(
        writers, "write_partitions", lambda *args, **kwargs: [str(tmp_path / "part0.parquet")]
    )
    monkeypatch.setattr(writers, "_create_hats_client", lambda client: _UnclosableClient())

    with caplog.at_level(logging.ERROR), pytest.raises(RuntimeError, match="import failed"):
        writers.write_hats_catalog(
            pd.DataFrame({"ra": [1.0], "dec": [2.0]}),
            {"save_as": "hats", "hats_artifact_name": "demo"},
            {"margin_threshold": 5.0},
            str(tmp_path),
            "_demo",
            "ra",
            "dec",
            client="shared-client",
        )

    staging_dirs = list(tmp_path.glob(".demo_staging_*"))
    assert len(staging_dirs) == 1
    assert "preserving staging directory" in caplog.text
    assert "staging directory was preserved for safety" in caplog.text
    shutil.rmtree(staging_dirs[0])


def test_write_hats_catalog_reuses_existing_collection_when_requested(monkeypatch, tmp_path):
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
        {"save_as": "hats", "hats_artifact_name": "demo", "on_existing": "reuse"},
        {"margin_threshold": 5.0},
        str(tmp_path),
        "_demo",
        "ra",
        "dec",
        client="fake_client",
    )

    assert result == (str(tmp_path / "demo"),)


def test_write_hats_catalog_rejects_existing_collection_by_default(monkeypatch, tmp_path):
    """Avoid silently reusing stale output when no policy is configured."""
    import pytest

    monkeypatch.setitem(
        sys.modules,
        "hats.io.validation",
        types.SimpleNamespace(is_valid_collection=lambda path: True),
    )

    with pytest.raises(FileExistsError, match="demo"):
        writers.write_hats_catalog(
            pd.DataFrame({"ra": [1.0], "dec": [2.0]}),
            {"save_as": "hats", "hats_artifact_name": "demo"},
            {},
            str(tmp_path),
            "_demo",
            "ra",
            "dec",
            client="fake_client",
        )


def test_write_hats_catalog_can_reject_existing_collection(monkeypatch, tmp_path):
    """Fail before staging when the configured HATS destination already exists."""
    import pytest

    monkeypatch.setitem(
        sys.modules,
        "hats.io.validation",
        types.SimpleNamespace(is_valid_collection=lambda path: True),
    )

    with pytest.raises(FileExistsError, match="demo"):
        writers.write_hats_catalog(
            pd.DataFrame({"ra": [1.0], "dec": [2.0]}),
            {"save_as": "hats", "hats_artifact_name": "demo", "on_existing": "error"},
            {"margin_threshold": 5.0},
            str(tmp_path),
            "_demo",
            "ra",
            "dec",
            client="fake_client",
        )


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
