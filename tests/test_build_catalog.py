"""Tests for the build_catalog convenience flow."""

from pathlib import Path

from science_catalogs.catalog import build_catalog


class _FakeCluster:
    comm = None

    def close(self):
        self.closed = True


class _FakeClient:
    def __init__(self, cluster):
        self.cluster = cluster
        self.closed = False

    def run(self, func):
        func()

    def close(self):
        self.closed = True


class _Prepared:
    def __init__(self):
        self.suffix = "_demo"
        self.output_cfg = {"save_as": "parquet"}


def _patch_runtime(monkeypatch):
    """Patch the runtime pieces that would start a real Dask cluster."""
    monkeypatch.setattr("science_catalogs.catalog.get_executor", lambda cfg: _FakeCluster())
    monkeypatch.setattr("science_catalogs.catalog.Client", _FakeClient)


def test_build_catalog_writes_parquet(monkeypatch):
    """Build parquet output when an explicit destination is provided."""
    _patch_runtime(monkeypatch)
    prepared = _Prepared()
    calls = {}

    monkeypatch.setattr("science_catalogs.catalog.load_catalog_config", lambda path: {"cluster": {}})

    def fake_prepare_catalog(path, config=None, client=None):
        calls["prepare_client"] = client
        return prepared

    monkeypatch.setattr("science_catalogs.catalog.prepare_catalog", fake_prepare_catalog)

    def fake_write_catalog(prepared, output_dir, client=None, output_format=None):
        calls["output_dir"] = output_dir
        calls["output_format"] = output_format
        return (f"{output_dir}/part0.parquet",)

    monkeypatch.setattr("science_catalogs.catalog.write_catalog", fake_write_catalog)

    result = build_catalog("config.yml", output_dir="/tmp/out", output_format="parquet")

    assert result == "/tmp/out/part0.parquet"
    assert calls["prepare_client"] is not None
    assert calls["output_dir"] == "/tmp/out"
    assert calls["output_format"] == "parquet"


def test_build_catalog_defaults_to_cwd_data(monkeypatch, tmp_path):
    """Default build output should fall back to ./data under the process cwd."""
    _patch_runtime(monkeypatch)
    prepared = _Prepared()
    captured = {}

    monkeypatch.setattr("science_catalogs.catalog.load_catalog_config", lambda path: {"cluster": {}})

    def fake_prepare_catalog(path, config=None, client=None):
        captured["prepare_client"] = client
        return prepared

    monkeypatch.setattr("science_catalogs.catalog.prepare_catalog", fake_prepare_catalog)

    def fake_write_catalog(prepared, output_dir, client=None, output_format=None):
        captured["output_dir"] = output_dir
        captured["output_format"] = output_format
        return (f"{output_dir}/part0.parquet", f"{output_dir}/part1.parquet")

    monkeypatch.setattr("science_catalogs.catalog.write_catalog", fake_write_catalog)
    monkeypatch.setattr("science_catalogs.catalog.Path.cwd", lambda: Path(tmp_path))

    result = build_catalog("config.yml")

    expected = str(Path(tmp_path) / "data")
    assert result == (f"{expected}/part0.parquet", f"{expected}/part1.parquet")
    assert captured["prepare_client"] is not None
    assert captured["output_dir"] == expected
    assert captured["output_format"] is None


def test_build_catalog_writes_hats(monkeypatch):
    """Build HATS output when the output format is overridden."""
    _patch_runtime(monkeypatch)
    prepared = _Prepared()
    prepared.output_cfg = {"save_as": "hats"}
    calls = {}

    monkeypatch.setattr("science_catalogs.catalog.load_catalog_config", lambda path: {"cluster": {}})

    def fake_prepare_catalog(path, config=None, client=None):
        calls["prepare_client"] = client
        return prepared

    monkeypatch.setattr("science_catalogs.catalog.prepare_catalog", fake_prepare_catalog)

    def fake_write_catalog(prepared, output_dir, client=None, output_format=None):
        calls["output_dir"] = output_dir
        calls["output_format"] = output_format
        return (f"{output_dir}/demo_collection",)

    monkeypatch.setattr("science_catalogs.catalog.write_catalog", fake_write_catalog)

    result = build_catalog("config.yml", output_dir="/tmp/out", output_format="hats")

    assert result == "/tmp/out/demo_collection"
    assert calls["prepare_client"] is not None
    assert calls["output_dir"] == "/tmp/out"
    assert calls["output_format"] == "hats"
