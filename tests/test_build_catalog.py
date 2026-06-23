"""Tests for the build_catalog convenience flow."""

from science_catalogs.catalog import CatalogResult, build_catalog


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
    monkeypatch.setattr("science_catalogs.catalog.get_executor", lambda cfg: _FakeCluster())
    monkeypatch.setattr("science_catalogs.catalog.Client", _FakeClient)


def test_build_catalog_memory_mode(monkeypatch):
    _patch_runtime(monkeypatch)
    prepared = _Prepared()

    monkeypatch.setattr("science_catalogs.catalog.load_catalog_config", lambda path: {"cluster": {}})
    monkeypatch.setattr("science_catalogs.catalog.prepare_catalog", lambda path, config=None: prepared)
    monkeypatch.setattr("science_catalogs.catalog.materialize_catalog", lambda prepared: [1, 2, 3])

    result = build_catalog("config.yml", output_mode="memory")

    assert isinstance(result, CatalogResult)
    assert result.output_mode == "memory"
    assert result.data == [1, 2, 3]
    assert result.suffix == "_demo"


def test_build_catalog_disk_mode(monkeypatch):
    _patch_runtime(monkeypatch)
    prepared = _Prepared()

    monkeypatch.setattr("science_catalogs.catalog.load_catalog_config", lambda path: {"cluster": {}})
    monkeypatch.setattr("science_catalogs.catalog.prepare_catalog", lambda path, config=None: prepared)
    monkeypatch.setattr(
        "science_catalogs.catalog.write_catalog",
        lambda prepared, output_dir, client=None: (f"{output_dir}/part0.parquet",),
    )

    result = build_catalog("config.yml", output_mode="disk", output_dir="/tmp/out")

    assert isinstance(result, CatalogResult)
    assert result.output_mode == "disk"
    assert result.written_paths == ("/tmp/out/part0.parquet",)
    assert result.suffix == "_demo"


def test_build_catalog_lsdb_mode(monkeypatch):
    _patch_runtime(monkeypatch)
    prepared = _Prepared()
    prepared.output_cfg = {"save_as": "hats"}

    monkeypatch.setattr("science_catalogs.catalog.load_catalog_config", lambda path: {"cluster": {}})
    monkeypatch.setattr("science_catalogs.catalog.prepare_catalog", lambda path, config=None: prepared)
    monkeypatch.setattr(
        "science_catalogs.catalog.materialize_lsdb_catalog",
        lambda prepared, output_dir, client=None: "fake_lsdb_catalog",
    )

    result = build_catalog("config.yml", output_mode="lsdb", output_dir="/tmp/out")

    assert isinstance(result, CatalogResult)
    assert result.output_mode == "lsdb"
    assert result.catalog == "fake_lsdb_catalog"
    assert result.suffix == "_demo"
