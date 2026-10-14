"""Tests for the build_catalog convenience flow."""

import warnings
from pathlib import Path

from science_catalogs.catalog import CatalogBuildPlan, CatalogBuildSpec, build_catalog


class _FakeCluster:
    comm = None

    def close(self):
        self.closed = True


class _FakeClient:
    last_waited_for = None

    def __init__(self, cluster):
        self.cluster = cluster
        self.closed = False

    def run(self, func):
        func()

    def sync(self, func, *args, **kwargs):
        return func(*args)

    def _run_on_scheduler(self, func):
        return func(type("Scheduler", (), {"workers": {}})())

    def wait_for_workers(self, n_workers, timeout=None):
        self.waited_for = (n_workers, timeout)
        type(self).last_waited_for = self.waited_for

    def close(self):
        self.closed = True


class _Prepared:
    def __init__(self):
        self.suffix = "_demo"
        self.output_cfg = {"save_as": "parquet"}
        self.input_files = ["input.parquet"]
        self.ddf = type("FakeDdf", (), {"npartitions": 1})()


def _patch_runtime(monkeypatch):
    """Patch the runtime pieces that would start a real Dask cluster."""
    monkeypatch.setattr("science_catalogs.catalog.get_executor", lambda cfg: _FakeCluster())
    monkeypatch.setattr("science_catalogs.catalog.Client", _FakeClient)
    monkeypatch.setattr(
        "science_catalogs.catalog._preflight_input_source",
        lambda cfg: {"source": "files", "input_files": ["input.parquet"]},
    )


def _patch_single_plan(monkeypatch, config=None):
    """Use a validated-looking single-catalog plan without reading a YAML file."""
    cfg = config or {"execution": {}}
    plan = CatalogBuildPlan(
        catalogs=[CatalogBuildSpec(name=None, config=cfg)],
        execution_cfg=cfg.get("execution", {}),
    )
    monkeypatch.setattr("science_catalogs.catalog.load_build_plan", lambda path: plan)


def test_build_catalog_writes_parquet(monkeypatch):
    """Build parquet output when an explicit destination is provided."""
    _patch_runtime(monkeypatch)
    prepared = _Prepared()
    calls = {}

    _patch_single_plan(monkeypatch)

    def fake_prepare_catalog(path, config=None, client=None, input_source=None):
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
    assert _FakeClient.last_waited_for == (1, 900)


def test_build_catalog_defaults_to_cwd_data(monkeypatch, tmp_path):
    """Default build output should fall back to ./data under the process cwd."""
    _patch_runtime(monkeypatch)
    prepared = _Prepared()
    captured = {}

    _patch_single_plan(monkeypatch)

    def fake_prepare_catalog(path, config=None, client=None, input_source=None):
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


def test_build_catalog_can_require_explicit_output_destination(monkeypatch):
    """Let CLI callers reject an accidental multi-terabyte write below the CWD."""
    _patch_single_plan(monkeypatch)

    import pytest

    with pytest.raises(ValueError, match="explicit output destination"):
        build_catalog("config.yml", require_output_path=True)


def test_build_catalog_accepts_configured_output_destination(monkeypatch):
    """Treat output.base_path as an explicit destination for CLI builds."""
    _patch_runtime(monkeypatch)
    prepared = _Prepared()
    prepared.output_cfg["base_path"] = "/configured/out"
    _patch_single_plan(monkeypatch, {"execution": {}, "output": {"base_path": "/configured/out"}})
    monkeypatch.setattr(
        "science_catalogs.catalog.prepare_catalog",
        lambda path, config=None, client=None, input_source=None: prepared,
    )
    monkeypatch.setattr(
        "science_catalogs.catalog.write_catalog",
        lambda prepared, output_dir, client=None, output_format=None: (f"{output_dir}/part.parquet",),
    )

    result = build_catalog("config.yml", require_output_path=True)

    assert result == "/configured/out/part.parquet"


def test_build_catalog_writes_hats(monkeypatch):
    """Build HATS output when the output format is overridden."""
    _patch_runtime(monkeypatch)
    prepared = _Prepared()
    prepared.output_cfg = {"save_as": "hats"}
    calls = {}

    _patch_single_plan(monkeypatch)

    def fake_prepare_catalog(path, config=None, client=None, input_source=None):
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


def test_build_catalog_suppresses_only_dask_large_graph_warning(monkeypatch):
    """Suppress Dask's graph-size advisory without hiding unrelated warnings."""
    _patch_runtime(monkeypatch)
    prepared = _Prepared()

    _patch_single_plan(monkeypatch)
    monkeypatch.setattr(
        "science_catalogs.catalog.prepare_catalog",
        lambda path, config=None, client=None, input_source=None: prepared,
    )

    def fake_write_catalog(prepared, output_dir, client=None, output_format=None):
        warnings.warn_explicit(
            "Sending large graph of size 56.44 MiB. This may cause some slowdown.",
            UserWarning,
            "distributed/client.py",
            3374,
            module="distributed.client",
        )
        warnings.warn("another warning", UserWarning)
        return (f"{output_dir}/part0.parquet",)

    monkeypatch.setattr("science_catalogs.catalog.write_catalog", fake_write_catalog)

    with warnings.catch_warnings(record=True) as captured:
        warnings.simplefilter("always")
        build_catalog("config.yml", output_dir="/tmp/out")

    assert [str(warning.message) for warning in captured] == ["another warning"]


def test_build_catalog_processes_batch_sequentially_on_one_cluster(monkeypatch):
    """Reuse one executor while preparing and writing each named catalog in order."""
    _patch_runtime(monkeypatch)
    plan = CatalogBuildPlan(
        catalogs=[
            CatalogBuildSpec(name="first", config={"input": {"catalog_path": "first"}}),
            CatalogBuildSpec(name="second", config={"input": {"catalog_path": "second"}}),
        ],
        execution_cfg={},
        is_batch=True,
    )
    monkeypatch.setattr("science_catalogs.catalog.load_build_plan", lambda path: plan)
    calls = []

    def fake_prepare(path, config=None, client=None, input_source=None):
        prepared = _Prepared()
        prepared.catalog_name = config["input"]["catalog_path"]
        calls.append(("prepare", prepared.catalog_name, client))
        return prepared

    def fake_write(prepared, output_dir, client=None, output_format=None):
        calls.append(("write", prepared.catalog_name, client))
        return (f"{output_dir}/{prepared.catalog_name}_collection",)

    monkeypatch.setattr("science_catalogs.catalog.prepare_catalog", fake_prepare)
    monkeypatch.setattr("science_catalogs.catalog.write_catalog", fake_write)

    result = build_catalog("batch.yml", output_dir="/tmp/out")

    assert result == {
        "first": "/tmp/out/first/first_collection",
        "second": "/tmp/out/second/second_collection",
    }
    assert [(kind, name) for kind, name, _ in calls] == [
        ("prepare", "first"),
        ("write", "first"),
        ("prepare", "second"),
        ("write", "second"),
    ]
    assert len({id(client) for _, _, client in calls}) == 1
