"""Tests for cheap and non-fatal distributed resource diagnostics."""

import logging
import threading
from types import SimpleNamespace

import pytest

from science_catalogs.utils import diagnostics


def test_worker_snapshot_uses_existing_heartbeats():
    """Read memory and spill data without visiting workers or enumerating tasks."""
    worker = SimpleNamespace(
        memory=SimpleNamespace(process=40, managed=10, unmanaged=30, unmanaged_recent=5),
        host="node",
        local_directory="/tmp/dask/worker",
        memory_limit=48,
        metrics={"spilled_bytes": {"disk": 6, "memory": 8}},
        processing={"a", "b"},
        status="running",
    )
    result = diagnostics._worker_snapshot(SimpleNamespace(workers={"tcp://node:1": worker}))
    assert result[0]["rss"] == 40
    assert result[0]["managed"] == 10
    assert result[0]["unmanaged"] == 30
    assert result[0]["spill_disk"] == 6
    assert result[0]["spill_memory"] == 8
    assert result[0]["processing"] == 2
    assert result[0]["local_directory"] == "/tmp/dask/worker"


def test_diagnostics_timeout_is_nonfatal(caplog):
    """Bound the RPC wait and allow the catalog to continue on diagnostic failure."""

    class _Client:
        _run_on_scheduler = None

        def sync(self, function, *args, **kwargs):
            assert kwargs["callback_timeout"] == 10
            raise TimeoutError("scheduler busy")

    with caplog.at_level(logging.WARNING):
        diagnostics._ClusterDiagnostics(_Client()).report()
    assert "pipeline continues" in caplog.text


def test_diagnostics_stops_monitor_on_build_failure(monkeypatch):
    """The periodic monitor cannot survive normal context cleanup on an exception."""
    sampled = threading.Event()
    monkeypatch.setattr(diagnostics._ClusterDiagnostics, "report", lambda self: sampled.set())
    with pytest.raises(RuntimeError, match="build failed"):
        with diagnostics.cluster_diagnostics(None, interval_seconds=0.01):
            assert sampled.wait(1)
            raise RuntimeError("build failed")
    assert not any(thread.name == "dask-memory-diagnostics" for thread in threading.enumerate())


@pytest.mark.parametrize("interval", [True, -1, 1, float("nan"), float("inf"), "300"])
def test_diagnostics_rejects_unbounded_or_excessive_polling(interval):
    """Reject invalid or excessively frequent diagnostic settings."""
    from science_catalogs.utils.config import normalize_catalog_config

    with pytest.raises(ValueError, match="diagnostics_interval_seconds"):
        normalize_catalog_config({"execution": {"diagnostics_interval_seconds": interval}})


def test_real_client_reports_spilling_and_memory(caplog):
    """Exercise bounded sampling on a real client without altering retained data."""
    from distributed import Client, LocalCluster

    with (
        LocalCluster(
            n_workers=1,
            threads_per_worker=1,
            processes=False,
            protocol="inproc",
            dashboard_address=None,
            worker_dashboard_address=None,
        ) as cluster,
        Client(cluster) as client,
        caplog.at_level(logging.INFO),
    ):
        future = client.submit(lambda: bytearray(1024 * 1024))
        assert len(future.result()) == 1024 * 1024
        sampler = diagnostics._ClusterDiagnostics(client)
        sampler.report()
        sampler.report()
        assert future.status == "finished"
        assert "spill/local_directory=" in caplog.text
        assert "managed=" in caplog.text
        assert "unmanaged=" in caplog.text
        assert "spill_disk=" in caplog.text
        assert "diagnostics unavailable" not in caplog.text
        assert caplog.text.count("spill/local_directory=") == 1
