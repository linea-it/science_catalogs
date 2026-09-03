"""Tests for Dask executor creation."""

from science_catalogs.executor import get_executor


def test_get_local_executor_disables_dashboard_by_default(monkeypatch):
    """Avoid starting Bokeh dashboard services unless explicitly configured."""
    captured = {}

    def fake_local_cluster(**kwargs):
        captured.update(kwargs)
        return "cluster"

    monkeypatch.setattr("science_catalogs.executor.LocalCluster", fake_local_cluster)

    cluster = get_executor({"executor": "local", "local": {"n_workers": 1}})

    assert cluster == "cluster"
    assert captured["dashboard_address"] is None


def test_get_slurm_executor_disables_dashboard_by_default(monkeypatch):
    """Avoid starting Bokeh dashboard services for SLURM clusters by default."""
    captured = {}

    class _FakeSlurmCluster:
        def __init__(self, **kwargs):
            captured.update(kwargs)

        def scale(self, jobs):
            captured["scale_jobs"] = jobs

    monkeypatch.setattr("science_catalogs.executor.SLURMCluster", _FakeSlurmCluster)

    cluster = get_executor({"executor": "slurm", "slurm": {"dask_scale_number": 3}})

    assert isinstance(cluster, _FakeSlurmCluster)
    assert captured["scheduler_options"]["dashboard_address"] is None
    assert captured["scale_jobs"] == 3


def test_get_slurm_executor_keeps_explicit_dashboard_address(monkeypatch):
    """Preserve a user-requested dashboard address."""
    captured = {}

    class _FakeSlurmCluster:
        def __init__(self, **kwargs):
            captured.update(kwargs)

        def scale(self, jobs):
            captured["scale_jobs"] = jobs

    monkeypatch.setattr("science_catalogs.executor.SLURMCluster", _FakeSlurmCluster)

    get_executor({"executor": "slurm", "slurm": {"dashboard_address": ":8787"}})

    assert captured["scheduler_options"]["dashboard_address"] == ":8787"
