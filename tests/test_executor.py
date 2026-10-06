"""Tests for Dask executor creation."""

from science_catalogs.executor import get_executor


def _slurm_config(**overrides):
    config = {"cores": 1, "processes": 1, "memory": "4GB", "walltime": "00:30:00"}
    config.update(overrides)
    return {"executor": "slurm", "slurm": config}


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
    assert captured["n_workers"] == 1
    assert captured["threads_per_worker"] == 1
    assert captured["processes"] is True


def test_get_slurm_executor_disables_dashboard_by_default(monkeypatch):
    """Avoid starting Bokeh dashboard services for SLURM clusters by default."""
    captured = {}

    class _FakeSlurmCluster:
        def __init__(self, **kwargs):
            captured.update(kwargs)

        def scale(self, jobs):
            captured["scale_jobs"] = jobs

    monkeypatch.setattr("science_catalogs.executor.SLURMCluster", _FakeSlurmCluster)

    cluster = get_executor(_slurm_config(dask_scale_number=3))

    assert isinstance(cluster, _FakeSlurmCluster)
    assert captured["scheduler_options"]["dashboard_address"] is None
    assert captured["scale_jobs"] == 3
    assert captured["death_timeout"] == 600


def test_get_slurm_executor_keeps_explicit_dashboard_address(monkeypatch):
    """Preserve a user-requested dashboard address."""
    captured = {}

    class _FakeSlurmCluster:
        def __init__(self, **kwargs):
            captured.update(kwargs)

        def scale(self, jobs):
            captured["scale_jobs"] = jobs

    monkeypatch.setattr("science_catalogs.executor.SLURMCluster", _FakeSlurmCluster)

    get_executor(_slurm_config(dashboard_address=":8787"))

    assert captured["scheduler_options"]["dashboard_address"] == ":8787"


def test_get_slurm_executor_keeps_explicit_death_timeout(monkeypatch):
    """Allow large filesystem preflight runs to tune worker connection timeouts."""
    captured = {}

    class _FakeSlurmCluster:
        def __init__(self, **kwargs):
            captured.update(kwargs)

        def scale(self, jobs):
            pass

    monkeypatch.setattr("science_catalogs.executor.SLURMCluster", _FakeSlurmCluster)

    get_executor(_slurm_config(death_timeout=1200))

    assert captured["death_timeout"] == 1200


def test_get_slurm_executor_requires_explicit_resources():
    """Reject site-dependent resource defaults before submitting jobs."""
    import pytest

    with pytest.raises(ValueError, match="cores, processes, memory, walltime"):
        get_executor({"executor": "slurm", "slurm": {}})
