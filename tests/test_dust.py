"""Tests for dustmaps runtime configuration."""

import json
import os
from pathlib import Path

from science_catalogs.utils.dust import configure_dustmaps_path


def test_configure_dustmaps_path_writes_runtime_config(monkeypatch, tmp_path):
    """Create a runtime config file without relying on HOME."""
    dust_dir = tmp_path / "dustmaps_data"
    dust_dir.mkdir()
    home_dir = tmp_path / "home"
    home_dir.mkdir()

    monkeypatch.delenv("DUSTMAPS_CONFIG_FNAME", raising=False)
    monkeypatch.setenv("HOME", str(home_dir))
    monkeypatch.setattr(
        "science_catalogs.utils.dust.tempfile.gettempdir",
        lambda: str(tmp_path),
    )

    configure_dustmaps_path({"path_to_dustmaps": str(dust_dir)})

    config_path = Path(os.environ["DUSTMAPS_CONFIG_FNAME"])
    assert config_path.exists()
    assert json.loads(config_path.read_text(encoding="utf-8")) == {"data_dir": str(dust_dir.resolve())}


def test_configure_dustmaps_path_prefers_home_config(monkeypatch, tmp_path):
    """Reuse ~/.dustmapsrc when it already exists."""
    dust_dir = tmp_path / "dustmaps_data"
    dust_dir.mkdir()
    home_dir = tmp_path / "home"
    home_dir.mkdir()
    home_config = home_dir / ".dustmapsrc"
    home_config.write_text(
        json.dumps({"data_dir": "/existing/dustmaps"}),
        encoding="utf-8",
    )

    monkeypatch.delenv("DUSTMAPS_CONFIG_FNAME", raising=False)
    monkeypatch.setenv("HOME", str(home_dir))
    monkeypatch.setattr(
        "science_catalogs.utils.dust.tempfile.gettempdir",
        lambda: str(tmp_path),
    )

    configure_dustmaps_path({"path_to_dustmaps": str(dust_dir)})

    assert os.environ["DUSTMAPS_CONFIG_FNAME"] == str(home_config.resolve())
    assert not (tmp_path / "science_catalogs").exists()


def test_configure_dustmaps_path_propagates_to_client(monkeypatch, tmp_path):
    """Mirror the runtime config setup to Dask workers when a client is available."""
    dust_dir = tmp_path / "dustmaps_data"
    dust_dir.mkdir()
    config_path = tmp_path / "dustmaps.json"
    seen = {}

    class _FakeClient:
        def run(self, func, *args):
            seen["func_name"] = func.__name__
            seen["args"] = args
            func(*args)

    monkeypatch.setenv("DUSTMAPS_CONFIG_FNAME", str(config_path))

    configure_dustmaps_path({"path_to_dustmaps": str(dust_dir)}, client=_FakeClient())

    assert seen["func_name"] == "_activate_existing_dustmaps_config"
    assert seen["args"] == (str(config_path),)
