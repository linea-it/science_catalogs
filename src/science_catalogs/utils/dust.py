"""Dustmaps utilities and caching."""

import json
import os
import sys
import tempfile
from hashlib import sha1
from importlib import import_module
from pathlib import Path
from typing import Any

DUST_QUERY_CACHE: dict[str, Any] = {}


def _normalize_dustmaps_path(path: str) -> str:
    return str(Path(path).expanduser().resolve())


def _default_dustmaps_config_fname(data_dir: str) -> str:
    digest = sha1(data_dir.encode("utf-8"), usedforsecurity=False).hexdigest()[:12]
    return str(Path(tempfile.gettempdir()) / "science_catalogs" / f"dustmaps_{digest}.json")


def _home_dustmaps_config_fname() -> str | None:
    home = os.environ.get("HOME")
    if not home:
        return None
    config_path = Path(home).expanduser() / ".dustmapsrc"
    if not config_path.is_file():
        return None
    return str(config_path.resolve())


def _write_dustmaps_config(config_fname: str, data_dir: str) -> None:
    config_path = Path(config_fname)
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(json.dumps({"data_dir": data_dir}, indent=2), encoding="utf-8")


def _activate_existing_dustmaps_config(config_fname: str) -> None:
    os.environ["DUSTMAPS_CONFIG_FNAME"] = config_fname
    if "dustmaps.config" in sys.modules:
        dustmaps_config = import_module("dustmaps.config")
        dustmaps_config.config.fname = config_fname
        dustmaps_config.config.load()


def _activate_generated_dustmaps_config(data_dir: str, config_fname: str) -> None:
    os.environ["DUSTMAPS_CONFIG_FNAME"] = config_fname
    os.environ["DUSTMAPS_PATH"] = data_dir
    _write_dustmaps_config(config_fname, data_dir)

    if "dustmaps.config" in sys.modules:
        dustmaps_config = import_module("dustmaps.config")
        dustmaps_config.config.fname = config_fname
        dustmaps_config.config._options = {"data_dir": data_dir}
        dustmaps_config.config._success = True


def _make_dust_query(name: str):
    name = (name or "sfd").strip().lower()
    if name == "sfd":
        from dustmaps.sfd import SFDQuery

        return SFDQuery()
    if name.startswith("bayestar"):
        from dustmaps.bayestar import BayestarQuery

        return BayestarQuery()
    if name == "planck":
        from dustmaps.planck import PlanckQuery

        return PlanckQuery()

    import importlib

    mod = importlib.import_module(f"dustmaps.{name}")
    for attr in dir(mod):
        if attr.lower().endswith("query"):
            return getattr(mod, attr)()
    raise ValueError(f"Unsupported dustmap '{name}'")


def get_dust_query(dust_cfg: dict[str, Any]):
    """Return a cached dust query instance."""
    name = (dust_cfg.get("use_dustmap") or "sfd").strip().lower()
    if name in DUST_QUERY_CACHE:
        return DUST_QUERY_CACHE[name]
    dq = _make_dust_query(name)
    DUST_QUERY_CACHE[name] = dq
    return dq


def configure_dustmaps_path(dust_cfg: dict[str, Any], client=None):
    """Configure the dustmaps data directory when set in config."""
    path = dust_cfg.get("path_to_dustmaps")
    if path:
        explicit_config = os.environ.get("DUSTMAPS_CONFIG_FNAME")
        if explicit_config:
            _activate_existing_dustmaps_config(explicit_config)
            if client is not None and hasattr(client, "run"):
                client.run(_activate_existing_dustmaps_config, explicit_config)
            return

        home_config = _home_dustmaps_config_fname()
        if home_config:
            _activate_existing_dustmaps_config(home_config)
            if client is not None and hasattr(client, "run"):
                client.run(_activate_existing_dustmaps_config, home_config)
            return

        data_dir = _normalize_dustmaps_path(path)
        config_fname = _default_dustmaps_config_fname(data_dir)
        _activate_generated_dustmaps_config(data_dir, config_fname)
        if client is not None and hasattr(client, "run"):
            client.run(_activate_generated_dustmaps_config, data_dir, config_fname)


__all__ = ["get_dust_query", "configure_dustmaps_path", "DUST_QUERY_CACHE"]
