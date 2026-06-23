"""Tests for the command-line interface."""

from science_catalogs import cli
from science_catalogs.catalog import CatalogResult


def test_cli_memory_mode(monkeypatch, capsys):
    monkeypatch.setattr(
        cli,
        "build_catalog",
        lambda config_path, output_mode, output_dir: CatalogResult(
            output_mode="memory",
            suffix="_demo",
            data=[1, 2, 3],
        ),
    )
    monkeypatch.setattr(
        "sys.argv",
        ["science-catalogs", "config.yml", "--output-mode", "memory"],
    )

    cli.main()
    out = capsys.readouterr().out
    assert "Computed dataframe with 3 rows" in out


def test_cli_lsdb_mode(monkeypatch, capsys, tmp_path):
    monkeypatch.setattr(
        cli,
        "build_catalog",
        lambda config_path, output_mode, output_dir: CatalogResult(
            output_mode="lsdb",
            suffix="_demo",
            catalog="fake_catalog",
        ),
    )
    monkeypatch.setattr(
        "sys.argv",
        ["science-catalogs", "config.yml", str(tmp_path), "--output-mode", "lsdb"],
    )

    cli.main()
    out = capsys.readouterr().out
    assert "Opened LSDB catalog" in out
