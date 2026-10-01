"""Tests for the command-line interface."""

import logging

from science_catalogs import cli


def test_cli_routes_progress_logs_to_stdout_and_warnings_to_stderr(monkeypatch, capsys):
    """Route progress logs to stdout while keeping warnings and errors on stderr."""

    def fake_build_catalog(config_path, output_dir, output_format=None):
        logger = logging.getLogger("science_catalogs.test")
        logger.info("pipeline progress")
        logger.warning("pipeline warning")
        logger.error("pipeline error")
        return f"{output_dir}/part0.parquet"

    monkeypatch.setattr(cli, "build_catalog", fake_build_catalog)
    monkeypatch.setattr("sys.argv", ["science-catalogs", "config.yml", "/tmp/out"])

    cli.main()

    captured = capsys.readouterr()
    assert "pipeline progress" in captured.out
    assert "pipeline progress" not in captured.err
    assert "pipeline warning" in captured.err
    assert "pipeline error" in captured.err


def test_cli_parquet_output(monkeypatch, capsys):
    """Report the number of written parquet partitions."""
    monkeypatch.setattr(
        cli,
        "build_catalog",
        lambda config_path, output_dir, output_format=None: (
            f"{output_dir}/part0.parquet",
            f"{output_dir}/part1.parquet",
        ),
    )
    monkeypatch.setattr(
        "sys.argv",
        ["science-catalogs", "config.yml", "/tmp/out", "--output-format", "parquet"],
    )

    cli.main()
    out = capsys.readouterr().out
    assert "Wrote 2 partition files" in out


def test_cli_hats_output(monkeypatch, capsys, tmp_path):
    """Report the final HATS artifact location."""
    monkeypatch.setattr(
        cli,
        "build_catalog",
        lambda config_path, output_dir, output_format=None: f"{output_dir}/demo_collection",
    )
    monkeypatch.setattr(
        "sys.argv",
        ["science-catalogs", "config.yml", str(tmp_path), "--output-format", "hats"],
    )

    cli.main()
    out = capsys.readouterr().out
    assert f"Wrote artifact to {tmp_path}/demo_collection" in out
