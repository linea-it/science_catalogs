"""Tests for the command-line interface."""

from science_catalogs import cli


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
