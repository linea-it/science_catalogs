"""Tests for multi-catalog batch configuration."""

from pathlib import Path

import pytest
import yaml

from science_catalogs.catalog import _resolve_input_source, load_build_plan, load_catalog_config

REPO_ROOT = Path(__file__).resolve().parents[1]
LARGE_DP2_CATALOGS = {
    "dia_object_forced_source",
    "dia_source",
    "object_forced_source",
    "object_shear_all",
    "source",
}
MANY_FILE_DP2_CATALOGS = {
    "dia_object_forced_source",
    "object_forced_source",
    "source",
}


def test_dp2_large_individual_catalogs_limit_staging_tasks():
    """Apply the safer HATS settings to every individual catalog with at least 1B rows."""
    individual_dir = REPO_ROOT / "examples/configs/dp2/individual"
    configs = {}
    for config_path in individual_dir.glob("lsst_dp2_*_to_hats.yml"):
        name = config_path.name.removeprefix("lsst_dp2_").removesuffix("_to_hats.yml")
        configs[name] = yaml.safe_load(config_path.read_text(encoding="utf-8"))

    configured_large_catalogs = {
        name for name, config in configs.items() if "target_rows_per_part" in config["output"]
    }
    assert configured_large_catalogs == LARGE_DP2_CATALOGS
    for name in LARGE_DP2_CATALOGS:
        config = configs[name]
        assert config["output"]["target_rows_per_part"] == 1_000_000
        assert config["collection"]["catalog"]["highest_healpix_order"] == 12


def test_dp2_many_file_individual_catalogs_use_scalable_preflight():
    """Keep individual high-file-count configs aligned with robust SLURM startup."""
    individual_dir = REPO_ROOT / "examples/configs/dp2/individual"
    for name in MANY_FILE_DP2_CATALOGS:
        config_path = individual_dir / f"lsst_dp2_{name}_to_hats.yml"
        config = yaml.safe_load(config_path.read_text(encoding="utf-8"))

        assert config["input"]["files_per_partition"] == 32
        assert config["input"]["parquet_metadata_workers"] == 16
        assert config["execution"]["worker_wait_timeout"] == 900
        assert config["execution"]["slurm"]["death_timeout"] == 600


def test_dp2_batch_contains_all_positional_non_solar_catalogs():
    """Keep the DP2 HATS example aligned with the spatial QA catalog selection."""
    plan = load_build_plan(str(REPO_ROOT / "examples/configs/dp2/all/lsst_dp2_to_hats.yml"))
    configs = {spec.name: spec.config for spec in plan.catalogs}

    assert plan.is_batch is True
    assert list(configs) == [
        "dia_object",
        "dia_object_forced_source",
        "dia_source",
        "object",
        "object_forced_source",
        "object_shear_all",
        "source",
        "visit_detector_table",
    ]
    assert {name: (cfg["input"]["ra_col"], cfg["input"]["dec_col"]) for name, cfg in configs.items()} == {
        "dia_object": ("ra", "dec"),
        "dia_object_forced_source": ("coord_ra", "coord_dec"),
        "dia_source": ("ra", "dec"),
        "object": ("coord_ra", "coord_dec"),
        "object_forced_source": ("coord_ra", "coord_dec"),
        "object_shear_all": ("ra", "dec"),
        "source": ("coord_ra", "coord_dec"),
        "visit_detector_table": ("ra", "dec"),
    }
    assert configs["source"]["input"]["catalog_pattern"] == "**/*.parq"
    for name, cfg in configs.items():
        if name in MANY_FILE_DP2_CATALOGS:
            assert cfg["input"]["files_per_partition"] == 32
            assert cfg["input"]["parquet_metadata_workers"] == 16
        else:
            assert "files_per_partition" not in cfg["input"]
            assert "parquet_metadata_workers" not in cfg["input"]
    assert plan.execution_cfg["worker_wait_timeout"] == 900
    assert plan.execution_cfg["slurm"]["death_timeout"] == 600
    assert configs["object"]["photometry"]["enabled"] is True
    assert len(configs["object"]["photometry"]["measurements"]) == 2
    assert configs["dia_object"]["collection"]["indexes"] == [
        {"column": "diaObjectId", "drop_duplicates": False}
    ]
    configured_large_catalogs = {
        name for name, cfg in configs.items() if "target_rows_per_part" in cfg["output"]
    }
    assert configured_large_catalogs == LARGE_DP2_CATALOGS
    for name in LARGE_DP2_CATALOGS:
        assert configs[name]["output"]["target_rows_per_part"] == 1_000_000
        assert configs[name]["collection"]["catalog"] == {
            "artifact_name": name,
            "highest_healpix_order": 12,
        }
    assert all(cfg["photometry"] == {"enabled": False} for name, cfg in configs.items() if name != "object")
    assert all(cfg["output"]["on_existing"] == "error" for cfg in configs.values())
    assert all(cfg["collection"]["margin"]["threshold_arcsec"] == 5.0 for cfg in configs.values())
    data_catalogs = {"object", "object_forced_source", "object_shear_all"}
    for name, config in configs.items():
        storage = "data" if name in data_catalogs else "mnt"
        root = f"<path-to-data-on-{storage}>"
        assert config["input"]["catalog_path"] == f"{root}/primary/catalogs/{name}"
        assert config["output"]["base_path"] == f"{root}/secondary/catalogs"


def test_dp2_individual_configs_exactly_match_batch_catalogs():
    """Prevent individual and batch execution paths from silently diverging."""
    individual_dir = REPO_ROOT / "examples/configs/dp2/individual"
    plan = load_build_plan(str(REPO_ROOT / "examples/configs/dp2/all/lsst_dp2_to_hats.yml"))

    for spec in plan.catalogs:
        individual = load_catalog_config(str(individual_dir / f"lsst_dp2_{spec.name}_to_hats.yml"))
        assert individual == spec.config, spec.name


def test_batch_defaults_are_deep_merged(tmp_path):
    """Allow catalog sections to override nested defaults without losing sibling values."""
    config_path = tmp_path / "batch.yml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "batch": {
                    "defaults": {
                        "metadata": {"release": "TEST"},
                        "photometry": {"enabled": False},
                        "output": {"save_as": "hats", "base_path": "/output"},
                        "collection": {"margin": {"threshold_arcsec": 5.0}},
                    },
                    "catalogs": [
                        {
                            "name": "demo",
                            "input": {"catalog_path": "/input", "ra_col": "ra", "dec_col": "dec"},
                            "output": {"hats_artifact_name": "demo_collection"},
                            "collection": {"catalog": {"artifact_name": "demo"}},
                        }
                    ],
                }
            }
        ),
        encoding="utf-8",
    )

    config = load_build_plan(str(config_path)).catalogs[0].config

    assert config["output"]["base_path"] == "/output"
    assert config["output"]["hats_artifact_name"] == "demo_collection"
    assert config["collection"] == {
        "margin": {"threshold_arcsec": 5.0},
        "catalog": {"artifact_name": "demo"},
    }


@pytest.mark.parametrize("duplicate_field", ["name", "artifact"])
def test_batch_rejects_duplicate_names_and_hats_artifacts(tmp_path, duplicate_field):
    """Reject output collisions before starting a distributed executor."""
    first_name = second_name = "same" if duplicate_field == "name" else "first"
    if duplicate_field != "name":
        second_name = "second"
    first_artifact = second_artifact = "same_collection" if duplicate_field == "artifact" else "first"
    if duplicate_field != "artifact":
        second_artifact = "second"
    config = {
        "batch": {
            "defaults": {"photometry": {"enabled": False}, "output": {"save_as": "hats"}},
            "catalogs": [
                {
                    "name": first_name,
                    "input": {"catalog_path": "/first"},
                    "output": {"hats_artifact_name": first_artifact},
                },
                {
                    "name": second_name,
                    "input": {"catalog_path": "/second"},
                    "output": {"hats_artifact_name": second_artifact},
                },
            ],
        }
    }
    config_path = tmp_path / "batch.yml"
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")

    with pytest.raises(ValueError, match="Duplicate batch"):
        load_build_plan(str(config_path))


def test_recursive_catalog_pattern_finds_band_directories(tmp_path, monkeypatch):
    """Resolve all band files beneath a catalog with recursive glob syntax."""
    expected = []
    for band in ("u", "g", "r"):
        band_dir = tmp_path / band
        band_dir.mkdir()
        path = band_dir / f"source_{band}.parq"
        path.write_text("placeholder", encoding="utf-8")
        expected.append(str(path))
    monkeypatch.setattr("science_catalogs.catalog._is_hats_catalog_path", lambda path: False)

    resolved = _resolve_input_source({"catalog_path": str(tmp_path), "catalog_pattern": "**/*.parq"})

    assert resolved == {"source": "files", "input_files": sorted(expected)}
