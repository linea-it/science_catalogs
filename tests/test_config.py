"""Unit tests for pure configuration helpers."""

import pytest

from science_catalogs.utils.config import (
    ConfigDeprecationWarning,
    ab_magnitude_zero_point,
    decide_photometry_suffix,
    decide_suffix_and_flags,
    normalize_catalog_config,
    resolve_invalid_handling,
    transformation_flags,
)


def test_decide_suffix_and_flags_for_flux_with_dered_and_mag():
    """Derive the expected suffix and flags for flux inputs with dereddening."""
    suffix, will_mag, will_dered_flux, will_dered_mag = decide_suffix_and_flags(
        {
            "which_release": "LSST_DP02",
            "input_col_type": "flux",
            "input_col_model": "cmodel",
        },
        compute_mag=True,
        compute_dered=True,
        dust_cfg={"use_dustmap": "sfd"},
    )

    assert suffix == "_LSST_DP02_mag_cmodel_sfd"
    assert will_mag is True
    assert will_dered_flux is True
    assert will_dered_mag is False


def test_decide_suffix_and_flags_for_mag_without_dered():
    """Leave the flags disabled when the input is already in magnitudes."""
    suffix, will_mag, will_dered_flux, will_dered_mag = decide_suffix_and_flags(
        {
            "which_release": "DES_DR2",
            "input_col_type": "mag",
        },
        compute_mag=False,
        compute_dered=False,
    )

    assert suffix == "_DES_DR2_mag"
    assert will_mag is False
    assert will_dered_flux is False
    assert will_dered_mag is False


def test_canonical_photometry_derives_ab_zero_point_from_flux_unit():
    """Derive rather than duplicate the physical AB zero point in YAML."""
    assert ab_magnitude_zero_point({"system": "AB", "flux_unit": "nJy"}) == 31.4
    assert ab_magnitude_zero_point({"system": "AB", "flux_unit": "uJy"}) == 23.9


def test_canonical_photometry_accepts_explicit_instrumental_zero_point():
    """Support non-Rubin catalogs whose calibrated fluxes use an instrumental scale."""
    assert ab_magnitude_zero_point({"system": "AB", "zero_point": 30.0}) == 30.0


def test_canonical_photometry_requires_unambiguous_calibration():
    """Reject both missing and competing flux calibration declarations."""
    with pytest.raises(ValueError, match="exactly one"):
        ab_magnitude_zero_point({"system": "AB"})
    with pytest.raises(ValueError, match="exactly one"):
        ab_magnitude_zero_point({"system": "AB", "flux_unit": "nJy", "zero_point": 31.4})


def test_canonical_photometry_rejects_nonphysical_reverse_conversion():
    """Do not silently claim that magnitudes can recover measured fluxes."""
    with pytest.raises(ValueError, match="Unsupported photometry conversion"):
        transformation_flags("mag", "flux")


@pytest.mark.parametrize(
    ("input_type", "output_type", "expected"),
    [
        ("flux", "flux", (False, False, False)),
        ("flux", "flux_dered", (False, True, False)),
        ("flux", "mag", (True, False, False)),
        ("flux", "mag_dered", (True, True, False)),
        ("flux_dered", "flux_dered", (False, False, False)),
        ("flux_dered", "mag_dered", (True, False, False)),
        ("mag", "mag", (False, False, False)),
        ("mag", "mag_dered", (False, False, True)),
        ("mag_dered", "mag_dered", (False, False, False)),
    ],
)
def test_all_supported_photometry_transitions(input_type, output_type, expected):
    """Keep the complete matrix of scientifically supported transformations explicit."""
    assert transformation_flags(input_type, output_type) == expected


def test_decide_canonical_photometry_suffix_for_multiple_models():
    """Represent multiple measurement models without a global model field."""
    cfg = {
        "input": {"which_release": "LSST_DP2"},
        "photometry": {
            "bands": ["g"],
            "magnitude": {"system": "AB", "flux_unit": "nJy"},
            "measurements": [
                {
                    "name": "psf",
                    "input": {
                        "type": "flux",
                        "value_pattern": "BAND_psfFlux",
                        "error_pattern": "BAND_psfFluxErr",
                    },
                    "output": {
                        "type": "mag",
                        "value_pattern": "BAND_psfMag",
                        "error_pattern": "BAND_psfMagErr",
                    },
                },
                {
                    "name": "cmodel",
                    "input": {
                        "type": "flux",
                        "value_pattern": "BAND_cModelFlux",
                        "error_pattern": "BAND_cModelFluxErr",
                    },
                    "output": {
                        "type": "mag",
                        "value_pattern": "BAND_cModelMag",
                        "error_pattern": "BAND_cModelMagErr",
                    },
                },
            ],
        },
    }

    suffix, flags = decide_photometry_suffix(cfg)

    assert suffix == "_LSST_DP2_mag_psf_cmodel"
    assert flags == (True, False, False)


def test_canonical_photometry_rejects_nonfinite_extinction_coefficients():
    """Do not accept coefficients that would silently contaminate every output row."""
    cfg = {
        "metadata": {"release": "TEST"},
        "input": {},
        "photometry": {
            "bands": ["g"],
            "dereddening": {"extinction_coefficients": {"g": float("nan")}},
            "measurements": [
                {
                    "name": "model",
                    "input": {
                        "type": "mag",
                        "value_pattern": "mag_BAND",
                        "error_pattern": "magerr_BAND",
                    },
                    "output": {
                        "type": "mag_dered",
                        "value_pattern": "mag_dered_BAND",
                        "error_pattern": "magerr_dered_BAND",
                    },
                }
            ],
        },
    }

    with pytest.raises(ValueError, match="finite numbers"):
        decide_photometry_suffix(cfg)


def test_normalize_catalog_config_migrates_known_aliases_without_losing_values():
    """Translate supported aliases to one canonical structure."""
    legacy = {
        "input": {"catalog_folder": "/data", "which_release": "DR1"},
        "output": {"hats_margin_threshold": 7.5},
        "cluster": {"executor": "local"},
    }

    with pytest.warns(ConfigDeprecationWarning):
        normalized = normalize_catalog_config(legacy)

    assert normalized["metadata"]["release"] == "DR1"
    assert normalized["input"]["catalog_path"] == "/data"
    assert normalized["execution"] == {"executor": "local"}
    assert normalized["collection"]["margin"]["threshold_arcsec"] == 7.5
    assert "cluster" not in normalized


def test_normalize_catalog_config_rejects_unknown_keys():
    """Fail fast on misspelled keys instead of silently ignoring them."""
    with pytest.raises(ValueError, match="save_ass"):
        normalize_catalog_config({"output": {"save_ass": "parquet"}})


def test_normalize_catalog_config_accepts_hats_partitioning_options():
    """Keep HATS row and spatial partition limits in the canonical catalog section."""
    normalized = normalize_catalog_config(
        {"collection": {"catalog": {"pixel_threshold": 2_000_000, "highest_healpix_order": 12}}}
    )

    assert normalized["collection"]["catalog"] == {
        "pixel_threshold": 2_000_000,
        "highest_healpix_order": 12,
    }


@pytest.mark.parametrize(
    ("key", "value", "message"),
    [
        ("pixel_threshold", 0, "positive integer"),
        ("pixel_threshold", True, "positive integer"),
        ("highest_healpix_order", -1, "integer from 0 to 29"),
        ("highest_healpix_order", 30, "integer from 0 to 29"),
        ("highest_healpix_order", True, "integer from 0 to 29"),
    ],
)
def test_normalize_catalog_config_rejects_invalid_hats_partitioning_options(key, value, message):
    """Reject limits that hats-import cannot use to partition a HATS catalog."""
    with pytest.raises(ValueError, match=message):
        normalize_catalog_config({"collection": {"catalog": {key: value}}})


def test_normalize_catalog_config_requires_explicit_slurm_resources():
    """Reject SLURM execution that depends on site-specific resource defaults."""
    with pytest.raises(ValueError, match="cores, processes, memory, walltime"):
        normalize_catalog_config({"execution": {"executor": "slurm", "slurm": {}}})


def test_normalize_catalog_config_accepts_complete_slurm_resources():
    """Accept a SLURM executor whose principal resource limits are explicit."""
    config = normalize_catalog_config(
        {
            "execution": {
                "executor": "slurm",
                "slurm": {
                    "cores": 1,
                    "processes": 1,
                    "memory": "48GB",
                    "walltime": "01:00:00",
                },
            }
        }
    )

    assert config["execution"]["slurm"]["memory"] == "48GB"


def test_resolve_nested_invalid_handling_preserves_all_operations():
    """Map nested value/error policies to every existing processing feature."""
    resolved = resolve_invalid_handling(
        {
            "cross_invalidate": True,
            "value": {
                "nonfinite_is_invalid": False,
                "limit": {"value": 50.0, "comparison": "less_or_equal", "use_absolute": False},
                "replacement": {"value": -99.0, "when": "paired_invalid"},
                "round_decimals": 3,
            },
            "error": {
                "limit": {"value": 10.0},
                "replacement": {"value": 99.0, "when": "invalid"},
                "round_decimals": 4,
            },
        }
    )

    assert resolved["how_to_replace_col_values"] == "only_with_invalid_err"
    assert resolved["how_to_replace_err_values"] == "all"
    assert resolved["limit_comparison_for_col"] == "less_or_equal"
    assert resolved["use_absolute_for_col_limits"] is False
    assert resolved["is_nan_and_inf_invalid_for_col"] is False
    assert resolved["round_col_decimal_cases"] == 3
    assert resolved["round_err_decimal_cases"] == 4
