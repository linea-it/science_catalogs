"""Tests for per-file catalog processing."""

import numpy as np
import pandas as pd
import pytest
import yaml

from science_catalogs.processing import process_dataframe, process_file_df


def _write_mag_dered_inputs(tmp_path, keep_key, keep_value):
    input_path = tmp_path / "input.csv"
    input_path.write_text(
        "\n".join(
            [
                "object_id,ra,dec,MAG_G_DERED,MAGERR_G",
                "1,10.0,-20.0,22.5,0.1",
            ]
        ),
        encoding="utf-8",
    )
    cfg_path = tmp_path / "config.yml"
    cfg = {
        "input": {
            "catalog_folder": str(tmp_path),
            "catalog_pattern": "*.csv",
            "which_release": "DES_DR2",
            "user_selected_cols": ["object_id", "ra", "dec", "MAG_G_DERED", "MAGERR_G"],
            "input_col_type": "mag_dered",
            "compute_magnitude": False,
            "compute_dereddening": False,
            "col_pattern": "MAG_BAND_DERED",
            "err_pattern": "MAGERR_BAND",
            "selected_bands": ["G"],
            "ra_col": "ra",
            "dec_col": "dec",
            "band_case": "lower_case",
            keep_key: keep_value,
        },
        "output": {
            "col_final_pattern": "mag_BAND",
            "err_final_pattern": "magerr_BAND",
            "band_case": "lower_case",
        },
    }
    cfg_path.write_text(yaml.safe_dump(cfg), encoding="utf-8")
    return input_path, cfg_path


def test_drops_input_columns_after_filter_or_transformation_key_even_without_transform(tmp_path):
    """Drop source photometry columns after final columns are created without requiring a transform."""
    input_path, cfg_path = _write_mag_dered_inputs(
        tmp_path,
        "keep_input_columns_after_filters_or_transformations",
        False,
    )

    df = process_file_df(str(input_path), str(cfg_path), False, False, False)

    assert "mag_g" in df.columns
    assert "magerr_g" in df.columns
    assert "MAG_G_DERED" not in df.columns
    assert "MAGERR_G" not in df.columns


def test_legacy_keep_input_columns_key_still_works(tmp_path):
    """Honor the old retention key when the new key is absent."""
    input_path, cfg_path = _write_mag_dered_inputs(
        tmp_path,
        "keep_input_columns_when_computing_mag_or_dered",
        True,
    )

    df = process_file_df(str(input_path), str(cfg_path), False, False, False)

    assert "mag_g" in df.columns
    assert "magerr_g" in df.columns
    assert "MAG_G_DERED" in df.columns
    assert "MAGERR_G" in df.columns


def test_process_dataframe_matches_file_wrapper(tmp_path):
    """Apply the same science logic when the input is already in memory."""
    input_path, cfg_path = _write_mag_dered_inputs(
        tmp_path,
        "keep_input_columns_after_filters_or_transformations",
        False,
    )

    config = yaml.safe_load(cfg_path.read_text(encoding="utf-8"))
    dataframe = pd.read_csv(input_path)

    from_file = process_file_df(str(input_path), str(cfg_path), False, False, False)
    from_dataframe = process_dataframe(
        dataframe,
        config,
        will_mag=False,
        will_dered_flux=False,
        will_dered_mag=False,
        source_name="memory.csv",
    )

    assert from_dataframe.equals(from_file)


def test_process_dataframe_handles_arrow_backed_coordinates_for_dust(monkeypatch):
    """Convert Arrow-backed coordinate columns before building SkyCoord."""
    df = pd.DataFrame(
        {
            "ra": [10.0, 11.0],
            "dec": [-20.0, -21.0],
            "flux_g": [100.0, 120.0],
            "fluxerr_g": [1.0, 1.2],
        },
        dtype="float64[pyarrow]",
    )
    cfg = {
        "input": {
            "user_selected_cols": ["ra", "dec", "flux_g", "fluxerr_g"],
            "input_col_type": "flux",
            "compute_magnitude": False,
            "compute_dereddening": True,
            "col_pattern": "flux_BAND",
            "err_pattern": "fluxerr_BAND",
            "selected_bands": ["g"],
            "ra_col": "ra",
            "dec_col": "dec",
            "band_case": "lower_case",
            "keep_input_columns_after_filters_or_transformations": True,
        },
        "output": {
            "col_final_pattern": "flux_dered_BAND",
            "err_final_pattern": "fluxerr_dered_BAND",
            "band_case": "lower_case",
            "A_EBV": {"g": 3.0},
        },
        "dust": {
            "use_dustmap": "sfd",
        },
    }

    monkeypatch.setattr(
        "science_catalogs.processing.get_dust_query",
        lambda dust_cfg: lambda coords: np.zeros(len(coords), dtype=float),
    )

    result = process_dataframe(
        df,
        cfg,
        will_mag=False,
        will_dered_flux=True,
        will_dered_mag=False,
        source_name="arrow_df",
    )

    assert "flux_dered_g" in result.columns
    assert "fluxerr_dered_g" in result.columns
    assert result["flux_dered_g"].tolist() == [100.0, 120.0]


def test_process_dataframe_computes_multiple_photometry_groups():
    """Compute PSF and cModel magnitudes together while retaining source fluxes."""
    df = pd.DataFrame(
        {
            "g_psfFlux": [1000.0],
            "g_psfFluxErr": [10.0],
            "g_cModelFlux": [2000.0],
            "g_cModelFluxErr": [20.0],
        }
    )
    cfg = {
        "input": {},
        "photometry": {
            "bands": ["g"],
            "keep_input_columns": True,
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
                        "dtype": "float32",
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
                        "dtype": "float32",
                    },
                },
            ],
        },
        "output": {},
    }

    result = process_dataframe(
        df,
        cfg,
        will_mag=True,
        will_dered_flux=False,
        will_dered_mag=False,
    )

    assert result["g_psfMag"].iloc[0] == pytest.approx(23.9, abs=1e-6)
    assert result["g_cModelMag"].iloc[0] == pytest.approx(23.147425, abs=1e-6)
    expected_error = 0.01 * 2.5 / np.log(10.0)
    assert result["g_psfMagErr"].iloc[0] == pytest.approx(expected_error, abs=1e-7)
    assert result["g_cModelMagErr"].iloc[0] == pytest.approx(expected_error, abs=1e-7)
    assert result["g_psfMag"].dtype == np.dtype("float32")
    assert {"g_psfFlux", "g_cModelFlux"}.issubset(result.columns)


def test_magnitude_conversion_matches_scisql_invalid_domain():
    """Return null-like NaN where Rubin sciSQL declares flux conversions undefined."""
    df = pd.DataFrame(
        {
            "g_flux": [1000.0, 0.0, -1.0, 1000.0],
            "g_fluxErr": [10.0, 10.0, 10.0, -1.0],
        }
    )
    cfg = {
        "input": {
            "selected_bands": ["g"],
            "col_pattern": "BAND_flux",
            "err_pattern": "BAND_fluxErr",
        },
        "output": {
            "col_final_pattern": "BAND_mag",
            "err_final_pattern": "BAND_magErr",
            "mag_offset": 31.4,
        },
    }

    result = process_dataframe(
        df,
        cfg,
        will_mag=True,
        will_dered_flux=False,
        will_dered_mag=False,
    )

    assert np.isfinite(result.loc[0, "g_mag"])
    assert np.isfinite(result.loc[0, "g_magErr"])
    assert result.loc[[1, 2], "g_mag"].isna().all()
    assert result.loc[[1, 2, 3], "g_magErr"].isna().all()
    assert np.isfinite(result.loc[3, "g_mag"])


def test_nested_invalid_handling_applies_independent_value_and_error_policies():
    """Retain limit, paired replacement, nonfinite, and rounding behavior in nested config."""
    df = pd.DataFrame(
        {
            "mag_g": [20.1234, 99.0, 21.0],
            "magerr_g": [0.1234, 0.2, np.inf],
        }
    )
    cfg = {
        "input": {},
        "photometry": {
            "bands": ["g"],
            "measurements": [
                {
                    "name": "auto",
                    "input": {
                        "type": "mag",
                        "value_pattern": "mag_BAND",
                        "error_pattern": "magerr_BAND",
                    },
                    "output": {
                        "type": "mag",
                        "value_pattern": "clean_mag_BAND",
                        "error_pattern": "clean_magerr_BAND",
                    },
                }
            ],
            "invalid_handling": {
                "value": {
                    "limit": {"value": 99.0},
                    "replacement": {"value": -999.0, "when": "invalid"},
                    "round_decimals": 2,
                },
                "error": {
                    "replacement": {"value": 999.0, "when": "invalid"},
                    "round_decimals": 3,
                },
            },
        },
    }

    result = process_dataframe(
        df,
        cfg,
        will_mag=False,
        will_dered_flux=False,
        will_dered_mag=False,
    )

    assert result["clean_mag_g"].tolist() == [20.12, -999.0, 21.0]
    assert result["clean_magerr_g"].tolist() == [0.123, 0.2, 999.0]


def test_flux_dereddening_scales_values_and_errors_with_nonzero_ebv(monkeypatch):
    """Apply the same physical extinction factor to flux and flux uncertainty."""
    df = pd.DataFrame({"ra": [10.0], "dec": [-20.0], "flux_g": [100.0], "fluxerr_g": [5.0]})
    cfg = {
        "input": {"ra_col": "ra", "dec_col": "dec"},
        "dust": {"use_dustmap": "sfd"},
        "photometry": {
            "bands": ["g"],
            "dereddening": {"extinction_coefficients": {"g": 3.0}},
            "measurements": [
                {
                    "name": "model",
                    "input": {
                        "type": "flux",
                        "value_pattern": "flux_BAND",
                        "error_pattern": "fluxerr_BAND",
                    },
                    "output": {
                        "type": "flux_dered",
                        "value_pattern": "flux_dered_BAND",
                        "error_pattern": "fluxerr_dered_BAND",
                    },
                }
            ],
        },
    }
    monkeypatch.setattr(
        "science_catalogs.processing.get_dust_query",
        lambda dust_cfg: lambda coords: np.full(len(coords), 0.2),
    )

    result = process_dataframe(
        df,
        cfg,
        will_mag=False,
        will_dered_flux=True,
        will_dered_mag=False,
    )

    factor = 10 ** (0.4 * 3.0 * 0.2)
    assert result["flux_dered_g"].iloc[0] == pytest.approx(100.0 * factor)
    assert result["fluxerr_dered_g"].iloc[0] == pytest.approx(5.0 * factor)


def test_magnitude_dereddening_subtracts_extinction_and_preserves_error(monkeypatch):
    """Subtract A_lambda from magnitudes without altering their measurement uncertainty."""
    df = pd.DataFrame({"ra": [10.0], "dec": [-20.0], "mag_g": [20.0], "magerr_g": [0.1]})
    cfg = {
        "input": {"ra_col": "ra", "dec_col": "dec"},
        "dust": {"use_dustmap": "sfd"},
        "photometry": {
            "bands": ["g"],
            "dereddening": {"extinction_coefficients": {"g": 3.0}},
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
    monkeypatch.setattr(
        "science_catalogs.processing.get_dust_query",
        lambda dust_cfg: lambda coords: np.full(len(coords), 0.2),
    )

    result = process_dataframe(
        df,
        cfg,
        will_mag=False,
        will_dered_flux=False,
        will_dered_mag=True,
    )

    assert result["mag_dered_g"].iloc[0] == pytest.approx(20.0 - 3.0 * 0.2)
    assert result["magerr_dered_g"].iloc[0] == pytest.approx(0.1)


def test_multiple_models_receive_dereddening_and_invalid_handling(monkeypatch):
    """Apply the complete science chain independently to every configured measurement."""
    df = pd.DataFrame(
        {
            "ra": [10.0, 11.0],
            "dec": [-20.0, -21.0],
            "g_psfFlux": [1000.0, -1.0],
            "g_psfFluxErr": [10.0, 2.0],
            "g_cModelFlux": [2000.0, 3000.0],
            "g_cModelFluxErr": [20.0, np.inf],
        }
    )

    def measurement(name):
        return {
            "name": name,
            "input": {
                "type": "flux",
                "value_pattern": f"BAND_{name}Flux",
                "error_pattern": f"BAND_{name}FluxErr",
            },
            "output": {
                "type": "mag_dered",
                "value_pattern": f"BAND_{name}Mag_dered",
                "error_pattern": f"BAND_{name}MagErr_dered",
            },
        }

    cfg = {
        "input": {"ra_col": "ra", "dec_col": "dec"},
        "dust": {"use_dustmap": "sfd"},
        "photometry": {
            "bands": ["g"],
            "magnitude": {"system": "AB", "flux_unit": "nJy"},
            "dereddening": {"extinction_coefficients": {"g": 2.0}},
            "measurements": [measurement("psf"), measurement("cModel")],
            "invalid_handling": {
                "value": {"replacement": {"value": -999.0, "when": "invalid"}},
                "error": {"replacement": {"value": 999.0, "when": "invalid"}},
            },
        },
    }
    monkeypatch.setattr(
        "science_catalogs.processing.get_dust_query",
        lambda dust_cfg: lambda coords: np.full(len(coords), 0.1),
    )

    result = process_dataframe(
        df,
        cfg,
        will_mag=True,
        will_dered_flux=True,
        will_dered_mag=False,
    )

    assert result["g_psfMag_dered"].iloc[0] == pytest.approx(23.9 - 0.2)
    assert result["g_cModelMag_dered"].iloc[0] == pytest.approx(31.4 - 2.5 * np.log10(2000) - 0.2)
    assert result["g_psfMagErr_dered"].iloc[0] == pytest.approx(0.01 / (np.log(10) * 0.4))
    assert result["g_cModelMagErr_dered"].iloc[0] == pytest.approx(0.01 / (np.log(10) * 0.4))
    assert result["g_psfMag_dered"].iloc[1] == -999.0
    assert result["g_psfMagErr_dered"].iloc[1] == 999.0
    assert np.isfinite(result["g_cModelMag_dered"].iloc[1])
    assert result["g_cModelMagErr_dered"].iloc[1] == 999.0


def test_cross_invalidate_replaces_both_members_of_an_invalid_pair():
    """Union value and error masks when cross invalidation is explicitly enabled."""
    df = pd.DataFrame({"mag_g": [20.0, np.inf], "magerr_g": [np.inf, 0.1]})
    cfg = {
        "input": {},
        "photometry": {
            "bands": ["g"],
            "measurements": [
                {
                    "name": "model",
                    "input": {
                        "type": "mag",
                        "value_pattern": "mag_BAND",
                        "error_pattern": "magerr_BAND",
                    },
                    "output": {
                        "type": "mag",
                        "value_pattern": "clean_mag_BAND",
                        "error_pattern": "clean_magerr_BAND",
                    },
                }
            ],
            "invalid_handling": {
                "cross_invalidate": True,
                "value": {"replacement": {"value": -99.0, "when": "invalid"}},
                "error": {"replacement": {"value": 99.0, "when": "invalid"}},
            },
        },
    }

    result = process_dataframe(df, cfg, will_mag=False, will_dered_flux=False, will_dered_mag=False)

    assert result["clean_mag_g"].tolist() == [-99.0, -99.0]
    assert result["clean_magerr_g"].tolist() == [99.0, 99.0]


@pytest.mark.parametrize(
    ("value_when", "expected_values"),
    [("invalid", [20.0, -1.0]), ("paired_invalid", [-1.0, 99.0])],
)
@pytest.mark.parametrize(
    ("error_when", "expected_errors"),
    [("invalid", [-2.0, 0.1]), ("paired_invalid", [99.0, -2.0])],
)
def test_invalid_and_paired_invalid_replacement_combinations(
    value_when, expected_values, error_when, expected_errors
):
    """Keep own-mask and paired-mask replacement independent for both channels."""
    df = pd.DataFrame({"mag_g": [20.0, 99.0], "magerr_g": [99.0, 0.1]})
    cfg = {
        "input": {},
        "photometry": {
            "bands": ["g"],
            "measurements": [
                {
                    "name": "model",
                    "input": {
                        "type": "mag",
                        "value_pattern": "mag_BAND",
                        "error_pattern": "magerr_BAND",
                    },
                    "output": {
                        "type": "mag",
                        "value_pattern": "clean_mag_BAND",
                        "error_pattern": "clean_magerr_BAND",
                    },
                }
            ],
            "invalid_handling": {
                "value": {
                    "limit": {"value": 99.0},
                    "replacement": {"value": -1.0, "when": value_when},
                },
                "error": {
                    "limit": {"value": 99.0},
                    "replacement": {"value": -2.0, "when": error_when},
                },
            },
        },
    }

    result = process_dataframe(df, cfg, will_mag=False, will_dered_flux=False, will_dered_mag=False)

    assert result["clean_mag_g"].tolist() == expected_values
    assert result["clean_magerr_g"].tolist() == expected_errors


@pytest.mark.parametrize(
    ("distance_config", "expected_distance"),
    [({"distance_col_pc": "distance_pc"}, [100.0, 200.0]), ({"distance_fixed_pc": 150.0}, [150.0, 150.0])],
)
def test_3d_dustmaps_receive_configured_distances(monkeypatch, distance_config, expected_distance):
    """Build 3D coordinates from either a catalog column or a fixed distance."""
    captured = {}
    df = pd.DataFrame(
        {
            "ra": [10.0, 11.0],
            "dec": [-20.0, -21.0],
            "distance_pc": [100.0, 200.0],
            "mag_g": [20.0, 21.0],
            "magerr_g": [0.1, 0.2],
        }
    )
    cfg = {
        "input": {"ra_col": "ra", "dec_col": "dec"},
        "dust": {"use_dustmap": "bayestar", **distance_config},
        "photometry": {
            "bands": ["g"],
            "dereddening": {"extinction_coefficients": {"g": 1.0}},
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

    def query(coords):
        captured["distance_pc"] = coords.distance.to_value("pc").tolist()
        return np.zeros(len(coords))

    monkeypatch.setattr("science_catalogs.processing.get_dust_query", lambda dust_cfg: query)

    process_dataframe(df, cfg, will_mag=False, will_dered_flux=False, will_dered_mag=True)

    assert captured["distance_pc"] == expected_distance


@pytest.mark.parametrize(
    ("column_type", "limit_key", "limit", "values", "expected_ids"),
    [
        ("flux", "flux_value", 100.0, [99.0, 100.0, 101.0], [2, 3]),
        ("mag", "mag_value", 20.0, [19.0, 20.0, 21.0], [1, 2]),
        ("flux", "mag_value", 26.4, [99.0, 100.0, 101.0], [2, 3]),
        ("mag", "flux_value", 100.0, [26.3, 26.4, 26.5], [1, 2]),
    ],
)
def test_initial_cut_is_inclusive_and_supports_cross_unit_limits(
    column_type, limit_key, limit, values, expected_ids
):
    """Apply inclusive brightness cuts in native units or via the AB zero point."""
    df = pd.DataFrame({"id": [1, 2, 3], "brightness": values})
    cfg = {
        "input": {
            "initial_cut": {
                "enabled": True,
                "column": "brightness",
                "column_type": column_type,
                limit_key: limit,
            },
            "selected_bands": [],
        },
        "output": {"mag_offset": 31.4},
    }

    result = process_dataframe(df, cfg, will_mag=False, will_dered_flux=False, will_dered_mag=False)

    assert result["id"].tolist() == expected_ids


def test_filter_selects_exact_value_and_optionally_drops_its_column():
    """Apply the configured equality filter before photometric processing."""
    df = pd.DataFrame({"id": [1, 2, 3], "primary": [True, False, True]})
    cfg = {
        "input": {
            "filter": {
                "enabled": True,
                "column": "primary",
                "value": True,
                "drop_column_after_filter": True,
            },
            "selected_bands": [],
        },
        "output": {},
    }

    result = process_dataframe(df, cfg, will_mag=False, will_dered_flux=False, will_dered_mag=False)

    assert result["id"].tolist() == [1, 3]
    assert "primary" not in result.columns
