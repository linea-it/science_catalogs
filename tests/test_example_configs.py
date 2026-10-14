"""End-to-end checks for the shipped example configurations."""

from pathlib import Path

import numpy as np
import pytest
import yaml
from pandas.testing import assert_frame_equal

from science_catalogs import processing
from science_catalogs.processing import MAG_CONV, process_file_df
from science_catalogs.utils.config import (
    ab_magnitude_zero_point,
    as_float_or_none,
    decide_photometry_suffix,
    decide_suffix_and_flags,
    resolve_invalid_handling,
    transformation_flags,
)
from science_catalogs.utils.io_readers import detect_and_read

REPO_ROOT = Path(__file__).resolve().parents[1]

EXAMPLE_CONFIGS = [
    REPO_ROOT / "examples/configs/des_dr2_mag_auto_dered.yml",
    REPO_ROOT / "examples/configs/lsst_dp02_cmodel_mag_dered.yml",
    REPO_ROOT / "examples/configs/lsst_dp1_cmodel_mag_dered.yml",
]


class _ZeroDustQuery:
    def __call__(self, coords):
        return np.zeros(len(coords), dtype=float)


def _load_config(path):
    with path.open(encoding="utf-8") as stream:
        return yaml.safe_load(stream)


def _configured_input_file(config):
    input_cfg = config["input"]
    catalog_path = REPO_ROOT / input_cfg["catalog_path"]
    return next(catalog_path.glob(input_cfg["catalog_pattern"]))


def _comparison_mask(values, threshold, how, use_abs):
    compared = np.abs(values) if use_abs else values
    if how == "greater_or_equal":
        return compared >= threshold
    if how == "less_or_equal":
        return compared <= threshold
    raise ValueError("Comparison must be 'greater_or_equal' or 'less_or_equal'")


def _invalid_masks(values, errors, invalid_cfg):
    invalid_value = np.zeros(len(values), dtype=bool)
    invalid_error = np.zeros(len(errors), dtype=bool)

    if invalid_cfg.get("set_limit_for_col"):
        invalid_value |= _comparison_mask(
            values,
            invalid_cfg.get("limit_value_for_col", 999.0),
            invalid_cfg.get("limit_comparison_for_col", "greater_or_equal"),
            invalid_cfg.get("use_absolute_for_col_limits", True),
        )
    if invalid_cfg.get("set_limit_for_err"):
        invalid_error |= _comparison_mask(
            errors,
            invalid_cfg.get("limit_value_for_err", 999.0),
            invalid_cfg.get("limit_comparison_for_err", "greater_or_equal"),
            invalid_cfg.get("use_absolute_for_err_limits", True),
        )

    if invalid_cfg.get("is_nan_and_inf_invalid_for_col", True):
        invalid_value |= ~np.isfinite(values)
    if invalid_cfg.get("is_nan_and_inf_invalid_for_err", True):
        invalid_error |= ~np.isfinite(errors)

    if (
        invalid_cfg.get("cross_invalidate")
        and invalid_cfg.get("how_to_replace_col_values") == "all"
        and invalid_cfg.get("how_to_replace_err_values") == "all"
    ):
        invalid_error |= invalid_value
        invalid_value |= invalid_error

    return invalid_value, invalid_error


def _replace_invalid(values, mask, replacement):
    if replacement is None:
        return np.where(mask, np.nan, values)
    return np.where(mask, replacement, values)


def _expected_from_config(config, input_file, will_mag, will_dered_flux, will_dered_mag):
    input_cfg = config["input"]
    output_cfg = config["output"]
    photometry_cfg = config.get("photometry")
    invalid_cfg = resolve_invalid_handling(
        photometry_cfg.get("invalid_handling", {})
        if photometry_cfg is not None
        else config.get("invalid_handling", {})
    )

    expected = detect_and_read(input_file, input_cfg["user_selected_cols"])

    filter_cfg = input_cfg.get("filter", {})
    if filter_cfg.get("enabled"):
        column = filter_cfg["column"]
        expected = expected[expected[column] == filter_cfg["value"]]
        if filter_cfg.get("drop_column_after_filter"):
            expected = expected.drop(columns=[column])

    if photometry_cfg is not None:
        band_case = photometry_cfg.get("band_case", "lower_case")
        magnitude_cfg = photometry_cfg.get("magnitude")
        mag_offset = ab_magnitude_zero_point(magnitude_cfg) if magnitude_cfg is not None else None
        groups = []
        for measurement in photometry_cfg["measurements"]:
            source = measurement["input"]
            target = measurement["output"]
            flags = transformation_flags(source["type"], target["type"])
            groups.append(
                (
                    measurement.get("bands", photometry_cfg["bands"]),
                    source["value_pattern"],
                    source["error_pattern"],
                    target["value_pattern"],
                    target["error_pattern"],
                    flags,
                )
            )
        keep_inputs = photometry_cfg.get("keep_input_columns", False)
    else:
        band_case = input_cfg.get("band_case") or output_cfg.get("band_case", "lower_case")
        mag_offset = output_cfg.get("mag_offset")
        groups = [
            (
                input_cfg["selected_bands"],
                input_cfg["col_pattern"],
                input_cfg["err_pattern"],
                output_cfg["col_final_pattern"],
                output_cfg["err_final_pattern"],
                (will_mag, will_dered_flux, will_dered_mag),
            )
        ]
        keep_inputs = input_cfg.get("keep_input_columns_after_filters_or_transformations", False)

    input_cols_to_drop = []
    final_cols = set()

    for bands, col_pattern, err_pattern, final_pattern, final_err_pattern, flags in groups:
        group_will_mag, group_will_dered_flux, group_will_dered_mag = flags
        for band in bands:
            col_in = col_pattern.replace("BAND", band)
            err_in = err_pattern.replace("BAND", band)
            band_fmt = band.lower() if band_case == "lower_case" else band.upper()
            final_col = final_pattern.replace("BAND", band_fmt)
            final_err = final_err_pattern.replace("BAND", band_fmt)
            final_cols.update([final_col, final_err])

            values = expected[col_in].astype(float, copy=False).values
            errors = expected[err_in].astype(float, copy=False).values

            if group_will_dered_flux:
                values = values * 1.0
                errors = errors * 1.0

            if group_will_mag:
                flux = values
                flux_error = errors
                valid_flux = np.isfinite(flux) & (flux > 0.0)
                valid_error = valid_flux & np.isfinite(flux_error) & (flux_error >= 0.0)
                values = np.full(flux.shape, np.nan)
                errors = np.full(flux_error.shape, np.nan)
                values[valid_flux] = -2.5 * np.log10(flux[valid_flux]) + float(mag_offset)
                errors[valid_error] = flux_error[valid_error] / (flux[valid_error] * MAG_CONV)

            if group_will_dered_mag:
                values = values - 0.0

            if invalid_cfg.get("replace_invalid_values"):
                invalid_values, invalid_errors = _invalid_masks(values, errors, invalid_cfg)
                col_replacement = as_float_or_none(invalid_cfg.get("col_value_to_replace"))
                err_replacement = as_float_or_none(invalid_cfg.get("err_value_to_replace"))

                if invalid_cfg.get("how_to_replace_col_values") == "all":
                    values = _replace_invalid(values, invalid_values, col_replacement)
                elif invalid_cfg.get("how_to_replace_col_values") == "only_with_invalid_err":
                    values = _replace_invalid(values, invalid_errors, col_replacement)

                if invalid_cfg.get("how_to_replace_err_values") == "all":
                    errors = _replace_invalid(errors, invalid_errors, err_replacement)
                elif invalid_cfg.get("how_to_replace_err_values") == "only_with_invalid_col":
                    errors = _replace_invalid(errors, invalid_values, err_replacement)

            if invalid_cfg.get("round_col"):
                values = np.round(values, int(invalid_cfg.get("round_col_decimal_cases", 5)))
            if invalid_cfg.get("round_err"):
                errors = np.round(errors, int(invalid_cfg.get("round_err_decimal_cases", 5)))

            expected[final_col] = values
            expected[final_err] = errors
            input_cols_to_drop.extend([col_in, err_in])

    if not keep_inputs:
        drop_cols = [
            col for col in set(input_cols_to_drop) if col in expected.columns and col not in final_cols
        ]
        expected = expected.drop(columns=drop_cols)

    if input_cfg.get("is_id_in_index", False):
        expected = expected.reset_index()

    return expected


@pytest.mark.parametrize("config_path", EXAMPLE_CONFIGS)
def test_example_config_outputs_match_expected_rows(config_path, monkeypatch):
    """Run each example config against its sample input and compare the output row by row."""
    config = _load_config(config_path)
    input_cfg = config["input"]
    input_file = _configured_input_file(config)
    if "photometry" in config:
        _, flags = decide_photometry_suffix(config)
        will_mag, will_dered_flux, will_dered_mag = flags
    else:
        _, will_mag, will_dered_flux, will_dered_mag = decide_suffix_and_flags(
            input_cfg,
            input_cfg.get("compute_magnitude", True),
            input_cfg.get("compute_dereddening", True),
            config.get("dust", {}),
        )
    monkeypatch.setattr(processing, "get_dust_query", lambda dust_cfg: _ZeroDustQuery())

    actual = process_file_df(str(input_file), str(config_path), will_mag, will_dered_flux, will_dered_mag)
    expected = _expected_from_config(config, input_file, will_mag, will_dered_flux, will_dered_mag)

    assert_frame_equal(
        actual.reset_index(drop=True),
        expected.reset_index(drop=True),
        check_dtype=False,
        check_exact=False,
        rtol=1e-12,
        atol=1e-12,
    )
