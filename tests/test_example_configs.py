"""End-to-end checks for the shipped example configurations."""

from pathlib import Path

import numpy as np
import pytest
import yaml
from pandas.testing import assert_frame_equal

from science_catalogs import processing
from science_catalogs.processing import MAG_CONV, process_file_df
from science_catalogs.utils.config import as_float_or_none, decide_suffix_and_flags
from science_catalogs.utils.io_readers import detect_and_read

EXAMPLE_CONFIGS = [
    Path("examples/configs/des_dr2_mag_auto_dered.yml"),
    Path("examples/configs/lsst_dp02_cmodel_mag_dered.yml"),
    Path("examples/configs/lsst_dp1_cmodel_mag_dered.yml"),
]


class _ZeroDustQuery:
    def __call__(self, coords):
        return np.zeros(len(coords), dtype=float)


def _load_config(path):
    with path.open(encoding="utf-8") as stream:
        return yaml.safe_load(stream)


def _configured_input_file(config):
    input_cfg = config["input"]
    return next(Path(input_cfg["catalog_folder"]).glob(input_cfg["catalog_pattern"]))


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
    invalid_cfg = config.get("invalid_handling", {})

    expected = detect_and_read(input_file, input_cfg["user_selected_cols"])

    filter_cfg = input_cfg.get("filter", {})
    if filter_cfg.get("enabled"):
        column = filter_cfg["column"]
        expected = expected[expected[column] == filter_cfg["value"]]
        if filter_cfg.get("drop_column_after_filter"):
            expected = expected.drop(columns=[column])

    band_case = input_cfg.get("band_case") or output_cfg.get("band_case", "lower_case")
    input_cols_to_drop = []
    final_cols = set()

    for band in input_cfg["selected_bands"]:
        col_in = input_cfg["col_pattern"].replace("BAND", band)
        err_in = input_cfg["err_pattern"].replace("BAND", band)
        band_fmt = band.lower() if band_case == "lower_case" else band.upper()
        final_col = output_cfg["col_final_pattern"].replace("BAND", band_fmt)
        final_err = output_cfg["err_final_pattern"].replace("BAND", band_fmt)
        final_cols.update([final_col, final_err])

        values = expected[col_in].astype(float, copy=False).values
        errors = expected[err_in].astype(float, copy=False).values

        if will_dered_flux:
            # The test dust query returns E(B-V)=0, so this is intentionally a no-op.
            values = values * 1.0
            errors = errors * 1.0

        if will_mag:
            with np.errstate(divide="ignore", invalid="ignore"):
                flux = values
                values = -2.5 * np.log10(flux) + float(output_cfg["mag_offset"])
                errors = errors / (flux * MAG_CONV)

        if will_dered_mag:
            # The test dust query returns E(B-V)=0, so this is intentionally a no-op.
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

    if not input_cfg.get("keep_input_columns_after_filters_or_transformations", False):
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
    _, will_mag, will_dered_flux, will_dered_mag = decide_suffix_and_flags(
        input_cfg,
        input_cfg.get("compute_magnitude", True),
        input_cfg.get("compute_dereddening", True),
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
