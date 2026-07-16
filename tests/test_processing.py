"""Tests for per-file catalog processing."""

import pandas as pd
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
