"""Unit tests for pure configuration helpers."""

from science_catalogs.utils.config import decide_suffix_and_flags


def test_decide_suffix_and_flags_for_flux_with_dered_and_mag():
    suffix, will_mag, will_dered_flux, will_dered_mag = decide_suffix_and_flags(
        {
            "which_release": "LSST_DP02",
            "input_col_type": "flux",
            "input_col_model": "cmodel",
            "use_dustmap": "sfd",
        },
        compute_mag=True,
        compute_dered=True,
    )

    assert suffix == "_LSST_DP02_mag_cmodel_sfd"
    assert will_mag is True
    assert will_dered_flux is True
    assert will_dered_mag is False


def test_decide_suffix_and_flags_for_mag_without_dered():
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
