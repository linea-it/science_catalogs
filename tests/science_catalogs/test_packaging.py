import science_catalogs


def test_version():
    """Check to see that we can get the package version"""
    assert science_catalogs.__version__ is not None
