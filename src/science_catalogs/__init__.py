"""Reusable tools for science catalog preparation."""

try:
    from science_catalogs._version import version as __version__
except ImportError:  # pragma: no cover
    __version__ = "0+unknown"


def build_catalog(*args, **kwargs):
    """Lazily execute the full catalog-building flow to disk."""
    from science_catalogs.catalog import build_catalog as _build_catalog

    return _build_catalog(*args, **kwargs)


def prepare_catalog(*args, **kwargs):
    """Lazily prepare the processed catalog without workflow-specific artifacts."""
    from science_catalogs.catalog import prepare_catalog as _prepare_catalog

    return _prepare_catalog(*args, **kwargs)


def materialize_catalog(*args, **kwargs):
    """Lazily compute the processed catalog and return data plus written paths."""
    from science_catalogs.catalog import materialize_catalog as _materialize_catalog

    return _materialize_catalog(*args, **kwargs)


def materialize_lsdb_catalog(*args, **kwargs):
    """Lazily write HATS output and return an LSDB Catalog plus its path."""
    from science_catalogs.catalog import (
        materialize_lsdb_catalog as _materialize_lsdb_catalog,
    )

    return _materialize_lsdb_catalog(*args, **kwargs)


def open_lsdb_catalog(*args, **kwargs):
    """Lazily open an LSDB Catalog from a HATS path on disk."""
    from science_catalogs.catalog import open_lsdb_catalog as _open_lsdb_catalog

    return _open_lsdb_catalog(*args, **kwargs)


def write_catalog(*args, **kwargs):
    """Lazily write the processed catalog to disk."""
    from science_catalogs.catalog import write_catalog as _write_catalog

    return _write_catalog(*args, **kwargs)


__all__ = [
    "build_catalog",
    "prepare_catalog",
    "materialize_catalog",
    "materialize_lsdb_catalog",
    "open_lsdb_catalog",
    "write_catalog",
    "__version__",
]
