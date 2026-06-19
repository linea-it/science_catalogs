"""Reusable tools for science catalog preparation."""

try:
    from science_catalogs._version import version as __version__
except ImportError:  # pragma: no cover
    __version__ = "0+unknown"


def run_pipeline(*args, **kwargs):
    """Compatibility shim for pipeline consumers kept during extraction."""
    from science_catalogs.pipeline import run_pipeline as _run_pipeline

    return _run_pipeline(*args, **kwargs)


def prepare_catalog(*args, **kwargs):
    """Lazily prepare the processed catalog without pipeline artifacts."""
    from science_catalogs.pipeline import prepare_catalog as _prepare_catalog

    return _prepare_catalog(*args, **kwargs)


def materialize_catalog(*args, **kwargs):
    """Lazily compute the processed catalog into memory."""
    from science_catalogs.pipeline import materialize_catalog as _materialize_catalog

    return _materialize_catalog(*args, **kwargs)


def materialize_lsdb_catalog(*args, **kwargs):
    """Lazily open the final processed catalog as an LSDB Catalog."""
    from science_catalogs.pipeline import (
        materialize_lsdb_catalog as _materialize_lsdb_catalog,
    )

    return _materialize_lsdb_catalog(*args, **kwargs)


def open_lsdb_catalog(*args, **kwargs):
    """Lazily open an LSDB Catalog from a HATS path on disk."""
    from science_catalogs.pipeline import open_lsdb_catalog as _open_lsdb_catalog

    return _open_lsdb_catalog(*args, **kwargs)


def write_catalog(*args, **kwargs):
    """Lazily write the processed catalog to disk."""
    from science_catalogs.pipeline import write_catalog as _write_catalog

    return _write_catalog(*args, **kwargs)


__all__ = [
    "prepare_catalog",
    "materialize_catalog",
    "materialize_lsdb_catalog",
    "open_lsdb_catalog",
    "write_catalog",
    "__version__",
]
