"""File readers and format detection."""

from collections.abc import Iterable
from pathlib import Path

import numpy as np
import pandas as pd
from astropy.io import fits


def _as_native_endian(values):
    """Return an array that pandas can handle on the current platform."""
    array = np.asarray(values)
    if array.dtype.byteorder in {"=", "|"}:
        return array
    return array.astype(array.dtype.newbyteorder("="), copy=False)


def read_fits_to_df_no_fix(filename, columns=None):
    """Read a FITS table into a dataframe."""
    with fits.open(filename, memmap=True) as hdul:
        data = hdul[1].data
        selected_columns = list(columns) if columns is not None and len(columns) > 0 else data.names
        df = pd.DataFrame({column: _as_native_endian(data[column]) for column in selected_columns})
    return df


def detect_and_read(path: str, columns: Iterable[str] | None):
    """Read CSV/Parquet/FITS files based on their extension."""
    suffix = Path(path).suffix.lower()
    columns = list(columns) if columns else None

    if suffix == ".csv":
        return pd.read_csv(path, usecols=columns)
    if suffix in {".parquet", ".pq", ".parq"}:
        try:
            return pd.read_parquet(path, columns=columns)
        except Exception:
            return pd.read_csv(path, usecols=columns)
    if suffix in {".fits", ".fit"}:
        return read_fits_to_df_no_fix(path, columns=columns)

    raise ValueError(f"Unsupported input file extension '{suffix}' for {path}")


__all__ = ["detect_and_read", "read_fits_to_df_no_fix"]
