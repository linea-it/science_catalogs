"""Tests for scalable Dask partition sizing."""

import dask.dataframe as dd
import pandas as pd

from science_catalogs.utils.partitioning import reorder_and_rechunk


def test_rechunk_uses_supplied_partition_counts_without_length_scan(monkeypatch):
    """Use Parquet footer counts instead of computing every source partition length."""
    source = dd.from_pandas(pd.DataFrame({"value": range(10)}), npartitions=2)

    original_map_partitions = source.map_partitions

    def reject_length_scan(func, *args, **kwargs):
        if func is len:
            raise AssertionError("partition lengths should come from metadata")
        return original_map_partitions(func, *args, **kwargs)

    monkeypatch.setattr(source, "map_partitions", reject_length_scan)

    result = reorder_and_rechunk(
        source,
        {"target_rows_per_part": 3},
        partition_row_counts=[5, 5],
    )

    assert result.npartitions == 4
    assert result.compute()["value"].tolist() == list(range(10))


def test_rechunk_rejects_mismatched_partition_counts():
    """Reject stale metadata that cannot describe the current Dask graph."""
    source = dd.from_pandas(pd.DataFrame({"value": range(4)}), npartitions=2)

    try:
        reorder_and_rechunk(
            source,
            {"target_rows_per_part": 2},
            partition_row_counts=[4],
        )
    except ValueError as exc:
        assert "must match" in str(exc)
    else:
        raise AssertionError("Expected mismatched partition counts to fail")
