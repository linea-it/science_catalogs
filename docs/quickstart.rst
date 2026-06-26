Quickstart
========================================================================================

This package is designed around two layers:

- ``prepare_catalog`` builds a lazy Dask representation of the processed catalog.
- ``build_catalog`` executes the full flow and writes the final artifact to disk.
- ``materialize_catalog`` and ``materialize_lsdb_catalog`` return ``data`` plus
  the written output ``path``.

Minimal interactive flow
----------------------------------------------------------------------------------------

.. code-block:: python

   from science_catalogs import (
       build_catalog,
       materialize_catalog,
       materialize_lsdb_catalog,
       prepare_catalog,
       write_catalog,
   )

   prepared = prepare_catalog("configs/catalog.yml")
   materialized = materialize_catalog(prepared, "./output")
   frame = materialized["data"]
   written = write_catalog(prepared, "./output")
   hats_catalog = materialize_lsdb_catalog(prepared, "./output")
   paths = build_catalog("configs/catalog.yml", output_dir="./output")

Example dataset
----------------------------------------------------------------------------------------

The repository includes a runnable example in ``examples/demo_catalog`` with
fictitious parquet inputs. To try it locally:

.. code-block:: console

   micromamba activate science_catalogs
   cd /path/to/science_catalogs
   python examples/demo_catalog/demo_all.py

That example validates:

- ``prepare_catalog``
- ``materialize_catalog``
- ``write_catalog``
- ``materialize_lsdb_catalog``
- ``open_lsdb_catalog``
- ``build_catalog`` for parquet and HATS output
