Quickstart
========================================================================================

This package is designed around two layers:

- ``prepare_catalog`` builds a lazy Dask representation of the processed catalog.
- ``build_catalog`` executes the full flow and returns data in memory, on disk,
  or reopened as LSDB.

Minimal interactive flow
----------------------------------------------------------------------------------------

.. code-block:: python

   from science_catalogs import (
       build_catalog,
       materialize_catalog,
       prepare_catalog,
       write_catalog,
   )

   prepared = prepare_catalog("configs/catalog.yml")
   frame = materialize_catalog(prepared)
   written = write_catalog(prepared, "./output")
   result = build_catalog("configs/catalog.yml", output_mode="memory")

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
- ``build_catalog`` in ``memory``, ``disk``, and ``lsdb`` modes
