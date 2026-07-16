Quickstart
========================================================================================

This package is designed around two layers:

- ``prepare_catalog`` builds a lazy Dask representation of the processed catalog.
- ``build_catalog`` executes the full flow and writes the final artifact to disk.
- ``materialize_catalog`` and ``materialize_lsdb_catalog`` return ``data`` plus
  the written output ``path``.

Use ``materialize_catalog`` only when the final dataframe is expected to fit in
memory. For larger catalogs, prefer ``build_catalog``, ``write_catalog``, or
``materialize_lsdb_catalog`` so the workflow stays distributed and disk-backed.

Input modes
----------------------------------------------------------------------------------------

``prepare_catalog`` can start from two kinds of inputs:

- a regular file or directory
- an existing HATS catalog opened through LSDB

For file-based inputs, use a single path:

.. code-block:: yaml

   input:
     catalog_path: /path/to/files_or_file
     catalog_pattern: "*.parquet"

If ``catalog_path`` is a single file, it is used directly. If it is a regular
directory, ``catalog_pattern`` is used to find the files to process.

For HATS-based inputs, point ``catalog_path`` to the existing catalog
directory:

.. code-block:: yaml

   input:
     catalog_path: /path/to/existing_hats_catalog

In both cases, ``user_selected_cols`` still defines the columns projected into
the processing step.

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
