science_catalogs
========================================================================================

``science_catalogs`` is a reusable toolkit for preparing science-ready catalogs
with Dask, LSDB, and HATS.

``prepare_catalog`` accepts a single input path and detects whether it points
to a regular file collection or an existing HATS catalog.

Beta public API
----------------------------------------------------------------------------------------

The beta API exposed for downstream workflows is:

- ``prepare_catalog``
- ``materialize_catalog``
- ``write_catalog``
- ``materialize_lsdb_catalog``
- ``open_lsdb_catalog``
- ``build_catalog``

The package is published on PyPI as ``science-catalogs`` and imported as
``science_catalogs``.

Installation
----------------------------------------------------------------------------------------

.. code-block:: console

   pip install science-catalogs

For development:

.. code-block:: console

   pip install -e '.[dev]'
   pre-commit install

Quick example
----------------------------------------------------------------------------------------

.. code-block:: python

   from science_catalogs import build_catalog, prepare_catalog, materialize_catalog

   prepared = prepare_catalog("configs/catalog.yml")
   materialized = materialize_catalog(prepared, "./output")
   frame = materialized["data"]

   paths = build_catalog("configs/catalog.yml", output_dir="./output")
   print(paths)

Interactive demo
----------------------------------------------------------------------------------------

A runnable example with fictitious parquet inputs lives in
``examples/demo_catalog``. It includes:

- sample parquet files
- YAML configs for parquet and HATS output
- documentation for using an existing HATS catalog as input
- an interactive walkthrough
- a smoke test script

See :doc:`quickstart` for the example flow.

.. toctree::
   :hidden:

   Home page <self>
   Quickstart <quickstart>
   API Reference <autoapi/index>
   Notebooks <notebooks>
