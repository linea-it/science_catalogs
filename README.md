# science_catalogs

`science_catalogs` is a reusable Python library for building science-ready catalogs
with LSDB-oriented workflows. The package focuses on the reusable core of the
processing stack:

- column selection
- column transformations
- row filtering
- output materialization to memory, partitioned files, or HATS catalogs

Report generation, workflow orchestration, and product declaration stay outside
this repository.

The package is published on PyPI as `science-catalogs` and imported in Python as
`science_catalogs`.

## Installation

```bash
pip install science-catalogs
```

For local development:

```bash
pip install -e '.[dev]'
```

Or, if you prefer a requirements file for a full developer environment including
build and PyPI publication tools:

```bash
pip install -r requirements-dev.txt
```

## Main API

```python
from science_catalogs import (
    build_catalog,
    materialize_catalog,
    materialize_lsdb_catalog,
    open_lsdb_catalog,
    prepare_catalog,
    write_catalog,
)
```

## Beta API

The beta public API is:

- `prepare_catalog`
- `materialize_catalog`
- `write_catalog`
- `materialize_lsdb_catalog`
- `open_lsdb_catalog`
- `build_catalog`

Legacy names based on `pipeline` are not part of the beta API.

## Usage

Prepare a catalog from a catalog-processing YAML configuration:

```python
from science_catalogs import prepare_catalog

prepared = prepare_catalog("configs/catalog.yml")
```

Materialize the processed data in memory as a pandas dataframe:

```python
from science_catalogs import materialize_catalog

frame = materialize_catalog(prepared)
```

Write the result to disk. The write mode follows the output configuration,
including HATS when `output.save_as: hats` is selected:

```python
from science_catalogs import write_catalog

written_paths = write_catalog(prepared, "./output")
```

Open the final result as an LSDB catalog after writing HATS output:

```python
from science_catalogs import materialize_lsdb_catalog

catalog = materialize_lsdb_catalog(prepared, "./output")
```

Execute the full flow from configuration in one call:

```python
from science_catalogs import build_catalog

result = build_catalog("configs/catalog.yml", output_mode="memory")
frame = result.data
```

If you already have a HATS catalog on disk, you can open it directly:

```python
from science_catalogs import open_lsdb_catalog

catalog = open_lsdb_catalog("./output/my_catalog")
```

## Scope

This package is intentionally limited to reusable catalog preparation logic.
Application-level concerns such as CLI execution, report generation, and
workflow product registration should live in the downstream workflow repository
that consumes this library.
