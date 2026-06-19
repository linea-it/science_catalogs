# science_catalogs

`science_catalogs` is a reusable Python library for building science-ready catalogs
with LSDB-oriented workflows. The package focuses on the reusable core of the
processing stack:

- column selection
- column transformations
- row filtering
- output materialization to memory, partitioned files, or HATS catalogs

Report generation, pipeline orchestration, and product declaration stay outside
this repository.

The Python package is exposed as `science_catalogs`.

## Installation

```bash
pip install -e '.[dev]'
```

## Main API

```python
from science_catalogs import (
    materialize_catalog,
    materialize_lsdb_catalog,
    open_lsdb_catalog,
    prepare_catalog,
    write_catalog,
)
```

## Usage

Prepare a catalog from a pipeline-style YAML configuration:

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

If you already have a HATS catalog on disk, you can open it directly:

```python
from science_catalogs import open_lsdb_catalog

catalog = open_lsdb_catalog("./output/my_catalog")
```

## Scope

This package is intentionally limited to reusable catalog preparation logic.
Pipeline-only concerns such as CLI execution, report generation, and workflow
product registration should live in the pipeline repository that consumes this
library.
