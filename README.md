# science_catalogs

`science_catalogs` is a reusable Python library for building science-ready
catalogs with LSDB-oriented workflows. The package focuses on the reusable core
of the processing stack:

- column selection
- column transformations
- row filtering
- output materialization to memory, partitioned files, or HATS catalogs

The package is published on PyPI as `science-catalogs` and imported in Python as
`science_catalogs`.

## Installation

```bash
pip install science-catalogs
```

For local development:

```bash
pip install -e '.[dev]'
pre-commit install --hook-type pre-commit --hook-type pre-push
```

The commit hook runs fast formatting, lint, and configuration checks. The pre-push
hook runs the unit-test suite without coverage overhead. Run every commit-stage check
on demand with `pre-commit run --all-files`; run the documentation build explicitly
with `pre-commit run --hook-stage manual sphinx-docs-build`.

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

### Command-line interface

Installing the package provides the `science-catalogs` command. The command executes
the complete pipeline described by a YAML file and writes the resulting artifact:

```bash
science-catalogs CONFIG_PATH [OUTPUT_DIR] [--output-format {parquet,hats}]
```

Use the output configuration from the YAML:

```bash
science-catalogs examples/configs/lsst_dp1_cmodel_mag_dered.yml
```

Write to an explicit directory:

```bash
science-catalogs examples/configs/lsst_dp1_cmodel_mag_dered.yml ./output/dp1
```

Override the configured format and build a HATS collection:

```bash
science-catalogs examples/configs/lsst_dp1_to_hats.yml ./output/dp1_hats \
  --output-format hats
```

`OUTPUT_DIR` takes precedence over `output.base_path`. The CLI requires one of these
destinations to be explicit, preventing large catalogs from accidentally filling the
current filesystem. The Python API retains `./data` as its fallback. Without
`--output-format`, the command uses `output.save_as` from the YAML, defaulting to
Parquet. The CLI format override accepts `parquet` and `hats`; CSV and HDF5 remain
available through `output.save_as` in the YAML.

Run `science-catalogs --help` for the complete command synopsis. Configuration is
validated before execution, so unknown keys and scientifically inconsistent
photometric transformations fail before catalog materialization begins.

A YAML file may also contain a `batch` section with shared `defaults` and a list
of named `catalogs`. Batch entries run one at a time on the same Dask executor and
the command reports the artifact produced for each catalog. See
`docs/configuration.rst` and `examples/configs/dp2/all/lsst_dp2_to_hats.yml` for
the full layout. Standalone configurations for each DP2 catalog are available in
`examples/configs/dp2/individual`.

### Python API

Prepare a catalog from a catalog-processing YAML configuration:

```python
from science_catalogs import prepare_catalog

prepared = prepare_catalog("configs/catalog.yml")
```

Materialize the processed data in memory and keep track of the written output
paths:

```python
from science_catalogs import materialize_catalog

result = materialize_catalog(prepared, "./output")
frame = result["data"]
paths = result["path"]
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

result = materialize_lsdb_catalog(prepared, "./output")
catalog = result["data"]
hats_path = result["path"]
```

Execute the full flow from configuration and persist parquet output in one call:

```python
from science_catalogs import build_catalog

paths = build_catalog("configs/catalog.yml", output_dir="./output")
```

Or force a HATS artifact from the same flow:

```python
hats_path = build_catalog(
    "configs/catalog.yml",
    output_dir="./output",
    output_format="hats",
)
```

If you already have a HATS catalog on disk, you can open it directly:

```python
from science_catalogs import open_lsdb_catalog

catalog = open_lsdb_catalog("./output/my_catalog")
```
