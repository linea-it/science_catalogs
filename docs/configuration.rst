Configuration reference
========================================================================================

Configuration is validated before a catalog is prepared. Unknown keys are rejected so
that spelling mistakes cannot silently change a scientific product. Sections that do
not apply to a run may be omitted; defaults are applied by the library.

Canonical layout
----------------------------------------------------------------------------------------

``metadata`` records provenance such as ``release``. ``input`` selects the source and
columns. ``photometry`` contains scientific transformations. ``output`` and
``collection`` control materialization, while ``execution`` describes the Dask runtime.

.. code-block:: yaml

   metadata:
     release: LSST_DP2

   input:
     catalog_path: /data/object
     catalog_pattern: "*.parq"
     files_per_partition: 1
     parquet_metadata_workers: 8
     user_selected_cols: [objectId, coord_ra, coord_dec, g_psfFlux, g_psfFluxErr]
     ra_col: coord_ra
     dec_col: coord_dec

   photometry:
     bands: [g]
     keep_input_columns: false
     magnitude:
       system: AB
       flux_unit: nJy
     measurements:
       - name: psf
         input:
           type: flux
           value_pattern: BAND_psfFlux
           error_pattern: BAND_psfFluxErr
         output:
           type: mag
           value_pattern: BAND_psfMag
           error_pattern: BAND_psfMagErr

   output:
     save_as: parquet

   execution:
     executor: local
     worker_wait_timeout: 900
     local:
       n_workers: 3
       threads_per_worker: 2

Invalid values
----------------------------------------------------------------------------------------

Value and uncertainty policies are independent. Each can flag non-finite values, apply
an inclusive limit, replace invalid entries, and round the result. ``when: invalid``
uses the channel's own invalid mask; ``when: paired_invalid`` uses the mask from the
other channel. A replacement value of ``null`` produces ``NaN``.

.. code-block:: yaml

   photometry:
     invalid_handling:
       cross_invalidate: false
       value:
         nonfinite_is_invalid: true
         limit:
           value: 99.0
           comparison: greater_or_equal
           use_absolute: true
         replacement:
           value: 99.0
           when: invalid
         round_decimals: 5
       error:
         nonfinite_is_invalid: true
         limit:
           value: 99.0
           comparison: greater_or_equal
           use_absolute: true
         replacement:
           value: 99.0
           when: paired_invalid

Omit ``limit``, ``replacement``, or ``round_decimals`` to disable only that operation.
Set ``invalid_handling.enabled: false`` to retain the policy while disabling all
replacement; detection settings remain available for a later run.

Compatibility aliases
----------------------------------------------------------------------------------------

Older configurations remain readable and emit ``ConfigDeprecationWarning``. The main
aliases are ``input.catalog_folder`` to ``input.catalog_path``, ``input.which_release``
to ``metadata.release``, ``cluster`` to ``execution``, and the old flat invalid-handling
keys to the nested value/error policy. New configurations should use only the canonical
layout.

HATS partitioning
----------------------------------------------------------------------------------------

For HATS output, ``collection.catalog.pixel_threshold`` controls the target maximum
number of rows in a spatial partition, while
``collection.catalog.highest_healpix_order`` controls how far dense regions may be
subdivided. When omitted, the installed ``hats-import`` defaults are used.

``output.target_rows_per_part`` controls the approximate number of rows in each
temporary input file passed to ``hats-import``. Increasing it reduces the number of
Dask mapping and splitting tasks; it does not define the final spatial partitions.
HATS still assigns every row to its HEALPix partition from the configured RA and Dec
columns. Temporary files are normally removed after the HATS client has stopped all
of its work. If that client cannot be stopped safely, the directory is preserved and
logged rather than being removed while workers may still be reading it.

For directories containing many small files, ``input.files_per_partition`` groups
that many files into one Dask partition. The default is 1, which avoids unexpectedly
combining large or wide files in memory. Increase it explicitly for datasets made of
many small files. When exact staging row targets are requested for Parquet inputs, row
counts are read from file footers instead of scanning the dataset, provided no row
filter or initial cut is enabled.
``input.parquet_metadata_workers`` controls the bounded footer-reading thread pool
(default 8).

For SLURM, ``execution.slurm.death_timeout`` controls how long a worker may take to
connect to the scheduler (default 600 seconds). ``execution.worker_wait_timeout``
optionally bounds how long the client waits for every requested worker to register;
without it, the client waits indefinitely.

.. code-block:: yaml

   output:
     save_as: hats
     hats_artifact_name: object_collection
     target_rows_per_part: 1000000

   collection:
     catalog:
       artifact_name: object
       pixel_threshold: 1000000
       highest_healpix_order: 12
     margin:
       threshold_arcsec: 5.0

Batch layout
----------------------------------------------------------------------------------------

A batch configuration runs catalogs sequentially on one shared Dask executor. Existing
single-catalog YAML files keep the canonical layout above and require no changes.
Defaults are recursively merged into each named catalog; catalog values take precedence.
This includes ``output.base_path``, so catalogs in one batch may write to different
storage roots while still sharing an executor.

.. code-block:: yaml

   batch:
     on_existing: error
     execution:
       executor: local
       local:
         n_workers: 3
         threads_per_worker: 2
     defaults:
       metadata:
         release: LSST_DP2
       photometry:
         enabled: false
       output:
         base_path: ./output
         save_as: hats
       collection:
         margin:
           threshold_arcsec: 5.0
     catalogs:
       - name: object
         input:
           catalog_path: ./input/object
           catalog_pattern: "*.parq"
           ra_col: coord_ra
           dec_col: coord_dec
         output:
           hats_artifact_name: object_collection
         collection:
           catalog:
             artifact_name: object
       - name: source
         input:
           catalog_path: ./input/source
           catalog_pattern: "**/*.parq"
           ra_col: coord_ra
           dec_col: coord_dec
         output:
           hats_artifact_name: source_collection
         collection:
           catalog:
             artifact_name: source

``execution`` belongs to the batch because all entries share one cluster. HATS artifact
names and catalog names must be unique. The ``on_existing`` policy is ``error`` by
default for batches and may be set to ``reuse`` or ``replace``. Single-catalog configs
retain the historical ``reuse`` default. Recursive patterns such as ``**/*.parq`` allow
one logical catalog to include band directories while preserving the band column.
Non-HATS batch outputs are placed in a subdirectory named after each catalog so that
partition filenames from different entries cannot collide.
