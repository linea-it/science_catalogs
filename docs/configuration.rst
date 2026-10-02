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
