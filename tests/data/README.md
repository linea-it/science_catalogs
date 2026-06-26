# Test Data Samples

This directory contains small catalog samples used by tests and local
development. The files are derived from larger source catalogs and reduced to
the columns required by the example configurations.

## Files

- `des_dr2_sample_1000.fits`
  - Source dataset: DES DR2 coadd catalog
  - Rows: 1000
  - Columns: only the columns referenced by
    `examples/configs/des_dr2_mag_auto_dered.yml`
  - Structure: FITS binary table, with `COADD_OBJECT_ID` as a regular column

- `lsst_dp02_scrambled_sample_1000.parq`
  - Source dataset: LSST DP0.2/DP02 object catalog
  - Rows: 1000
  - Columns: only the columns referenced by
    `examples/configs/lsst_dp02_cmodel_mag_dered.yml`
  - Structure: parquet file with `objectId` preserved as the dataframe index,
    matching the original DP02 object-table layout
  - Privacy handling: column values were independently permuted, including the
    `objectId` index, coordinates, fluxes, flux errors, and metadata columns

- `lsst_dp1_scrambled_sample_1000.parq`
  - Source dataset: LSST DP1 object catalog
  - Rows: 1000
  - Columns: only the columns referenced by
    `examples/configs/lsst_dp1_cmodel_mag_dered.yml`
  - Structure: parquet file with `objectId` as a regular column, matching the
    original DP1 object-catalog layout
  - Privacy handling: column values were independently permuted, including
    `objectId`, coordinates, fluxes, flux errors, and metadata columns

## Notes

The LSST samples are not row-faithful object records. Their columns keep the
original dtypes and value distributions needed for pipeline tests, but the
independent permutations break the association between object identifiers,
sky coordinates, and photometry.
