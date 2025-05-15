
# ECCO Dataset Production system-level tests

## Integration and system-level end-to-end test/demonstration examples:

The following examples can either be run as configured, or used as
starting points for other datatype production examples:

- SSH\_native\_latlon\_local: Generation of native and latlon format
  granules from local input, with output to local directory. See
  "Configuring ECCO Dataset Production to run locally" in ../README.md
  for description.

- SSH\_native\_latlon\_local\_remote: Generation of native and latlon
  format granules from remote input data, with output to local
  directory.  See "Configuring ECCO Dataset Production to run in
  local/remote mode" in ../README.md for description.

## Functionality

- edp\_aws\_s3\_sync: Tests various AWS S3 data sync operations:
  upload, download, and copy within AWS.

- edp\_create\_job\_task\_list: Tests creation of task lists based on
  high-level job definition files.

## Test data:

- ./data/config/: ECCO Dataset Production configuration file examples.

- ./data/ecco_grids/: ECCO grid definitions and download script.

- ./data/ecco\_mapping\_factors: ECCO mapping factors (interpolation
  to latlon grids) and download script.

- ./data/ecco\_results/: Sample ECCO MDS results data.  See also
  download\_selected\_data.sh helper script that downloads selected
  MDS files used in edp\_create\_job\_task\_list (above).

## Remarks

- Tests that rely on AWS S3 access require an AWS account with login
  privileges.
