
Complete example illustrating generation of native and latlon format
granules from remote input, with output to local directory.

See "Configuring ECCO Dataset Production to run in local/remote mode"
in top-level ../../README.md for details.

If ECCO-Dataset-Production has not been cloned using the
`--recurse-submodules` option, the ../ECCO-v4-Configurations
submodule, referred to here, can be init/updated using:

    git submodule init
    git submodule update

Also, note that certain AWS S3 bucket names have been assumed in the
submittal scripts described below, and may need to be changed
depending on your actual AWS S3 layout. Since the resulting granules
produced here will be the same as the results in
`../SSH_native_latlon_local`, that example's input data could also be
used to populate some test AWS S3 buckets, with corresponding changes
to the submittal scripts here.

- Task list generation:
  `$ ./edp_create_job_task_list_SSH_native_latlon_mon_mean.sh <ver>`,
  where `<ver>` is either 'V4r4' or 'V4r5'.  Output file(s)
  `SSH_native_latlon_mon_mean_tasks_<ver>.json` can be compared with
  verification results saved as
  `SSH_native_latlon_mon_mean_tasks_<ver>.json.verif`.
  
- Granule generation based on generated task list:
  `$ ./edp_generate_dataproducts_SSH_native_latlon_mon_mean.sh <ver>`
  where `<ver>` is either 'V4r4' or 'V4r5'.  Note that the KEYGEN and
  PROFILE variables can be removed if running outside of the JPL
  federated login environment.  Since the resulting granules will be
  identical, see verification results in
  `../SSH_native_latlon_local/<ver>_verif.tgz`.
