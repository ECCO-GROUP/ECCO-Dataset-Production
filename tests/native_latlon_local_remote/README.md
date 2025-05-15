
Complete example illustrating generation of select native and latlon
format granules from remote input, with output to local directory.

See "Configuring ECCO Dataset Production to run in local/remote mode"
in top-level ../../README.md for details.

If ECCO-Dataset-Production has not been cloned using the
`--recurse-submodules` option, the ../ECCO-v4-Configurations
submodule, referred to here, can be init/updated using:

    git submodule init
    git submodule update

Also, note that certain AWS S3 bucket names have been assumed in the
submittal scripts described below, and will likely need to be changed
depending on your actual AWS S3 layout. Since the resulting granules
produced here will be the same as the results in
`../native_latlon_local`, that example's input data could also be used
to populate some test AWS S3 buckets, with corresponding changes to
the submittal scripts here.

- Task list generation:
  `$ ./edp_create_job_task_list.sh <ver>`, where `<ver>` is either
  'V4r4' or 'V4r5'.  Output file(s) `tasks_<ver>.json` can be compared
  with verification results saved as `tasks_<ver>.json.verif`.
  
- Granule generation based on generated task list:
  `$ ./edp_generate_dataproducts.sh <ver>` where `<ver>` is either
  'V4r4' or 'V4r5'.  Note that the KEYGEN and PROFILE variables can be
  removed if not running within an AWS IAM Identity Center (SSO)
  environment.  Results can be compared with local results generated
  in `../native_latlon_local`.
