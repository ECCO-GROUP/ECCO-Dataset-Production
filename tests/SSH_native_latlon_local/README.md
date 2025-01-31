
Complete example illustrating generation of native and latlon format
granules from local input, with output to local directory.

See "Configuring ECCO Dataset Production to run locally" in top-level
../../README.md for details.

If ECCO-Dataset-Production has not been cloned using the
`--recurse-submodules` option, the ../ECCO-v4-Configurations
submodule, referred to here, can be init/updated using:

    git submodule init
    git submodule update

Steps include:

- Task list generation:
  `$ ./edp_create_job_task_list_SSH_native_latlon_mon_mean.sh <ver>`,
  where `<ver>` is either 'V4r4' or 'V4r5'.  Output file(s)
  `SSH_native_latlon_mon_mean_tasks_<ver>.json` can be compared with
  verification results saved as
  `SSH_native_latlon_mon_mean_tasks_<ver>.json.verif`.
  
- Granule generation based on generated task list:
  `$ ./edp_generate_dataproducts_SSH_native_latlon_mon_mean.sh <ver>`,
  where `<ver>` is either 'V4r4' or 'V4r5'.  Resulting granules in
  `./<ver>` can be compared with verification results in
  `<ver>_verif.tgz`.
