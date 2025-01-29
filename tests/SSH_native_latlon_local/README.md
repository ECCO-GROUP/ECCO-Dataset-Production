
Complete example illustrating generation of native and latlon format
granules from local input, with output to local directory.

See "Configuring ECCO Dataset Production to run locally" in top-level
../../README.md for details.

If ECCO-Dataset-Production has not been cloned using the
`--recurse-submodules` option, ../ECCO-v4-Configurations, referred to
in this test case, can be cloned using:

    git submodule init
    git submodule update

Steps include:

- Task list generation:
  `$ ./edp_create_job_task_list_SSH_native_latlon_mon_mean.sh <ver>`,
  where `<ver>` is either 'V4r4' or 'V4r5'.  Output file(s)
  `SSH_native_latlon_mon_mean_tasks_<ver>.json` can be compared with
  saved results in `SSH_native_latlon_mon_mean_tasks_<ver>.json.sav`.
  
- Granule generation based on generated task list:
  `$ ./edp_generate_dataproducts_SSH_native_latlon_mon_mean.sh <ver>`,
  where `<ver>` is either 'V4r4' or 'V4r5'.  Resulting granules in
  `./<ver>` can be compared with saved results in `./<ver>_sav`.
