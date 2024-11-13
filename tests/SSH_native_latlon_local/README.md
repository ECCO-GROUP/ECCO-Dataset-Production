
Complete example illustrating generation of native and latlon format
granules from local input, with output to local directory.

See "Configuring ECCO Dataset Production to run locally" in top-level
../../README.md for details.

Steps include:

- Task list generation:
  `$ ./edp_create_job_task_list_SSH_native_latlon_mon_mean.sh`
  Output file `SSH_native_latlon_mon_mean_tasks.json` can be compared
  with saved results in `SSH_native_latlon_mon_mean_tasks.json.sav`
  
- Granule generation based on generated task list:
  `$ ./edp_generate_dataproducts_SSH_native_latlon_mon_mean.sh`
  Resulting granules in `./V4r4` can be compared with saved results in
  `./V4r4_sav`
