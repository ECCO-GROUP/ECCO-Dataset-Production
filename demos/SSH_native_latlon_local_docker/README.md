
Variation on `../SSH_native_latlon_local/\`
`edp_create_job_task_list_SSH_native_latlon_mon_mean.sh` that
demonstrates running in (local) Docker container mode.

Steps include:

- Task list generation:

  `$ edp_create_job_task_list_SSH_native_latlon_mon_mean.sh <ver>`
  where `<ver>` is either 'V4r4' or 'V4r5' to locally generate a task
  list (i.e., not from a dockerized application) that references
  container mount points. Though derived from
  `../SSH_native_latlon_local` note that the modifications here also
  create local `./input` and `./output` directories that can
  conveniently be bind-mounted to a docker container.

- Docker-based granule generation using generated task list:

  `$ docker_run_edp_generate_dataproducts_SSH_native_latlon_mon_mean.sh`
  spins up a container that mounts the above-generated i/o directories
  and communicates the task list via the TASKLIST environment
  variable. Generated granules are written to `./output`, per
  instructions defined in the task list descriptor.

Prerequisites:

Docker build of ecco\_dataset\_production base image and
edp\_generate\_dataproducts "executable". Ref.
`../../containers/docker/README.md`.
