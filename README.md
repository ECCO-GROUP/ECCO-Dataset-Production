
# ECCO Dataset Production

Tools and utilities, as stated in the initial release, "to turn raw
model output into glorious self-describing granules for widespread
distribution".

Original release by Duncan Bark, in collaboration with Ian Fenty,
Summer 2022, subsequently restructured and updated by Greg Moore and
Ian Fenty.


## Introduction

As described [elsewhere](https://ecco-group.org/home.htm), ECCO
results are computed with respect to a discretized
[latitude-longitude-cap](https://ecco-group.org/docs/v4r4_user_guide.pdf)
grid, for both 2D and 3D results sets, at regular time intervals, and
ouput in [MITgcm file
formats](https://mitgcm.readthedocs.io/en/latest/).

For general distribution however (e.g., via
[PO.DAAC](https://podaac.jpl.nasa.gov/)), it's useful to convert these
raw output files to more intuitive day/date-stamped snapshot, daily
mean, and monthly mean files, with appropriate metadata, in both
native and latitude-longitude grid formats. Though the operations
themselves are straightforward, the processing load isn't;
approximately 20 terabytes of output for the full ECCO V4r4
collection, with over 200 terabytes expected for ECCO V4r5. Further
exponential growth is anticipated as both grid resolution and time
extents continue to increase.  Computational and storage costs thus
call for taking advantage of distributed and cloud-based workflows, a
central theme of ECCO Dataset Production.

Though ECCO Dataset Production is capable of operating on extremely
large data collections, it applies just as well to smaller, limited
production data sets; in short, anything for which wider distribution
in easier-to-process formats is desired.


## Installation and Configuration

ECCO Dataset Production installs as a standard Python package, and
defines several command-line entry points so that key functional
components may also be invoked at the shell script level.  Docker and
Singularity files have been included in order to support cloud- and
high performance compute-based based workflows.

The full ECCO Dataset Production pipeline also relies on the presence
of ECCO grid, mapping factors, and configuration (meta)data, and the
manner in which these data are referenced will depend on the mode in
which ECCO Dataset Production is configured and run:

- Local: All code and input data are local, with output to local
  storage

- Local/Remote: ECCO Dataset Production runs locally, with either
  input and/or output to local or remote (cloud-based) storage

- Remote: ECCO Dataset Production runs in the cloud (e.g., AWS EC2
  compute instance, AWS Batch, etc.), with all input and output
  to/from cloud storage (e.g., AWS S3)

Each will be discussed separately.


### Configuring ECCO Dataset Production to run locally

Installing ECCO Dataset Production locally, with input and output to
local storage, is perhaps the simplest way to get started and to
familiarize oneself with the dataset production process. Steps
include:

- Clone the repository
- Install the Python package
- Source ECCO grid, mapping factors, and configuration data and some
  test ECCO results data
- Using the script-based interface, run and examine intermediate and
  output granule data

#### Clone the repository

Clone the repository (here, into a local working directory, `my_edp`):

	$ mkdir my_edp && cd my_edp
    $ git clone https://github.com/ECCO-GROUP/ECCO-Dataset-Production.git
	$ cd ECCO-Dataset-Production

Test cases supporting each of the production modes can be found in
`./tests`. If you wish to get started using any of them you'll also
need to clone the `ECCO-v4-Configurations` submodule which provides
ancillary data used during ECCO output granule generation:

    $ git submodule init
    $ git submodule update

#### Python package install

Installation is the same as for any other Python package, either
directly in the Python system libraries or in a virtual
environment. For example:

	# set up a virtual environment "edp":
	$ mkdir ~/venvs
	$ python -m venv ~/venvs/edp
	# and activate it:
	$ source ~/venvs/edp/bin/activate
	
	# install ecco_dataset_production using the virtual environment if activated,
	# or in the Python system libraries if not:
	$ cd ./production  # i.e., the directory containing pyproject.toml
	$ pip install .
	# verify the package install:
	$ python
	>>> import ecco_dataset_production as edp
	>>> help(edp)

As mentioned previously, the Python package install also exposes
several command-line entry points (ref. `pyproject.toml
[project.scripts]` block) that allow ECCO Dataset Production
high-level functionality to also be invoked at the shell script level.  Two
key ones are task list generation and data product generation (more on
both of these in a moment), and their presence can be verified by
printing their respective help menus to standard output:

	$ edp_create_job_task_list --help
	$ edp_generate_dataproducts --help

	
#### Source ECCO grid, mapping factors, and test data

Since, in this case, ECCO Dataset Production will be run locally, all
input data must also be local; this includes ECCO grid data, mapping
factors (weighting factors for native to latlon grid conversion), and
ECCO results data.  Selected ECCO V4r4 test data (and the scripts that
have been used to acquire them) can be found in
`./tests/data/ecco_grids`, `./tests/data/ecco_mapping_factors`, and
`./tests/data/ecco_results`.

#### Run

ECCO Dataset Production is invoked using a two-step process: task list
generation, followed by dataset generation.

The first step, task list generation, produces a json-formatted file
that describes each output granule that is to be produced, its inputs,
and some dynamically-created metadata that will be added to the
completed granule. Task lists can be generated from compact job
description input files using `edp_create_job_task_list`, a
command-line entry point for the Python application routine in
`apps/create_job_task_list.py`.

The second step, output granule generation, parses this task list and
creates either 2- or 3-D native or latlon output granules per the task
list definitions, and can be invoked by `edp_generate_dataproducts`, a
similar command-line entry point for the Python application routine in
`apps/generate_dataproducts.py`.  This second step is entirely
deterministic in that only those granules described in the task list
will be produced, anywhere from a single granule to potentially
thousands.  Since the json-formatted file is text-editable, it's
possible to break it up prior to granule generation (either manually
or via a simple preprocessor) and to distribute the file pieces
amongst several processors; an "embarassingly" parallel approach to
dataset production.

`./tests/SSH_native_latlon_local` includes a complete granule
generation example for a (small) collection of 2D native and latlon
sea surface height datasets. The steps include:

1. `SSH_native_latlon_mon_mean_jobs.txt` (provided) describes the
granule generation task in compact format, with each line consisting
of a Python list expression of the form
`[<metadata_groupings_id>,<product_type>,<frequency>,<time_steps>]`.
`<metadata_groupings_id>` is an integer that refers to the sequence
number in the `ECCO-v4-Configurations` "groupings" metadata files
(e.g., `./ECCO-v4-Configurations/ECCOv4 Release
4/metadata/ECCOv4r4_groupings_for_native_datasets.json` in the case of
native datasets), `<product_type>` is the string "native" or
"latlon", `<frequency>` is the string "SNAP", "AVG\_DAY", or
"AVG\_MON", and `<time_steps>` is either an explicit list of available
integer solution times (`[t1,t2,...]`) or the string "all".

2. Next, the provided script,
`edp_create_job_task_list_SSH_native_latlon_mon_mean.sh` invokes the
command-line entry point, `edp_create_job_task_list` with
`SSH_native_latlon_mon_mean_jobs.txt` as input, along with other
necessary source and destination descriptors, to generate a task list,
`SSH_native_latlon_mon_mean_tasks_<ver>.json` (where `<ver>` is either
'V4r4' or 'V4r5'; see script comments).  Verification file output,
`SSH_native_latlon_mon_mean_tasks_<ver>.json.verif` have been included
for comparison.  Note that each list element in the resulting `json`
file is an object providing a complete description (granule,
variables, metadata, etc.) of each individual granule generation task.

3. With `SSH_native_latlon_mon_mean_tasks_<ver>.json` now available,
the script `edp_generate_dataproducts_SSH_native_latlon_mon_mean.sh`
(see script comments) invokes the `edp_generate_dataproducts` command
to generate the 2D sea surface height native and latlon NetCDF
granules described in the task list. Note that, per the task list
`granule` field, output is to a directory tree with root
`./<ver>`. Verification output has been saved as `<ver>_verif.tgz` for
comparison.


### Configuring ECCO Dataset Production to run in local/remote mode

Running ECCO Dataset Production in local/remote mode is almost exactly
the same as local mode, with local installation of the Python package
and its dependencies as before, the only difference(s) now being that
some or all of the input and results data are located on AWS S3. Setup
is exactly the same as for the local case; clone the repository and
install the Python package either directly, or within a virtual
environment.

`./tests/SSH_native_latlon_local_remote` contains a variation of
`./tests/SSH_native_latlon_local` with minor changes to reference AWS
S3 storage locations for grid, mapping factor, and ECCO results
data. Note that a certain AWS S3 layout has been assumed, and the
submittal scripts may need to be modified according to your
configuration. For testing purposes output granule storage is still
local, in case privileges don't exist to write to existing ECCO AWS S3
storage. Testing the AWS S3 output features is straightforward,
however: simply create an AWS S3 bucket with write privileges and
point to it using the script's `ecco_destination_root` argument.

Note that the only change to the job description file,
`SSH_native_latlon_mon_mean_jobs.txt`, is to explicitly call out the
desired timesteps rather than "all" which, in this case, would
otherwise create a task list for every sea surface height input file
set found by the `ecco_source_root` argument in
`edp_create_job_task_list_SSH_native_latlon_mon_mean.sh`.

Running ECCO Dataset Production in local/remote mode proceeds as in
the local case: first invoke the task generation script to produce a
task list, and then reference this task list in the granule generation
script to produce the output granules.  The saved results in
`tests/SSH_native_latlon_local/V4r4_sav` can be used for comparison.


### Configuring ECCO Dataset Production to run remotely

