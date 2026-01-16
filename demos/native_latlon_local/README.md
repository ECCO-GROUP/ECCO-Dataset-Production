
## native\_latlon\_local

Complete example illustrating setup and generation of select native
and latlon granules from local input, with output to local
directories.

### Setup:

The ECCO-Dataset-Production Python package must be installed per
`../../README.md`.

If the ECCO-Dataset-Production repository has not been cloned using
the `--recurse-submodules` option, the `../ECCO-v4-Configurations`
submodule, referenced in this example and others, can be init/updated
using:

    $ git submodule init
    $ git submodule update

### Running the demo:

The example can be run by executing the two provided scripts in
sequence, for either V4r4 or V4r5, for example:

    # Step 1:
    $ ./edp_create_job_task_list.sh V4r5    # Phase 1: tasklist generation

    # Step 2:
    $ ./edp_generate_datasets.sh V4r5       # Phase 2: ECCO dataset (NetCDF file) generation

### Results and verification:

The dynamically-generated tasklist(s) produced in Phase 1 are
json-formatted files that provide recipes for creating the datasets to
be generated in Phase 2. Each tasklist object specifies a dataset that
is to be created, along with its required input quantities, paths to
ancillary data, and dynamically-generated metadata.  Per
`edp_create_job_task_list.sh` they are written as `tasks_${ver}.json`,
and can be validated against the set of verified results saved in the
included `*.verif` files:

    $ diff tasks_V4r4.json tasks_V4r4.json.verif
    $ diff tasks_V4r5.json tasks_V4r5.json.verif

Phase 2, output granule generation, is driven entirely from the Phase
1 tasklist(s). As configured, the resulting NetCDF files are available
in the local directories `./V4r4` and/or `./V4r5`, and can be
inspected and/or postprocessed as with any NetCDF dataset.

### Discussion:

The two scripts provided in this demo, `edp_create_job_task_list.sh`
and `edp_generate_datasets.sh`, demonstrate calls to two
ECCO-Dataset-Production package command-line entry points,
`edp_create_job_task_list` and `edp_generate_datasets`, respectively
(See the `[project.scripts]` section of `../../pyproject.toml` for the
list of others), and can be used as general-purpose templates.
`--help` for these command-line entrypoints displays a list of their
input and output requirements:

    # Phase 1 task list generation:
    $ edp_create_job_task_list --help

    # Phase 2 dataset generation:
    $ edp_generate_datasets --help

From the `--help` output, note that task list generation requires an
input "jobfile" (not to be confused with "jobs" in the AWS Batch
processing sense), along with references to directories containing
ECCO source (results) files, ECCO grid files, ECCO mapping factors,
ECCO metadata, and an ECCO Dataset Production configuration
file. Phase 2, ECCO dataset generation, requires only the tasklist
generated in Phase 1.

The jobfile input to Phase 1 tasklist generation deserves further
explanation:

Jobfiles provide a compact, high-level mechanism for selecting a
predefined ECCO response "collection", projection type ("1D", "native"
or "latlon"), time averaging interval (i.e., "frequency"), and output
time steps. It does so via reference to the "groupings" json files in
the `ECCO-v4-Configurations` submodule,
`./ECCO-v4-Configurations/ECCO*Release*/metadata`. Using
`jobs_V4r4.txt` as an example:

    # native groupings:

    [0,'native','AVG_MON','all']    # dynamic sea surface height and model sea level anomaly
    [17,'native','AVG_MON','all']   # ocean velocity

    # latlon groupings:

    [0,'latlon','AVG_MON','all']    # dynamic sea surface height
    [11,'latlon','AVG_MON','all']   # ocean velocity

Each line in the jobfile (comment and blank lines and trailing
comments are allowed, input is case-insensitive, and strings are
delimited by either single (') or double (") quotes)) defines a Python
list object with fields:

    [ <metadata_groupings_id>, <product_type>, <frequency>, <time_steps> ]

where `<metadata_groupings_id>` is a zeros-based integer object ID from
0 through N, `<product_type>` is one of '1D', 'latlon', or 'native',
`<frequency>` is one of 'SNAP', 'AVG\_MON', or 'AVG\_DAY', and `<time_steps>`
is either a Python list of integer time steps or 'all'.

To take the first entry in `jobs_V4r4.txt` as an example, a
`metadata_groupings_id` of '0' and `product_type` of "native" together
select the first (zero-th) object defined in
`ECCOv4r4_groupings_for_native_datasets.json`, reproduced here:

    [
       {
        "name" : "dynamic sea surface height and model sea level anomaly",
        "fields" : "SSH, SSHIBC, SSHNOIBC, ETAN",
        "comment": "SSH (dynamic sea surface height) = SSHNOIBC (dynamic sea surface without the inverse barometer correction) - SSHIBC (inverse barometer correction). The inverted barometer correction accounts for variations in sea surface height due to atmospheric pressure variations. Note: ETAN is model sea level anomaly and should not be compared with satellite altimetery products, see SSH and ETAN for more details.",
        "product": "native",
        "filename" : "SEA_SURFACE_HEIGHT",
        "dimension" : "2D",
        "frequency" : "AVG_DAY, AVG_MON, SNAP"
       },
       ...

Thus, a set of datasets will be produced for "all" available ECCO
monthly averages (based, in other words, on the input quantities found
in `../data/ecco_results/`, as specified by the `--ecco_source_root`
argument to `edp_create_job_task_list`), will include the filename
string "SEA\_SURFACE\_HEIGHT", and will contain the response
variables, "SSH", "SSHIBC", "SSHNOIBC", and "ETAN".

Similarly, the last latlon grouping in `jobs_V4r4.txt`, selects the
11-th (zeros-based) object in
`ECCOv4r4_groupings_for_latlon_datasets.json`, or:

    ...
    {
     "name": "ocean velocity",
     "fields": "EVEL, NVEL, WVELMASS",
     "field_components": {"EVEL":{"x":"UVEL", "y":"VVEL"}, "NVEL":{"x":"UVEL", "y":"VVEL"}},
     "field_orientations": {"EVEL":"zonal", "NVEL":"meridional"},
     "product": "latlon",
     "variable_rename" : "WVELMASS:WVEL",
     "filename" : "OCEAN_VELOCITY",
     "dimension" : "3D",
     "frequency" : "AVG_DAY, AVG_MON"
    },
    ...

Thus a collection of 3D "OCEAN_VELOCITY" datasets will be produced for
all available months, and will include the variables "EVEL", "NVEL",
and "WVELMASS". Additionally, the latlon variables "EVEL" and "NVEL"
will be vector summed using the underlying ECCO x and y component
velocity fields, here identified as "UVEL" and "VVEL", respectively.

Since the Phase 2 dataset generation process is entirely driven by the
Phase 1-generated task lists, it's worth inspecting the json task list
file(s) first to ensure the datasets that are to be created are as
expected. Note, too, that since the task lists simply define a list of
objects, they may be easily broken up into separate files for
stepwise, parallel, or cloud-hosted batch dataset generation, and may
also be used to verify coverage, job completion status, and so on.
