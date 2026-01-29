
CLI Scripts Reference
=====================

The ECCO Dataset Production package provides five command-line interface (CLI)
scripts that support the end-to-end workflow of generating ECCO datasets.
These scripts are installed as entry points when the package is installed
and can be invoked directly from the command line.

Workflow Overview
-----------------

The typical production workflow follows these steps:

.. code-block:: text

    ┌─────────────────────────────────────────────────────────────────────────┐
    │                         ONE-TIME SETUP                                  │
    ├─────────────────────────────────────────────────────────────────────────┤
    │  1. edp_create_factors     Generate grid mapping factors                │
    │                            (interpolation weights, land masks)          │
    └─────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
    ┌─────────────────────────────────────────────────────────────────────────┐
    │                      PRODUCTION PIPELINE                                │
    ├─────────────────────────────────────────────────────────────────────────┤
    │  2. edp_create_job_files   Parse metadata groupings → job files (.txt)  │
    │                                       │                                 │
    │                                       ▼                                 │
    │  3. edp_create_job_task_list  Scan source files → task lists (.json)    │
    │                                       │                                 │
    │                                       ▼                                 │
    │  4. edp_generate_datasets  Execute tasks → NetCDF granules (.nc)        │
    └─────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
    ┌─────────────────────────────────────────────────────────────────────────┐
    │                         DISTRIBUTION                                    │
    ├─────────────────────────────────────────────────────────────────────────┤
    │  5. edp_aws_s3_sync        Sync datasets to/from AWS S3                 │
    └─────────────────────────────────────────────────────────────────────────┘


Script Documentation
--------------------

Click on each script for complete documentation including usage, arguments,
input/output files, and execution flow diagrams.

.. toctree::
   :maxdepth: 1

   script_create_factors
   script_create_job_files
   script_create_job_task_list
   script_generate_datasets
   script_aws_s3_sync


Quick Reference
---------------

:doc:`script_create_factors`
    Creates 2D/3D grid mapping factors, land masks, and lat/lon grid files.
    Run once before dataset generation.

:doc:`script_create_job_files`
    Generates job specification files from ECCO metadata groupings JSON.
    One job file per dataset/frequency combination.

:doc:`script_create_job_task_list`
    Scans available MDS source files and generates detailed task lists (JSON)
    that serve as "recipes" for dataset generation.

:doc:`script_generate_datasets`
    The primary production script. Reads task lists, processes input data,
    applies transformations, and writes NetCDF granules.

:doc:`script_aws_s3_sync`
    Wrapper for AWS S3 sync with multiprocessing support for uploads
    and SSO authentication handling.


Complete Workflow Example
-------------------------

The following shell script illustrates a complete end-to-end workflow:

.. code-block:: bash

    #!/bin/bash
    # ECCO Dataset Production - Complete Workflow Example

    # Configuration
    CONFIG="./config/V4r5_config.yaml"
    METADATA_DIR="./metadata"
    SOURCE_ROOT="/data/ECCOV4r5"
    DEST_ROOT="/output/datasets"
    GRID_LOC="/data/ECCOV4r5/grid"
    FACTORS_LOC="/data/mapping_factors"

    # 1. Create mapping factors (one-time setup)
    edp_create_factors \
        --cfgfile "$CONFIG" \
        --dims 2 3 \
        -l INFO

    # 2. Generate job files from groupings
    mkdir -p ./jobs
    edp_create_job_files \
        --groupings_file "$METADATA_DIR/groupings_for_latlon_datasets.json" \
        --output_dir ./jobs \
        -l INFO

    # 3. Create task lists from job files
    mkdir -p ./tasklists
    for jobfile in ./jobs/*_jobs.txt; do
        outfile="./tasklists/$(basename "${jobfile%.txt}").json"
        edp_create_job_task_list \
            --jobfile "$jobfile" \
            --ecco_source_root "$SOURCE_ROOT" \
            --ecco_destination_root "$DEST_ROOT" \
            --ecco_grid_loc "$GRID_LOC" \
            --ecco_mapping_factors_loc "$FACTORS_LOC" \
            --ecco_metadata_loc "$METADATA_DIR" \
            --ecco_cfg_loc "$CONFIG" \
            --outfile "$outfile" \
            -l INFO
    done

    # 4. Generate datasets from task lists
    for tasklist in ./tasklists/*.json; do
        edp_generate_datasets \
            --tasklist "$tasklist" \
            -l INFO
    done

    # 5. Sync results to S3 (optional)
    edp_aws_s3_sync \
        --src "$DEST_ROOT" \
        --dest s3://ecco-production-datasets \
        --nproc 8 \
        -l INFO

