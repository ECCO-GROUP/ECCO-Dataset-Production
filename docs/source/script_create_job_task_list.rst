
edp_create_job_task_list
========================

Creates detailed task lists from ECCO Dataset Production job files by scanning
available source files and generating comprehensive task descriptions.

Overview
--------

``edp_create_job_task_list`` is the most complex script in the ECCO Dataset
Production pipeline. It scans available source files (either locally or on S3),
matches them against job specifications, and generates detailed task lists
that serve as "recipes" for dataset generation.

Task lists provide explicit specifications including all input file locations,
output destinations, and metadata. They enable distributed and containerized
processing and support error checking.


Usage
-----

.. code-block:: bash

    edp_create_job_task_list --jobfile JOBFILE
                             --ecco_source_root ECCO_SOURCE_ROOT
                             --ecco_destination_root ECCO_DESTINATION_ROOT
                             [--ecco_grid_loc ECCO_GRID_LOC]
                             [--ecco_mapping_factors_loc ECCO_MAPPING_FACTORS_LOC]
                             [--ecco_metadata_loc ECCO_METADATA_LOC]
                             [--ecco_cfg_loc ECCO_CFG_LOC]
                             [--outfile OUTFILE]
                             [--keygen KEYGEN] [--profile PROFILE]
                             [-l LOG_LEVEL]


Arguments
---------

``--jobfile``
    Path and filename of the ECCO Dataset Production jobs text file.
    Each line contains a Python list-style specifier::

        [metadata_groupings_id, product_type, frequency, time_steps]

    Where:

    - ``metadata_groupings_id``: Integer (0 through N)
    - ``product_type``: One of ``1D``, ``latlon``, or ``native``
    - ``frequency``: One of ``SNAP``, ``AVG_MON``, or ``AVG_DAY``
    - ``time_steps``: List of integers or ``all``

``--ecco_source_root``
    ECCO results root location. Can be a local directory path (e.g.,
    ``/ecco_nfs_1/shared/ECCOV4r5``) or an AWS S3 URI (e.g.,
    ``s3://ecco-model-granules/V4r4``).

``--ecco_destination_root``
    ECCO Dataset Production output root location. Local path or S3 URI.

``--ecco_grid_loc``
    Directory or S3 location containing ECCO grid files (``XC.*``, ``YC.*``,
    ``*latlon*.nc``, ``*native*.nc``, ``available_diagnostics.log``).

``--ecco_mapping_factors_loc``
    Directory or S3 location containing ECCO mapping factors (subdirectories:
    ``3D``, ``land_mask``, ``latlon_grid``, ``sparse``).

``--ecco_metadata_loc``
    Directory or S3 location containing ECCO metadata JSON files
    (``*_groupings_for_{1D,latlon,native}_datasets.json``, global metadata, etc.).

``--ecco_cfg_loc``
    Path to ECCO Dataset Production configuration file (YAML), or S3 location.

``--outfile``
    Output file for the task list (JSON format).
    Default: stdout

``--keygen``
    For AWS IAM Identity Center (SSO) environments, path to the federated
    login key generation script.

``--profile``
    AWS credential profile name for SSO environments (e.g., ``saml-pub``).

``-l, --log``
    Set logging level.
    Default: ``WARNING``


Entry Point
-----------

**Module:** ``ecco_dataset_production.apps.create_job_task_list``

**Function:** ``main()``


Input Files
-----------

**Primary Inputs:**

+---------------------------------------+---------------------------------------+
| File                                  | Description                           |
+=======================================+=======================================+
| ``*_jobs.txt``                        | Job specification file from           |
|                                       | ``edp_create_job_files``              |
+---------------------------------------+---------------------------------------+
| ``product_generation_config.yaml``    | ECCO configuration file (YAML)        |
+---------------------------------------+---------------------------------------+

**ECCO Source Files (scanned from** ``--ecco_source_root`` **):**

+---------------------------------------+---------------------------------------+
| File Pattern                          | Description                           |
+=======================================+=======================================+
| ``diags_daily/{VAR}_day_mean/``       | Daily mean diagnostic output          |
| ``{VAR}_day_mean.{TSTEP}.data``       |                                       |
+---------------------------------------+---------------------------------------+
| ``diags_daily/{VAR}_day_mean/``       | Metadata for daily mean files         |
| ``{VAR}_day_mean.{TSTEP}.meta``       |                                       |
+---------------------------------------+---------------------------------------+
| ``diags_monthly/{VAR}_mon_mean/``     | Monthly mean diagnostic output        |
| ``{VAR}_mon_mean.{TSTEP}.data``       |                                       |
+---------------------------------------+---------------------------------------+
| ``diags_monthly/{VAR}_mon_mean/``     | Metadata for monthly mean files       |
| ``{VAR}_mon_mean.{TSTEP}.meta``       |                                       |
+---------------------------------------+---------------------------------------+
| ``diags_inst/{VAR}_day_snap/``        | Instantaneous (snapshot) output       |
| ``{VAR}_day_snap.{TSTEP}.data``       |                                       |
+---------------------------------------+---------------------------------------+
| ``diags_inst/{VAR}_day_snap/``        | Metadata for snapshot files           |
| ``{VAR}_day_snap.{TSTEP}.meta``       |                                       |
+---------------------------------------+---------------------------------------+

**Metadata Files (from** ``--ecco_metadata_loc`` **):**

+-----------------------------------------------+-------------------------------+
| File                                          | Description                   |
+===============================================+===============================+
| ``*_groupings_for_latlon_datasets.json``      | Dataset groupings for latlon  |
+-----------------------------------------------+-------------------------------+
| ``*_groupings_for_native_datasets.json``      | Dataset groupings for native  |
+-----------------------------------------------+-------------------------------+
| ``*_groupings_for_1D_datasets.json``          | Dataset groupings for 1D      |
+-----------------------------------------------+-------------------------------+

**MDS File Format:**

- ``.data``: Raw binary floating-point array (big-endian float32)
- ``.meta``: Text file with array dimensions, precision, and metadata

**Example MDS Filename:**

.. code-block:: text

    SSH_mon_mean.0000000732.data
    │   │        │
    │   │        └── Timestep number (model time units)
    │   └── Averaging period (mon_mean, day_mean, day_snap)
    └── Variable name


Output Files
------------

+---------------------------------------+---------------------------------------+
| File                                  | Description                           |
+=======================================+=======================================+
| ``*_tasks.json``                      | JSON task list for                    |
|                                       | ``edp_generate_datasets``             |
+---------------------------------------+---------------------------------------+

**Task List Format (JSON array of task objects):**

.. code-block:: json

    [
      {
        "granule": "/output/V4r5/latlon/mon_mean/SEA_SURFACE_HEIGHT/SEA_SURFACE_HEIGHT_mon_mean_1992-01-16T12:00:00_V4r5_latlon_0p50deg.nc",
        "variables": {
          "SSH": [
            [
              "/input/diags_monthly/SSH_mon_mean/SSH_mon_mean.0000000732.data",
              "/input/diags_monthly/SSH_mon_mean/SSH_mon_mean.0000000732.meta"
            ]
          ],
          "SSHIBC": [
            [
              "/input/diags_monthly/SSHIBC_mon_mean/SSHIBC_mon_mean.0000000732.data",
              "/input/diags_monthly/SSHIBC_mon_mean/SSHIBC_mon_mean.0000000732.meta"
            ]
          ]
        },
        "ecco_cfg_loc": "/config/V4r5_config.yaml",
        "ecco_grid_loc": "/grid/",
        "ecco_mapping_factors_loc": "/factors/",
        "ecco_metadata_loc": "/metadata/",
        "dynamic_metadata": {
          "name": "Dynamic sea surface height...",
          "dimension": "2D",
          "time_coverage_start": "1992-01-01T00:00:00",
          "time_coverage_end": "1992-02-01T00:00:00",
          "time_coverage_center": "1992-01-16T12:00:00",
          "time_long_name": "center time of averaging period",
          "time_coverage_duration": "P1M",
          "time_coverage_resolution": "P1M",
          "summary": "This dataset contains monthly-averaged..."
        }
      }
    ]

**Task Object Fields:**

- ``granule``: Full output path/URI for the NetCDF file
- ``variables``: Dict mapping variable names to lists of [.data, .meta] pairs
- ``ecco_*_loc``: Paths to required resource directories
- ``dynamic_metadata``: Time bounds, coverage info, and descriptions


Examples
--------

**Basic usage with local files:**

.. code-block:: bash

    edp_create_job_task_list \
        --jobfile ./jobs/SSH_latlon_AVG_MON_jobs.txt \
        --ecco_source_root /data/ECCOV4r5 \
        --ecco_destination_root /output/datasets \
        --ecco_grid_loc /data/ECCOV4r5/grid \
        --ecco_mapping_factors_loc /data/mapping_factors \
        --ecco_metadata_loc ./metadata \
        --ecco_cfg_loc ./config/V4r5_config.yaml \
        --outfile ./tasklists/SSH_latlon_tasks.json \
        -l INFO

**Using S3 source with SSO authentication:**

.. code-block:: bash

    edp_create_job_task_list \
        --jobfile ./jobs/THETA_latlon_AVG_MON_jobs.txt \
        --ecco_source_root s3://ecco-model-granules/V4r5 \
        --ecco_destination_root s3://my-bucket/datasets \
        --ecco_grid_loc s3://ecco-model-granules/V4r5/grid \
        --ecco_mapping_factors_loc /local/mapping_factors \
        --ecco_metadata_loc ./metadata \
        --ecco_cfg_loc ./config/V4r5_config.yaml \
        --keygen /usr/local/bin/aws-login-pub.darwin.amd64 \
        --profile saml-pub \
        --outfile ./tasklists/THETA_tasks.json

**Output to stdout (for piping):**

.. code-block:: bash

    edp_create_job_task_list \
        --jobfile ./jobs/SSH_latlon_AVG_MON_jobs.txt \
        --ecco_source_root /data/ECCOV4r5 \
        --ecco_destination_root /output/datasets \
        | jq '.[0]'  # View first task


Execution Flow Diagram
----------------------

.. mermaid::

   %%{init: {'theme': 'neutral', 'themeVariables': { 'edgeLabelBackground':'#ffffff'}}}%%
   flowchart TD
       subgraph init["INITIALIZATION"]
           main["<b>main()</b>"]
           parser["create_parser()<br/>Define CLI arguments"]
           parse["Parse command-line arguments"]
       end

       subgraph setup["SETUP & VALIDATION"]
           create_task["<b>create_job_task_list()</b>"]
           init_config["Initialize ECCODatasetProductionConfig"]
           validate["Validate ecco_source_root<br/>and ecco_destination_root"]
           s3_check{{"S3 source?"}}
           s3_setup["Update SSO credentials<br/>Setup boto3 session<br/>Create S3 client"]
           load_meta["Load ECCOMetadata<br/>(dataset_groupings)"]
       end

       subgraph collect["STEP 1: COLLECT INPUTS"]
           job_loop["For each line in jobfile"]
           parse_job["Parse job spec<br/>[id, type, freq, timesteps]"]
           get_meta["Get job_metadata<br/>from dataset_groupings"]
           freq_pat["Determine frequency patterns"]
           var_loop["For each variable in fields"]
           comp_check{{"field_components?"}}
           collect_comp["Collect inputs for<br/>each component"]
           scan_files["Scan for .data/.meta files<br/>Group pairs by timestep"]
           rename["Apply variable_rename<br/>(if specified)"]
       end

       subgraph build["STEP 2: BUILD TASKS"]
           get_ts["Get union of all<br/>available timesteps"]
           ts_loop["For each timestep"]
           calc_time["Calculate time bounds"]
           build_name["Build output filename"]
           create_dict["Create task dictionary"]
           append["Append to task_list"]
       end

       subgraph output["OUTPUT"]
           return_list["Return task_list"]
           write_json["Write task_list to outfile (JSON)"]
       end

       init --> setup
       setup --> collect
       collect --> build
       build --> output

       main --> parser --> parse
       create_task --> init_config --> validate --> s3_check
       s3_check -->|Yes| s3_setup --> load_meta
       s3_check -->|No| load_meta
       job_loop --> parse_job --> get_meta --> freq_pat --> var_loop
       var_loop --> comp_check
       comp_check -->|Yes| collect_comp --> rename
       comp_check -->|No| scan_files --> rename
       get_ts --> ts_loop --> calc_time --> build_name --> create_dict --> append
       return_list --> write_json

       style init fill:#e3f2fd,stroke:#1565c0,stroke-width:2px,color:#0d47a1
       style setup fill:#e8f5e9,stroke:#2e7d32,stroke-width:2px,color:#1b5e20
       style collect fill:#fff3e0,stroke:#e65100,stroke-width:2px,color:#bf360c
       style build fill:#f3e5f5,stroke:#7b1fa2,stroke-width:2px,color:#4a148c
       style output fill:#fce4ec,stroke:#c2185b,stroke-width:2px,color:#880e4f

       style main fill:#bbdefb,stroke:#1565c0,color:#0d47a1
       style parser fill:#bbdefb,stroke:#1565c0,color:#0d47a1
       style parse fill:#bbdefb,stroke:#1565c0,color:#0d47a1
       style create_task fill:#c8e6c9,stroke:#2e7d32,color:#1b5e20
       style init_config fill:#c8e6c9,stroke:#2e7d32,color:#1b5e20
       style validate fill:#c8e6c9,stroke:#2e7d32,color:#1b5e20
       style s3_check fill:#c8e6c9,stroke:#2e7d32,color:#1b5e20
       style s3_setup fill:#c8e6c9,stroke:#2e7d32,color:#1b5e20
       style load_meta fill:#c8e6c9,stroke:#2e7d32,color:#1b5e20
       style job_loop fill:#ffe0b2,stroke:#e65100,color:#bf360c
       style parse_job fill:#ffe0b2,stroke:#e65100,color:#bf360c
       style get_meta fill:#ffe0b2,stroke:#e65100,color:#bf360c
       style freq_pat fill:#ffe0b2,stroke:#e65100,color:#bf360c
       style var_loop fill:#ffe0b2,stroke:#e65100,color:#bf360c
       style comp_check fill:#ffe0b2,stroke:#e65100,color:#bf360c
       style collect_comp fill:#ffe0b2,stroke:#e65100,color:#bf360c
       style scan_files fill:#ffe0b2,stroke:#e65100,color:#bf360c
       style rename fill:#ffe0b2,stroke:#e65100,color:#bf360c
       style get_ts fill:#e1bee7,stroke:#7b1fa2,color:#4a148c
       style ts_loop fill:#e1bee7,stroke:#7b1fa2,color:#4a148c
       style calc_time fill:#e1bee7,stroke:#7b1fa2,color:#4a148c
       style build_name fill:#e1bee7,stroke:#7b1fa2,color:#4a148c
       style create_dict fill:#e1bee7,stroke:#7b1fa2,color:#4a148c
       style append fill:#e1bee7,stroke:#7b1fa2,color:#4a148c
       style return_list fill:#f8bbd9,stroke:#c2185b,color:#880e4f
       style write_json fill:#f8bbd9,stroke:#c2185b,color:#880e4f

       linkStyle default stroke:#333,stroke-width:2px


Detailed Flow Description
-------------------------

1. Job Specification Parsing
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Each line in the job file contains a Python list literal:

.. code-block:: text

    [metadata_groupings_id, product_type, frequency, time_steps]

Examples:

.. code-block:: text

    [0, 'latlon', 'AVG_MON', 'all']
    [5, 'native', 'SNAP', [732, 1428, 2124]]

2. Frequency Pattern Mapping
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The frequency string maps to directory and filename patterns:

+------------+-------------------+------------------+
| Frequency  | Path Pattern      | File Pattern     |
+============+===================+==================+
| AVG_DAY    | diags_daily       | day_mean         |
+------------+-------------------+------------------+
| AVG_MON    | diags_monthly     | mon_mean         |
+------------+-------------------+------------------+
| SNAP       | diags_inst        | day_snap         |
+------------+-------------------+------------------+

3. Source File Discovery
^^^^^^^^^^^^^^^^^^^^^^^^

For each variable in the job metadata's ``fields`` list:

a. **Simple Variables:**
   Direct mapping from one MDS file to one output variable.

b. **Vector Components:**
   Variables built from multiple component inputs (e.g., velocity vectors).

**File Matching:**

- For S3: Uses paginated ``list_objects_v2`` API
- For local: Uses ``os.walk()`` to traverse directory tree

4. Task Generation
^^^^^^^^^^^^^^^^^^

For each unique timestep where any variable data exists:

a. **Time Bounds Calculation:**
   Uses ``ecco_time.make_time_bounds_metadata()``

b. **Output Filename Construction:**
   Uses ``ECCOGranuleFilestr`` for standardized naming

c. **Task Dictionary Creation:**
   Combines all inputs, outputs, and metadata


Key Module Dependencies
-----------------------

.. code-block:: text

    ecco_dataset_production.apps.create_job_task_list
        |
        +---> ecco_dataset_production.configuration
        |       +---> ECCODatasetProductionConfig
        |
        +---> ecco_dataset_production.ecco_file
        |       +---> ECCOMDSFilestr (MDS filename parsing)
        |       +---> ECCOGranuleFilestr (output filename generation)
        |
        +---> ecco_dataset_production.ecco_metadata
        |       +---> ECCOMetadata (groupings loader)
        |
        +---> ecco_dataset_production.ecco_time
        |       +---> make_time_bounds_metadata()
        |
        +---> ecco_dataset_production.aws.ecco_aws
        |       +---> is_s3_uri()
        |
        +---> boto3 (S3 operations)
        +---> ast (literal_eval for job parsing)


Error Handling
--------------

- Missing ``ecco_source_root`` or ``ecco_destination_root`` raises ``RuntimeError``
- Invalid frequency values raise ``ValueError``
- Invalid product type values raise ``ValueError``
- Missing input files for a variable/timestep generate warnings but don't fail
- Non-parseable job file lines are logged and skipped (supports comments)

