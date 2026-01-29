
edp_create_job_files
====================

Generates ECCO Dataset Production job files from ECCO Configuration groupings
files. One job file is created per dataset/frequency combination.

Overview
--------

``edp_create_job_files`` parses ECCO metadata groupings JSON files and generates
job specification files. Job files serve as intermediate specifications that
define which datasets need to be produced, and can be further expanded into
detailed task lists by ``edp_create_job_task_list``.


Usage
-----

.. code-block:: bash

    edp_create_job_files --groupings_file GROUPINGS_FILE
                         [--output_dir OUTPUT_DIR] [-l LOG_LEVEL]


Arguments
---------

``--groupings_file``
    Path and filename of the ECCO Configurations groupings JSON file.
    The filename must contain one of: ``native``, ``latlon``, or ``1D`` to
    indicate the grid type.

``--output_dir``
    Directory where job files will be written. Created if it doesn't exist.
    Default: ``.``

``-l, --log``
    Set logging level. Choices: ``DEBUG``, ``INFO``, ``WARNING``, ``ERROR``,
    ``CRITICAL``.
    Default: ``WARNING``


Entry Point
-----------

**Module:** ``ecco_dataset_production.apps.create_job_files``

**Function:** ``main()``


Input Files
-----------

+---------------------------------------+---------------------------------------+
| File                                  | Description                           |
+=======================================+=======================================+
| ``groupings_for_{type}_datasets.json``| ECCO Configuration groupings file     |
|                                       | containing dataset definitions.       |
|                                       | ``{type}`` is one of: ``native``,     |
|                                       | ``latlon``, or ``1D``.                |
+---------------------------------------+---------------------------------------+

**Groupings File Format (JSON array):**

.. code-block:: json

    [
      {
        "name": "Dynamic sea surface height...",
        "filename": "SEA_SURFACE_HEIGHT",
        "fields": "SSH, SSHIBC, SSHNOIBC",
        "frequency": "AVG_MON, AVG_DAY",
        "dimension": "2D"
      },
      {
        "name": "Ocean temperature and salinity",
        "filename": "OCEAN_TEMPERATURE_SALINITY",
        "fields": "THETA, SALT",
        "frequency": "AVG_MON",
        "dimension": "3D",
        "variable_rename": "THETA:OCEAN_TEMPERATURE"
      }
    ]

**Required Fields:**

- ``name``: Human-readable dataset description
- ``filename``: Base filename for output datasets
- ``fields``: Comma-separated list of MDS variable names
- ``frequency``: Comma-separated list of temporal frequencies
- ``dimension``: Either "2D" or "3D"

**Optional Fields:**

- ``variable_rename``: Rename variable (format: ``old_name:new_name``)
- ``field_components``: For vector quantities, component mapping
- ``comment``: Additional dataset-specific comments


Output Files
------------

+---------------------------------------+---------------------------------------+
| File                                  | Description                           |
+=======================================+=======================================+
| ``{filename}_{grid}_{freq}_jobs.txt`` | Job specification file for one        |
|                                       | dataset/frequency combination         |
+---------------------------------------+---------------------------------------+

**Naming Convention:**

- ``{filename}``: From groupings ``filename`` field
- ``{grid}``: Grid type (``native``, ``latlon``, or ``1d``)
- ``{freq}``: Frequency (``AVG_MON``, ``AVG_DAY``, or ``SNAP``)

**Job File Format:**

.. code-block:: text

    # Human-readable dataset name:
    [groupings_id, 'grid_type', 'frequency', 'timesteps']

**Example Output:**

Given input ``groupings_for_latlon_datasets.json`` with 2 datasets:

.. code-block:: text

    output_dir/
    ├── SEA_SURFACE_HEIGHT_latlon_AVG_MON_jobs.txt
    ├── SEA_SURFACE_HEIGHT_latlon_AVG_DAY_jobs.txt
    ├── OCEAN_TEMPERATURE_SALINITY_latlon_AVG_MON_jobs.txt
    └── OCEAN_TEMPERATURE_SALINITY_latlon_AVG_DAY_jobs.txt

**Example Job File Contents** (``SEA_SURFACE_HEIGHT_latlon_AVG_MON_jobs.txt``):

.. code-block:: text

    # Dynamic sea surface height and model sea level anomaly:
    [0, 'latlon', 'AVG_MON', 'all']


Examples
--------

**Basic usage:**

.. code-block:: bash

    edp_create_job_files \
        --groupings_file ./metadata/groupings_for_latlon_datasets.json \
        --output_dir ./jobs \
        -l INFO

**Generate native grid job files:**

.. code-block:: bash

    edp_create_job_files \
        --groupings_file ./metadata/groupings_for_native_datasets.json \
        --output_dir ./jobs/native

**Generate 1D dataset job files:**

.. code-block:: bash

    edp_create_job_files \
        --groupings_file ./metadata/groupings_for_1D_datasets.json \
        --output_dir ./jobs/1D


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

       subgraph setup["SETUP"]
           create_job["<b>create_job_files()</b>"]
           grid_type["Determine grid_type from filename<br/>(native, latlon, or 1d)"]
           set_ts["Set timesteps = 'all'"]
           load_json["Load groupings JSON file"]
           create_dir["Create output_dir if needed"]
       end

       subgraph generate["JOB FILE GENERATION"]
           group_loop["For each group in groupings"]
           freq_loop["For each frequency in group"]
           build_name["Build job_filename<br/>{filename}_{grid_type}_{freq}_jobs.txt"]
           build_desc["Build job_description<br/>[id, grid_type, freq, timesteps]"]
           write_file["Write job file<br/># {name}:<br/>[id, grid_type, freq, timesteps]"]
       end

       init --> setup
       setup --> generate

       main --> parser --> parse
       create_job --> grid_type --> set_ts --> load_json --> create_dir
       group_loop --> freq_loop --> build_name --> build_desc --> write_file
       write_file --> freq_loop

       style init fill:#e3f2fd,stroke:#1565c0,stroke-width:2px,color:#0d47a1
       style setup fill:#e8f5e9,stroke:#2e7d32,stroke-width:2px,color:#1b5e20
       style generate fill:#fff3e0,stroke:#e65100,stroke-width:2px,color:#bf360c

       style main fill:#bbdefb,stroke:#1565c0,color:#0d47a1
       style parser fill:#bbdefb,stroke:#1565c0,color:#0d47a1
       style parse fill:#bbdefb,stroke:#1565c0,color:#0d47a1
       style create_job fill:#c8e6c9,stroke:#2e7d32,color:#1b5e20
       style grid_type fill:#c8e6c9,stroke:#2e7d32,color:#1b5e20
       style set_ts fill:#c8e6c9,stroke:#2e7d32,color:#1b5e20
       style load_json fill:#c8e6c9,stroke:#2e7d32,color:#1b5e20
       style create_dir fill:#c8e6c9,stroke:#2e7d32,color:#1b5e20
       style group_loop fill:#ffe0b2,stroke:#e65100,color:#bf360c
       style freq_loop fill:#ffe0b2,stroke:#e65100,color:#bf360c
       style build_name fill:#ffe0b2,stroke:#e65100,color:#bf360c
       style build_desc fill:#ffe0b2,stroke:#e65100,color:#bf360c
       style write_file fill:#ffe0b2,stroke:#e65100,color:#bf360c

       linkStyle default stroke:#333,stroke-width:2px


Detailed Flow Description
-------------------------

1. Argument Parsing
^^^^^^^^^^^^^^^^^^^

The script accepts the following arguments:

- ``--groupings_file``: Path to ECCO Configuration groupings JSON file
- ``--output_dir``: Directory for output job files (created if needed)
- ``-l/--log``: Logging level

2. Grid Type Detection
^^^^^^^^^^^^^^^^^^^^^^

The script determines the grid type by examining the groupings filename:

+------------------------+----------------+
| Filename Contains      | Grid Type      |
+========================+================+
| ``*native*``           | ``native``     |
+------------------------+----------------+
| ``*latlon*``           | ``latlon``     |
+------------------------+----------------+
| ``*1d*`` or ``*1D*``   | ``1d``         |
+------------------------+----------------+

This is case-insensitive matching using ``fnmatch``. If none match, a
``RuntimeException`` is raised.

3. Groupings File Structure
^^^^^^^^^^^^^^^^^^^^^^^^^^^

The input groupings JSON file contains a list of dataset group definitions.
Each group specifies one or more variables to combine into a single dataset,
along with the temporal frequencies at which to produce them.

4. Job File Generation
^^^^^^^^^^^^^^^^^^^^^^

For each group, the script:

a. **Parses Frequencies:**
   The ``frequency`` field may contain multiple comma-separated values
   (e.g., ``"AVG_MON, AVG_DAY"``). Each frequency generates a separate job file.

b. **Builds Job Filename:**
   ``{filename}_{grid_type}_{frequency}_jobs.txt``

c. **Creates Job Description:**
   A Python list containing:

   - ``id``: Zero-based index in the groupings array
   - ``grid_type``: Detected from filename (native/latlon/1d)
   - ``freq``: Frequency string (AVG_MON, AVG_DAY, SNAP)
   - ``timesteps``: Always ``'all'`` (processes all available timesteps)

d. **Writes Job File:**
   Comment line with dataset name, followed by job specification.


Key Module Dependencies
-----------------------

.. code-block:: text

    ecco_dataset_production.apps.create_job_files
        |
        +---> json (groupings file parsing)
        |
        +---> fnmatch (grid type detection)
        |
        +---> os (directory creation, path handling)
        |
        +---> logging


Notes
-----

- The ``timesteps`` field is currently hardcoded to ``'all'``
- Future versions may support explicit timestep specification
- Job files can be manually edited to customize processing
- Lines beginning with ``#`` are comments and ignored by downstream tools

