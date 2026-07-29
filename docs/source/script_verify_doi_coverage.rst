
edp_verify_doi_coverage
========================

Verifies that all dataset groupings defined in metadata JSON files have
corresponding DOI entries in the PO.DAAC dataset CSV file.

Overview
--------

``edp_verify_doi_coverage`` performs consistency checks to ensure that every dataset
has proper DOI registration before publication through NASA's PO.DAAC (Physical
Oceanography Distributed Active Archive Center).

When preparing ECCO datasets for distribution, several consistency checks are required:

- **Grouping definitions**: Each dataset grouping must be defined in metadata JSON files
- **DOI entries**: Each grouping must have corresponding DOI entries in the PO.DAAC CSV
- **DOI uniqueness**: Each DOI must be unique (no duplicate DOIs across datasets)
- **No orphans**: All CSV entries should correspond to a grouping definition

The tool performs three bidirectional checks:

1. **Groupings → CSV**: Do all groupings have DOI entries?
2. **CSV → Groupings**: Do all CSV entries correspond to a grouping?
3. **DOI Uniqueness**: Are all DOIs unique?


Usage
-----

.. code-block:: bash

    edp_verify_doi_coverage -m METADATA_DIR -c CSV_FILE
                            [-l LOG_LEVEL] [-v] [-q]


Arguments
---------

Required Arguments
^^^^^^^^^^^^^^^^^^

``-m, --metadata-dir``
    Path to directory containing groupings JSON files.
    Must contain:

    - ``groupings_for_native_datasets.json``
    - ``groupings_for_latlon_datasets.json``
    - ``groupings_for_1D_datasets.json``

``-c, --csv-file``
    Path to CSV file with DOI information.
    Must have column: ``DATASET.FILENAME``
    Typically also has: ``DATASET.SHORT_NAME``, ``DATASET.PERSISTENT_ID``

Optional Arguments
^^^^^^^^^^^^^^^^^^

``-l, --log``
    Logging level: ``DEBUG``, ``INFO``, ``WARNING``, ``ERROR``, ``CRITICAL``.
    Default: ``INFO``

``-v, --verbose``
    Show detailed debug output (equivalent to ``--log DEBUG``).

``-q, --quiet``
    Only show summary and errors (equivalent to ``--log WARNING``).


Entry Point
-----------

**Module:** ``ecco_dataset_production.apps.verify_doi_coverage``

**Function:** ``main()``


Examples
--------

**Basic verification:**

.. code-block:: bash

    edp_verify_doi_coverage \
        --metadata-dir "/path/to/ECCO-v4-Configurations/ECCOv4 Release 6/metadata" \
        --csv-file "/path/to/metadata/PODAAC_dataset_table_V4r6_prelim.csv"

**With verbose output:**

.. code-block:: bash

    edp_verify_doi_coverage \
        --metadata-dir "/path/to/metadata" \
        --csv-file "/path/to/PODAAC_dataset_table.csv" \
        --verbose

**Quiet mode (summary only):**

.. code-block:: bash

    edp_verify_doi_coverage \
        --metadata-dir "/path/to/metadata" \
        --csv-file "/path/to/PODAAC_dataset_table.csv" \
        --quiet


How It Works
------------

1. Load Groupings
^^^^^^^^^^^^^^^^^

Loads all groupings from three JSON files:

- ``groupings_for_1D_datasets.json`` - Time series and global averages
- ``groupings_for_native_datasets.json`` - Native LLC90 grid datasets
- ``groupings_for_latlon_datasets.json`` - Regular lat-lon grid datasets

Each grouping has a ``filename`` field (e.g., ``"SEA_SURFACE_HEIGHT"``)

2. Load DOI CSV
^^^^^^^^^^^^^^^

Loads the PO.DAAC dataset table CSV, which contains rows like:

.. code-block:: csv

    DATASET.LONG_NAME,DATASET.SHORT_NAME,DATASET.FILENAME,DATASET.PERSISTENT_ID
    "ECCO Sea Surface Height...",ECCO_L4_SSH_05DEG_DAILY_V4R6,SEA_SURFACE_HEIGHT_day_mean_1992-01-01_ECCO_V4r6_latlon_0p50deg.nc,10.5067/ECG5D-SSH46

3. Check DOI Uniqueness
^^^^^^^^^^^^^^^^^^^^^^^

Verifies that all DOIs (``DATASET.PERSISTENT_ID``) in the CSV are unique.
Each dataset must have its own unique DOI.

If duplicate DOIs are found, the tool reports:

- The duplicate DOI value
- All filenames using that DOI

4. Check Reverse Coverage (CSV → Groupings)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Checks that every CSV entry corresponds to a grouping definition. This catches:

- Orphaned CSV entries (DOIs assigned to datasets that don't have groupings)
- Typos or naming mismatches between CSV filenames and grouping filenames

For each CSV entry, the tool checks if the filename starts with any known grouping
prefix. If not, it's flagged as an orphan.

**Known Exceptions:**

- ``GRID_GEOMETRY_*`` files - Grid geometry files (both native and latlon) are
  metadata files that don't have data groupings. These are automatically recognized
  and won't be flagged as errors.

5. Expand Groupings by Frequency
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Each grouping definition includes a ``frequency`` field (e.g., ``"AVG_DAY, AVG_MON, SNAP"``).
The tool expands each grouping into separate expected datasets:

**Time-Varying Datasets** (have date strings in filenames):

- Grouping: ``SEA_SURFACE_HEIGHT`` with frequency ``"AVG_DAY, AVG_MON, SNAP"``
- Expands to 3 expected datasets:

  - ``SEA_SURFACE_HEIGHT_day_mean_1992-01-01_...`` (daily)
  - ``SEA_SURFACE_HEIGHT_mon_mean_1992-01_...`` (monthly)
  - ``SEA_SURFACE_HEIGHT_snap_1992-01-01T000000_...`` (snapshot)

- Search patterns: ``{filename}_{freq_pattern}_``

**Time-Invariant Datasets** (no date strings in filenames):

- Grouping: ``OCEAN_3D_MIXING_COEFFS`` with frequency ``"TIME_INVARIANT"``
- Expands to 1 expected dataset:

  - ``OCEAN_3D_MIXING_COEFFS_ECCO_V4r6_native_llc0090.nc``

- Search pattern: ``{filename}_ECCO_`` (no date, goes straight to version)

6. Match Filenames
^^^^^^^^^^^^^^^^^^

For each grouping, the tool checks if the CSV contains any filenames that
**start with** the grouping's filename prefix.

**Example:** Grouping ``"SEA_SURFACE_HEIGHT"`` matches CSV entries:

- ``SEA_SURFACE_HEIGHT_day_mean_1992-01-01_ECCO_V4r6_latlon_0p50deg.nc``
- ``SEA_SURFACE_HEIGHT_mon_mean_1992-01_ECCO_V4r6_latlon_0p50deg.nc``
- ``SEA_SURFACE_HEIGHT_snap_1992-01-01T000000_ECCO_V4r6_native_llc0090.nc``

7. Report Results
^^^^^^^^^^^^^^^^^

- **Pass**: Exit code 0 if ALL checks pass:

  - All DOIs are unique
  - All CSV entries have corresponding groupings
  - All groupings have DOI entries

- **Fail**: Exit code 1 if ANY of these conditions are true:

  - Duplicate DOIs found in CSV
  - CSV entries without corresponding groupings (orphans)
  - Expected datasets missing DOI entries


Output Examples
---------------

Successful Verification
^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: text

    INFO       Loading groupings from /path/to/metadata
    INFO       Loaded 3 1D groupings from groupings_for_1D_datasets.json
    INFO       Loaded 30 native groupings from groupings_for_native_datasets.json
    INFO       Loaded 14 latlon groupings from groupings_for_latlon_datasets.json
    INFO       Loading DOI CSV from /path/to/PODAAC_dataset_table.csv
    INFO       Loaded 99 DOI entries from /path/to/PODAAC_dataset_table.csv
    INFO
    INFO       ======================================================================
    INFO       DOI UNIQUENESS CHECK
    INFO       ======================================================================
    INFO       All 99 DOIs are unique
    INFO       ======================================================================
    INFO
    INFO       ======================================================================
    INFO       EXPECTED DATASETS (GROUPINGS × FREQUENCIES)
    INFO       ======================================================================
    INFO       [1D] - 3 expected datasets:
    INFO         - GLOBAL_MEAN_ATM_SURFACE_PRES
    INFO         - GLOBAL_MEAN_SEA_LEVEL
    INFO         - SBO_CORE_PRODUCTS
    INFO       [NATIVE] - 66 expected datasets:
    INFO         - SEA_SURFACE_HEIGHT [AVG_DAY]
    INFO         - SEA_SURFACE_HEIGHT [AVG_MON]
    INFO         - SEA_SURFACE_HEIGHT [SNAP]
    INFO         ...
    INFO
    INFO       ======================================================================
    INFO       REVERSE CHECK: CSV ENTRIES → GROUPINGS
    INFO       ======================================================================
    INFO       Found 2 known exceptions (e.g., GRID_GEOMETRY files)
    INFO       All 98 CSV entries correspond to groupings or known exceptions
    INFO       ======================================================================
    INFO
    INFO       ======================================================================
    INFO       VERIFICATION SUMMARY
    INFO       ======================================================================
    INFO       Total expected datasets: 96
    INFO         - With DOI entries: 96
    INFO         - Missing DOI entries: 0
    INFO       ======================================================================
    INFO       VERIFICATION PASSED: All checks successful
    INFO         - All DOIs are unique
    INFO         - All CSV entries have corresponding groupings
    INFO         - All groupings have DOI entries

Failed Verification - Duplicate DOIs
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: text

    INFO       ======================================================================
    INFO       DOI UNIQUENESS CHECK
    INFO       ======================================================================
    ERROR      Found 2 duplicate DOIs:
    ERROR
    ERROR      DOI: 10.5067/ECG5D-SSH46
    ERROR        Used by 2 datasets:
    ERROR          - SEA_SURFACE_HEIGHT_day_mean_1992-01-01_ECCO_V4r6_latlon_0p50deg.nc
    ERROR          - OCEAN_BOTTOM_PRESSURE_day_mean_1992-01-01_ECCO_V4r6_latlon_0p50deg.nc
    ERROR
    ERROR      VERIFICATION FAILED: 2 duplicate DOIs found

Failed Verification - Missing DOI Entries
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: text

    INFO       ======================================================================
    INFO       VERIFICATION SUMMARY
    INFO       ======================================================================
    INFO       Total expected datasets: 96
    INFO         - With DOI entries: 72
    INFO         - Missing DOI entries: 24
    INFO       ======================================================================
    WARNING
    WARNING    MISSING DOI ENTRIES:
    WARNING    ----------------------------------------------------------------------
    WARNING    [NATIVE] OCEAN_3D_MIXING_COEFFS [TIME-INVARIANT]
    WARNING        Name: ocean three-dimensional parameterized mixing coefficients
    WARNING        Search pattern: OCEAN_3D_MIXING_COEFFS_time-invariant_
    WARNING        Reason: No matching entries in CSV
    WARNING
    ERROR      VERIFICATION FAILED: 24 expected datasets missing DOI entries


Use Cases
---------

1. Pre-Publication Check
^^^^^^^^^^^^^^^^^^^^^^^^

Before submitting datasets to PO.DAAC, verify all groupings have DOIs:

.. code-block:: bash

    edp_verify_doi_coverage \
        --metadata-dir ./metadata \
        --csv-file ./metadata/PODAAC_dataset_table.csv

2. CI/CD Integration
^^^^^^^^^^^^^^^^^^^^

Add to continuous integration to catch missing DOIs automatically:

.. code-block:: bash

    # In your CI script
    if ! edp_verify_doi_coverage --metadata-dir ./metadata --csv-file ./podaac.csv --quiet; then
        echo "ERROR: Some datasets missing DOI entries"
        exit 1
    fi

3. Version Comparison
^^^^^^^^^^^^^^^^^^^^^

Check coverage differences between releases:

.. code-block:: bash

    # V4r5
    edp_verify_doi_coverage \
        --metadata-dir "ECCOv4 Release 5/metadata" \
        --csv-file "ECCOv4 Release 5/metadata/podaac.csv" > v4r5_report.txt

    # V4r6
    edp_verify_doi_coverage \
        --metadata-dir "ECCOv4 Release 6/metadata" \
        --csv-file "ECCOv4 Release 6/metadata/podaac.csv" > v4r6_report.txt

    # Compare
    diff v4r5_report.txt v4r6_report.txt


Input File Requirements
-----------------------

Groupings JSON Files
^^^^^^^^^^^^^^^^^^^^

Must contain a ``filename`` field in each grouping:

.. code-block:: json

    [
      {
        "name": "sea surface height",
        "fields": "SSH, SSHNOIBC, SSHIBC, ETAN",
        "filename": "SEA_SURFACE_HEIGHT",
        "dimension": "2D",
        "frequency": "AVG_DAY, AVG_MON, SNAP"
      }
    ]

DOI CSV File
^^^^^^^^^^^^

Must have column: ``DATASET.FILENAME``

Expected format:

.. code-block:: csv

    DATASET.LONG_NAME,DATASET.SHORT_NAME,DATASET.FILENAME,DATASET.PERSISTENT_ID
    "ECCO Sea Surface Height...",ECCO_L4_SSH_05DEG_DAILY_V4R6,SEA_SURFACE_HEIGHT_day_mean_1992-01-01_ECCO_V4r6_latlon_0p50deg.nc,10.5067/ECG5D-SSH46


Exit Codes
----------

- ``0``: Success - all checks pass:

  - All DOIs unique
  - All CSV entries have corresponding groupings
  - All groupings have DOI entries

- ``1``: Failure - one or more checks failed:

  - Duplicate DOIs found
  - Orphaned CSV entries found
  - Groupings missing DOI entries


Troubleshooting
---------------

"CSV missing required column: DATASET.FILENAME"
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The CSV file doesn't have the expected column name. Check the CSV header row.

"Groupings file not found"
^^^^^^^^^^^^^^^^^^^^^^^^^^

One or more groupings JSON files are missing from the metadata directory. Ensure:

- ``groupings_for_1D_datasets.json``
- ``groupings_for_native_datasets.json``
- ``groupings_for_latlon_datasets.json``

All exist in the specified directory.

False Positives: Grouping Has DOI But Tool Says Missing
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Check that the filename in the grouping JSON exactly matches the prefix used
in the CSV filenames.

**Grouping filename**: ``OCEAN_TEMPERATURE``

**CSV filename**: ``OCEAN_TEMP_day_mean_...`` ← **Won't match!**

Fix: Update grouping filename to ``OCEAN_TEMP`` or CSV filenames to start with
``OCEAN_TEMPERATURE_``.

Orphaned CSV Entries
^^^^^^^^^^^^^^^^^^^^

If CSV entries are flagged as orphans, investigate:

**Known exceptions** (automatically handled):

- ``GRID_GEOMETRY_*`` files - Grid metadata files recognized as exceptions

**Naming mismatches** (need to be fixed):

- CSV has ``OCEAN_VORT_DIV_KE`` but grouping has ``OCEAN_3D_VORT_DIV_KE``

  - Fix: Update CSV filenames to match grouping names
  - Or: Update grouping filenames to match CSV

**Missing groupings** (need to be added):

- CSV entries for new datasets that haven't had groupings defined yet

  - Fix: Add grouping definitions to the appropriate JSON file


Related Tools
-------------

- ``edp_create_job_task_list``: Uses these groupings to generate tasks
- ``edp_generate_datasets``: Produces the actual datasets
- ``utils/metadata_consistency/``: Various metadata validation scripts

