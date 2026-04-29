
edp_subset_tasklists
====================

Creates subsets of ECCO tasklist JSON files for testing and quick production runs.
Supports multiple sampling strategies including temporal sampling, statistical sampling,
and custom index selection.

Overview
--------

``edp_subset_tasklists`` is a utility for generating representative subsets of
full production tasklists. This is useful for:

- **Testing:** Validate pipeline changes on small datasets before full production
- **Development:** Quick iterations during development without processing all timesteps
- **Debugging:** Isolate specific time periods or entries for investigation
- **Demos:** Create sample datasets for documentation or demonstrations

The tool supports 10 different sampling modes, from simple first/last selection
to sophisticated temporal distribution strategies.


Usage
-----

.. code-block:: bash

    edp_subset_tasklists INPUT_PATH --output_dir OUTPUT_DIR --mode MODE
                         [-n COUNT] [--step STEP] [--indices INDICES]
                         [--percent PERCENT] [--pattern PATTERN]
                         [--seed SEED] [-l LOG_LEVEL]


Arguments
---------

``INPUT_PATH``
    Path to input tasklist JSON file or directory containing multiple tasklist files.
    If a directory, all matching files (see ``--pattern``) will be processed.

``--output_dir``
    Directory where subset tasklist files will be written. Created if it doesn't exist.
    Required.

``--mode``
    Subset sampling mode. Required. Choices:

    - ``first``: Select first N entries
    - ``last``: Select last N entries
    - ``first-middle-last``: Select first, middle, and last entry (3 total)
    - ``random``: Select N random entries
    - ``every-nth``: Select every Nth entry (use ``--step``)
    - ``spread``: Evenly distribute N entries across entire range
    - ``bookends``: Select first N and last N entries
    - ``indices``: Select specific indices (use ``--indices``)
    - ``percentage``: Select X percent from start (use ``--percent``)
    - ``alternating``: Select every other entry

``-n, --count``
    Number of entries to select. Applies to: ``first``, ``last``, ``random``,
    ``spread``, and ``bookends`` modes.
    Default: ``12``

``--step``
    Step size for ``every-nth`` mode (select every Nth entry).
    Default: ``10``

``--indices``
    Comma-separated list of indices for ``indices`` mode.
    Example: ``"0,5,10,100,407"``

``--percent``
    Percentage of entries to select for ``percentage`` mode (0-100).
    Example: ``5`` for 5% of entries

``--pattern``
    File pattern to match when INPUT_PATH is a directory.
    Default: ``*.json``

``--seed``
    Random seed for reproducible ``random`` sampling.

``-l, --log``
    Set logging level. Choices: ``DEBUG``, ``INFO``, ``WARNING``, ``ERROR``,
    ``CRITICAL``.
    Default: ``INFO``


Entry Point
-----------

**Module:** ``ecco_dataset_production.apps.subset_tasklists``

**Function:** ``main()``


Input Files
-----------

+---------------------------------------+---------------------------------------+
| File                                  | Description                           |
+=======================================+=======================================+
| ``{dataset}_{grid}_{freq}_jobs.json`` | Tasklist JSON file containing array   |
|                                       | of dataset generation tasks           |
+---------------------------------------+---------------------------------------+

**Tasklist File Format (JSON array):**

.. code-block:: json

    [
      {
        "granule": "s3://.../OCEAN_VELOCITY_mon_mean_1992-01_...",
        "variables": { ... },
        "ecco_cfg_loc": "s3://.../config.yaml",
        "dynamic_metadata": { ... }
      },
      {
        "granule": "s3://.../OCEAN_VELOCITY_mon_mean_1992-02_...",
        "variables": { ... },
        "ecco_cfg_loc": "s3://.../config.yaml",
        "dynamic_metadata": { ... }
      }
    ]


Output Files
------------

+---------------------------------------+---------------------------------------+
| File                                  | Description                           |
+=======================================+=======================================+
| ``{dataset}_{grid}_{freq}_jobs.json`` | Subset tasklist with same filename    |
|                                       | as input, containing selected entries |
+---------------------------------------+---------------------------------------+

Output files maintain the same structure and naming as inputs, but contain
only the selected subset of entries.


Examples
--------

**Select first 12 entries (default):**

.. code-block:: bash

    edp_subset_tasklists tasklists/OCEAN_VELOCITY_native_AVG_MON_jobs.json \
        --output_dir ./test_tasklists \
        --mode first

**Select last 5 entries:**

.. code-block:: bash

    edp_subset_tasklists tasklists/OCEAN_VELOCITY_native_AVG_MON_jobs.json \
        --output_dir ./test_tasklists \
        --mode last -n 5

**Select first, middle, and last entry (temporal coverage):**

.. code-block:: bash

    edp_subset_tasklists tasklists/ \
        --output_dir ./test_tasklists \
        --mode first-middle-last

**Select 10 evenly-distributed entries:**

.. code-block:: bash

    edp_subset_tasklists tasklists/ \
        --output_dir ./test_tasklists \
        --mode spread -n 10

**Select every 50th entry:**

.. code-block:: bash

    edp_subset_tasklists tasklists/ \
        --output_dir ./test_tasklists \
        --mode every-nth --step 50

**Select 20 random entries (reproducible):**

.. code-block:: bash

    edp_subset_tasklists tasklists/ \
        --output_dir ./test_tasklists \
        --mode random -n 20 --seed 42

**Select first 3 and last 3 entries:**

.. code-block:: bash

    edp_subset_tasklists tasklists/ \
        --output_dir ./test_tasklists \
        --mode bookends -n 3

**Select specific indices:**

.. code-block:: bash

    edp_subset_tasklists tasklists/OCEAN_VELOCITY_native_AVG_MON_jobs.json \
        --output_dir ./test_tasklists \
        --mode indices --indices "0,10,100,200,407"

**Select 5% of entries:**

.. code-block:: bash

    edp_subset_tasklists tasklists/ \
        --output_dir ./test_tasklists \
        --mode percentage --percent 5

**Process only monthly files:**

.. code-block:: bash

    edp_subset_tasklists tasklists/ \
        --output_dir ./test_tasklists \
        --mode last -n 12 \
        --pattern "*MON*"

**Process all files alternating (every other entry):**

.. code-block:: bash

    edp_subset_tasklists tasklists/ \
        --output_dir ./test_tasklists \
        --mode alternating


Sampling Modes in Detail
-------------------------

first
^^^^^

Selects the first N entries from the tasklist.

**Use case:** Test processing of early timesteps (e.g., 1992 data)

**Formula:** ``entries[0:N]``

**Example:** For N=12 on 408-entry tasklist, selects entries 0-11
(Jan 1992 - Dec 1992 for monthly data)


last
^^^^

Selects the last N entries from the tasklist.

**Use case:** Test processing of recent timesteps (e.g., 2025 data)

**Formula:** ``entries[-N:]``

**Example:** For N=12 on 408-entry tasklist, selects entries 396-407
(Jan 2025 - Dec 2025 for monthly data)


first-middle-last
^^^^^^^^^^^^^^^^^

Selects exactly three entries: first, middle, and last.

**Use case:** Quick temporal coverage check across the entire time range

**Formula:** ``[entries[0], entries[total//2], entries[-1]]``

**Example:** For 408-entry tasklist, selects entries [0, 204, 407]
(Jan 1992, Jan 2009, Dec 2025)


random
^^^^^^

Randomly samples N entries from the tasklist. Use ``--seed`` for reproducibility.

**Use case:** Statistical validation, unbiased sampling

**Formula:** ``random.sample(entries, N)``

**Example:** For N=20 with seed=42, always selects the same 20 random entries


every-nth
^^^^^^^^^

Selects every Nth entry (systematic sampling).

**Use case:** Regular temporal intervals (e.g., every 50 months ≈ 4 years)

**Formula:** ``entries[::N]``

**Example:** For step=50 on 408-entry tasklist, selects entries
[0, 50, 100, 150, 200, 250, 300, 350, 400] (9 entries)


spread
^^^^^^

Evenly distributes N entries across the entire range with mathematically
equal spacing.

**Use case:** Representative temporal coverage, time-series validation

**Formula:** For each position i in [0, N-1]:
``index = i × (total - 1) / (N - 1)``

**Example:** For N=10 on 408-entry tasklist:

- Entry 0 (Jan 1992)
- Entry 45 (Oct 1995)
- Entry 90 (Jul 1999)
- Entry 135 (Apr 2003)
- Entry 180 (Jan 2007)
- Entry 226 (Nov 2010)
- Entry 271 (Aug 2014)
- Entry 316 (May 2018)
- Entry 361 (Feb 2022)
- Entry 407 (Dec 2025)

This provides consistent coverage from start to end, ensuring one sample
from each ~10% of the time range.


bookends
^^^^^^^^

Selects the first N and last N entries (edges only).

**Use case:** Test both historical and recent periods without middle data

**Formula:** ``entries[:N] + entries[-N:]``

**Example:** For N=3 on 408-entry tasklist, selects entries
[0, 1, 2, 405, 406, 407] (6 total)


indices
^^^^^^^

Selects entries at specific user-provided indices.

**Use case:** Target specific known timesteps or problematic periods

**Formula:** ``[entries[i] for i in user_indices if 0 <= i < total]``

**Example:** ``--indices "0,10,100,200,407"`` selects exactly those 5 entries


percentage
^^^^^^^^^^

Selects the first X% of entries.

**Use case:** Process a fixed fraction of the dataset

**Formula:** ``entries[:int(total × percent / 100)]``

**Example:** For percent=5 on 408-entry tasklist, selects first 20 entries
(5% × 408 ≈ 20)


alternating
^^^^^^^^^^^

Selects every other entry (equivalent to ``every-nth`` with step=2).

**Use case:** 50% sampling with regular intervals

**Formula:** ``entries[::2]``

**Example:** For 408-entry tasklist, selects 204 entries (all even indices)


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
           subset_func["<b>subset_tasklists()</b>"]
           validate_path["Validate input path<br/>(file or directory)"]
           create_dir["Create output directory"]
           glob_files["Collect input files<br/>(glob pattern matching)"]
       end

       subgraph process["PROCESSING"]
           file_loop["For each tasklist file"]
           read_json["Load JSON array"]
           subset_entries["<b>subset_entries()</b><br/>Apply sampling mode"]
           write_json["Write subset to output"]
       end

       subgraph modes["SAMPLING MODES"]
           mode_check{"Check mode"}
           first_mode["first: [0:N]"]
           last_mode["last: [-N:]"]
           fml_mode["first-middle-last:<br/>[0, mid, -1]"]
           random_mode["random:<br/>random.sample(N)"]
           every_mode["every-nth: [::step]"]
           spread_mode["spread:<br/>evenly distribute N"]
           bookends_mode["bookends:<br/>[0:N] + [-N:]"]
           indices_mode["indices:<br/>[i for i in list]"]
           percent_mode["percentage:<br/>[0:X%]"]
           alt_mode["alternating: [::2]"]
       end

       init --> setup
       setup --> process
       process --> modes

       main --> parser --> parse
       subset_func --> validate_path --> create_dir --> glob_files
       file_loop --> read_json --> subset_entries --> write_json
       write_json --> file_loop

       subset_entries --> mode_check
       mode_check --> first_mode
       mode_check --> last_mode
       mode_check --> fml_mode
       mode_check --> random_mode
       mode_check --> every_mode
       mode_check --> spread_mode
       mode_check --> bookends_mode
       mode_check --> indices_mode
       mode_check --> percent_mode
       mode_check --> alt_mode

       style init fill:#e3f2fd,stroke:#1565c0,stroke-width:2px,color:#0d47a1
       style setup fill:#e8f5e9,stroke:#2e7d32,stroke-width:2px,color:#1b5e20
       style process fill:#fff3e0,stroke:#e65100,stroke-width:2px,color:#bf360c
       style modes fill:#f3e5f5,stroke:#6a1b9a,stroke-width:2px,color:#4a148c

       style main fill:#bbdefb,stroke:#1565c0,color:#0d47a1
       style parser fill:#bbdefb,stroke:#1565c0,color:#0d47a1
       style parse fill:#bbdefb,stroke:#1565c0,color:#0d47a1
       style subset_func fill:#c8e6c9,stroke:#2e7d32,color:#1b5e20
       style validate_path fill:#c8e6c9,stroke:#2e7d32,color:#1b5e20
       style create_dir fill:#c8e6c9,stroke:#2e7d32,color:#1b5e20
       style glob_files fill:#c8e6c9,stroke:#2e7d32,color:#1b5e20
       style file_loop fill:#ffe0b2,stroke:#e65100,color:#bf360c
       style read_json fill:#ffe0b2,stroke:#e65100,color:#bf360c
       style subset_entries fill:#ffe0b2,stroke:#e65100,color:#bf360c
       style write_json fill:#ffe0b2,stroke:#e65100,color:#bf360c

       linkStyle default stroke:#333,stroke-width:2px


Use Cases
---------

Pipeline Testing
^^^^^^^^^^^^^^^^

Before running full production, validate changes on a small subset:

.. code-block:: bash

    # Create test subset (first year of monthly data)
    edp_subset_tasklists /data/tasklists/ \
        --output_dir /data/test_tasklists \
        --mode first -n 12

    # Run generation on test subset
    edp_generate_datasets --tasklist /data/test_tasklists/OCEAN_VELOCITY_native_AVG_MON_jobs.json


Temporal Coverage Validation
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Verify processing works across entire time range:

.. code-block:: bash

    # Sample 10 evenly-spaced timesteps across 34 years
    edp_subset_tasklists /data/tasklists/ \
        --output_dir /data/coverage_test \
        --mode spread -n 10


Edge Case Testing
^^^^^^^^^^^^^^^^^

Test specific problematic periods:

.. code-block:: bash

    # Test first month, transition period, and last month
    edp_subset_tasklists /data/tasklists/ \
        --output_dir /data/edge_tests \
        --mode first-middle-last


Performance Benchmarking
^^^^^^^^^^^^^^^^^^^^^^^^

Benchmark processing speed on representative sample:

.. code-block:: bash

    # Random 5% sample for performance testing
    edp_subset_tasklists /data/tasklists/ \
        --output_dir /data/benchmark \
        --mode percentage --percent 5 --seed 42


Quick Demonstration Dataset
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Create small demo datasets spanning the full time range:

.. code-block:: bash

    # Bookends: first 6 months and last 6 months
    edp_subset_tasklists /data/tasklists/ \
        --output_dir /data/demo \
        --mode bookends -n 6


Key Module Dependencies
-----------------------

.. code-block:: text

    ecco_dataset_production.apps.subset_tasklists
        |
        +---> json (tasklist parsing/writing)
        |
        +---> random (random sampling mode)
        |
        +---> pathlib (file path handling)
        |
        +---> logging


Notes
-----

- All modes preserve the original structure and format of tasklist entries
- Output files use the same filenames as input files
- When processing directories, files are processed in sorted order
- Empty tasklists (0 entries) are handled gracefully
- Invalid indices in ``indices`` mode are skipped with a warning
- The ``spread`` mode uses mathematical distribution for precise coverage
- The ``random`` mode uses Python's ``random.sample()`` without replacement
- For reproducible random sampling, always specify ``--seed``

