
edp_aws_s3_sync
===============

A Python wrapper for AWS CLI S3 sync operations with multiprocessing support
for uploads and SSO authentication handling.

Overview
--------

``edp_aws_s3_sync`` provides convenient synchronization of ECCO datasets and
related files with AWS S3 storage. It supports local-to-remote, remote-to-local,
and remote-to-remote synchronization with optional multiprocessing for
local-to-remote operations.

Key features:

- Multiprocessing support for parallel uploads (local → S3)
- SSO authentication handling for institutional environments
- Dry-run mode for previewing operations
- Directory structure preservation


Usage
-----

.. code-block:: bash

    edp_aws_s3_sync [--src SRC] [--dest DEST] [--nproc NPROC]
                    [--keygen KEYGEN] [--profile PROFILE]
                    [--dryrun] [-l LOG_LEVEL]


Arguments
---------

``--src``
    Source location (local path or AWS S3 URI).
    Default: ``.``

``--dest``
    Destination location (local path or AWS S3 URI).
    Default: ``.``

``--nproc``
    Maximum number of parallel sync processes for local-to-remote operations.
    Default: ``1``

``--keygen``
    For AWS SSO environments, path to the federated login key generation
    script.

``--profile``
    AWS credential profile name (e.g., ``saml-pub``, ``default``).

``--dryrun``
    Enable AWS S3 CLI ``--dryrun`` mode to preview operations without
    executing them.

``-l, --log``
    Set logging level. Choices: ``DEBUG``, ``INFO``, ``WARNING``, ``ERROR``,
    ``CRITICAL``.
    Default: ``WARNING``


Entry Point
-----------

**Module:** ``ecco_dataset_production.apps.aws_s3_sync``

**Function:** ``main()``


Input Files
-----------

+---------------------------+--------------------------------------------------+
| File Type                 | Description                                      |
+===========================+==================================================+
| Source files/directories  | Any files at the ``--src`` location to be synced |
|                           | (local path or S3 URI)                           |
+---------------------------+--------------------------------------------------+

**Source Location Examples:**

- Local directory: ``/output/datasets/V4r5/``
- S3 URI: ``s3://ecco-datasets/V4r5/``


Output Files
------------

+---------------------------+--------------------------------------------------+
| File Type                 | Description                                      |
+===========================+==================================================+
| Synced files/directories  | Mirror of source files at ``--dest`` location    |
|                           | (local path or S3 URI)                           |
+---------------------------+--------------------------------------------------+

**Destination Location Examples:**

- Local directory: ``/local/datasets/V4r5/``
- S3 URI: ``s3://my-bucket/datasets/V4r5/``

**Note:** The sync operation preserves directory structure and only transfers
files that are new or modified (based on size and timestamp).


Examples
--------

**Upload local directory to S3:**

.. code-block:: bash

    edp_aws_s3_sync \
        --src /output/datasets/V4r5 \
        --dest s3://ecco-datasets/V4r5 \
        --nproc 4 \
        -l INFO

**Download from S3 to local:**

.. code-block:: bash

    edp_aws_s3_sync \
        --src s3://ecco-datasets/V4r5 \
        --dest /local/datasets/V4r5 \
        -l INFO

**Preview sync operation (dry run):**

.. code-block:: bash

    edp_aws_s3_sync \
        --src /output/datasets \
        --dest s3://my-bucket/datasets \
        --dryrun

**Sync with SSO authentication:**

.. code-block:: bash

    edp_aws_s3_sync \
        --src /output/datasets \
        --dest s3://ecco-production/datasets \
        --keygen /usr/local/bin/aws-login-pub.darwin.amd64 \
        --profile saml-pub \
        --nproc 8

**Sync between S3 buckets:**

.. code-block:: bash

    edp_aws_s3_sync \
        --src s3://source-bucket/datasets \
        --dest s3://dest-bucket/datasets


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

       subgraph detect["MODE DETECTION"]
           aws_sync["<b>aws_s3_sync()</b>"]
           mode_check{{"Sync mode?"}}
       end

       subgraph local_s3["LOCAL → S3 (Multiprocess)"]
           sync_local["<b>sync_local_to_remote()</b>"]
           walk["Walk source directory tree<br/>(bottom-up)"]
           dir_loop["For each directory"]
           check_block["Check for blocking child syncs"]
           wait_slot["Wait for process slot<br/>(nproc limit)"]
           update_creds1["update_credentials()<br/>(if keygen provided)"]
           spawn["Spawn subprocess:<br/>aws s3 sync"]
           add_list["Add to process list"]
           wait_all["Wait for all processes<br/>to complete"]
       end

       subgraph remote["S3 → S3 or S3 → LOCAL"]
           sync_remote["<b>sync_remote_to_remote_or_local()</b>"]
           update_creds2["update_credentials()<br/>(if keygen provided)"]
           exec_sync["Execute single subprocess:<br/>aws s3 sync"]
           wait_done["Wait for completion"]
       end

       init --> detect

       main --> parser --> parse
       aws_sync --> mode_check
       mode_check -->|"Local → S3"| local_s3
       mode_check -->|"S3 → S3<br/>S3 → Local"| remote
       sync_local --> walk --> dir_loop
       dir_loop --> check_block --> wait_slot --> update_creds1 --> spawn --> add_list
       add_list --> dir_loop
       dir_loop --> wait_all
       sync_remote --> update_creds2 --> exec_sync --> wait_done

       style init fill:#e3f2fd,stroke:#1565c0,stroke-width:2px,color:#0d47a1
       style detect fill:#e8f5e9,stroke:#2e7d32,stroke-width:2px,color:#1b5e20
       style local_s3 fill:#fff3e0,stroke:#e65100,stroke-width:2px,color:#bf360c
       style remote fill:#f3e5f5,stroke:#7b1fa2,stroke-width:2px,color:#4a148c

       style main fill:#bbdefb,stroke:#1565c0,color:#0d47a1
       style parser fill:#bbdefb,stroke:#1565c0,color:#0d47a1
       style parse fill:#bbdefb,stroke:#1565c0,color:#0d47a1
       style aws_sync fill:#c8e6c9,stroke:#2e7d32,color:#1b5e20
       style mode_check fill:#c8e6c9,stroke:#2e7d32,color:#1b5e20
       style sync_local fill:#ffe0b2,stroke:#e65100,color:#bf360c
       style walk fill:#ffe0b2,stroke:#e65100,color:#bf360c
       style dir_loop fill:#ffe0b2,stroke:#e65100,color:#bf360c
       style check_block fill:#ffe0b2,stroke:#e65100,color:#bf360c
       style wait_slot fill:#ffe0b2,stroke:#e65100,color:#bf360c
       style update_creds1 fill:#ffe0b2,stroke:#e65100,color:#bf360c
       style spawn fill:#ffe0b2,stroke:#e65100,color:#bf360c
       style add_list fill:#ffe0b2,stroke:#e65100,color:#bf360c
       style wait_all fill:#ffe0b2,stroke:#e65100,color:#bf360c
       style sync_remote fill:#e1bee7,stroke:#7b1fa2,color:#4a148c
       style update_creds2 fill:#e1bee7,stroke:#7b1fa2,color:#4a148c
       style exec_sync fill:#e1bee7,stroke:#7b1fa2,color:#4a148c
       style wait_done fill:#e1bee7,stroke:#7b1fa2,color:#4a148c

       linkStyle default stroke:#333,stroke-width:2px


Detailed Flow Description
-------------------------

1. Sync Mode Detection
^^^^^^^^^^^^^^^^^^^^^^

The ``aws_s3_sync()`` function determines the sync mode by checking if source
and destination are S3 URIs:

+------------------+------------------+----------------------------------+
| Source           | Destination      | Mode                             |
+==================+==================+==================================+
| Local path       | S3 URI           | Local-to-remote (multiprocess)   |
+------------------+------------------+----------------------------------+
| S3 URI           | S3 URI           | Remote-to-remote (single process)|
+------------------+------------------+----------------------------------+
| S3 URI           | Local path       | Remote-to-local (single process) |
+------------------+------------------+----------------------------------+

2. Local-to-Remote Sync (Upload)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

For uploads, the script uses a sophisticated multiprocessing approach:

**Directory Traversal:**
   The source directory is walked bottom-up (``topdown=False``), ensuring
   child directories are synced before their parents.

**Process Management:**
   - Maintains a list of running subprocess handles
   - Tracks blocking processes (child directories still syncing)
   - Respects the ``nproc`` limit for concurrent operations

**Credential Refresh:**
   Before each sync subprocess, credentials are refreshed using the provided
   ``keygen`` script (important for long-running operations where SSO tokens
   may expire).

**Subprocess Spawning:**
   Each directory sync is executed as:

   .. code-block:: bash

       aws s3 sync --quiet <local_dir> <s3_uri> [--profile <profile>] [--dryrun]

3. Remote Sync Operations
^^^^^^^^^^^^^^^^^^^^^^^^^

For S3-to-S3 or S3-to-local operations, a simpler single-process approach
is used:

- Credentials are refreshed once at the start
- A single ``aws s3 sync`` command is executed
- The script waits for completion


Key Module Dependencies
-----------------------

.. code-block:: text

    ecco_dataset_production.apps.aws_s3_sync
        |
        +---> ecco_dataset_production.aws.ecco_aws_s3_sync
                |
                +---> ecco_dataset_production.aws.ecco_aws (is_s3_uri utility)
                |
                +---> subprocess (AWS CLI execution)
                |
                +---> os (directory walking)


Error Handling
--------------

- SSO credential generation failures exit with error code 1
- Invalid source/destination combinations log an error and exit
- Individual sync process failures are logged with return codes


Notes
-----

- Both source and destination must exist prior to running
- S3 buckets must be created beforehand (e.g., using ``aws s3 mb``)
- The ``--quiet`` flag is always passed to reduce AWS CLI output
- Multiprocessing is only available for local-to-remote operations

