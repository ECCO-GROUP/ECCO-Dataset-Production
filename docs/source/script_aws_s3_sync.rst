
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

.. code-block:: text

    main()
      |
      +---> create_parser()
      |       |
      |       +---> Define CLI arguments (src, dest, nproc, keygen, profile, dryrun, log)
      |
      +---> Parse command-line arguments
      |
      +---> aws_s3_sync()  [ecco_dataset_production.aws.ecco_aws_s3_sync]
              |
              +---> Determine sync mode based on src/dest types
              |
              +---> [If local -> S3]
              |       |
              |       +---> sync_local_to_remote()
              |               |
              |               +---> Walk source directory tree (bottom-up)
              |               |
              |               +---> For each directory:
              |                       |
              |                       +---> Check for blocking child syncs
              |                       |
              |                       +---> Wait for process slot (nproc limit)
              |                       |
              |                       +---> update_credentials() [if keygen provided]
              |                       |
              |                       +---> Spawn subprocess: aws s3 sync
              |                       |
              |                       +---> Add to process list
              |               |
              |               +---> Wait for all processes to complete
              |
              +---> [If S3 -> S3 or S3 -> local]
                      |
                      +---> sync_remote_to_remote_or_local()
                              |
                              +---> update_credentials() [if keygen provided]
                              |
                              +---> Execute single subprocess: aws s3 sync
                              |
                              +---> Wait for completion


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

