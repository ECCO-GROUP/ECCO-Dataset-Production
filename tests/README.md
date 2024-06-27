
ECCO Dataset Production system-level tests
==========================================

Functionality
-------------

    - edp_aws_s3_sync: Tests various AWS S3 data sync operations: upload,
      download, and copy within AWS.

    - edp_create_job_task_list: Tests creation of task lists based on high-level
      job definition files.

Test data:
----------

    - ./data/config/: ECCO Dataset Production configuration file examples.

    - ./data/ecco_granules/: A selection of representative ECCO Dataset
      Production results data.

    - ./data/ecco_grids/: ECCO grid definitions.

    - ./data/ecco_results/: Sample ECCO MDS results data.  See also
      download_selected_data.sh helper script that downloads selected MDS files
      per the data requirements in edp_create_job_task_list (above).

Notes
-----

	- Tests that rely on AWS S3 access require an AWS account with login privileges.
