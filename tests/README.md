
ECCO Dataset Production system-level tests:
===========================================

Functionality:
--------------

    - edp_create_job_file_list: Tests creation of task lists based on high-level
      job definition files.

    - edp_aws_s3_sync: Tests various AWS S3 data sync operations: upload,
      download, and copy within AWS.

Test data:
----------

    - ecco_model_granules: Directory containing some representative ECCO results
      data, used in the above.
