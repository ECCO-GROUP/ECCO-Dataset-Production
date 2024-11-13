
# ECCO task list creation (only) tests

Each of the included tests generates a task list from a jobs description file
(`jobs_example.txt`), and outputs the resulting list to a corresponding json
file.

- `edp_create_job_file_list_test_1.sh`: creates a task list based on
  locally-stored ECCO results. Compare with results saved in
  `edp_create_job_file_list_test_1.json.sav`.

- `edp_create_job_file_list_test_2.sh`: creates a task list based on
  limited AWS S3-stored ECCO results (requires a prior data upload;
  see Remarks). Compare with results saved in
  `edp_create_job_file_list_test_2.json.sav`.

- `edp_create_job_file_list_test_3.sh`: creates a task list based on
  full AWS S3-stored ECCO results (requires presence of ECCO results,
  and retrieving S3 bucket object list may take some time (tens of
  minutes) depending on network connection speed; see Remarks).
  Compare with results saved in
  `edp_create_job_file_list_test_3.json.sav`.

## Remarks:

- Note that all examples reference a local config file, (currently
  `../../processing/configs/product_generation_config_updated.yaml`),
  and those that reference AWS S3-stored data require an AWS account
  with login privileges.

