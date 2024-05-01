
ECCO task list creation tests
-----------------------------

Each of the included tests generates a task list from a jobs description file
(`jobs_example.txt`), and outputs the resulting list to a corresponding json
file.

    - `edp_create_job_file_list_test_1.sh`: creates a task list based on
      locally-stored ECCO results

    - `edp_create_job_file_list_test_2.sh`: creates a task list based on limited
      AWS S3-stored ECCO results (requires a prior data upload; see comments)

    - `edp_create_job_file_list_test_3.sh`: creates a task list based on full
      AWS S3-stored ECCO results (requires presence of ECCO results, and
      retrieving S3 bucket object list may take some time (tens of minutes)
      depending on network connection speed; see comments)

Note that all examples reference a local config file,
`product_generation_config.yaml`, and those that reference AWS S3-stored data
require an AWS account with login privileges.

