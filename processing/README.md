# **/processing/**
Contains all the code, data, metadata, config, etc. files necessary for processing of ECCO datasets either locally (with local files or AWS S3 sourced files) or via AWS Lambda.

## **Structure**
    /processing/
        configs/
        ecco_grids/
        logs/
        mapping_factors/
        metadata/
        misc_scripts/
        src/
        tmp/
        ecr_push.sh

## **Descriptions**
### **configs/**
- Contains all the configuration files which control the processing, which are as follows:
  - **aws_config.yaml**
    - Control and option values related to AWS processes (memory, bucket(s), etc.)
  - **product_generation_config.yaml**
    - General processing control and option values (paths, grid, etc.)
  - **jobs.txt**
    - List of jobs to process, includes grouping number, product type, frequency, and number of timesteps to process (eg. 0,latlon,AVG_MON,all)
  - **created_jobs.txt**
    - Jobs file created automatically from user prompts when the user passes the "--create_jobs" argument to "master_script.py"
  - **README.md**
    - Provides more information on the files contained within configs/

### **ecco_grids/**
- Contains all the necessary grid data and meta files. This includes the ECCO grid geometry file as well. (eg. AngleCS.data, AngleCS.meta, GRID_GEOMETRY_ECCO_V4r4_native_llc0090.nc). Organized by ECCO version.

### **logs/**
- Contains all the log files produced from processing via AWS Lambda, organized by date of run

### **mapping_factors/**
- Contains all the mapping files. This includes the "original" mapping files for *2D*, *3D*, and *all*, along with the *land masks*, the *latlon_grid* values, and the *sparse matrices*. Organized by ECCO version.
  - **README.md**
    - Provdes more information on the files contained within mapping_factors/

### **metadata/**
- Contains all the metadata JSON/CSV files. This includes *groupings_for_latlon_datasets.json*, *global_metadata_for_latlon_datasets.json*, etc. Organized by ECCO version.
  - **README.md**
    - Provdes more information on the files contained within metadata/

### **misc_scripts/**
- Contains misc scripts related to ECCO Dataset Production. These scripts have not not been generalized to be be user agnostic, so use at your own risk.
    - **log_review.py**
        - Produces plots of time vs cost for provided AWS Lambda log files produced by the processing code
    - **s3_costs.py**
        - Simple script that determines the monthly cost for a provided size of data on all S3 options

### **src/**
- Contains all the code files required for processing. The files, and structure is as follows:
  - **ecco_code/**
    - ECCO python functions needed for processing. These files (except for __init__.py and .gitignore) have been copied from the ecco_code_dir with ecco_code_name values given in product_generation_config.yaml.
  - **lambda_code/**
    - *Dockerfile*, *app.py*, *entry.sh*, *requirements.txt* files needed to build and initialize the AWS lambda functions.
  - **utils**
    - Contains multiple code files containing utility functions used for pre-processing, processing, and post-processing.
    - **aws_login/**
      - Directory containing scripts needed for logging into AWS and getting the necessary AWS credentials. See https://github.jpl.nasa.gov/cloud/Access-Key-Generation for a description of the files used
    - **ecco_cloud_utils/**
      - A collection of files and functions from ECCO-ACCESS. These are only used in the creation of the mapping factors via master_script.py. These files (except for the __init__.py and .gitignore files) are copied from the local ECCO-ACCESS repository pointed to by the ecco_access_dir value in product_generation_config.yaml.
    - **credentials_utils.py**
      - Contains functions necessary for getting the user's AWS credentials
    - **file_utils.py**
      - Contains functions for getting the available files either from S3 or from a local directory
    - **gen_netcdf_utils.py**
      - Contains functions used while processing ECCO granules into netCDFs for PODAAC
    - **jobs_utils.py**
      - Contains functions for creating, calculating, and running jobs for processing
    - **lambda_utils.py**
      - Contains functions for updating, creating, and invoking AWS Lambda functions
    - **logging_utils.py**
      - Contains functions for getting, processing, and saving AWS Lambda logs via AWS CloudWatch
    - **mapping_factors_utils.py**
      - Contains functions for creating and getting the mapping factors (factors, land masks, sparse matrices, etc.)
  - **ecco_gen_for_podaac_cloud.py**
    - The primary code file, responsible for the processing of the job passed to the *generate_netcdfs()* function
  - **master_script.py**
    - The point of interaction between the user and the processing code. This script is how the user starts, controls, etc. all processing.
  - **README.md**
    - Provdes more information on the files contained within src/

### **tmp/**
- Contains all the temporary model granule files and processed netCDF datasets when running locally. This directory may not be present at all times.

### **ecr_push.sh**
- Shell script to update AWS ECR image with the Docker image built from the Dockerfile in /processing/src/lambda_code/{ecco_version}/
