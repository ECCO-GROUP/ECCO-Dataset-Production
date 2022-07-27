# **/aws/**
Contains all the code, data, metadata, config, etc. files necessary for processing of ECCO datasets.

## **Structure**
    /aws/
        configs/
        ecco_grids/
        logs/
        mapping_factors/
        metadata/
        src/

## **Descriptions**
### configs/
- Contains all the configuration files which control the processing. The files are as follows:
  - **aws_config.yaml**
    - Control and option values related to AWS processes (memory, bucket(s), etc.)
  - **product_generation_config.yaml**
    - General processing control and option values (paths, grid, etc.)
  - **jobs.txt**
    - List of jobs to process, includes grouping number, product type, frequency, and number of timesteps to process (eg. 0,latlon,AVG_MON,all)


### ecco_grids/
- Contains all the necessary grid data and meta files. This includes the ECCO grid geometry file as well. (eg. AngleCS.data, AngleCS.meta, GRID_GEOMETRY_ECCO_V4r4_native_llc0090.nc)


### logs/
- Contains all the log files produced from processing via AWS Lambda, organized via date of run


### mapping_factors/
- Contains all the mapping files. This includes the "original" mapping files for *2D*, *3D*, and *all*, along with the *lan masks*, the *latlon_grid* values, and the *sparse matrices*.


### metadata/
- Contains all the metadata JSON/CSV files. This includes *groupings_for_latlon_datasets.json*, *global_metadata_for_latlon_datasets.json*, etc.


### src/
- Contains all the code files required for processing. The files, and structure is as follows:
  - **ecco_cloud_utils**
    - ECCO_ACCESS ecco_cloud_utils functions needed for processing. This only includes the necessary functions, all extraneous functions/imports/etc have been removed.
  - **ecco_v4_py**
    - ECCO_v4_py functions needed for processing. This has been modified in a similar fashion to *ecco_cloud_utils*
  - **lambda_code**
    - *Dockerfile*, *app.py*, *entry.sh*, *requirements.txt* files needed to build and initialize the AWS lambda functions.
  - **utils**
    - Contains multiple code files containing utility functions used for pre-processing, processing, and post-processing.
      - ***aws_login/***
        - Contains scripts used to login and get credentials for AWS
      - ***aws_utils.py***
        - Contains functions used for AWS processing (lambda function handling, logging, etc.)
      - ***create_factors_utils.py***
        - Contains functions used for the creation of the mapping factors
      - ***gen_netcdf_utils.py***
        - Contains functions used during processing (transformation, metadata handling, etc.)
  - **ecco_gen_for_podaac_cloud.py**
    - The primary code file, responsible for the processing of the job passed to the *generate_netcdfs()* function
  - **master_script.py**
    - The point of interaction between the user and the processing code. This script is how the user starts, controls, etc. all processing.




