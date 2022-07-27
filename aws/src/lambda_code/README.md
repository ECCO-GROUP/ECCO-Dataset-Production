# **/lambda_code/**
Contains the following files:
- **app.py**
  - This is the file contains the *handler()* function which each lambda job calls when starting. From there, the main processing script is imported and ran using the *payload* passed when invoking the lambda job.
- **Dockerfile**
  - This is the Dockerfile used to create the lambda functions used for processing. This Dockerfile is structured such that when creating the image using this Dockerfile, your working directory must be */aws*
- **entry.sh**
  - Shellscript used for the entry point for each lambda job.
- **requirements.txt**
  - Python package names required for processing. This is file is read in the Dockerfile and the packages within are installed via pip to the lambda function's local directory.