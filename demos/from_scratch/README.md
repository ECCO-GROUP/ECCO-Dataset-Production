# EDP From Scratch

This is.an example of how to run the EDP scripts locally from scratch for
execution in AWS Batch. The scripts can be run in sequential order to bootstrap
a completely new clean environment.

Execute each of the scripts from step 0 (`0_environment-prep.sh`) to step 6
(`6_batch-run.sh`) in a fresh working directory. This demo was tested with a
Python 3.14 installation.

## AWS Setup

This demo assumes that AWS has been preconfigured. To check that AWS is properly
configured, run the following command:

```
aws sts get-caller-identity
```

If the command returns successfully, your environment has been successfully
setup for AWS.

## Steps

### 0. Environment Prep

This step creates a fresh venv and clones the ECCO-Dataset-Production repos.

### 1. Docker Image Prep

This step builds the generate-datasets Docker image and pushes it to ECR for
later use.

### 