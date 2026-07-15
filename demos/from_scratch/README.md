# EDP From Scratch Demo

This demo shows how to run the ECCO Dataset Production (EDP) workflow from scratch
for execution in AWS Batch. The scripts can be run in sequential order to bootstrap
a completely new clean environment and process ECCO datasets.

Execute each of the scripts from step 0 (`0_environment-prep.sh`) to step 5
(`5-batch-run.sh`) in a fresh working directory. This demo was tested with Python 3.10+.

## Prerequisites

### AWS Setup

This demo assumes AWS has been preconfigured with appropriate credentials and permissions.
To verify AWS is properly configured, run:

```bash
aws sts get-caller-identity
```

If the command returns your AWS account details, your environment is ready.

### Required AWS Resources

Before running this demo, ensure the following AWS resources exist:

- **ECR Repository**: For storing the Docker image (e.g., `ecco-dataset-production`)
- **S3 Buckets**: For source data, destination datasets, and tasklists
- **AWS Batch**: Job definition and job queue configured
  - Recommended: Non-spot queue for production runs
  - Optional: Spot queue for cost savings on non-critical jobs

### Local Dependencies

- **Git**: For cloning repositories
- **Python 3.10+**: For running EDP scripts
- **Docker**: For building container images
- **AWS CLI**: For interacting with AWS services

## Workflow Overview

The demo follows the standard EDP workflow:

```
0. Environment Setup
   ↓
1. Docker Image Build & Push
   ↓
2. Generate Job Files (from metadata groupings)
   ↓
3. Create Task Lists (scan source data)
   ↓
4. Create Test Subset (optional - first/middle/last)
   ↓
5. Submit to AWS Batch (process datasets)
```

## Steps

### 0. Environment Prep

**Script**: `0_environment-prep.sh`

Creates a fresh Python virtual environment and clones required repositories.

**What it does**:
- Clones `ECCO-Dataset-Production` repository
- Clones `ECCO-v4-Configurations` repository (metadata)
- Creates Python virtual environment (`venv/`)
- Installs ECCO Dataset Production package

**Run**:
```bash
bash 0_environment-prep.sh
```

**Expected output**:
- `ECCO-Dataset-Production/` directory
- `ECCO-v4-Configurations/` directory
- `venv/` directory with installed packages

---

### 1. Docker Image Prep

**Script**: `1_docker-image-prep.sh`

Builds the dataset generation Docker image and pushes it to Amazon ECR.

**What it does**:
- Builds Docker image using `docker/docker_aws_build.sh`
- Pushes image to ECR using `docker/docker_aws_push.sh`

**Prerequisites**:
- Docker daemon running
- ECR repository created (e.g., `ecco-dataset-production`)
- AWS credentials with ECR push permissions

**Run**:
```bash
bash 1_docker-image-prep.sh
```

**Expected output**:
- Docker image built locally
- Image pushed to ECR with latest tag

**Note**: This step takes several minutes depending on network speed and Docker build cache.

---

### 2. Create Job Files

**Script**: `2_create-job-files.sh`

Generates job specification files from ECCO metadata groupings.

**What it does**:
- Reads groupings JSON from `ECCO-v4-Configurations`
- Generates one job file per dataset/frequency combination
- Writes job files to `./jobs/` directory

**Run**:
```bash
bash 2_create-job-files.sh
```

**Input**:
- `ECCO-v4-Configurations/ECCOv4 Release 6/metadata/groupings_for_native_datasets.json`

**Output**:
- `./jobs/{DATASET}_{GRID}_{FREQ}_jobs.txt` files
- Example: `jobs/OCEAN_VELOCITY_native_AVG_MON_jobs.txt`

**Expected result**: ~60 job files for native grid datasets

---

### 3. Create Job Task Lists

**Script**: `3_create-job-task-list.sh`

Generates detailed task lists by scanning available source data files.

**What it does**:
- Reads job files from `./jobs/`
- Scans S3 source data to find available timesteps
- Creates comprehensive JSON task lists
- Writes task lists to `./tasklists/` directory

**⚠️ Important**: Edit this script to set your S3 paths:

```bash
SOURCE_ROOT="s3://your-bucket/source/V4r6/"
DEST_ROOT="s3://your-bucket/destination/V4r6/"
GRID_LOC="s3://your-bucket/ecco_grids/V4r5/grid_ECCOV4r5/"
FACTORS_LOC="s3://your-bucket/ecco-mapping-factors/V4r5/"
METADATA_DIR="s3://your-bucket/ecco-metadata/V4r6/"
CONFIG="s3://your-bucket/ecco-metadata/V4r6/config_V4r6.yaml"
```

**Run**:
```bash
# Edit the script first to set your S3 paths!
bash 3_create-job-task-list.sh
```

**Input**:
- Job files from `./jobs/`
- S3 source data (MDS files)
- ECCO configuration and metadata

**Output**:
- `./tasklists/{DATASET}_{GRID}_{FREQ}_jobs.json` files
- Each file contains array of granule generation tasks

**Expected result**: ~60 tasklist files, typically 400+ entries each for monthly data

---

### 4. Create Test Subset (First-Middle-Last)

**Script**: `4-first-middle-last.sh`

Creates a small subset of tasklists for testing before running full production.

**What it does**:
- Uses the `edp_subset_tasklists` CLI tool
- Selects first, middle, and last entry from each tasklist
- Writes subset to `./first-middle-last-tasklists/`
- Useful for validating the pipeline on representative samples

**Run**:
```bash
bash 4-first-middle-last.sh
```

**Input**:
- Full tasklists from `./tasklists/`

**Output**:
- `./first-middle-last-tasklists/{DATASET}_{GRID}_{FREQ}_jobs.json`
- Each file contains only 3 entries (first, middle, last)

**Alternative modes**: The `edp_subset_tasklists` CLI tool supports multiple sampling strategies:

```bash
# Create first-middle-last subset (default in script)
edp_subset_tasklists ./tasklists/ \
  --output_dir ./first-middle-last-tasklists \
  --mode first-middle-last \
  -l INFO

# Create first 12 months for testing one year
edp_subset_tasklists ./tasklists/ \
  --output_dir ./test-year \
  --mode first -n 12 \
  -l INFO

# Create last 12 months for testing recent data
edp_subset_tasklists ./tasklists/ \
  --output_dir ./test-recent \
  --mode last -n 12 \
  -l INFO

# Create 10 evenly-distributed samples across the full time range
edp_subset_tasklists ./tasklists/ \
  --output_dir ./spread-sample \
  --mode spread -n 10 \
  -l INFO

# Create 5% random sample for statistical validation
edp_subset_tasklists ./tasklists/ \
  --output_dir ./random-sample \
  --mode percentage --percent 5 \
  -l INFO
```

**Pro tip**: For comprehensive testing, create multiple subsets:
```bash
# Quick validation (3 samples per dataset)
bash 4-first-middle-last.sh

# Full year test (12 months per dataset)
edp_subset_tasklists ./tasklists/ --output_dir ./test-year --mode first -n 12 -l INFO

# Temporal coverage (10 samples evenly spread)
edp_subset_tasklists ./tasklists/ --output_dir ./spread-test --mode spread -n 10 -l INFO
```

---

### 5. Submit to AWS Batch

**Script**: `5-batch-run.sh`

Uploads tasklists to S3 and submits AWS Batch jobs for dataset generation.

**What it does**:
- Syncs tasklists from local directory to S3
- Submits one AWS Batch job per tasklist file
- Each job processes one dataset/frequency combination

**⚠️ Important**: Edit this script to set your configuration:

```bash
S3_TASKLIST_ROOT="s3://your-bucket/tasks/"  # S3 path for tasklists
JOB_DEFINITION=your-job-definition          # AWS Batch job definition
JOB_QUEUE=your-job-queue                    # AWS Batch job queue
```

**Run**:
```bash
# Edit the script first to set your AWS Batch configuration!
bash 5-batch-run.sh
```

**Input**:
- Tasklists from `./first-middle-last-tasklists/` (or use full `./tasklists/`)

**Output**:
- Tasklists uploaded to S3
- AWS Batch jobs submitted (one per tasklist)
- Generated NetCDF datasets written to destination S3 bucket

**Monitor jobs**:
```bash
# List running jobs
aws batch list-jobs --job-queue your-job-queue --job-status RUNNING

# View job details
aws batch describe-jobs --jobs <job-id>

# View CloudWatch logs
aws logs tail /aws/batch/job --follow
```

---

## Full Production Run

After validating with the test subset, run full production by modifying step 5:

```bash
#!/bin/bash
set -eo pipefail

S3_TASKLIST_ROOT="s3://your-bucket/tasks/full/"

JOB_DEFINITION=ecco-dataset-production-job-definition_no_spot
JOB_QUEUE=edpq_no_spot

echo "Copying full tasklists to S3..."
aws s3 sync "./tasklists/" "$S3_TASKLIST_ROOT"  # Use full tasklists!

for tasklist in $(aws s3 ls $S3_TASKLIST_ROOT | awk '{print $4}'); do
    aws batch submit-job \
      --job-name              $(echo $tasklist | sed 's/_task//;s/.json//') \
      --job-queue             $JOB_QUEUE \
      --job-definition        $JOB_DEFINITION \
      --container-overrides   "{\"environment\": [{\"name\": \"TASKLIST\", \"value\": \"$S3_TASKLIST_ROOT$tasklist\" } ] }"
done
```

## Troubleshooting

### Script Fails at Step 1 (Docker)

**Problem**: Docker build fails or push to ECR fails

**Solutions**:
- Ensure Docker daemon is running: `docker ps`
- Check ECR repository exists: `aws ecr describe-repositories`
- Authenticate to ECR: `aws ecr get-login-password | docker login --username AWS --password-stdin <account>.dkr.ecr.<region>.amazonaws.com`

### Script Fails at Step 3 (Task Lists)

**Problem**: Cannot find source files in S3

**Solutions**:
- Verify S3 paths are correct and accessible
- Check AWS credentials have S3 read permissions
- Test S3 access: `aws s3 ls s3://your-bucket/source/V4r6/`

### AWS Batch Jobs Fail

**Problem**: Jobs fail in AWS Batch

**Solutions**:
- Check CloudWatch logs for error messages
- Verify job definition has correct IAM role with S3 permissions
- Ensure Docker image exists in ECR
- Check job definition environment variables are set correctly
- Verify compute environment has sufficient resources

### No Output Datasets

**Problem**: Jobs succeed but no output in destination S3

**Solutions**:
- Check job logs for write errors
- Verify destination S3 path is correct and writable
- Check IAM role has S3 write permissions to destination bucket

## Estimated Runtime

For ECCO V4r6 (34 years, monthly data):

- **Test run** (3 entries × 60 datasets): ~30-60 minutes
- **Single dataset** (408 months): ~2-4 hours depending on complexity
- **Full production** (all datasets): ~2-5 days with parallel processing

## Output Structure

Generated datasets follow this structure:

```
s3://destination-bucket/V4r6/
├── native/
│   ├── mon_mean/
│   │   ├── OCEAN_VELOCITY/
│   │   │   ├── OCEAN_VELOCITY_mon_mean_1992-01_ECCO_V4r6_native_llc0090.nc
│   │   │   ├── OCEAN_VELOCITY_mon_mean_1992-02_ECCO_V4r6_native_llc0090.nc
│   │   │   └── ...
│   │   ├── OCEAN_TEMPERATURE_SALINITY/
│   │   └── ...
│   └── day_mean/
└── ...
```

## Next Steps

After successful dataset generation:

1. **Validate outputs**: Check NetCDF files for correctness
2. **Generate checksums**: Create MD5/SHA256 checksums for data integrity
3. **Create metadata**: Generate dataset-level metadata and documentation
4. **Distribute**: Sync to public S3 buckets or archives (e.g., PO.DAAC)

## References

- [EDP Documentation](https://ecco-dataset-production.readthedocs.io/)
- [ECCO-v4-Configurations](https://github.com/ECCO-GROUP/ECCO-v4-Configurations)
- [AWS Batch Documentation](https://docs.aws.amazon.com/batch/)
