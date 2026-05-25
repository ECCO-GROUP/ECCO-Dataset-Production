#!/usr/bin/env python3
"""
Download the first N timesteps from ECCO MDS binary file pairs stored in AWS S3.

================================================================================
PURPOSE
================================================================================

Efficiently download a subset of ECCO model output for testing, prototyping, or
small-scale analysis without downloading entire multi-year datasets. This tool is
designed to help developers and researchers work with ECCO data locally without
needing to download decades of global ocean model output.

TYPICAL USE CASES:
    - Rapid prototyping of data processing pipelines
    - Testing metadata transformations before full-scale production runs
    - Local development and debugging of ECCO dataset production workflows
    - Creating minimal test datasets for CI/CD pipelines
    - Educational demonstrations with real ECCO data

================================================================================
HOW IT WORKS
================================================================================

The script follows this workflow:

1. **S3 Discovery**: Scans the S3 bucket for directories matching the pattern
   VARNAME_mon_mean/ (e.g., ETAN_mon_mean/, SSH_mon_mean/, THETA_mon_mean/)

2. **File Inventory**: Lists all .data and .meta file pairs in each directory.
   MDS (MIT General Circulation Model Data Storage) format uses paired files:
   - .data: Binary data array
   - .meta: ASCII metadata describing dimensions, precision, timestep

3. **Timestep Parsing**: Extracts timestep numbers from filenames following the
   pattern: VARNAME_mon_mean.NNNNNNNNNN.data where NNNNNNNNNN is a 10-digit
   zero-padded integer (e.g., 0000000732, 0000001464)

4. **Selection**: Identifies the first N unique timesteps (sorted numerically),
   ensuring both .data and .meta files are included for each timestep

5. **Download**: Transfers selected files from S3 to local filesystem, preserving
   the original directory structure

6. **Skip Existing**: By default, skips files that already exist locally. Use
   --force-redownload to overwrite existing files.

================================================================================
KEY FEATURES
================================================================================

FILE MANAGEMENT:
    - Smart skip: Avoids re-downloading files that already exist locally
    - Force redownload: Optional --force-redownload flag to overwrite existing files
    - Directory structure preservation: Maintains original S3 directory layout locally
    - Paired file integrity: Always downloads both .data and .meta files together

FILTERING:
    - Variable groupings: Download only variables from specific dataset groupings
    - Grouping preview: List all available groupings before downloading
    - Timestep control: Configurable number of timesteps per variable

WORKFLOW INTEGRATION:
    - Dry-run mode: Preview what would be downloaded without transferring data
    - Debug logging: Multiple logging levels (DEBUG, INFO, WARNING, ERROR)
    - AWS profile support: Works with AWS SSO and standard credential configurations

PERFORMANCE:
    - Minimal data transfer: Only downloads requested timesteps, not entire datasets
    - Efficient S3 pagination: Handles large directories with thousands of files
    - Skip existing files: Resumes interrupted downloads without redundant transfers

================================================================================
COMMAND-LINE ARGUMENTS
================================================================================

REQUIRED:
    s3_path             S3 URI to ECCO data directory
                        Format: s3://bucket-name/path/to/data/
                        Example: s3://ecco-model-output/V4r4/mon_mean/

    local_dir           Local destination directory for downloaded files
                        Created automatically if it doesn't exist
                        Example: ./test_data or /data/ecco/subset

OPTIONAL:
    -n, --num-timesteps N
                        Number of timesteps to download per variable
                        Default: 12
                        Example: -n 24 downloads first 24 timesteps

    --profile PROFILE   AWS profile name for authentication
                        Default: 'saml-pub'
                        Example: --profile default

    --groupings-file PATH
                        Path to JSON file containing dataset groupings
                        Required for --grouping and --list-groupings options
                        Example: ECCOv4r4_groupings_for_native_datasets.json

    --grouping NAME     Download only variables from a specific grouping
                        Requires --groupings-file
                        Example: --grouping OCEAN_BOTTOM_PRESSURE

    --list-groupings    List all available groupings and exit
                        Requires --groupings-file
                        Does not perform any downloads

    --dry-run           Show what would be downloaded without actually downloading
                        Useful for previewing operations and estimating data volume
                        Also reports which files would be skipped (already exist)

    --force-redownload  Redownload files even if they already exist locally
                        Overwrites existing files
                        Use when you suspect local files are corrupted or outdated

    --log LEVEL         Logging verbosity level
                        Choices: DEBUG, INFO, WARNING, ERROR
                        Default: INFO
                        DEBUG shows every file operation

================================================================================
USAGE EXAMPLES
================================================================================

BASIC USAGE:

    # Download first 12 timesteps from all variables (default)
    python download_first_timesteps.py s3://ecco-model/V4r4/mon_mean/ ./data

    # Download first 24 timesteps instead of default 12
    python download_first_timesteps.py s3://ecco-model/V4r4/mon_mean/ ./data -n 24

    # Use a different AWS profile
    python download_first_timesteps.py s3://ecco-model/V4r4/mon_mean/ ./data \\
        --profile my-aws-profile

WORKING WITH GROUPINGS:

    # List all available dataset groupings
    python download_first_timesteps.py s3://dummy ./dummy \\
        --groupings-file ECCO-v4-Configurations/ECCOv4r4_groupings_for_native_datasets.json \\
        --list-groupings

    # Download only variables from ocean bottom pressure grouping
    python download_first_timesteps.py s3://ecco-model/V4r4/mon_mean/ ./data \\
        --groupings-file ECCO-v4-Configurations/ECCOv4r4_groupings_for_native_datasets.json \\
        --grouping OCEAN_BOTTOM_PRESSURE

    # Download multiple specific groupings (run command for each grouping)
    python download_first_timesteps.py s3://ecco-model/V4r4/mon_mean/ ./data \\
        --groupings-file ECCOv4r4_groupings_for_native_datasets.json \\
        --grouping SEA_SURFACE_HEIGHT

PREVIEW AND DEBUG:

    # Dry run: see what would be downloaded without actually downloading
    python download_first_timesteps.py s3://ecco-model/V4r4/mon_mean/ ./data --dry-run

    # Dry run with debug logging to see detailed file operations
    python download_first_timesteps.py s3://ecco-model/V4r4/mon_mean/ ./data \\
        --dry-run --log DEBUG

    # Check which files would be skipped (already exist locally)
    python download_first_timesteps.py s3://ecco-model/V4r4/mon_mean/ ./data \\
        --dry-run --log INFO

RESUMING AND REDOWNLOADING:

    # Resume an interrupted download (skips files that already exist)
    python download_first_timesteps.py s3://ecco-model/V4r4/mon_mean/ ./data

    # Force redownload everything (overwrite existing files)
    python download_first_timesteps.py s3://ecco-model/V4r4/mon_mean/ ./data \\
        --force-redownload

    # Force redownload with debug logging
    python download_first_timesteps.py s3://ecco-model/V4r4/mon_mean/ ./data \\
        --force-redownload --log DEBUG

PRODUCTION WORKFLOWS:

    # Download minimal test dataset for CI/CD pipeline
    python download_first_timesteps.py s3://ecco-model/V4r4/mon_mean/ ./ci_test_data \\
        -n 2 --log WARNING

    # Download larger subset for local development
    python download_first_timesteps.py s3://ecco-model/V4r4/mon_mean/ ./dev_data \\
        -n 36 --grouping OCEAN_TEMPERATURE_SALINITY

================================================================================
FILE STRUCTURE
================================================================================

INPUT (S3):
    s3://bucket/prefix/
    ├── ETAN_mon_mean/
    │   ├── ETAN_mon_mean.0000000732.data
    │   ├── ETAN_mon_mean.0000000732.meta
    │   ├── ETAN_mon_mean.0000001464.data
    │   ├── ETAN_mon_mean.0000001464.meta
    │   └── ...
    ├── SSH_mon_mean/
    │   └── ...
    └── THETA_mon_mean/
        └── ...

OUTPUT (Local):
    local_dir/
    ├── ETAN_mon_mean/
    │   ├── ETAN_mon_mean.0000000732.data
    │   ├── ETAN_mon_mean.0000000732.meta
    │   ├── ETAN_mon_mean.0000001464.data
    │   └── ETAN_mon_mean.0000001464.meta
    ├── SSH_mon_mean/
    │   └── ...
    └── THETA_mon_mean/
        └── ...

================================================================================
GROUPINGS FILE FORMAT
================================================================================

The groupings JSON file defines logical collections of ECCO variables. Each
grouping contains:

    {
        "filename": "OCEAN_BOTTOM_PRESSURE",
        "name": "Ocean Bottom Pressure",
        "fields": "PHIBOT, OBP",
        "dimension": "2D",
        "frequency": "AVG_MON"
    }

Fields:
    - filename: Grouping identifier (used with --grouping flag)
    - name: Human-readable description
    - fields: Comma-separated list of variable names to download
    - dimension: 2D or 3D (informational)
    - frequency: Averaging period (informational)

================================================================================
REQUIREMENTS
================================================================================

PYTHON DEPENDENCIES:
    - Python 3.7 or later
    - boto3: AWS SDK for Python (pip install boto3)

AWS CONFIGURATION:
    - AWS credentials configured via one of:
      * AWS SSO (aws sso login --profile saml-pub)
      * AWS CLI configuration (~/.aws/credentials)
      * Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
      * IAM role (when running on EC2/ECS)

S3 PERMISSIONS:
    - s3:ListBucket (to list directories and files)
    - s3:GetObject (to download files)

================================================================================
NOTES
================================================================================

- Timestep numbers are zero-padded 10-digit integers representing model timesteps
- The script assumes directory names follow the pattern: VARNAME_mon_mean/
- Both .data and .meta files are always downloaded together for each timestep
- Directory structure from S3 is preserved in local destination
- Existing files are skipped by default (use --force-redownload to override)
- Dry-run mode does not require S3 read permissions (except for listing)

================================================================================
AUTHOR & VERSION
================================================================================

Part of the ECCO Dataset Production toolkit
Repository: https://github.com/ECCO-GROUP/ECCO-Dataset-Production
"""

import argparse
import boto3
import os
import sys
from pathlib import Path
from collections import defaultdict
import re
import logging
import json

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
log = logging.getLogger(__name__)


def load_groupings(groupings_file):
    """
    Load dataset groupings from JSON file.

    Args:
        groupings_file: Path to groupings JSON file

    Returns:
        list: List of grouping dictionaries
    """
    with open(groupings_file, 'r') as f:
        return json.load(f)


def get_variables_for_grouping(groupings, grouping_name):
    """
    Extract variable names for a specific grouping.

    Args:
        groupings: List of grouping dictionaries
        grouping_name: Value of 'filename' field to match (e.g., 'OCEAN_BOTTOM_PRESSURE')

    Returns:
        list: Variable names, or None if grouping not found
    """
    for grouping in groupings:
        if grouping.get('filename') == grouping_name:
            # Parse comma-separated fields
            fields_str = grouping.get('fields', '')
            variables = [v.strip() for v in fields_str.split(',')]
            return variables
    return None


def parse_timestep(filename):
    """
    Extract timestep number from MDS filename.

    Args:
        filename: e.g., 'ETAN_mon_mean.0000000732.data'

    Returns:
        int: timestep number, or None if not found
    """
    match = re.search(r'\.(\d{10})\.(?:data|meta)$', filename)
    if match:
        return int(match.group(1))
    return None


def extract_variable_from_directory(directory_name):
    """
    Extract variable name from directory path.

    Args:
        directory_name: e.g., 'ETAN_mon_mean/' or 'path/to/OBP_mon_mean/'

    Returns:
        str: Variable name (e.g., 'ETAN', 'OBP'), or None if pattern doesn't match
    """
    basename = directory_name.rstrip('/').split('/')[-1]
    match = re.match(r'^([A-Za-z0-9_]+)_mon_mean$', basename)
    if match:
        return match.group(1)
    return None


def filter_directories_by_variables(directories, variables):
    """
    Filter directory list to only include specified variables.

    Args:
        directories: List of directory paths
        variables: List of variable names to keep

    Returns:
        list: Filtered directory paths
    """
    if not variables:
        return directories

    filtered = []
    for directory in directories:
        var_name = extract_variable_from_directory(directory)
        if var_name in variables:
            filtered.append(directory)

    return filtered


def list_s3_directories(s3_client, bucket, prefix=''):
    """
    List top-level directories (common prefixes) in S3 bucket.

    Args:
        s3_client: boto3 S3 client
        bucket: S3 bucket name
        prefix: Optional prefix to filter directories

    Returns:
        list: Directory prefixes (e.g., ['ETAN_mon_mean/', 'PHIBOT_mon_mean/'])
    """
    directories = []
    paginator = s3_client.get_paginator('list_objects_v2')

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter='/'):
        if 'CommonPrefixes' in page:
            for prefix_info in page['CommonPrefixes']:
                directories.append(prefix_info['Prefix'])

    return directories


def list_files_in_directory(s3_client, bucket, directory):
    """
    List all files in an S3 directory.

    Args:
        s3_client: boto3 S3 client
        bucket: S3 bucket name
        directory: S3 directory prefix

    Returns:
        list: File keys in the directory
    """
    files = []
    paginator = s3_client.get_paginator('list_objects_v2')

    for page in paginator.paginate(Bucket=bucket, Prefix=directory):
        if 'Contents' in page:
            for obj in page['Contents']:
                key = obj['Key']
                # Skip the directory itself
                if key != directory:
                    files.append(key)

    return files


def get_first_n_timesteps(files, n=12):
    """
    Identify the first N unique timesteps from a list of MDS files.

    Args:
        files: List of S3 keys
        n: Number of timesteps to retrieve

    Returns:
        list: S3 keys for the first N timesteps (includes both .data and .meta)
    """
    # Group files by timestep
    timestep_files = defaultdict(list)

    for file_key in files:
        filename = os.path.basename(file_key)
        timestep = parse_timestep(filename)
        if timestep is not None:
            timestep_files[timestep].append(file_key)

    # Sort timesteps and take first N
    sorted_timesteps = sorted(timestep_files.keys())[:n]

    # Collect all files for these timesteps
    selected_files = []
    for timestep in sorted_timesteps:
        selected_files.extend(timestep_files[timestep])

    return selected_files


def download_file(s3_client, bucket, s3_key, local_path, force_redownload=False):
    """
    Download a file from S3 to local path.

    Args:
        s3_client: boto3 S3 client
        bucket: S3 bucket name
        s3_key: S3 object key
        local_path: Local file path
        force_redownload: If True, redownload even if file exists locally

    Returns:
        bool: True if file was downloaded, False if skipped
    """
    # Check if file already exists
    if not force_redownload and os.path.exists(local_path):
        log.debug(f"Skipping (already exists): {local_path}")
        return False

    # Create parent directory if needed
    Path(local_path).parent.mkdir(parents=True, exist_ok=True)

    log.debug(f"Downloading s3://{bucket}/{s3_key} -> {local_path}")
    s3_client.download_file(bucket, s3_key, local_path)
    return True


def main():
    parser = argparse.ArgumentParser(
        description='Download first N timesteps from ECCO MDS file pairs in S3',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Download first 12 timesteps from all directories
  python download_first_timesteps.py s3://my-bucket/ecco_results/ ./local_data

  # Download first 24 timesteps with specific AWS profile
  python download_first_timesteps.py s3://my-bucket/ecco_results/ ./local_data -n 24 --profile my-profile

  # List available groupings
  python download_first_timesteps.py s3://dummy ./dummy \\
      --groupings-file ECCO-v4-Configurations/ECCOv4r4_groupings_for_native_datasets.json \\
      --list-groupings

  # Download only variables from a specific grouping
  python download_first_timesteps.py s3://my-bucket/ecco_results/ ./local_data \\
      --groupings-file ECCO-v4-Configurations/ECCOv4r4_groupings_for_native_datasets.json \\
      --grouping OCEAN_BOTTOM_PRESSURE

  # Download with debug logging
  python download_first_timesteps.py s3://my-bucket/ecco_results/ ./local_data --log DEBUG
        """
    )

    parser.add_argument('s3_path', help='S3 path (s3://bucket/prefix/)')
    parser.add_argument('local_dir', help='Local destination directory')
    parser.add_argument('-n', '--num-timesteps', type=int, default=12,
                        help='Number of timesteps to download per variable (default: 12)')
    parser.add_argument('--profile', default='saml-pub',
                        help='AWS profile name (default: saml-pub)')
    parser.add_argument('--groupings-file',
                        help='Path to dataset groupings JSON file')
    parser.add_argument('--grouping',
                        help='Grouping filename to download (e.g., OCEAN_BOTTOM_PRESSURE)')
    parser.add_argument('--list-groupings', action='store_true',
                        help='List available groupings from groupings file and exit')
    parser.add_argument('--log', default='INFO',
                        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                        help='Logging level (default: INFO)')
    parser.add_argument('--dry-run', action='store_true',
                        help='Show what would be downloaded without actually downloading')
    parser.add_argument('--force-redownload', action='store_true',
                        help='Redownload files even if they already exist locally')

    args = parser.parse_args()

    # Set logging level
    log.setLevel(getattr(logging, args.log))

    # Handle --list-groupings
    if args.list_groupings:
        if not args.groupings_file:
            log.error("--list-groupings requires --groupings-file to be specified")
            sys.exit(1)

        groupings = load_groupings(args.groupings_file)
        print(f"\nAvailable groupings in {args.groupings_file}:")
        print("=" * 80)
        for grouping in groupings:
            filename = grouping.get('filename', 'N/A')
            name = grouping.get('name', 'N/A')
            fields = grouping.get('fields', 'N/A')
            dimension = grouping.get('dimension', 'N/A')
            frequency = grouping.get('frequency', 'N/A')
            print(f"\nFilename: {filename}")
            print(f"  Name: {name}")
            print(f"  Variables: {fields}")
            print(f"  Dimension: {dimension}")
            print(f"  Frequency: {frequency}")
        print("\n" + "=" * 80)
        print(f"Total: {len(groupings)} groupings")
        sys.exit(0)

    # Validate grouping arguments
    if args.grouping and not args.groupings_file:
        log.error("--grouping requires --groupings-file to be specified")
        sys.exit(1)

    # Parse S3 path
    if not args.s3_path.startswith('s3://'):
        log.error("S3 path must start with s3://")
        sys.exit(1)

    s3_path_parts = args.s3_path[5:].split('/', 1)
    bucket = s3_path_parts[0]
    prefix = s3_path_parts[1] if len(s3_path_parts) > 1 else ''

    log.info(f"S3 Bucket: {bucket}")
    log.info(f"S3 Prefix: {prefix}")
    log.info(f"Local directory: {args.local_dir}")
    log.info(f"Timesteps per variable: {args.num_timesteps}")
    if args.force_redownload:
        log.info("Force redownload: enabled (will overwrite existing files)")

    # Create S3 client
    session_kwargs = {}
    if args.profile:
        session_kwargs['profile_name'] = args.profile

    session = boto3.Session(**session_kwargs)
    s3_client = session.client('s3')

    # Load groupings and filter variables if specified
    selected_variables = None
    if args.groupings_file:
        log.info(f"Loading groupings from {args.groupings_file}")
        groupings = load_groupings(args.groupings_file)
        log.info(f"Loaded {len(groupings)} groupings")

        if args.grouping:
            selected_variables = get_variables_for_grouping(groupings, args.grouping)
            if selected_variables is None:
                log.error(f"Grouping '{args.grouping}' not found in {args.groupings_file}")
                sys.exit(1)
            log.info(f"Selected grouping '{args.grouping}' with variables: {', '.join(selected_variables)}")

    # List directories
    log.info("Discovering variable directories...")
    directories = list_s3_directories(s3_client, bucket, prefix)

    if not directories:
        log.error(f"No directories found in s3://{bucket}/{prefix}")
        sys.exit(1)

    log.info(f"Found {len(directories)} variable directories")

    # Filter directories based on selected variables
    if selected_variables:
        directories = filter_directories_by_variables(directories, selected_variables)
        log.info(f"Filtered to {len(directories)} directories matching selected variables")

        if not directories:
            log.error(f"No directories found matching variables: {', '.join(selected_variables)}")
            sys.exit(1)

    # Process each directory
    total_files = 0
    skipped_files = 0
    for directory in directories:
        var_name = directory.rstrip('/').split('/')[-1]
        log.info(f"\nProcessing {var_name}...")

        # List files in directory
        files = list_files_in_directory(s3_client, bucket, directory)
        log.info(f"  Found {len(files)} files")

        # Get first N timesteps
        selected_files = get_first_n_timesteps(files, args.num_timesteps)
        log.info(f"  Selected {len(selected_files)} files for first {args.num_timesteps} timesteps")

        # Download files
        var_downloaded = 0
        var_skipped = 0
        for s3_key in selected_files:
            # Construct local path preserving directory structure
            relative_path = s3_key
            if prefix:
                relative_path = s3_key[len(prefix):].lstrip('/')

            local_path = os.path.join(args.local_dir, relative_path)

            if args.dry_run:
                if not args.force_redownload and os.path.exists(local_path):
                    log.info(f"  Would skip (exists): {s3_key}")
                    var_skipped += 1
                else:
                    log.info(f"  Would download: {s3_key} -> {local_path}")
                    var_downloaded += 1
            else:
                if download_file(s3_client, bucket, s3_key, local_path, args.force_redownload):
                    total_files += 1
                    var_downloaded += 1
                else:
                    skipped_files += 1
                    var_skipped += 1

        if args.dry_run:
            log.info(f"  Would download {var_downloaded} files, skip {var_skipped} files")
        else:
            log.info(f"  Downloaded {var_downloaded} files, skipped {var_skipped} files (already exist)")

    log.info(f"\n{'Dry run' if args.dry_run else 'Download'} complete!")
    if not args.dry_run:
        log.info(f"Total files downloaded: {total_files}")
        log.info(f"Total files skipped: {skipped_files}")
        log.info(f"Files saved to: {args.local_dir}")


if __name__ == '__main__':
    main()
