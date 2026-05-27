# ECCO Dataset Production - Developer Guide

## Project Overview

**ECCO Dataset Production** is a Python toolset for transforming NASA's ECCO (Estimating the Circulation and Climate of the Ocean) ocean state estimates from raw MITgcm model output into production-ready NetCDF datasets for distribution through NASA's PO.DAAC (Physical Oceanography Distributed Active Archive Center).

**Key Purpose**: Transform multi-terabyte MITgcm binary data into time-stamped, metadata-rich NetCDF granules on both native LLC90 grid and regular 0.5° lat-lon grids.

**Scale**: Processes decades of global ocean data (1992-2018 for V4r4, extending through later years for newer releases), generating multi-terabyte datasets with consistent metadata, coordinate systems, and quality attributes.

**Deployment**: Runs locally for custom datasets or on AWS Batch for large-scale production.

## Repository Statistics

- **Size**: ~3.9 GB (includes test data and submodules)
- **Python Files**: 61 core modules
- **Main Package**: `src/ecco_dataset_production/` (~2,668 lines across 11 core modules)
- **Current Version**: 1.0
- **Active Branches**: 
  - `main`: stable production code
  - `grid_freedom`: work on flexible grid generation and mapping factors
  - `v4r6_neverending_story`: V4r6 support with time-invariant fields
  - `subset-tasklist`: CLI tools for task list manipulation

## Architecture Overview

### High-Level Data Flow

```
Input Sources          Task Generation         Granule Processing           Output
━━━━━━━━━━━━━━         ━━━━━━━━━━━━━━━         ━━━━━━━━━━━━━━━━━━         ━━━━━━━━
                                                                            
Job File    ──────┐                                                         
              │                                                             
MDS Files   ─────┼──→ create_job_task_list ──→ generate_datasets ──→ NetCDF
              │          (Phase 1)                  (Phase 2)         Granules
Grid Files  ─────┤                                     │                    
              │                                        ├─→ ECCOMDSDataset   
Mapping     ─────┤                                     │                    
Factors           │                                    ├─→ ECCOGrid         
              │                                        │                    
Metadata    ─────┤                                     └─→ ECCOMappingFactors
              │                                                              
Config      ──────┘                                                          
(YAML)
```

### Two-Phase Workflow

**Phase 1: Task List Generation** (`edp_create_job_task_list`)
- Reads high-level job specifications from a simple text file
- Discovers available input files in source directories (local or S3)
- Consults metadata groupings to determine which variables go together
- Generates a comprehensive JSON task list, where each task = one output granule
- Each task contains: output filename, input file lists per variable, metadata, resource locations

**Phase 2: Dataset Generation** (`edp_generate_datasets`)
- Reads task list from Phase 1
- For each task:
  - Loads MDS binary files via `ecco_v4_py`
  - Applies grid transformations (native → lat-lon if needed)
  - Performs vector transformations (UV → EW/NS components)
  - Applies land masks
  - Adds coordinates, metadata, and encoding
  - Writes compliant NetCDF4 granule
- Completely driven by task list (no other inputs needed)

### Core Modules

#### Configuration & Setup

**`configuration.py`** (86 lines)
- `ECCODatasetProductionConfig`: Loads YAML config files
- Supports local paths and S3 URIs
- Returns empty string (not exception) for missing keys
- Key parameters: model timing, precision, grid resolution, DOI metadata

**`ecco_task.py`** (286 lines)
- `ECCOTask`: Wrapper around task dictionaries
- Properties: `is_2d`, `is_3d`, `is_latlon`, `is_native`, `variable_names`
- Extracts grid type and averaging period from filenames
- Central data structure passed through the entire pipeline

#### Grid & Coordinate Management

**`ecco_grid.py`** (223 lines)
- `ECCOGrid`: Loads and caches native and lat-lon grid files
- Lazy-loads grid datasets (only when accessed)
- Handles local directories, tar archives, and S3 locations
- Properties: `native_grid`, `latlon_grid`, `native_wet_point_indices`
- Supports both `GRID_GEOMETRY_*_native_*.nc` and `GRID_GEOMETRY_*_latlon_*.nc`

**`ecco_mapping_factors.py`** (179 lines)
- `ECCOMappingFactors`: Manages sparse interpolation matrices
- Loads LZMA-compressed (.xz) pickle files for efficiency
- Depth-level-specific transformations (one matrix per vertical level)
- Methods: `native_to_latlon_mapping_factors(level)`, `latlon_land_mask(level)`
- Properties: `latitude_bounds`, `longitude_bounds`, `depth_bounds`
- Source of truth for lat-lon coordinate definitions

#### Data Processing

**`ecco_dataset.py`** (626 lines)
- `ECCOMDSDataset`: Core data container and transformer
- Loads MITgcm MDS binary files (.data/.meta pairs) via `ecco_v4_py`
- Vector field transformations: UV components → EW/NS (East-West, North-South)
- Native → lat-lon interpolation using sparse matrix multiplication
- Land masking for both native and lat-lon grids
- Methods: `as_native(var)`, `as_latlon(var)`, `transform_to_target_grid()`

**`ecco_generate_datasets.py`** (635 lines)
- `ecco_make_granule()`: Main orchestrator for single granule creation
- Coordinates all resources: grid, mapping factors, metadata
- Calls `set_granule_ancillary_data()` for coordinates
- Calls `set_granule_metadata()` for attributes
- Applies NetCDF4 compression (zlib=True, complevel=5, shuffle=True)
- Handles both 2D and 3D fields

#### Metadata Management

**`ecco_metadata.py`** (148 lines)
- `ECCOMetadata`: Loads metadata from JSON files
- Structure: `variable_attributes`, `coordinate_attributes`, `global_attributes`
- Separate metadata for different grid types (1D, native, latlon)
- Merges static metadata with dynamic (time-varying) metadata from tasks

**`ecco_podaac_metadata.py`** (88 lines)
- `ECCO_PodaacMetadata`: Handles PO.DAAC-specific CSV metadata
- Links ShortName → DOI, UUID, metadata links
- Used for official NASA Earthdata distribution

#### Time & File Management

**`ecco_time.py`** (93 lines)
- Time coordinate calculations and formatting
- ISO 8601 duration codes (P1D=daily, P1M=monthly, PT0S=snapshot)
- Converts model timesteps → datetime objects
- Handles different averaging periods

**`ecco_file.py`** (229 lines)
- `ECCOGranuleFilestr`: Parses and generates standardized filenames
- Format: `{prefix}_{period}_{date}_ECCO_{version}_{grid_type}_{grid_label}.nc`
- Example: `SEA_SURFACE_HEIGHT_mon_mean_2010-01_ECCO_V4r4_latlon_0p50deg.nc`
- Extracts: averaging period, date, grid type, grid label

### Command-Line Tools

All exposed via `pyproject.toml` `[project.scripts]`:

1. **`edp_create_job_task_list`**: Phase 1 - Generate task list from job file
2. **`edp_generate_datasets`**: Phase 2 - Execute tasks and produce NetCDF files
3. **`edp_create_factors`**: Generate mapping factors (sparse matrices, land masks)
4. **`edp_create_job_files`**: Create job files from templates
5. **`edp_subset_tasklists`**: Filter/split task lists for parallel processing
6. **`edp_aws_s3_sync`**: Sync datasets to/from AWS S3

### AWS Integration

**`aws/` subpackage**:
- `ecco_aws_s3_cp.py`: Copy files to/from S3
- `ecco_aws_s3_sync.py`: Sync directories with S3
- `utils.py`: S3 URI detection, AWS SSO authentication helpers
- Supports AWS IAM Identity Center (SSO) with `keygen` and `profile` parameters

## Configuration System

### YAML Configuration Files

Located in `configs/`:
- `config_V4r4.yaml`: ECCO Version 4 Release 4 (1992-2017)
- `config_V4r5.yaml`: ECCO Version 4 Release 5
- `config_V4r6.yaml`: ECCO Version 4 Release 6 (in development)

### Key Configuration Parameters

```yaml
ecco_version: 'V4r4'
product_version: 'Version 4, Release 4'

# Grid labels for filenames
ecco_production_filestr_grid_label:
    latlon: '0p50deg'
    native: 'llc0090'

# Model timing
model_start_time: '1992-01-01T12:00:00'
model_end_time: '2017-12-31T12:00:00'
model_timestep_units: 'h'
model_timestep: 1

# Grid transformation
latlon_grid_resolution: 0.5
latlon_grid_area_extent: [-180.0, 90.0, 180.0, -90.0]
latlon_effective_grid_radius: null  # defaults to ~78.5 km

# Data format
array_precision: 'float32'
num_vertical_levels: 50

# Compression
netcdf4_compression_encodings:
    zlib: True
    complevel: 5
    shuffle: True
```

**Important**: File paths (grid files, mapping factors, source data) are NOT in config. They're command-line arguments for portability.

## Grid Transformations & Mapping Factors

### The LLC90 Native Grid

ECCO uses the "Lat-Lon-Cap" (LLC) 90x90 grid:
- Complex topology: 5 connected faces
- Higher resolution near poles
- Fits ocean boundaries naturally
- Size: 13 tiles × 90×90 points = ~100K surface points

### Mapping to Regular Lat-Lon

**Why**: Most users expect regular grids (latitude × longitude)

**How**: Precomputed sparse matrices
- One sparse matrix per depth level (50 levels for 3D)
- Uses nearest-neighbor bin averaging
- Effective radius: ~78.5 km (configurable)
- Stored as compressed pickles (.xz format)

### Mapping Factors Structure

```
mapping_factors/
├── latlon_grid/
│   └── latlon_grid.xz              # Source of truth for coordinates
├── sparse/
│   ├── sparse_matrix_0.npz         # Surface interpolation matrix
│   ├── sparse_matrix_1.npz         # Level 1 matrix
│   └── ...                          # One per depth level
├── land_mask/
│   ├── ecco_latlon_land_mask_0.xz  # Surface mask
│   └── ...                          # One per depth level
├── 3D/
│   ├── ecco_latlon_grid_mappings_3D_0.xz
│   └── ...
├── ecco_latlon_grid_mappings_all.xz
└── ecco_latlon_grid_mappings_2D.xz
```

### Generating Mapping Factors

```bash
# Generate both 2D and 3D mapping factors
edp_create_factors \
    --cfgfile configs/config_V4r4.yaml \
    --grid_file /path/to/GRID_GEOMETRY_ECCO_V4r4_native_llc0090.nc \
    --output_dir /path/to/mapping_factors \
    2 3 \
    --log INFO

# Force regeneration
edp_create_factors \
    --cfgfile configs/config_V4r4.yaml \
    --grid_file /path/to/GRID_GEOMETRY_ECCO_V4r4_native_llc0090.nc \
    --output_dir /path/to/mapping_factors \
    2 3 \
    --force \
    --log INFO
```

**Critical**: Mapping factors are version-specific and resolution-specific. Changing `latlon_grid_resolution` requires regenerating ALL mapping factors.

## Job Files & Metadata Groupings

### Job File Format

Simple text format, one job per line:

```text
# Format: [grouping_id, grid_type, frequency, timesteps]

# Native grid examples
[0, 'native', 'AVG_MON', 'all']     # SSH and sea level anomaly
[17, 'native', 'AVG_MON', 'all']    # Ocean velocity

# Lat-lon grid examples
[0, 'latlon', 'AVG_MON', 'all']     # SSH
[11, 'latlon', 'AVG_MON', 'all']    # Ocean velocity

# Specific timesteps
[5, 'latlon', 'AVG_DAY', [0, 1, 2, 10, 20]]  # Days 0,1,2,10,20 only
```

**Frequency codes**:
- `'SNAP'`: Instantaneous snapshots
- `'AVG_DAY'`: Daily averages
- `'AVG_MON'`: Monthly averages

### Metadata Groupings

Stored in `ECCO-v4-Configurations` submodule (managed separately):
- `ECCOv4r4_groupings_for_native_datasets.json`
- `ECCOv4r4_groupings_for_latlon_datasets.json`
- `ECCOv4r4_groupings_for_1D_datasets.json`

Each grouping defines:
- **name**: Human-readable dataset name
- **fields**: Variables to include (comma-separated)
- **filename**: Output file prefix
- **dimension**: '2D' or '3D'
- **frequency**: Supported averaging periods
- **field_components** (latlon only): Vector component mapping
  - Example: `{"EVEL": {"x": "UVEL", "y": "VVEL"}}`
- **field_orientations** (latlon only): 'zonal' or 'meridional'

## Key Dependencies

From `pyproject.toml`:

```python
dependencies = [
    'boto3',           # AWS SDK
    'bs4',             # HTML parsing
    'netCDF4',         # NetCDF I/O
    'numpy',
    'scipy',           # Sparse matrices
    'xarray',          # N-D labeled arrays
    'xmitgcm',         # MITgcm file I/O
    'xgcm',            # Grid-aware operations
    'ecco_cloud_utils @ git+https://github.com/ECCO-GROUP/ECCO-ACCESS/#subdirectory=ecco-cloud-utils',
    'ecco_v4_py @ git+https://github.com/ECCO-GROUP/ECCOv4-py'
]
```

**Critical external packages**:
- **`xmitgcm`**: Reads MITgcm binary (.data/.meta) files
- **`ecco_v4_py`**: Core ECCO-specific grid operations and transformations
- **`ecco_cloud_utils`**: AWS utilities for ECCO data access

## Development Patterns & Conventions

### Logging

All modules use Python `logging`:
```python
import logging
log = logging.getLogger('edp.'+__name__)
```

Set via command-line: `--log DEBUG|INFO|WARNING|ERROR|CRITICAL`

### Error Handling

- Configuration: Missing keys log WARNING and return `''` (not exception)
- File I/O: Raise clear errors with file paths
- AWS: Check for S3 URIs with `aws.utils.is_s3_uri(path)`

### Code Organization

```
src/ecco_dataset_production/
├── __init__.py           # Package-level docs with architecture diagram
├── configuration.py      # Config loading
├── ecco_*.py             # Core modules (grid, dataset, metadata, etc.)
├── apps/                 # CLI entry points
│   ├── create_factors.py
│   ├── create_job_task_list.py
│   ├── generate_datasets.py
│   └── ...
├── aws/                  # AWS integration
│   ├── ecco_aws_s3_cp.py
│   ├── ecco_aws_s3_sync.py
│   └── utils.py
└── utils/                # Helper utilities
    ├── gen_netcdf_utils.py
    ├── mapping_factors_utils.py
    └── split_task_json.py
```

### Naming Conventions

- Classes: `ECCOClassName` (all caps ECCO)
- Files: `ecco_snake_case.py`
- CLI tools: `edp_snake_case`
- Grid labels: `llc0090`, `0p50deg` (no dots in filenames)

## Testing & Validation

### Test Suite

Located in `tests/`:
- `test_ecco_file.py`: Filename parsing and generation
- `test_ecco_time.py`: Time coordinate calculations

### Demo Examples

Located in `demos/`:

**`demos/native_latlon_local/`**: Best starting point
- Complete end-to-end example with local files
- Includes verification files (`.verif`) to diff against
- Shell scripts showing typical workflow:
  ```bash
  ./edp_create_job_task_list.sh V4r5   # Phase 1
  ./edp_generate_datasets.sh V4r5       # Phase 2
  ```

**`demos/native_latlon_local_remote/`**: Mixed local/remote processing

**`demos/SSH_native_latlon_local_docker/`**: Docker-based processing

**`demos/edp_create_job_task_list/`**: Job file examples

### Verification Workflow

1. Run Phase 1, compare task list against `.verif` file:
   ```bash
   diff tasks_V4r4.json tasks_V4r4.json.verif
   ```

2. Run Phase 2, inspect output with `ncdump`:
   ```bash
   ncdump -h output.nc
   ```

3. Check metadata compliance:
   - All required CF-1.8 attributes present
   - Time bounds correct
   - Coordinates properly assigned
   - Land masking applied

## Docker Deployment

### Dockerfiles

Located in `docker/`:
- Multiple stages for local dev and AWS deployment
- Uses Miniconda base with pinned environment

### Docker Compose

- `docker-compose.dev.yaml`: Local development
- `docker-compose.aws.yaml`: AWS Batch deployment

### Environment Variables

Set in `.env` file:
```bash
ECCO_VERSION=V4r4
AWS_PROFILE=saml-pub
AWS_REGION=us-west-2
```

## Recent Development Activity

### Current Branch: `grid_freedom`

Recent commits focus on:
1. **Flexible grid generation**: Removing hard-coded paths and assumptions
2. **Mapping factors improvements**:
   - Made parameters configurable via command-line (not hard-coded)
   - Fixed bug: effective grid radius not converted from km to meters
   - Added explicit methods for target grid effective radius calculation
   - Limited maximum neighbors for bin-averaging to 100
   - Support for 2D-only sparse matrix generation
3. **Enhanced logging**: Extensive debug statements for grid parameters
4. **Config cleanup**: Moved file paths from YAML to command-line arguments

### Recent Merges to `main`

- **#99**: `subset_tasklists` CLI tool for task list manipulation
- **#98**: `from-scratch` tutorial documentation
- **#96**: Variable validation utilities
- **#94**: Extract and task splitting fixes

### Branch: `v4r6_neverending_story`

Adding support for:
- Time-invariant granules (KAPGM, KAPREDI, GRID_GEOMETRY)
- Updated metadata structure for V4r6

## Utilities

Located in `utils/`:

### Grid Geometry Generation

**`generate_GRID_GEOMETRY/`**:
- `create_latlon_grid_geometry.py`: Transform native grid geometry to lat-lon
- `gen_ECCO_V4r4_auxillary_native_grid_file_for_PODAAC.py`
- `gen_ECCO_V4r5_auxillary_native_grid_file_for_PODAAC.py`

### Metadata Validation

**`metadata_consistency/`**:
- `check_variable_metadata_key_consistency.py`
- `check_variable_metadata_unit_consistency.py`
- `check_variable_metadata_for_duplicates_and_sort.py`
- `compare_groupings.py`
- `validate_ecco_variables_across_sources.py`

### Task Management

**`tasklist_utils/`**:
- `load_and_scatter_json_tasks.py`: Distribute tasks across workers

### Missing Granules

**`identify_missing_granules/`**:
- `aws_find_missing_granules.py`: Check for gaps in AWS S3 datasets
- `split_jsons_to_single_entries.py`

## Documentation

### User Documentation

Located in `docs/`:
- Sphinx-based documentation
- Extensions in `docs/source/_ext/`
- Theme: `sphinx_rtd_theme` (Read the Docs)

### README Files

- `README.md`: Top-level project overview
- `configs/README_mapping_factors_generation.md`: Detailed mapping factors guide
- `demos/*/README.md`: Example-specific instructions
- `utils/README*.md`: Utility-specific documentation

## Common Workflows

### Typical Production Pipeline

```bash
# 1. Generate mapping factors (one-time per version/resolution)
edp_create_factors \
    --cfgfile configs/config_V4r4.yaml \
    --grid_file /data/grids/GRID_GEOMETRY_ECCO_V4r4_native_llc0090.nc \
    --output_dir /data/mapping_factors/V4r4 \
    2 3

# 2. Create job file (jobs.txt)
# Manually edit or generate from templates

# 3. Generate task list (Phase 1)
edp_create_job_task_list \
    --jobfile jobs.txt \
    --ecco_source_root /data/ecco_results/V4r4 \
    --ecco_destination_root /data/output/V4r4 \
    --ecco_grid_loc /data/grids/V4r4 \
    --ecco_mapping_factors_loc /data/mapping_factors/V4r4 \
    --ecco_metadata_loc /data/metadata/V4r4 \
    --ecco_cfg_loc configs/config_V4r4.yaml \
    --outfile tasks_V4r4.json \
    --log INFO

# 4. Generate datasets (Phase 2)
edp_generate_datasets tasks_V4r4.json --log INFO

# 5. (Optional) Sync to AWS S3
edp_aws_s3_sync \
    --source /data/output/V4r4 \
    --dest s3://ecco-datasets/V4r4 \
    --log INFO
```

### AWS Batch Processing

For large-scale production:
1. Split task list into chunks: `edp_subset_tasklists`
2. Submit array job to AWS Batch (one task per array element)
3. Each worker processes its subset independently
4. S3 as shared storage for inputs/outputs

### Changing Grid Resolution

To generate 0.25° products instead of 0.5°:

1. Create new config: `config_V4r4_0p25deg.yaml`
   ```yaml
   latlon_grid_resolution: 0.25
   ecco_production_filestr_grid_label:
       latlon: '0p25deg'
   ```

2. Generate new mapping factors:
   ```bash
   edp_create_factors \
       --cfgfile configs/config_V4r4_0p25deg.yaml \
       --grid_file /data/grids/GRID_GEOMETRY_ECCO_V4r4_native_llc0090.nc \
       --output_dir /data/mapping_factors/V4r4_0p25deg \
       2 3
   ```

3. Use new config and mapping factors in task list generation

## Troubleshooting

### Common Issues

**"Undefined configuration parameter"**: Missing key in YAML. Module logs WARNING and returns `''`. Check if key is spelled correctly.

**Mapping factors load slowly**: Files are compressed with LZMA. First access per depth level takes time. Consider keeping in warm storage for production.

**Memory issues with 3D datasets**: Each depth level loads separately. Large 50-level datasets need significant RAM. Use AWS instances with ≥32GB for full 3D processing.

**S3 authentication failures**: Ensure AWS credentials are configured. For SSO, use `--keygen` and `--profile` parameters.

**Missing input files**: Check that `ecco_source_root` contains expected directory structure. Use `--log DEBUG` to see file discovery process.

**Mismatched coordinates**: Ensure all tools (create_factors, generate_datasets, create_latlon_grid_geometry) use the same mapping_factors directory.

### Debug Logging

Enable detailed logging:
```bash
edp_generate_datasets tasks.json --log DEBUG 2>&1 | tee debug.log
```

Key log messages to watch:
- `"Using configuration data per"`: Config loaded
- `"Fetching X to Y"`: S3 downloads
- `"Loading mapping factors from"`: Sparse matrix access
- `"Loading grid from"`: Grid file access
- `"Processing variable X"`: Per-variable operations

## Key Contacts & Resources

**GitHub**: https://github.com/ECCO-GROUP/ECCO-Dataset-Production

**ECCO Group**: https://ecco-group.org/

**PO.DAAC**: https://podaac.jpl.nasa.gov/

**Developers**:
- Ian Fenty (lead): ifenty@jpl.nasa.gov
- Greg Moore: greg.moore@jpl.nasa.gov

**Related Repositories**:
- `ECCO-v4-Configurations`: Submodule with metadata groupings
- `ECCOv4-py`: Core ECCO Python utilities
- `ECCO-ACCESS`: Cloud utilities and access patterns

## Future Directions

Based on recent branch activity:

1. **Grid flexibility** (`grid_freedom` branch): Making grid parameters fully configurable, removing hard-coded assumptions

2. **Time-invariant fields** (`v4r6_neverending_story`): Support for static fields like grid geometry in dataset collections

3. **V4r6 support**: Extending to next ECCO release with updated metadata

4. **Enhanced validation**: More comprehensive checks for metadata consistency and completeness

5. **Performance optimization**: Faster mapping factor loading, parallel processing improvements
