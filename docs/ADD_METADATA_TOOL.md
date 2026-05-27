# Adding Metadata to Bare NetCDF Files

## Overview

The `edp_add_metadata` tool applies ECCO metadata (global, variable, and coordinate attributes) to existing NetCDF files without requiring external grid files or mapping factors. This is specifically designed for "bare" NetCDF files (like grid geometry files) that already contain their coordinate data but need standardized ECCO metadata added.

## Key Differences from Other Tools

- **`edp_add_metadata`**: For bare NetCDF files that already contain coordinates. Does NOT load external grid files or mapping factors.
- **`process_time_invariant_granule()`**: Requires grid files and/or mapping factors to add coordinate bounds.
- **`edp_generate_datasets`**: Full pipeline that loads MDS binary files, transforms data, and creates granules from scratch.

## Use Cases

1. **Grid Geometry Files**: Adding metadata to grid geometry NetCDF files created by external tools
2. **Pre-processed Data**: Adding ECCO-compliant metadata to NetCDF files from other sources
3. **Metadata Updates**: Updating metadata on existing files while preserving data
4. **Bootstrapping**: Creating initial grid files that will later be used by other tools

## Command-Line Usage

### Basic Example

```bash
# Dimension is auto-detected from the file
edp_add_metadata \
    --input bare_grid.nc \
    --output GRID_GEOMETRY_ECCO_V4r6_native_llc0090.nc \
    --metadata /path/to/ECCO-v4-Configurations/metadata/V4r6 \
    --config configs/config_V4r6.yaml \
    --grid-type native
```

### With Attribute Stripping

If your input file has existing attributes you want to replace:

```bash
edp_add_metadata \
    --input input_with_old_metadata.nc \
    --output output_with_new_metadata.nc \
    --metadata /path/to/metadata \
    --config configs/config_V4r6.yaml \
    --strip-attributes \
    --log DEBUG
```

### For Lat-Lon Grid Files

```bash
edp_add_metadata \
    --input bare_latlon_grid.nc \
    --output GRID_GEOMETRY_ECCO_V4r6_latlon_0p50deg.nc \
    --metadata /path/to/metadata \
    --config configs/config_V4r6.yaml \
    --grid-type latlon \
    --dimension 2D
```

### For 3D Files

```bash
# Dimension auto-detected if file has Z, k, k_l, etc. dimensions
edp_add_metadata \
    --input 3d_data.nc \
    --output output_3d.nc \
    --metadata /path/to/metadata \
    --config configs/config_V4r6.yaml

# Or explicitly specify 3D
edp_add_metadata \
    --input 3d_data.nc \
    --output output_3d.nc \
    --metadata /path/to/metadata \
    --config configs/config_V4r6.yaml \
    --dimension 3D
```

## Python API Usage

You can also use this functionality directly in Python:

```python
from ecco_dataset_production import ecco_generate_datasets

ecco_generate_datasets.apply_metadata_to_netcdf(
    input_netcdf='bare_grid.nc',
    output_netcdf='GRID_GEOMETRY_ECCO_V4r6_native_llc0090.nc',
    ecco_metadata_loc='/path/to/metadata',
    cfg='configs/config_V4r6.yaml',
    grid_type='native',
    is_2d=True,
    strip_attributes=False,
    log_level='INFO'
)
```

## Command-Line Options

### Required Arguments

- `-i, --input`: Path to input NetCDF file
- `-o, --output`: Path for output NetCDF file with metadata applied
- `-m, --metadata`: Path to ECCO metadata directory (or S3 bucket/prefix)
- `-c, --config`: Path to ECCO configuration YAML file (or S3 object)

### Optional Arguments

- `-g, --grid-type`: Grid type - `native` or `latlon` (default: `native`)
- `-d, --dimension`: Dataset dimension - `2D` or `3D` (default: auto-detect from file)
  - Auto-detection checks for vertical dimensions: Z, k, k_l, k_u, k_p1
  - If any of these dimensions exist, dataset is 3D; otherwise 2D
- `-s, --strip-attributes`: Strip all existing attributes before applying new ones
- `-l, --log`: Logging level - `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL` (default: `INFO`)

### AWS S3 Options

If metadata or config files are on S3:

- `--keygen`: AWS SSO key generation script path
- `--profile`: AWS profile name (e.g., `saml-pub`, `default`)

## What Gets Added

The tool applies the following metadata:

### Variable Attributes
- `long_name`: Human-readable variable name
- `standard_name`: CF standard name (if applicable)
- `units`: Physical units
- `valid_min` / `valid_max`: Data range (for numeric types)
- `_FillValue`: Fill value for missing data (type-appropriate)
- **Data type preservation**: Boolean and integer types (uint8, int8, uint16, int16, int32, int64) are preserved with their original precision; only floating-point types are converted to the configured precision

### Coordinate Attributes
- Coordinate metadata for appropriate grid type (native/latlon)
- Dimension information
- Bounds information (if coordinates already exist)

### Global Attributes
- Dataset identification (`id`, `title`, `product_name`)
- Spatial coverage (`geospatial_*`)
- Temporal coverage (`product_time_coverage_*`)
- DOI and metadata links (`identifier_product_doi`, `metadata_link`)
- Creation timestamps (`date_created`, `date_modified`, etc.)
- GCMD keywords
- UUID
- All other ECCO-standard global attributes

### Encoding
- NetCDF4 compression (zlib, complevel=5, shuffle)
- Appropriate data types:
  - **Boolean variables**: Preserved as bool (no fill value)
  - **Integer variables**: Preserved with original precision
    - `uint8` → NetCDF `u1` (unsigned byte)
    - `int8` → NetCDF `i1` (signed byte)
    - `uint16` → NetCDF `u2` (unsigned short)
    - `int16` → NetCDF `i2` (signed short)
    - `int32/int64` → NetCDF `i4` (signed int)
  - **Floating-point variables**: Converted to float32 (by default, configurable)
- Fill values (type-appropriate for each data type)

## Grid Geometry File Special Handling

When the output filename contains `GRID_GEOMETRY`, the tool automatically uses dataset-specific descriptions from `grid_geometry_dataset_descriptions.json` in the metadata directory. These descriptions are templates that get populated with values from the config file:

**Template Variables:**
- `{llc_grid_size}` - Native grid size (e.g., 90)
- `{llc_code}` - Native grid code (e.g., llc90)
- `{product_version}` - ECCO product version (e.g., Version 4, Release 6)
- `{ecco_version}` - Short version code (e.g., V4r6)
- `{latlon_grid_resolution}` - Lat-lon grid resolution (e.g., 0.5)

**Native Grid Geometry Example:**
```
This dataset provides geometric parameters for the lat-lon-cap 90 (llc90) native 
model grid from the ECCO Version 4, Release 6 ocean and sea-ice state estimate. 
Parameters include areas and lengths of grid cell sides; horizontal and vertical 
coordinates of grid cell centers and corners; grid rotation angles; and global 
domain geometry including bathymetry and land/ocean masks.
```

**Lat-Lon Grid Geometry Example:**
```
This dataset provides geometric parameters for the regular 0.5-degree lat-lon grid 
from the ECCO Version 4, Release 6 (V4r6) ocean and sea-ice state estimate. 
Parameters include areas and lengths of grid cell sides and the horizontal and 
vertical coordinates of grid cell centers and corners. Additional information 
related to the global domain geometry (e.g., bathymetry and land/ocean masks) are 
also included. However, users should note these domain geometry fields are 
approximations because they have been interpolated from the ECCO lat-lon-cap 90 
(llc90) native model grid.
```

For non-GRID_GEOMETRY files, the summary is constructed as:
```
{dataset_summary} + {project_summary from config}
```

## Automatic Dimension Detection

The tool automatically detects whether your dataset is 2D or 3D by inspecting the NetCDF dimensions:

- **3D detection**: If any of these dimensions exist: `Z`, `k`, `k_l`, `k_u`, `k_p1`
- **2D detection**: If no vertical dimensions found

The auto-detection is logged so you can verify:
```
INFO ... Auto-detected 3D dataset (found vertical dimension "k")
```

You can override auto-detection by explicitly specifying `--dimension 2D` or `--dimension 3D`.

## Input File Requirements

Your input NetCDF file should:

1. **Already contain coordinate data**: The tool does NOT add coordinates from external sources
2. **Have recognizable variable names**: Variable names should match those in the metadata JSON files
3. **Be readable by xarray**: Standard NetCDF3/4 format

## Metadata Directory Structure

The metadata directory should contain JSON files with naming patterns:

- `*variable_metadata*.json`: Variable-specific attributes
- `*coordinate_metadata_for_native_datasets*.json`: Native grid coordinate attributes
- `*coordinate_metadata_for_latlon_datasets*.json`: Lat-lon grid coordinate attributes
- `*global_metadata_for_all_datasets*.json`: Common global attributes
- `*global_metadata_for_native_datasets*.json`: Native-specific global attributes
- `*global_metadata_for_latlon_datasets*.json`: Lat-lon-specific global attributes
- `grid_geometry_dataset_descriptions.json`: Dataset-specific descriptions for GRID_GEOMETRY files

## Configuration File

The YAML config file should include:

- `ecco_version`: Version string (e.g., 'V4r6')
- `product_version`: Product version string (e.g., 'Version 4, Release 6')
- `array_precision`: Data type (e.g., 'float32')
- `netcdf4_compression_encodings`: Compression settings
- `doi_prefix`, `doi_authority`: DOI metadata
- `model_start_time`, `model_end_time`: Model time range
- `history`, `source`, `references`: Documentation strings
- `project_summary`: General ECCO project description
- `llc_grid_size`: Native grid size (e.g., 90)
- `llc_code`: Native grid code (e.g., 'llc90')
- `latlon_grid_resolution`: Lat-lon grid resolution (e.g., 0.5)

## Troubleshooting

### "Undefined configuration parameter" warnings

Missing keys in YAML config. Check that your config file has all required fields.

### "No such file or directory" for metadata

Ensure metadata path points to directory containing JSON files, not individual files.

### Variables missing metadata

Variable names in your NetCDF must match those defined in `*variable_metadata*.json`. Check variable names with:

```bash
ncdump -h input.nc | grep variables:
```

### Coordinate metadata not applied

Ensure:
1. Your grid type (`--grid-type`) matches your data
2. Coordinate names in NetCDF match those in metadata JSON files
3. Appropriate coordinate metadata JSON file exists

## Examples

See the `demos/grid/` directory for complete examples.

## Related Tools

- `edp_generate_datasets`: Full granule generation from MDS files
- `edp_create_factors`: Generate mapping factors for grid transformations
- `create_latlon_grid_geometry.py`: Transform native grid to lat-lon

## Further Reading

- [ECCO Dataset Production README](../README.md)
- [Mapping Factors Guide](../configs/README_mapping_factors_generation.md)
- [CLAUDE.md](../CLAUDE.md) - Complete codebase documentation
