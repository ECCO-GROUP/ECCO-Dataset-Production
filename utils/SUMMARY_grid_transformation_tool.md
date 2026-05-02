# Grid Transformation Tool - Summary

## Problem Solved

**Circular Workflow Issue**: Previously, latlon grid geometry files were created "by hand" using Jupyter notebooks with custom interpolation logic that was **different** from the main EDP pipeline's transformation method. This caused inconsistencies between:
- Coordinate values in the latlon grid geometry file
- Coordinate values in processed latlon granules

## Solution

A new tool (`create_latlon_grid_geometry.py`) that uses the **exact same** transformation infrastructure as the main EDP pipeline:
- Same sparse matrices (mapping factors)
- Same wet point extraction logic  
- Same land masking
- Guaranteed consistency

## Files Created

1. **`create_latlon_grid_geometry.py`** - Main transformation tool
   - Uses `ECCOGrid`, `ECCOMappingFactors` from main EDP code
   - Transforms native grid geometry → latlon grid geometry
   - Matches `ecco_dataset.ECCOMDSDataset.as_latlon()` logic exactly

2. **`README_create_latlon_grid_geometry.md`** - Complete documentation
   - Usage examples
   - Argument descriptions
   - Technical details
   - Verification instructions

3. **`example_create_latlon_grid.sh`** - Ready-to-run example script
   - Pre-configured for demo data
   - Easy to modify for production use

## Correct Workflow Order

```
┌─────────────────────────────────────────────────────────────┐
│  STEP 1: Create Native Grid Geometry (manual/existing)     │
│                                                             │
│  Output: GRID_GEOMETRY_ECCO_V4r4_native_llc0090.nc        │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│  STEP 2: Create Latlon Grid Geometry (THIS NEW TOOL)       │
│                                                             │
│  $ python create_latlon_grid_geometry.py \                 │
│      --native_grid_file GRID_GEOMETRY_*_native_*.nc \      │
│      --ecco_grid_loc /path/to/grids \                      │
│      --mapping_factors_loc /path/to/factors \              │
│      --output_file GRID_GEOMETRY_*_latlon_*.nc             │
│                                                             │
│  Uses: SAME mapping factors as main pipeline               │
│  Output: GRID_GEOMETRY_ECCO_V4r4_latlon_0p50deg.nc        │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│  STEP 3: Process Regular Granules (main EDP pipeline)      │
│                                                             │
│  $ edp_create_job_task_list --jobfile jobs.txt ...         │
│  $ edp_generate_datasets --tasklist tasks.json             │
│                                                             │
│  Uses: SAME mapping factors → consistent coordinates!       │
└─────────────────────────────────────────────────────────────┘
```

## Key Technical Details

### Transformation Method (matches pipeline exactly)

**For 2D variables:**
```python
var_wet = var[native_wet_point_indices[0]]     # Extract only wet points
var_latlon = sparse_matrix.T.dot(var_wet)       # Apply sparse matrix
var_latlon[~land_mask] = NaN                    # Mask land
var_latlon = var_latlon.reshape(nlat, nlon)    # Reshape to grid
```

**For 3D variables:**
```python
for each depth level z:
    var_z_wet = var[z][native_wet_point_indices[z]]
    var_z_latlon = sparse_matrix[z].T.dot(var_z_wet)
    var_z_latlon[~land_mask] = NaN
    var_z_latlon = var_z_latlon.reshape(nlat, nlon)
```

### Dependencies

- Uses existing EDP classes: `ECCOGrid`, `ECCOMappingFactors`
- No new dependencies beyond main EDP requirements
- Works with local or S3-hosted mapping factors

## Benefits

1. **Consistency**: Grid coordinates match across all products
2. **Single Source of Truth**: One transformation method for everything
3. **Maintainability**: Changes to pipeline logic automatically apply
4. **Validation**: Same code = same results, easier to verify
5. **Reproducibility**: Clear, documented, scriptable workflow

## Quick Start

```bash
# Navigate to utils directory
cd ECCO-Dataset-Production/utils

# Edit example script with your paths
nano example_create_latlon_grid.sh

# Run it
./example_create_latlon_grid.sh
```

Or run directly:

```bash
python create_latlon_grid_geometry.py \
    --native_grid_file path/to/GRID_GEOMETRY_ECCO_V4r4_native_llc0090.nc \
    --ecco_grid_loc path/to/grid_files \
    --mapping_factors_loc path/to/mapping_factors \
    --output_file GRID_GEOMETRY_ECCO_V4r4_latlon_0p50deg.nc \
    --ecco_cfg_loc ../configs/config_V4r4.yaml
```

## Verification

After running, verify consistency:

```python
import xarray as xr
import numpy as np

# Load both grid files
native = xr.open_dataset('GRID_GEOMETRY_*_native_*.nc')
latlon = xr.open_dataset('GRID_GEOMETRY_*_latlon_*.nc')

# Check dimensions
print(f"Latlon grid: {latlon.dims}")
# Expected: {'latitude': 360, 'longitude': 720, 'k': 50, ...}

# Check coordinate ranges
print(f"Longitude range: {latlon.longitude.min().values} to {latlon.longitude.max().values}")
print(f"Latitude range: {latlon.latitude.min().values} to {latlon.latitude.max().values}")

# Verify land masking
print(f"Land points (NaN): {np.isnan(latlon.XC).sum().values}")

# Compare with a processed granule (after running pipeline)
granule = xr.open_dataset('SEA_SURFACE_HEIGHT_*_latlon_*.nc')
print(f"\nCoordinate consistency check:")
print(f"Grid lon matches granule: {np.allclose(latlon.longitude, granule.longitude)}")
print(f"Grid lat matches granule: {np.allclose(latlon.latitude, granule.latitude)}")
```

## Future Integration

This tool could be added to the main CLI as:
```bash
edp_create_latlon_grid_geometry --native_grid_file ... --output_file ...
```

Simply add entry point to `pyproject.toml`:
```toml
[project.scripts]
edp_create_latlon_grid_geometry = 'ecco_dataset_production.apps.create_latlon_grid_geometry:main'
```

## References

- Main pipeline transformation: `src/ecco_dataset_production/ecco_dataset.py:387-501`
- Mapping factors: `src/ecco_dataset_production/ecco_mapping_factors.py`
- Grid loader: `src/ecco_dataset_production/ecco_grid.py`
- Old manual method: `utils/generate_GRID_GEOMETRY/gen_ECCO_V4r4_auxillary_native_grid_file_for_PODAAC.py`
