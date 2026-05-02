# Create Latlon Grid Geometry Tool

## Purpose

This tool creates a latlon (0.5° regular grid) version of the ECCO grid geometry file from the native (LLC90) grid geometry file. It uses **exactly the same mapping factors and transformation logic** that the main EDP pipeline uses for regular granule processing, ensuring complete consistency.

## Problem Solved

Previously, the latlon grid geometry files were created "by hand" using different interpolation methods than the main pipeline. This caused inconsistencies between:
- Grid coordinate values in the latlon grid geometry file
- Grid coordinate values in the processed latlon granules

Now, the grid geometry transformation uses the same code path as granule processing.

## Workflow Order

1. **Create native grid geometry file** (manual/existing process)
2. **Create latlon grid geometry file** ← THIS TOOL
3. **Process hundreds of thousands of granules** (using main EDP pipeline)

All three steps now use consistent coordinate systems and interpolation methods.

## Usage

### Basic Example

```bash
python utils/create_latlon_grid_geometry.py \
    --native_grid_file /path/to/GRID_GEOMETRY_ECCO_V4r4_native_llc0090.nc \
    --mapping_factors_loc /path/to/mapping_factors \
    --output_file GRID_GEOMETRY_ECCO_V4r4_latlon_0p50deg.nc \
    --ecco_cfg_loc configs/config_V4r4.yaml
```

**Note**: The native grid file MUST contain the `hFacC` variable.

### Transform Only Specific Variables

```bash
python utils/create_latlon_grid_geometry.py \
    --native_grid_file GRID_GEOMETRY_ECCO_V4r4_native_llc0090.nc \
    --mapping_factors_loc /path/to/mapping_factors \
    --output_file GRID_GEOMETRY_ECCO_V4r4_latlon_0p50deg.nc \
    --variables "XC,YC,XG,YG,hFacC,hFacW,hFacS,Depth"
```

## Arguments

### Required Arguments

- `--native_grid_file`: Path to the native (LLC90) grid geometry NetCDF file
  - **Must contain `hFacC` variable** for wet point calculation
  - This is the file you want to transform to latlon
  
- `--mapping_factors_loc`: Directory containing mapping factors
  - Contains lat/lon grid definition and transformation matrices
  
- `--output_file`: Output path for the latlon grid geometry NetCDF file

### Optional Arguments

- `--ecco_cfg_loc`: Path to ECCO configuration YAML file (e.g., `configs/config_V4r4.yaml`)
  - Used for compression settings and metadata
  - If not provided, uses default compression settings
  
- `--variables`: Comma-separated list of specific variables to transform
  - If not provided, auto-detects and transforms all 2D and 3D fields
  - Example: `"XC,YC,hFacC,Depth"`

- `-l, --log`: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
  - Default: INFO

## How It Works

1. **Loads the native grid geometry file** as an xarray Dataset (the file to transform)

2. **Verifies hFacC variable exists** and calculates wet point indices
   - Requires `hFacC` to be present in the native grid file
   - Uses `hFacC > 0` to determine wet/dry points at each depth level
   - Raises an error if `hFacC` is missing

3. **Loads the mapping factors** - the same sparse matrices used by the main pipeline
   - `sparse_matrix_2D`: For surface/2D fields
   - `sparse_matrix_3D`: For 3D depth-varying fields  
   - `land_mask`: To mask land points
   - **IMPORTANT**: Also contains the latlon grid definition (lat/lon coordinates)
     in `mapping_factors/latlon_grid/latlon_grid.xz`

4. **Extracts lat/lon coordinates from mapping factors**
   - These are the coordinates that were used to create the sparse matrices
   - This ensures perfect consistency (no circular dependency!)

5. **Auto-detects transformable variables**
   - 2D fields: (tile, j, i) dimensions
   - 3D fields: (k, tile, j, i) dimensions
   - Skips pure coordinate variables (tile, i, j, k, etc.)

6. **Transforms each variable**
   - Extracts only wet points (where hFacC > 0)
   - Applies sparse matrix multiplication: `sparse_matrix.T.dot(var_wet)`
   - Applies land mask (sets land points to NaN)
   - Reshapes to latlon grid (latitude, longitude)

7. **Creates output dataset**
   - Copies variable attributes from native file
   - Copies and updates global attributes
   - Applies NetCDF4 compression
   - Writes to output file

## Technical Details

### Where Do Lat/Lon Coordinates Come From?

**KEY INSIGHT**: The lat/lon coordinates come from the **mapping factors**, not from an existing latlon grid file!

The mapping factors directory contains:
```
mapping_factors/
├── latlon_grid/
│   └── latlon_grid.xz          ← SOURCE OF LAT/LON COORDINATES
├── land_mask/
│   ├── ecco_latlon_land_mask_0.xz
│   ├── ecco_latlon_land_mask_1.xz
│   └── ...
└── sparse/
    ├── sparse_matrix_0.npz
    ├── sparse_matrix_1.npz
    └── ...
```

The `latlon_grid.xz` file contains:
- `latitude_bounds`: Shape (nlat+1, 2) - cell boundaries
- `longitude_bounds`: Shape (nlon+1, 2) - cell boundaries  
- `depth_bounds`: Shape (nz+1, 2) - depth boundaries

Cell **centers** are calculated as the midpoint of the bounds:
```python
latitude = (lat_bounds[:-1, 0] + lat_bounds[1:, 0]) / 2.0
longitude = (lon_bounds[:-1, 0] + lon_bounds[1:, 0]) / 2.0
```

This ensures the coordinates used in the output file are **exactly the same** as those used to create the sparse interpolation matrices. No circular dependency!

### Mapping Factors

The tool uses the `ECCOMappingFactors` class from the main EDP code, which loads:
- Sparse interpolation matrices (CSR format)
- Land masks for 2D and 3D grids
- Latlon grid definition (coordinates from `latlon_grid.xz`)

### Transformation Method

For **2D variables**:
```python
native_data_vector = native_data.ravel()  # Flatten to 1D
latlon_data_vector = sparse_matrix_2D.dot(native_data_vector)
latlon_data_vector[~land_mask_2D] = NaN
latlon_data = latlon_data_vector.reshape(nlat, nlon)
```

For **3D variables**:
```python
for each depth level k:
    native_data_vector = native_data[k, :, :, :].ravel()
    latlon_data_vector = sparse_matrix_3D[k].dot(native_data_vector)
    latlon_data_vector[~land_mask] = NaN
    latlon_data[k, :, :] = latlon_data_vector.reshape(nlat, nlon)
```

This is **identical** to the method in `ecco_dataset.ECCOMDSDataset.as_latlon()`.

## Example Output

```
============================================================
Creating latlon grid geometry from native grid
============================================================
INFO Loading configuration from configs/config_V4r4.yaml
INFO Loading native grid file: GRID_GEOMETRY_ECCO_V4r4_native_llc0090.nc
INFO   Found 45 data variables
INFO Loading mapping factors from: /path/to/mapping_factors
INFO   Mapping factors loaded successfully
INFO Auto-detecting transformable variables...
INFO   2D variables (15): ['XC', 'YC', 'XG', 'YG', 'RAC', 'RAS', 'RAW', 'DXC', 'DYC', 'DXG', 'DYG', ...]
INFO   3D variables (8): ['hFacC', 'hFacW', 'hFacS', 'Depth', ...]

Transforming 2D variables...
INFO Transforming XC (2D)...
INFO Transforming YC (2D)...
INFO Transforming XG (2D)...
...

Transforming 3D variables...
INFO Transforming hFacC (3D)...
INFO Transforming hFacW (3D)...
...

INFO Creating output dataset...
INFO Writing output to: GRID_GEOMETRY_ECCO_V4r4_latlon_0p50deg.nc

============================================================
SUCCESS! Latlon grid geometry file created.
Output: GRID_GEOMETRY_ECCO_V4r4_latlon_0p50deg.nc
Variables transformed: 23
============================================================
```

## Verification

After creating the latlon grid geometry file, verify consistency:

```python
import xarray as xr

# Load both files
native_grid = xr.open_dataset('GRID_GEOMETRY_ECCO_V4r4_native_llc0090.nc')
latlon_grid = xr.open_dataset('GRID_GEOMETRY_ECCO_V4r4_latlon_0p50deg.nc')

# Check dimensions
print(latlon_grid.dims)
# Expected: {'latitude': 360, 'longitude': 720, 'k': 50, ...}

# Check a transformed variable
print(latlon_grid.XC.min(), latlon_grid.XC.max())
print(latlon_grid.YC.min(), latlon_grid.YC.max())

# Verify land masking
import numpy as np
print(f"Land points (NaN): {np.isnan(latlon_grid.XC).sum().values}")
```

## Comparison with Old Method

| Aspect | Old Method | New Method (This Tool) |
|--------|-----------|------------------------|
| Interpolation | Custom/manual implementation | Same sparse matrices as main pipeline |
| Code path | Separate Jupyter notebook | Uses EDP production classes |
| Consistency | Potential drift | Guaranteed consistency |
| Maintenance | Duplicate code | Single source of truth |

## Integration with Main Pipeline

The latlon grid geometry file created by this tool is referenced in:
- Task list generation (`edp_create_job_task_list`)
- Dataset generation (`edp_generate_datasets`)
- Grid loading (`ecco_grid.ECCOGrid`)

All components now use consistently interpolated coordinates.

## Related Files

- Main EDP transformation: `src/ecco_dataset_production/ecco_dataset.py`
- Mapping factors loader: `src/ecco_dataset_production/ecco_mapping_factors.py`
- Grid loader: `src/ecco_dataset_production/ecco_grid.py`
- Old manual method: `utils/generate_GRID_GEOMETRY/gen_ECCO_V4r4_auxillary_native_grid_file_for_PODAAC.py`

## Future Enhancements

Potential additions:
- Add to main CLI as `edp_create_latlon_grid_geometry`
- Support for other target grids (not just 0.5°)
- Parallel processing for large 3D variables
- Validation checks against expected coordinate ranges
