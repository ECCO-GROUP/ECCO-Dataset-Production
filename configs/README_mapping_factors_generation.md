# Generating Mapping Factors

The mapping factors (sparse interpolation matrices, land masks, and lat-lon grid definitions) are precomputed transformations that map data from the native LLC90 grid to a regular 0.5° lat-lon grid.

## Configuration Parameters

The config files (`config_V4r4.yaml`, `config_V4r5.yaml`, `config_V4r6.yaml`) include the grid definition parameters:

```yaml
#---------------------------------------------------------------------
# Mapping factors generation parameters:
# Note: Grid file paths and mapping factors directory are specified
# as command-line arguments to the tools that need them.
#---------------------------------------------------------------------

# Resolution of the lat-lon grid used for transformation (degrees):
latlon_grid_resolution: 0.5

# Area extent for lat-lon grid [lon_min, lat_max, lon_max, lat_min]:
latlon_grid_area_extent: [-180.0, 90.0, 180.0, -90.0]

# Effective grid cell radius for mapping (km).
# If not specified (null), defaults to: (111/2)*sqrt(2) ≈ 78.5 km
latlon_effective_grid_radius: null

# Use custom grid and factors (false = use ECCO grids):
custom_grid_and_factors: false

# Number of vertical levels:
num_vertical_levels: 50
```

**Note:** File paths are specified as command-line arguments, not in the config files. This makes the config files portable across different systems.

## Generating Mapping Factors

The `edp_create_factors` tool now accepts file paths as command-line arguments, making the config files portable across systems.

### Command-Line Usage

```bash
# Generate both 2D and 3D mapping factors
edp_create_factors \
    --cfgfile configs/config_V4r4.yaml \
    --grid_file /path/to/GRID_GEOMETRY_ECCO_V4r4_native_llc0090.nc \
    --output_dir /path/to/output/mapping_factors \
    2 3 \
    --log INFO

# Generate only 2D factors
edp_create_factors \
    --cfgfile configs/config_V4r4.yaml \
    --grid_file /path/to/GRID_GEOMETRY_ECCO_V4r4_native_llc0090.nc \
    --output_dir /path/to/output/mapping_factors \
    2 \
    --log INFO

# Force recalculation even if factors already exist
edp_create_factors \
    --cfgfile configs/config_V4r4.yaml \
    --grid_file /path/to/GRID_GEOMETRY_ECCO_V4r4_native_llc0090.nc \
    --output_dir /path/to/output/mapping_factors \
    2 3 \
    --force \
    --log INFO
```

### Required Arguments

- `--cfgfile`: Configuration file with grid definition parameters (resolution, extent, etc.)
- `--grid_file`: Path to native ECCO grid geometry NetCDF file (must contain hFacC, XC, YC, etc.)
- `--output_dir`: Directory where mapping factors will be written
- `dims`: Space-separated list of dimensions to generate (2, 3, or both)

### Optional Arguments

- `--log`: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL; default: WARNING)
- `--force`: Force recalculation of mapping factors even if they already exist (by default, existing factors are reused)

This will create the following directory structure:

```
./mapping_factors/
├── ecco_latlon_grid_mappings_all.xz
├── ecco_latlon_grid_mappings_2D.xz
├── 3D/
│   ├── ecco_latlon_grid_mappings_3D_0.xz
│   ├── ecco_latlon_grid_mappings_3D_1.xz
│   └── ... (one file per depth level)
├── latlon_grid/
│   └── latlon_grid.xz          # Contains lat/lon coordinates and bounds
├── land_mask/
│   ├── ecco_latlon_land_mask_0.xz
│   ├── ecco_latlon_land_mask_1.xz
│   └── ... (one file per depth level)
└── sparse/
    ├── sparse_matrix_0.npz
    ├── sparse_matrix_1.npz
    └── ... (one sparse matrix per depth level)
```

## What Gets Stored

### `latlon_grid/latlon_grid.xz`
Contains the **source of truth** for lat-lon coordinates:
- `latitude_bounds`: (nlat+1, 2) array of cell boundaries
- `longitude_bounds`: (nlon+1, 2) array of cell boundaries
- `depth_bounds`: (nz+1, 2) array of depth boundaries

Cell centers are calculated as midpoints of the bounds.

### `sparse/sparse_matrix_*.npz`
Sparse CSR matrices that transform native grid wet points to lat-lon grid points.
One matrix per depth level (0 to nz-1).

### `land_mask/ecco_latlon_land_mask_*.xz`
Land masks for each depth level. Points over land are NaN.

## Important Notes

1. **Grid resolution is fixed**: Once mapping factors are created with a specific resolution (e.g., 0.5°), that resolution is "baked into" the files. To change resolution, you must regenerate all mapping factors.

2. **Coordinates come from mapping factors**: When processing granules or creating grid geometry files, the lat-lon coordinates always come from `latlon_grid/latlon_grid.xz`, ensuring consistency.

3. **Large computation**: Generating mapping factors for 3D fields with 50 depth levels can take significant time and memory.

4. **One-time setup**: For each ECCO version, mapping factors are typically generated once and then reused for all granule processing.

## Changing Resolution

To generate mapping factors at a different resolution (e.g., 0.25°):

1. Edit the config file:
   ```yaml
   latlon_grid_resolution: 0.25
   mapping_factors_dir: './mapping_factors_0p25deg'
   ```

2. Run the create_factors command:
   ```bash
   edp_create_factors --cfgfile configs/config_V4r4_0p25deg.yaml 2 3
   ```

3. Update the grid label in your config:
   ```yaml
   ecco_production_filestr_grid_label:
       latlon: '0p25deg'
   ```

This creates a completely new set of mapping factors at the new resolution.

## Related Tools

- **`edp_create_factors`**: Generate mapping factors (this document)
- **`utils/create_latlon_grid_geometry.py`**: Transform grid geometry files using existing mapping factors
- **`edp_generate_datasets`**: Process granules using mapping factors

All three tools use the same mapping factors to ensure coordinate consistency.
