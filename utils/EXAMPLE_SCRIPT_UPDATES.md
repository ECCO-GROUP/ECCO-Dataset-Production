# Example Script Updates

## Changes Made to `example_create_latlon_grid.sh`

### 1. Clarified Header Comments

**Before:**
```bash
# Directory containing grid files (both native and latlon)
# This is the same directory used by the main pipeline
ECCO_GRID_LOC="../demos/data/ecco_grids"
```

**After:**
```bash
# Directory containing ECCO native grid with hFacC variable
# This is used ONLY to calculate wet point indices (where hFacC > 0)
# Must contain a file matching pattern: *native*.nc with hFacC variable
ECCO_GRID_LOC="../demos/data/ecco_grids"

# Directory containing mapping factors
# Must contain:
#   - latlon_grid/latlon_grid.xz  (defines lat/lon coordinates)
#   - sparse/sparse_matrix_*.npz  (transformation matrices)
#   - land_mask/ecco_latlon_land_mask_*.xz  (land masks)
MAPPING_FACTORS_LOC="../demos/data/ecco_mapping_factors"
```

Key change: Explicitly states that `ECCO_GRID_LOC` is **only for hFacC**, not for existing latlon grids.

### 2. Added Important Note at Top

```bash
# IMPORTANT: Lat/lon coordinates come from the mapping_factors, not from
# an existing latlon grid file. This eliminates circular dependencies!
```

### 3. Enhanced Output Messages

**Before:**
```bash
echo "Configuration:"
echo "  Native grid file: $NATIVE_GRID_FILE"
echo "  ECCO grid location: $ECCO_GRID_LOC"
```

**After:**
```bash
echo "How it works:"
echo "  1. Loads native grid geometry file (to transform)"
echo "  2. Loads native grid with hFacC (for wet point indices)"
echo "  3. Gets lat/lon coordinates from mapping factors"
echo "  4. Applies same transformation as main EDP pipeline"
echo ""
echo "Configuration:"
echo "  Native grid file (input):    $NATIVE_GRID_FILE"
echo "  ECCO grid loc (for hFacC):   $ECCO_GRID_LOC"
echo "  Mapping factors (coords):    $MAPPING_FACTORS_LOC"
```

Clarifies the role of each path and the workflow.

### 4. Added Prerequisites Check Section

New section that validates:
- ✓ Native grid file exists
- ✓ ECCO grid directory exists
- ✓ Native grid with hFacC is present
- ✓ Mapping factors directory exists
- ✓ Critical files exist (latlon_grid.xz, sparse matrices)

This catches configuration errors **before** running the script.

### 5. Enhanced Success Message

**Added:**
```bash
echo "IMPORTANT NOTES:"
echo "  - Lat/lon coordinates came from mapping_factors/latlon_grid/latlon_grid.xz"
echo "  - These are the SAME coordinates used to create the sparse matrices"
echo "  - No circular dependency - no existing latlon grid file was needed!"
echo ""
echo "Next steps:"
echo "  1. Verify coordinates: python -c \"import xarray as xr; ...\""
echo "  2. Process granules: edp_create_job_task_list ... && edp_generate_datasets ..."
echo "  3. Verify consistency: Compare grid coords with granule coords"
```

Helps users understand:
- Where coordinates came from
- That no circular dependency exists
- What to do next

## Summary of Key Messages

The updated script now clearly communicates:

1. **No circular dependency**: Coordinates come from mapping_factors, not from an existing latlon grid
2. **Purpose of each path**:
   - `NATIVE_GRID_FILE`: The file being transformed
   - `ECCO_GRID_LOC`: Only for hFacC (wet point calculation)
   - `MAPPING_FACTORS_LOC`: Source of coordinates AND transformation matrices
3. **Prerequisites**: Checks everything exists before running
4. **Workflow explanation**: 4-step process clearly described
5. **Next steps**: Guidance on verification and using the output

## Running the Script

```bash
cd utils
./example_create_latlon_grid.sh
```

If prerequisites are missing, it will report exactly what's needed:
```
ERROR: No *native*.nc file found in ../demos/data/ecco_grids
This file is needed to calculate wet point indices (hFacC > 0).
```

If successful:
```
============================================================
SUCCESS!
============================================================

Output file created: ./GRID_GEOMETRY_ECCO_V4r4_latlon_0p50deg.nc

IMPORTANT NOTES:
  - Lat/lon coordinates came from mapping_factors/latlon_grid/latlon_grid.xz
  - These are the SAME coordinates used to create the sparse matrices
  - No circular dependency - no existing latlon grid file was needed!
```
