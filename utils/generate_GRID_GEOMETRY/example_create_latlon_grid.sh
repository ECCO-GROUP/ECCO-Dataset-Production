#!/bin/bash
#
# Example script showing how to create a latlon grid geometry file
# from a native grid geometry file using the same mapping factors
# as the main EDP pipeline.
#
# This ensures consistency between grid coordinates and granule coordinates.
#
# IMPORTANT: Lat/lon coordinates come from the mapping_factors, not from
# an existing latlon grid file. This eliminates circular dependencies!
#

# Exit on error
set -e

# =============================================================================
# CONFIGURATION - Modify these paths for your environment
# =============================================================================

# Path to the native grid geometry file (input) - the file to transform
# This file MUST contain the hFacC variable for wet point calculation
NATIVE_GRID_FILE="/Users/ifenty/tmp/grids/GRID_GEOMETRY_ECCO_V4r4_native_llc0090.nc"

# Directory containing mapping factors
# Must contain:
#   - latlon_grid/latlon_grid.xz  (defines lat/lon coordinates)
#   - sparse/sparse_matrix_*.npz  (transformation matrices)
#   - land_mask/ecco_latlon_land_mask_*.xz  (land masks)
MAPPING_FACTORS_LOC="../demos/data/ecco_mapping_factors/V4r4/"

# Output file path
OUTPUT_FILE="/Users/ifenty/tmp/grids/GRID_GEOMETRY_ECCO_V4r4_latlon_0p50deg_test.nc"

# ECCO configuration file (optional, but recommended for compression settings)
ECCO_CFG_LOC="../configs/config_V4r4.yaml"

# Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
LOG_LEVEL="INFO"

# =============================================================================
# Optional: Specify variables to transform
# =============================================================================

# Leave empty to auto-detect and transform all variables
# Or specify comma-separated list, e.g.:
# VARIABLES="XC,YC,XG,YG,hFacC,hFacW,hFacS,Depth"
VARIABLES=""

# =============================================================================
# PREREQUISITES CHECK
# =============================================================================

echo "Checking prerequisites..."
echo ""

# Check if native grid file exists
if [ ! -f "$NATIVE_GRID_FILE" ]; then
    echo "ERROR: Native grid file not found: $NATIVE_GRID_FILE"
    echo "This is the file you want to transform to latlon."
    exit 1
fi
echo "  ✓ Native grid file exists: $(basename "$NATIVE_GRID_FILE")"

# Check if it contains hFacC (using ncdump if available)
if command -v ncdump &> /dev/null; then
    if ncdump -h "$NATIVE_GRID_FILE" 2>/dev/null | grep -q "hFacC"; then
        echo "  ✓ hFacC variable found in native grid file"
    else
        echo "ERROR: hFacC variable NOT found in native grid file"
        echo "The native grid file must contain hFacC for wet point calculation."
        echo "Please provide a complete ECCO grid geometry file."
        exit 1
    fi
else
    echo "  ⚠ Cannot verify hFacC (ncdump not found) - will check at runtime"
fi

# Check if mapping factors directory exists
if [ ! -d "$MAPPING_FACTORS_LOC" ]; then
    echo "ERROR: Mapping factors directory not found: $MAPPING_FACTORS_LOC"
    echo "This must contain latlon_grid/, sparse/, and land_mask/ subdirectories."
    exit 1
fi

# Check for critical files in mapping factors
if [ ! -f "$MAPPING_FACTORS_LOC/latlon_grid/latlon_grid.xz" ]; then
    echo "ERROR: Missing $MAPPING_FACTORS_LOC/latlon_grid/latlon_grid.xz"
    echo "This file contains the lat/lon grid definition (source of coordinates)."
    exit 1
fi
echo "  ✓ Found latlon grid definition: latlon_grid/latlon_grid.xz"

SPARSE_COUNT=$(find "$MAPPING_FACTORS_LOC/sparse" -name "sparse_matrix_*.npz" 2>/dev/null | wc -l)
if [ "$SPARSE_COUNT" -eq 0 ]; then
    echo "ERROR: No sparse matrices found in $MAPPING_FACTORS_LOC/sparse/"
    exit 1
fi
echo "  ✓ Found $SPARSE_COUNT sparse transformation matrices"

echo ""
echo "All prerequisites satisfied!"
echo ""

# =============================================================================
# RUN THE TRANSFORMATION
# =============================================================================

echo "============================================================"
echo "Creating latlon grid geometry file"
echo "============================================================"
echo ""
echo "How it works:"
echo "  1. Loads native grid geometry file (contains variables AND hFacC)"
echo "  2. Calculates wet point indices from hFacC"
echo "  3. Gets lat/lon coordinates from mapping factors (no circular dependency!)"
echo "  4. Applies same transformation as main EDP pipeline"
echo ""
echo "Configuration:"
echo "  Native grid file (input):    $NATIVE_GRID_FILE"
echo "  Mapping factors (coords):    $MAPPING_FACTORS_LOC"
echo "  Output file:                 $OUTPUT_FILE"
echo "  Config file:                 $ECCO_CFG_LOC"
echo ""

# Build command with required arguments
CMD="python create_latlon_grid_geometry.py \
    --native_grid_file $NATIVE_GRID_FILE \
    --mapping_factors_loc $MAPPING_FACTORS_LOC \
    --output_file $OUTPUT_FILE \
    --log $LOG_LEVEL"

# Add optional arguments if provided
if [ -n "$ECCO_CFG_LOC" ]; then
    CMD="$CMD --ecco_cfg_loc $ECCO_CFG_LOC"
fi

if [ -n "$VARIABLES" ]; then
    CMD="$CMD --variables $VARIABLES"
fi

# Execute
echo "Running command:"
echo "$CMD"
echo ""

eval $CMD

# =============================================================================
# VERIFICATION
# =============================================================================

if [ -f "$OUTPUT_FILE" ]; then
    echo ""
    echo "============================================================"
    echo "SUCCESS!"
    echo "============================================================"
    echo ""
    echo "Output file created: $OUTPUT_FILE"
    echo ""
    echo "Quick verification:"

    # If ncdump is available, show file structure
    if command -v ncdump &> /dev/null; then
        echo ""
        echo "Dimensions:"
        ncdump -h "$OUTPUT_FILE" | grep "dimensions:" -A 10
        echo ""
        echo "Variables:"
        ncdump -h "$OUTPUT_FILE" | grep "variables:" -A 20 | head -25
    else
        echo "  (Install ncdump to see file structure)"
    fi

    echo ""
    echo "IMPORTANT NOTES:"
    echo "  - Lat/lon coordinates came from mapping_factors/latlon_grid/latlon_grid.xz"
    echo "  - These are the SAME coordinates used to create the sparse matrices"
    echo "  - No circular dependency - no existing latlon grid file was needed!"
    echo ""
    echo "You can now use this file with the main EDP pipeline."
    echo "Grid coordinates will be consistent with processed granules."
    echo ""
    echo "Next steps:"
    echo "  1. Verify coordinates: python -c \"import xarray as xr; ds=xr.open_dataset('$OUTPUT_FILE'); print(ds.dims); print(ds.longitude.values[:5])\""
    echo "  2. Process granules: edp_create_job_task_list ... && edp_generate_datasets ..."
    echo "  3. Verify consistency: Compare grid coords with granule coords"
else
    echo ""
    echo "ERROR: Output file was not created!"
    exit 1
fi
