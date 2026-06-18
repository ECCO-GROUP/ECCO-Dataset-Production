# Circular Dependency Fix

## The Problem You Spotted

You correctly identified a **circular dependency** in the original code at lines 151-152:

```python
# WRONG - This requires an existing latlon grid file!
nlat = grid.latlon_grid['latitude'].shape[0]
nlon = grid.latlon_grid['longitude'].shape[0]
```

This tried to load an **existing latlon grid NetCDF file** to get the lat/lon coordinates - but that's the very file we're trying to create! 

## The Solution

The lat/lon coordinates actually come from the **mapping factors**, not from an existing grid file. The mapping factors directory structure is:

```
mapping_factors/
├── latlon_grid/
│   └── latlon_grid.xz          ← This contains the lat/lon grid definition!
├── land_mask/
│   └── ecco_latlon_land_mask_*.xz
└── sparse/
    └── sparse_matrix_*.npz
```

The `latlon_grid.xz` file contains the grid definition (latitude/longitude bounds) that was used to **create** the sparse interpolation matrices. This is the **source of truth** for coordinates.

## What Was Changed

### 1. Removed dependency on `ECCOGrid`

**Before:**
```python
from ecco_dataset_production import ecco_grid

grid = ecco_grid.ECCOGrid(grid_loc=ecco_grid_loc, ...)
nlat = grid.latlon_grid['latitude'].shape[0]  # Requires latlon file!
```

**After:**
```python
# Load native grid directly (just for hFacC)
native_grid_ds = xr.open_dataset('path/to/native_grid.nc')
native_wet_indices = calculate_native_wet_point_indices(native_grid_ds)
```

### 2. Get coordinates from mapping factors

**Before:**
```python
latitude = grid.latlon_grid['latitude'].data   # From existing latlon file
longitude = grid.latlon_grid['longitude'].data
```

**After:**
```python
# Get from mapping factors (no circular dependency!)
lat_bounds = mapping_factors.latitude_bounds    # From latlon_grid.xz
lon_bounds = mapping_factors.longitude_bounds

# Calculate cell centers from bounds
latitude = (lat_bounds[:-1, 0] + lat_bounds[1:, 0]) / 2.0
longitude = (lon_bounds[:-1, 0] + lon_bounds[1:, 0]) / 2.0
```

### 3. Updated function signature

**Before:**
```python
def transform_variable_to_latlon(native_ds, var_name, grid, mapping_factors, is_3d=False):
    wet_indices = grid.native_wet_point_indices[z]  # From ECCOGrid
```

**After:**
```python
def transform_variable_to_latlon(native_ds, var_name, native_grid_ds,
                                  native_wet_indices, mapping_factors, is_3d=False):
    wet_pts = native_wet_indices[z]  # Precomputed from hFacC
```

## Why This Works

The mapping factors were created using a specific lat/lon grid definition. That same grid definition is stored in `mapping_factors/latlon_grid/latlon_grid.xz`.

By reading the grid definition from the mapping factors, we ensure:
1. **No circular dependency** - we don't need an existing latlon grid file
2. **Perfect consistency** - coordinates match the sparse matrices exactly
3. **Single source of truth** - mapping factors define the grid

## Data Flow

```
mapping_factors/latlon_grid/latlon_grid.xz
    │
    ├─> latitude_bounds, longitude_bounds
    │       │
    │       └─> Calculate cell centers → latitude, longitude coordinates
    │
    └─> Used to assign coordinates in output DataArrays

mapping_factors/sparse/sparse_matrix_*.npz
    │
    └─> Transform native → latlon using these matrices
        (which were created using the same grid definition!)
```

## Files Modified

1. **`create_latlon_grid_geometry.py`**:
   - Added `calculate_native_wet_point_indices()` function
   - Removed `ECCOGrid` dependency
   - Get coordinates from `mapping_factors.latitude_bounds` / `longitude_bounds`
   - Updated function calls to pass `native_wet_indices` instead of `grid`

2. **`README_create_latlon_grid_geometry.md`**:
   - Added "Where Do Lat/Lon Coordinates Come From?" section
   - Clarified the workflow steps
   - Explained coordinate calculation from bounds

## Verification

You can verify the coordinates are correct by checking:

```python
import lzma
import pickle
import numpy as np

# Load from mapping factors
with lzma.open('mapping_factors/latlon_grid/latlon_grid.xz', 'rb') as f:
    latlon_grid = pickle.load(f)

lat_bounds = latlon_grid[0]['lat']  # Shape: (361, 2)
lon_bounds = latlon_grid[0]['lon']  # Shape: (721, 2)

# Calculate centers
latitude = (lat_bounds[:-1, 0] + lat_bounds[1:, 0]) / 2.0  # Shape: (360,)
longitude = (lon_bounds[:-1, 0] + lon_bounds[1:, 0]) / 2.0  # Shape: (720,)

print(f"Latitude range: {latitude.min():.2f} to {latitude.max():.2f}")
print(f"Longitude range: {longitude.min():.2f} to {longitude.max():.2f}")
```

Expected output for 0.5° grid:
```
Latitude range: -89.75 to 89.75
Longitude range: 0.25 to 359.75
```

## Summary

Your question uncovered a critical issue! The fix ensures:
- ✅ No circular dependency
- ✅ Coordinates from the authoritative source (mapping factors)
- ✅ Perfect consistency with main pipeline
- ✅ Tool can run without pre-existing latlon grid file

Thank you for catching this! 🎯
