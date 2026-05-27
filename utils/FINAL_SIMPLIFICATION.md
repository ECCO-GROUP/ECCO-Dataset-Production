# Final Simplification - One Grid File Only

## The Change

**Removed `--ecco_grid_loc` parameter completely**. The tool now requires that `native_grid_file` contains `hFacC`.

## Why This Makes Sense

Standard ECCO grid geometry files **always include hFacC**. It's a fundamental part of the grid definition. There's no reason to support edge cases where it's missing.

## What Changed

### Command Line Arguments

**Before (confusing):**
```bash
python create_latlon_grid_geometry.py \
    --native_grid_file grid.nc \          # File to transform
    --ecco_grid_loc /path/to/grids \      # Another grid for hFacC?
    --mapping_factors_loc /path/factors \ # Coords and matrices
    --output_file output.nc
```

**After (simple):**
```bash
python create_latlon_grid_geometry.py \
    --native_grid_file grid.nc \          # Has everything we need!
    --mapping_factors_loc /path/factors \ # Coords and matrices
    --output_file output.nc
```

### Error Handling

**Now:** Clear error if hFacC is missing:
```
RuntimeError: Native grid file must contain 'hFacC' variable for wet point calculation.
File: /path/to/grid.nc
Variables found: ['XC', 'YC', 'XG', 'YG', ...]
Please provide a complete ECCO grid geometry file with hFacC.
```

No fallback logic, no confusion about which file to use.

### Code Changes

**Removed:**
- `--ecco_grid_loc` argument
- Fallback logic to load separate grid file
- Complex conditional checks
- `native_grid_ds` separate variable
- Duplicate file handling

**Simplified:**
- One grid file variable: `native_ds`
- Direct check: `if 'hFacC' not in native_ds: raise error`
- Single wet point calculation: `calculate_native_wet_point_indices(native_ds)`

## Function Signature

**Before:**
```python
def transform_variable_to_latlon(native_ds, var_name, native_grid_ds,
                                  native_wet_indices, mapping_factors, is_3d=False):
```

**After:**
```python
def transform_variable_to_latlon(native_ds, var_name, native_wet_indices,
                                  mapping_factors, is_3d=False):
```

One less parameter!

## Required Files Structure

### Native Grid File (Input)
**Must contain:**
- Variables to transform (XC, YC, hFacC, hFacW, Depth, etc.)
- `hFacC` variable for wet point calculation

### Mapping Factors Directory
**Must contain:**
```
mapping_factors/
├── latlon_grid/
│   └── latlon_grid.xz          ← Lat/lon coordinates
├── land_mask/
│   └── ecco_latlon_land_mask_*.xz
└── sparse/
    └── sparse_matrix_*.npz     ← Transformation matrices
```

That's it! Two inputs, no confusion.

## What Each Input Does

| Input | Purpose | Contains |
|-------|---------|----------|
| `native_grid_file` | File to transform | Variables + hFacC |
| `mapping_factors_loc` | Transform definition | Lat/lon coords + matrices |
| `output_file` | Where to write | N/A (output) |

## Benefits

### 1. Simpler Mental Model
"Transform this grid file using these mapping factors" - that's it!

### 2. Clearer Errors
If hFacC is missing, you get a clear error telling you exactly what's needed.

### 3. Less Code
- No fallback logic
- No duplicate file tracking
- No conditional flow

### 4. Fewer Questions
Users won't ask "Why do I need two grid locations?"

### 5. Standard Use Case
Supports what 99% of users need - transforming a complete grid geometry file.

## Edge Cases Removed

**No longer supported:**
- Grid file without hFacC (not a valid ECCO grid geometry file)
- Loading hFacC from a separate file (unnecessary complexity)
- Mixed grid sources (confusing)

**Philosophy:** If your grid file doesn't have hFacC, fix the grid file, don't add fallback logic.

## Updated Documentation

All documentation updated to reflect the simplified workflow:
- [`README_create_latlon_grid_geometry.md`](README_create_latlon_grid_geometry.md) - Removed ecco_grid_loc references
- [`example_create_latlon_grid.sh`](example_create_latlon_grid.sh) - Simplified prerequisites check
- [`SIMPLIFIED_ARGUMENTS.md`](SIMPLIFIED_ARGUMENTS.md) - Historical context
- This document - Final state

## Verification

Check if your grid file is complete:
```bash
ncdump -h GRID_GEOMETRY_*.nc | grep hFacC
```

If you see:
```
	double hFacC(k, tile, j, i) ;
```

You're good to go!

If not:
```
ERROR: Your grid file is incomplete. Get a proper ECCO grid geometry file.
```

## Summary

| Aspect | Before | After |
|--------|--------|-------|
| Required arguments | 4 | 3 |
| Grid file inputs | 2 (native_grid_file + ecco_grid_loc) | 1 (native_grid_file) |
| Fallback logic | Yes (complex) | No (simple) |
| Error clarity | Confusing | Crystal clear |
| Mental model | "Use grid file, but maybe another for hFacC?" | "Transform this grid file" |
| Code complexity | Medium | Low |

## Final Command

```bash
python create_latlon_grid_geometry.py \
    --native_grid_file GRID_GEOMETRY_ECCO_V4r4_native_llc0090.nc \
    --mapping_factors_loc /path/to/mapping_factors \
    --output_file GRID_GEOMETRY_ECCO_V4r4_latlon_0p50deg.nc
```

Three required arguments. That's it. Simple! ✨
