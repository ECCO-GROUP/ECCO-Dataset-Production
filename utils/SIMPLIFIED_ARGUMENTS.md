# Simplified Arguments - No More Redundancy!

## The Question

**"Why do I need to specify both a `native_grid_file` AND an `ecco_grid_loc`?"**

**Answer**: You don't! (Usually)

## The Problem

The original design had redundancy:
- `native_grid_file`: The grid geometry file to transform
- `ecco_grid_loc`: A directory to load... another native grid file with hFacC

But if `native_grid_file` is a **grid geometry file**, it should already contain `hFacC`!

## The Solution

### New Logic

1. **Load the native grid file** (the one being transformed)
2. **Check if it contains hFacC**
   - ✅ If yes: Use it! (most common case)
   - ❌ If no: Fall back to `ecco_grid_loc` (rare)

### Updated Arguments

**Required:**
```bash
--native_grid_file    # The grid file to transform (should have hFacC)
--mapping_factors_loc # Where lat/lon coords and sparse matrices live
--output_file         # Where to write the output
```

**Optional:**
```bash
--ecco_grid_loc       # Only if native_grid_file lacks hFacC (rare)
--ecco_cfg_loc        # For compression settings (optional)
--variables           # To transform only specific vars (optional)
```

## Usage Examples

### Typical Case (Recommended)

```bash
python create_latlon_grid_geometry.py \
    --native_grid_file GRID_GEOMETRY_ECCO_V4r4_native_llc0090.nc \
    --mapping_factors_loc /path/to/mapping_factors \
    --output_file GRID_GEOMETRY_ECCO_V4r4_latlon_0p50deg.nc
```

**Result**: Uses hFacC from the native grid file itself. Simple!

### Rare Case (Fallback)

```bash
# Only if native_grid_file doesn't have hFacC
python create_latlon_grid_geometry.py \
    --native_grid_file grid_without_hfacc.nc \
    --ecco_grid_loc /path/to/grids_with_hfacc \
    --mapping_factors_loc /path/to/mapping_factors \
    --output_file output.nc
```

## What Changed in the Code

### Before (Redundant)

```python
# Load file to transform
native_ds = xr.open_dataset(native_grid_file)

# Load ANOTHER file just for hFacC
native_grid_ds = xr.open_dataset(ecco_grid_loc + '/some_native.nc')
native_wet_indices = calculate_wet_points(native_grid_ds)
```

### After (Smart)

```python
# Load file to transform
native_ds = xr.open_dataset(native_grid_file)

# Check if it already has hFacC
if 'hFacC' in native_ds:
    # Use it directly!
    native_wet_indices = calculate_wet_points(native_ds)
else:
    # Fallback: load from ecco_grid_loc
    native_grid_ds = xr.open_dataset(ecco_grid_loc + '/some_native.nc')
    native_wet_indices = calculate_wet_points(native_grid_ds)
```

## What Each Argument Does

| Argument | Purpose | When Needed |
|----------|---------|-------------|
| `native_grid_file` | File to transform to latlon | Always |
| `mapping_factors_loc` | Source of lat/lon coords & matrices | Always |
| `output_file` | Where to write result | Always |
| `ecco_grid_loc` | Fallback source of hFacC | Only if native_grid_file lacks hFacC |
| `ecco_cfg_loc` | Compression & metadata settings | Optional |
| `variables` | Subset of vars to transform | Optional |

## Benefits

### 1. **Simpler Command Line**

**Before:**
```bash
python create_latlon_grid_geometry.py \
    --native_grid_file grid.nc \
    --ecco_grid_loc /path/to/grids \      # Why do I need this?
    --mapping_factors_loc /path/factors \
    --output_file out.nc
```

**After:**
```bash
python create_latlon_grid_geometry.py \
    --native_grid_file grid.nc \
    --mapping_factors_loc /path/factors \
    --output_file out.nc
```

### 2. **No Confusion**

Users don't wonder: "Why two paths to grid files?"

### 3. **Clearer Purpose**

Each argument has a clear, non-redundant purpose.

### 4. **Backward Compatible**

The tool still accepts `--ecco_grid_loc` for edge cases where it's needed.

## When Would You Need `ecco_grid_loc`?

Only in rare scenarios:

1. **Minimal grid file**: You have a native grid file with just XC, YC, etc. but no hFacC
2. **Partial grid file**: The grid file you're transforming is incomplete
3. **Testing**: You want to use hFacC from a different grid for some reason

For standard ECCO grid geometry files, **you don't need it**.

## Updated Example Script

The `example_create_latlon_grid.sh` now:

1. **Defaults to empty** for `ECCO_GRID_LOC`
2. **Checks if hFacC exists** in native_grid_file
3. **Only uses ecco_grid_loc** if hFacC is missing
4. **Explains clearly** what's happening

```bash
# In example_create_latlon_grid.sh:
ECCO_GRID_LOC=""  # Usually not needed!
```

## Summary

| Question | Answer |
|----------|--------|
| Do I need both arguments? | **No!** Just `native_grid_file` in most cases |
| When do I need `ecco_grid_loc`? | Only if `native_grid_file` lacks hFacC (rare) |
| What if I provide both? | Tool uses native_grid_file's hFacC first, ecco_grid_loc as fallback |
| Is this backward compatible? | Yes! Old commands with both arguments still work |

## Verification

Check if your grid file has hFacC:

```bash
# Quick check
ncdump -h GRID_GEOMETRY_*.nc | grep hFacC

# If it prints "double hFacC(k, tile, j, i)" or similar:
# ✓ You don't need ecco_grid_loc!

# If it doesn't print anything:
# ✗ You'll need to provide ecco_grid_loc
```

Or just run the tool - it will tell you if hFacC is missing!

```bash
$ python create_latlon_grid_geometry.py --native_grid_file grid.nc ...

Loading native grid file: grid.nc
  Found 45 data variables
  ✓ Found hFacC in native_grid_file - using it for wet point calculation
  # ^ Look for this message!
```
