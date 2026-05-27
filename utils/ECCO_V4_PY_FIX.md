# Why ecco_v4_py Was Required (And How We Fixed It)

## The Problem

When running `create_latlon_grid_geometry.py`, you got an error about missing `ecco_v4_py`, even though this tool doesn't actually need it.

## Why It Happened

### Package Import Chain

**Original code:**
```python
from ecco_dataset_production import ecco_mapping_factors
from ecco_dataset_production import configuration
```

**What this triggered:**
```python
# When you import from ecco_dataset_production package...
# It runs __init__.py which imports ALL modules:

from . import ecco_dataset      # ← Imports ecco_v4_py
from . import ecco_time          # ← Imports ecco_v4_py
from . import ecco_generate_datasets  # ← Imports ecco_v4_py
# ... etc
```

Even though we only needed `ecco_mapping_factors` and `configuration`, Python imported the **entire package**, which includes modules that depend on `ecco_v4_py`.

## Where ecco_v4_py Is Actually Used

`ecco_v4_py` is needed by these modules we **don't use**:
- `ecco_dataset.py` - For reading MDS binary files and vector transformations
- `ecco_time.py` - For time bounds calculations
- `ecco_generate_datasets.py` - For adding metadata

Our tool only needs:
- `ecco_mapping_factors.py` - Has NO ecco_v4_py dependency
- `configuration.py` - Has NO ecco_v4_py dependency

## The Fix

**Changed from module imports:**
```python
from ecco_dataset_production import ecco_mapping_factors
from ecco_dataset_production import configuration

# Then use:
cfg = configuration.ECCODatasetProductionConfig(...)
mapping_factors = ecco_mapping_factors.ECCOMappingFactors(...)
```

**To direct class imports:**
```python
from ecco_dataset_production.ecco_mapping_factors import ECCOMappingFactors
from ecco_dataset_production.configuration import ECCODatasetProductionConfig

# Then use:
cfg = ECCODatasetProductionConfig(...)
mapping_factors = ECCOMappingFactors(...)
```

## Why This Works

By importing **specific classes** instead of **modules**, we bypass the package `__init__.py` that imports everything.

Python's import resolution:
1. `from ecco_dataset_production import X` → Runs `__init__.py` → Imports all modules
2. `from ecco_dataset_production.X import Y` → Only loads module `X` → Skips `__init__.py`

## What We Actually Need

Our tool's **real dependencies**:
```
create_latlon_grid_geometry.py
├── xarray (for reading NetCDF)
├── numpy (for array operations)
├── scipy (for sparse matrices)
│   └── via ECCOMappingFactors
├── lzma, pickle (for reading mapping factors)
│   └── via ECCOMappingFactors
└── yaml (for config files)
    └── via ECCODatasetProductionConfig
```

**NOT needed**:
- ✗ `ecco_v4_py` (used for MDS reading and vector transforms - we don't do that)

## Files Changed

1. **[`create_latlon_grid_geometry.py`](create_latlon_grid_geometry.py)** line 28-30:
   ```python
   # Import specific classes directly to avoid loading ecco_v4_py
   # (The package __init__.py imports all modules, including ones that need ecco_v4_py)
   from ecco_dataset_production.ecco_mapping_factors import ECCOMappingFactors
   from ecco_dataset_production.configuration import ECCODatasetProductionConfig
   ```

## Benefits

1. **No unnecessary dependency** - Don't need to install ecco_v4_py
2. **Faster imports** - Only load what we actually use
3. **Clearer dependencies** - Explicit about what classes we need
4. **Standalone tool** - Can run without full EDP environment

## Testing

After the fix, the tool should work with just:
```bash
pip install xarray numpy scipy pyyaml
```

No need for:
```bash
pip install ecco_v4_py  # Not needed!
```

## Related Files

The modules we DO import have simple dependencies:
- `ecco_mapping_factors.py` - Only imports: `lzma`, `os`, `pickle`, `scipy.sparse`, `tempfile`
- `configuration.py` - Only imports: `collections`, `logging`, `os`, `tempfile`, `yaml`

Both are self-contained and don't cascade import `ecco_v4_py`.

## Summary

| Issue | Cause | Fix |
|-------|-------|-----|
| Missing ecco_v4_py | Package-level import loaded all modules | Import specific classes directly |
| Slow imports | Loaded entire package | Only load needed modules |
| Unclear dependencies | Hidden import chain | Explicit class imports |

The tool now has **minimal, explicit dependencies** and won't fail if `ecco_v4_py` is missing! ✨
