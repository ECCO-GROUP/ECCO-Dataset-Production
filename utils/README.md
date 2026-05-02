
# ECCO Dataset Production utilities

This directory contains a collection of routines and utilities that
have been found to be generally useful in a production setting.


## create\_latlon\_grid\_geometry.py

**NEW** - Utility for creating latlon grid geometry files from native
grid geometry files using the **same mapping factors and transformation
logic** as the main EDP pipeline. This ensures consistency between grid
coordinates and processed granule coordinates, solving the circular
workflow problem where hand-created grid files used different
interpolation methods.

**Key features:**
- Uses exact same sparse matrices as main pipeline
- Guarantees coordinate consistency across all products
- Supports both local and S3-hosted mapping factors
- Auto-detects 2D and 3D variables to transform

**Quick start:**
```bash
python create_latlon_grid_geometry.py \
    --native_grid_file GRID_GEOMETRY_*_native_*.nc \
    --ecco_grid_loc /path/to/grid_files \
    --mapping_factors_loc /path/to/mapping_factors \
    --output_file GRID_GEOMETRY_*_latlon_*.nc
```

See [`README_create_latlon_grid_geometry.md`](README_create_latlon_grid_geometry.md)
for complete documentation and [`example_create_latlon_grid.sh`](example_create_latlon_grid.sh)
for a ready-to-run example.


## extract\_first\_mid\_last\_tasks.py

Utility for extracting first, middle, and last tasks from individual,
or collections of, full task lists. Particularly useful for
incremental dataset production testing.  Since this utility can be run
either locally or remotely, with or without cloud-hosted data, it's
also a useful template for developing other flexible, standalone,
dataset production tools.

