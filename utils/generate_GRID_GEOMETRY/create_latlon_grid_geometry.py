#!/usr/bin/env python3
"""
Create latlon grid geometry NetCDF file from native grid geometry file.

This tool ensures consistency by using the same mapping factors and transformation
logic that the main EDP pipeline uses for regular granule processing.

Usage:
    python create_latlon_grid_geometry.py \
        --native_grid_file GRID_GEOMETRY_ECCO_V4r4_native_llc0090.nc \
        --mapping_factors_loc /path/to/mapping_factors \
        --output_file GRID_GEOMETRY_ECCO_V4r4_latlon_0p50deg.nc
"""

import argparse
import datetime
import importlib.util
import logging
import sys
import xarray as xr
import numpy as np
from pathlib import Path

# parameters for area calculation
R_Earth = 6371.008800000000000000e3 # m
R_Earth_sq = R_Earth **2

# Direct module loading to bypass package __init__.py that imports ecco_v4_py
# We only need ecco_mapping_factors and configuration, which don't depend on ecco_v4_py

# Create stub modules for dependencies we don't need
# ecco_mapping_factors imports 'aws' but we only use local files
class StubAWS:
    """Stub AWS module since we only work with local files"""
    class utils:
        @staticmethod
        def is_s3_uri(path):
            return False

    class ecco_aws_s3_sync:
        @staticmethod
        def aws_s3_sync(*args, **kwargs):
            raise RuntimeError("AWS S3 not supported in this tool")

# Create stub for ecco_task since we don't use task-based workflow
class StubECCOTask:
    """Stub ecco_task module since we don't use task objects"""
    pass

# Create a minimal stub package to hold our stub modules
class StubPackage:
    """Minimal package stub"""
    aws = StubAWS()
    ecco_task = StubECCOTask()

# Register stub modules in the package namespace
sys.modules['ecco_dataset_production'] = StubPackage()
sys.modules['ecco_dataset_production.aws'] = StubAWS()
sys.modules['ecco_dataset_production.ecco_task'] = StubECCOTask()

# Get paths to the specific modules we need
src_dir = Path(__file__).parent.parent / 'src' / 'ecco_dataset_production'
mapping_factors_path = src_dir / 'ecco_mapping_factors.py'
configuration_path = src_dir / 'configuration.py'

# Now load the real modules into the package namespace
def load_module_in_package(module_name, module_path, package_name):
    """Load a Python module as part of a package"""
    full_name = f"{package_name}.{module_name}"
    spec = importlib.util.spec_from_file_location(full_name, module_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[full_name] = module
    spec.loader.exec_module(module)
    return module

# Load modules as part of the ecco_dataset_production package
ecco_mapping_factors = load_module_in_package('ecco_mapping_factors', mapping_factors_path, 'ecco_dataset_production')
configuration = load_module_in_package('configuration', configuration_path, 'ecco_dataset_production')

# Get the classes we need
ECCOMappingFactors = ecco_mapping_factors.ECCOMappingFactors
ECCODatasetProductionConfig = configuration.ECCODatasetProductionConfig


logging.basicConfig(
    format='%(levelname)-10s %(asctime)s %(message)s',
    level=logging.INFO)
log = logging.getLogger('create_latlon_grid_geometry')


def create_parser():
    """Set up command-line arguments.

    Returns:
        argparse.ArgumentParser instance.
    """
    parser = argparse.ArgumentParser(
        description="""Create latlon grid geometry file from native grid geometry
        using the same mapping factors as the main EDP pipeline.""",
        formatter_class=argparse.RawDescriptionHelpFormatter)

    parser.add_argument('--native_grid_file', required=True, help="""
        Path to the native grid geometry NetCDF file
        (e.g., GRID_GEOMETRY_ECCO_V4r4_native_llc0090.nc).
        This file MUST contain the hFacC variable for wet point calculation.""")

    parser.add_argument('--mapping_factors_loc', required=True, help="""
        Directory containing ECCO mapping factors (3D, land_mask, latlon_grid,
        and sparse subdirectories).""")

    parser.add_argument('--output_file', required=True, help="""
        Output path for latlon grid geometry NetCDF file
        (e.g., GRID_GEOMETRY_ECCO_V4r4_latlon_0p50deg.nc).""")

    parser.add_argument('--ecco_cfg_loc', help="""
        Path to ECCO Dataset Production configuration YAML file
        (e.g., ../configs/config_V4r4.yaml). Used for metadata and settings.""")

    parser.add_argument('--variables', help="""
        Comma-separated list of variables to transform. If not specified,
        will attempt to transform all 2D and 3D fields found in the native file.
        Example: 'XC,YC,hFacC,Depth'""")

    parser.add_argument('-l', '--log', dest='log_level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        default='INFO', help="""
        Set logging level (default: %(default)s).""")

    return parser


def identify_transformable_variables(ds):
    """Identify which variables in the dataset can be transformed to latlon.

    Args:
        ds (xarray.Dataset): Native grid dataset.

    Returns:
        dict: Dictionary with keys '2D' and '3D', each containing list of variable names.
    """
    transformable = {'2D': [], '3D': []}

    # Skip coordinate-only variables
    skip_vars = {'tile', 'i', 'j', 'k', 'i_g', 'j_g', 'k_u', 'k_l', 'k_p1'}

    for var_name in ds.data_vars:
        if var_name in skip_vars:
            continue

        var = ds[var_name]
        dims = var.dims

        # Check dimensionality
        # 2D: (tile, j, i) or variations with i_g, j_g
        # 3D: (k, tile, j, i) or variations

        if 'tile' in dims:
            # Count spatial dimensions (excluding tile, k-like dimensions)
            spatial_dims = [d for d in dims if d in ['i', 'j', 'i_g', 'j_g']]
            depth_dims = [d for d in dims if d in ['k', 'k_u', 'k_l', 'k_p1']]

            if len(spatial_dims) >= 2:
                if depth_dims:
                    transformable['3D'].append(var_name)
                    log.debug(f"Found 3D variable: {var_name} with dims {dims}")
                else:
                    transformable['2D'].append(var_name)
                    log.debug(f"Found 2D variable: {var_name} with dims {dims}")

    return transformable


def calculate_native_wet_point_indices(native_grid_ds):
    """Calculate native grid wet point indices from hFacC.

    Args:
        native_grid_ds (xarray.Dataset): Native grid dataset with hFacC variable.

    Returns:
        dict: Dictionary keyed by depth level (0=surface, ..., nz-1=bottom)
              with values as tuple of arrays from np.where(hFacC[z,:] > 0).
    """
    native_wet_point_indices = {}
    hFacC = native_grid_ds['hFacC']

    for z in range(hFacC.shape[0]):
        native_wet_point_indices[z] = np.where(hFacC[z, :] > 0)

    return native_wet_point_indices


def transform_variable_to_latlon(native_ds, var_name, native_wet_indices,
                                  mapping_factors, is_3d=False):
    """Transform a single variable from native to latlon grid.

    This function uses the EXACT same transformation logic as the main EDP
    pipeline (ecco_dataset.ECCOMDSDataset.as_latlon), ensuring consistency.

    Args:
        native_ds (xarray.Dataset): Native grid dataset with variables and hFacC.
        var_name (str): Variable name to transform.
        native_wet_indices (dict): Precomputed wet point indices by depth.
        mapping_factors (ECCOMappingFactors): Mapping factors object.
        is_3d (bool): Whether this is a 3D variable.

    Returns:
        xarray.DataArray: Transformed variable on latlon grid.
    """
    log.info(f"Transforming {var_name} ({'3D' if is_3d else '2D'})...")

    native_var = native_ds[var_name]

    # Get latlon grid dimensions and coordinates from mapping factors
    # This is the SOURCE OF TRUTH for lat/lon coordinates - not from an existing
    # latlon grid file (which we're creating!). The mapping factors contain
    # the grid definition that was used to create the sparse matrices.
    latitude = mapping_factors.lats_1D
    longitude = mapping_factors.lons_1D
    nlat, nlon = mapping_factors.latlon_shape

    if is_3d:
        # Get depth dimension name
        depth_dim = None
        for dim in native_var.dims:
            if dim in ['k', 'k_u', 'k_l', 'k_p1']:
                depth_dim = dim
                break

        if depth_dim is None:
            raise ValueError(f"Could not find depth dimension in 3D variable {var_name}")

        nz = native_var.sizes[depth_dim]

        # Allocate output array
        variable_as_latlon = np.zeros((nz, nlat, nlon))

        # Load native data (squeeze out any singleton dimensions)
        var = native_var.values.squeeze()

        # Process each depth level
        for z in range(nz):
            # Extract data at depth level z - shape will be (tile, j, i) or similar
            var_z = var[z, :]

            # Extract only wet points at this level using tuple indexing
            # native_wet_indices[z] is a tuple of (tile_idx, j_idx, i_idx) arrays
            wet_pts = native_wet_indices[z]
            var_z_wet = var_z[wet_pts]  # Direct tuple indexing on numpy array

            # Apply sparse matrix transformation (note the .T transpose!)
            var_z_latlon = mapping_factors.native_to_latlon_mapping_factors(level=z).T.dot(
                var_z_wet)

            # Apply land mask
            var_z_latlon_land_masked = np.where(
                np.isnan(mapping_factors.latlon_land_mask(level=z)),
                np.nan,
                var_z_latlon)

            # Reshape to lat x lon grid
            variable_as_latlon[z, :, :] = var_z_latlon_land_masked.reshape(nlat, nlon)

        # Create DataArray with proper coordinates
        latlon_var = xr.DataArray(
            variable_as_latlon,
            dims=[depth_dim, 'latitude', 'longitude'],
            coords={
                depth_dim: native_ds[depth_dim],
                'latitude': latitude,
                'longitude': longitude
            },
            name=var_name
        )

    else:
        # 2D variable - operate on surface (z=0) only

        # Allocate output array
        variable_as_latlon = np.zeros((nlat, nlon))

        # Load native data (squeeze out any singleton dimensions)
        var = native_var.values.squeeze()

        # Extract only wet points at surface (z=0) using tuple indexing
        wet_pts = native_wet_indices[0]
        var_wet = var[wet_pts]  # Direct tuple indexing on numpy array

        # Apply sparse matrix transformation (note the .T transpose!)
        var_latlon = mapping_factors.native_to_latlon_mapping_factors(level=0).T.dot(
            var_wet)

        # Apply land mask
        var_latlon_land_masked = np.where(
            np.isnan(mapping_factors.latlon_land_mask(level=0)),
            np.nan,
            var_latlon)

        # Reshape to lat x lon grid
        variable_as_latlon[:, :] = var_latlon_land_masked.reshape(nlat, nlon)

        # Create DataArray
        latlon_var = xr.DataArray(
            variable_as_latlon,
            dims=['latitude', 'longitude'],
            coords={
                'latitude': latitude,
                'longitude': longitude
            },
            name=var_name
        )

    # Copy attributes from original variable
    latlon_var.attrs = native_var.attrs.copy()

    return latlon_var



def area_of_latlon_grid_cell(lon0, lon1, lat0, lat1):
    #https://gis.stackexchange.com/questions/29734/how-to-calculate-area-of-1-x-1-degree-cells-in-a-raster


    #It is a consequence of a theorem of Archimedes (c. 287-212 BCE) that
    #for a spherical model of the earth, the area of a cell spanning
    #longitudes l0 to l1 (l1 > l0) and latitudes f0 to f1 (f1 > f0) equals

    #(sin(f1) - sin(f0)) * (l1 - l0) * R^2

    #where
    #
    #    l0 and l1 are expressed in radians (not degrees or whatever).
    #    l1 - l0 is calculated modulo 2*pi (e.g., -179 - 181 = 2 degrees, not -362 degrees).
    #
    #    R is the authalic Earth radius, almost exactly 6371 km.
    #    (sin(f1) - sin(f0)) * (l1 - l0) * R^2


    A = (np.sin(np.deg2rad(lat1)) - np.sin(np.deg2rad(lat0))) * \
        (np.deg2rad(lon1) - np.deg2rad(lon0)) * \
        R_Earth_sq

    return A


def area_of_latlon_grid(lon0, lon1, lat0, lat1, dx, dy, less_output=False):
    # Calculates area of a latlon grid with edges lon0 and lon1
    # lat0 and lat1 with grid spacing of dx and dy

    # lons and lats are in degrees
    # dx and y are in degrees

    # resulting array has columns of lon, rows of lat.

    # Using -180, 180, -90, 90 we get total area of 510065.88 x10^6 km^2
    # using     R_Earth = 6371.0088e3 # m

    num_grid_cells_x = int((lon1-lon0)/dx)
    num_grid_cells_y = int((lat1-lat0)/dy)

    lons_grid_cell_edges = np.linspace(lon0, lon1, num_grid_cells_x + 1)
    lats_grid_cell_edges = np.linspace(lat0, lat1, num_grid_cells_y + 1)

    A = np.zeros((num_grid_cells_y))

    if not less_output:
        print(lons_grid_cell_edges)
        print(lats_grid_cell_edges)

    for lat_i in range(num_grid_cells_y):
        A[lat_i] = area_of_latlon_grid_cell(lons_grid_cell_edges[0], lons_grid_cell_edges[1],\
         lats_grid_cell_edges[lat_i], lats_grid_cell_edges[lat_i+1])

    results = dict()
    results['area'] =np.tile(A, (num_grid_cells_x,1)).T
    results['lon_cell_edges'] = lons_grid_cell_edges
    results['lat_cell_edges'] = lats_grid_cell_edges
    results['num_grid_cells_x'] = num_grid_cells_x
    results['num_grid_cells_y'] = num_grid_cells_y

    return results

def create_latlon_grid_geometry(
    native_grid_file=None,
    mapping_factors_loc=None,
    output_file=None,
    ecco_cfg_loc=None,
    variables=None,
    log_level='INFO'):
    """Create latlon grid geometry file from native grid using EDP mapping factors.

    Args:
        native_grid_file (str): Path to native grid geometry NetCDF file (must contain hFacC).
        mapping_factors_loc (str): Path to mapping factors directory.
        output_file (str): Output path for latlon grid geometry file.
        ecco_cfg_loc (str): Path to ECCO configuration YAML file.
        variables (str): Comma-separated list of variables to transform, or None for all.
        log_level (str): Logging level.
    """
    log.setLevel(log_level)

    log.info("="*60)
    log.info("Creating latlon grid geometry from native grid")
    log.info("="*60)

    # Load configuration if provided
    cfg = None
    if ecco_cfg_loc:
        log.info(f"Loading configuration from {ecco_cfg_loc}")
        cfg = ECCODatasetProductionConfig(cfgfile=ecco_cfg_loc)

    # Load native grid file (the file being transformed)
    log.info(f"Loading native grid file: {native_grid_file}")
    native_ds = xr.open_dataset(native_grid_file)
    log.info(f"  Found {len(native_ds.data_vars)} data variables")

    # Check for hFacC variable (required for wet point calculation)
    if 'hFacC' not in native_ds:
        raise RuntimeError(
            f"Native grid file must contain 'hFacC' variable for wet point calculation.\n"
            f"File: {native_grid_file}\n"
            f"Variables found: {list(native_ds.data_vars)}\n"
            f"Please provide a complete ECCO grid geometry file with hFacC.")

    log.info("  ✓ Found hFacC variable")

    # Calculate wet point indices from hFacC
    log.info("  Calculating wet point indices from hFacC...")
    native_wet_indices = calculate_native_wet_point_indices(native_ds)
    log.info(f"  Computed wet points for {len(native_wet_indices)} depth levels")

    # Load mapping factors
    log.info(f"Loading mapping factors from: {mapping_factors_loc}")
    mapping_factors = ECCOMappingFactors(
        mapping_factors_loc=mapping_factors_loc
    )
    log.info("  Mapping factors loaded successfully")

    # Identify variables to transform
    if variables:
        var_list = [v.strip() for v in variables.split(',')]
        log.info(f"Transforming specified variables: {var_list}")
        # Determine which are 2D vs 3D
        all_transformable = identify_transformable_variables(native_ds)
        vars_2d = [v for v in var_list if v in all_transformable['2D']]
        vars_3d = [v for v in var_list if v in all_transformable['3D']]
    else:
        log.info("Auto-detecting transformable variables...")
        all_transformable = identify_transformable_variables(native_ds)
        vars_2d = all_transformable['2D']
        vars_3d = all_transformable['3D']

    log.info(f"  2D variables ({len(vars_2d)}): {vars_2d}")
    log.info(f"  3D variables ({len(vars_3d)}): {vars_3d}")

    # Transform variables
    latlon_vars = {}

    log.info("\nTransforming 2D variables...")
    for var_name in vars_2d:
        try:
            latlon_var = transform_variable_to_latlon(
                native_ds, var_name, native_wet_indices,
                mapping_factors, is_3d=False)
            latlon_vars[var_name] = latlon_var
        except Exception as e:
            log.error(f"  Failed to transform {var_name}: {e}")
            log.debug("", exc_info=True)

    log.info("\nTransforming 3D variables...")
    for var_name in vars_3d:
        try:
            latlon_var = transform_variable_to_latlon(
                native_ds, var_name, native_wet_indices,
                mapping_factors, is_3d=True)
            latlon_vars[var_name] = latlon_var
        except Exception as e:
            log.error(f"  Failed to transform {var_name}: {e}")
            log.debug("", exc_info=True)

    # make 2D area variable using area_of_latlon_grid function
    # calculate the areas of the lat-lon grid
    ea_area = ea.area_of_latlon_grid(-180, 180, -90, 90, \
                                               data_res, data_res,\
                                               less_output=True);
    lat_lon_grid_area = ea_area['area']

    # Create output dataset
    log.info("\nCreating output dataset...")
    latlon_ds = xr.Dataset(latlon_vars)

    # Copy global attributes from native file
    latlon_ds.attrs = native_ds.attrs.copy()

    # Update attributes to reflect latlon grid
    latlon_ds.attrs['grid_type'] = 'latlon'
    latlon_ds.attrs['grid_label'] = '0p50deg'
    if 'title' in latlon_ds.attrs:
        latlon_ds.attrs['title'] = latlon_ds.attrs['title'].replace('native', 'latlon')
        latlon_ds.attrs['title'] = latlon_ds.attrs['title'].replace('llc0090', '0p50deg')

    # Add processing history
    history_entry = f"{datetime.datetime.now().isoformat()}: Created latlon grid geometry using EDP mapping factors"
    if 'history' in latlon_ds.attrs:
        latlon_ds.attrs['history'] = history_entry + "; " + latlon_ds.attrs['history']
    else:
        latlon_ds.attrs['history'] = history_entry

    # Write output file
    log.info(f"\nWriting output to: {output_file}")

    # Set up encoding for compression
    encoding = {}
    if cfg:
        comp_settings = cfg.get('netcdf4_compression_encodings', {})
    else:
        comp_settings = {'zlib': True, 'complevel': 5, 'shuffle': True}

    for var_name in latlon_ds.data_vars:
        encoding[var_name] = comp_settings.copy()

    latlon_ds.to_netcdf(output_file, encoding=encoding)

    log.info("\n" + "="*60)
    log.info("SUCCESS! Latlon grid geometry file created.")
    log.info(f"Output: {output_file}")
    log.info(f"Variables transformed: {len(latlon_vars)}")
    log.info("="*60)

    # Close datasets
    native_ds.close()
    latlon_ds.close()


def main():
    """Command-line entry point."""
    parser = create_parser()
    args = parser.parse_args()

    create_latlon_grid_geometry(
        native_grid_file=args.native_grid_file,
        mapping_factors_loc=args.mapping_factors_loc,
        output_file=args.output_file,
        ecco_cfg_loc=args.ecco_cfg_loc,
        variables=args.variables,
        log_level=args.log_level
    )


if __name__ == '__main__':
    main()
