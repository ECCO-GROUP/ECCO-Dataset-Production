#!/usr/bin/env python3
"""Command-line tool for adding ECCO metadata to bare NetCDF files.

This tool applies ECCO metadata (global, variable, and coordinate attributes) to
existing NetCDF files without requiring external grid files or mapping factors.
It's specifically designed for grid geometry files that already contain their
coordinate data.
"""

import argparse
import logging

from .. import ecco_generate_datasets


# initialize root logger:
logging.basicConfig(
    format='%(levelname)-10s %(asctime)s %(message)s')


def create_parser():
    """Set up command-line arguments for add_metadata.

    Returns:
        argparse.ArgumentParser instance.
    """
    parser = argparse.ArgumentParser(
        description="""Apply ECCO metadata to an existing NetCDF file. This tool
        is designed for 'bare' NetCDF files (like grid geometry files) that already
        contain their coordinate data but need metadata attributes added.""",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Add metadata to a native grid geometry file (auto-detect dimension)
  edp_add_metadata \\
      --input bare_grid.nc \\
      --output GRID_GEOMETRY_ECCO_V4r6_native_llc0090.nc \\
      --metadata /path/to/metadata \\
      --config configs/config_V4r6.yaml \\
      --grid-type native

  # Add metadata with explicit 3D dimension and attribute stripping
  edp_add_metadata \\
      --input input.nc \\
      --output output.nc \\
      --metadata /path/to/metadata \\
      --config configs/config_V4r6.yaml \\
      --dimension 3D \\
      --strip-attributes \\
      --log DEBUG
        """)

    parser.add_argument('-i', '--input', required=True,
        help='Path to input NetCDF file')

    parser.add_argument('-o', '--output', required=True,
        help='Path for output NetCDF file with metadata applied')

    parser.add_argument('-m', '--metadata', required=True,
        help='Path to ECCO metadata directory, or AWS S3 bucket/prefix')

    parser.add_argument('-c', '--config', required=True,
        help='Path to ECCO configuration YAML file, or AWS S3 object')

    parser.add_argument('-g', '--grid-type',
        choices=['native', 'latlon'],
        default='native',
        help='Grid type (default: %(default)s)')

    parser.add_argument('-d', '--dimension',
        choices=['2D', '3D'],
        default=None,
        help='Dataset dimension (default: auto-detect from file)')

    parser.add_argument('-s', '--strip-attributes',
        action='store_true',
        help='Strip all existing attributes before applying new ones')

    parser.add_argument('-l', '--log',
        dest='log_level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        default='INFO',
        help='Set logging level (default: %(default)s)')

    parser.add_argument('--keygen',
        help="""If metadata or config reference AWS S3 endpoints and if running in an
        institutionally-managed AWS IAM Identity Center (SSO) environment, (path and)
        name of federated login key generation script (e.g.,
        /usr/local/bin/aws-login.darwin.universal, etc.)""")

    parser.add_argument('--profile',
        help="""Optional profile name to be used in combination with keygen (e.g.,
        'saml-pub', 'default', etc.)""")

    return parser


def main():
    """Command-line entry point."""
    parser = create_parser()
    args = parser.parse_args()

    # application-level logger:
    log = logging.getLogger('edp')
    log.setLevel(args.log_level)

    # Determine is_2d parameter
    if args.dimension is None:
        is_2d = None  # Auto-detect
    else:
        is_2d = (args.dimension == '2D')

    # Call the core function
    ecco_generate_datasets.apply_metadata_to_netcdf(
        input_netcdf=args.input,
        output_netcdf=args.output,
        ecco_metadata_loc=args.metadata,
        cfg=args.config,
        grid_type=args.grid_type,
        is_2d=is_2d,
        strip_attributes=args.strip_attributes,
        log_level=args.log_level,
        keygen=args.keygen,
        profile=args.profile)

    log.info('Done!')
