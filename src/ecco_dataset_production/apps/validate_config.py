#!/usr/bin/env python
"""
Validate ECCO Dataset Production configuration files against the yamale schema.

Usage:
    edp_validate_config config_V4r6.yaml
    edp_validate_config --show-config config_V4r6.yaml
    edp_validate_config --show-defaults
"""

import argparse
import json
import sys

from ..config import ECCODatasetProductionConfig
from ..config.schema import Schema


def main():
    parser = argparse.ArgumentParser(
        description='Validate ECCO configuration files against the schema'
    )
    parser.add_argument(
        'config_file',
        nargs='?',
        help='Configuration file to validate'
    )
    parser.add_argument(
        '--show-config',
        action='store_true',
        help='Show the validated configuration (defaults are always applied)'
    )
    parser.add_argument(
        '--show-defaults',
        action='store_true',
        help='Show only the default values that would be applied'
    )

    args = parser.parse_args()

    # Show defaults and exit if requested
    if args.show_defaults:
        schema = Schema()
        print(json.dumps(schema.get_defaults(), indent=2))
        sys.exit(0)

    # Require config file
    if not args.config_file:
        parser.error("config_file is required unless using --show-defaults")

    try:
        # Config is automatically validated and defaults applied on load
        cfg = ECCODatasetProductionConfig(args.config_file)

        if args.show_config:
            print(json.dumps(dict(cfg), indent=2, default=str))
        else:
            print(f"✓ {args.config_file} is valid")

        sys.exit(0)
    except FileNotFoundError:
        print(f"Error: File not found: {args.config_file}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error: {str(e)}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
