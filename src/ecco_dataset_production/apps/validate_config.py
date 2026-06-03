#!/usr/bin/env python
"""
Validate ECCO Dataset Production configuration files against the yamale schema.

Usage:
    ecco_validate_config config_V4r6.yaml
    ecco_validate_config config_V4r*.yaml
    ecco_validate_config --with-defaults config_V4r6.yaml  # Show defaults applied
"""

import argparse
from pathlib import Path
import sys

from ..config import ECCODatasetProductionConfig
from ..config.schema import Schema


def main():
    parser = argparse.ArgumentParser(
        description='Validate ECCO configuration files against the schema'
    )
    parser.add_argument(
        'config_files',
        nargs='*',
        help='Configuration file(s) to validate'
    )
    parser.add_argument(
        '--with-defaults',
        action='store_true',
        help='Show configuration with defaults applied'
    )
    parser.add_argument(
        '--show-defaults',
        action='store_true',
        help='Show only the default values that would be applied'
    )

    args = parser.parse_args()

    # Show defaults and exit if requested
    if args.show_defaults:
        import json
        schema = Schema()
        print("\n" + "="*60)
        print("Default Configuration Values")
        print("="*60 + "\n")
        print(json.dumps(schema.get_defaults(), indent=2))
        print()
        sys.exit(0)

    # Require config files if not just showing defaults
    if not args.config_files:
        parser.error("config_files is required unless using --show-defaults")

    # Validate each config file
    all_valid = True
    results = []

    for config_file in args.config_files:
        config_path = Path(config_file)

        if not config_path.exists():
            results.append((False, None, f"✗ File not found: {config_file}"))
            all_valid = False
            continue

        try:
            # Config is automatically validated and defaults applied on load
            cfg = ECCODatasetProductionConfig(str(config_path))

            if args.with_defaults:
                # Return config data with defaults already applied
                results.append((True, dict(cfg), f"✓ {config_path.name} is valid"))
            else:
                # Just validate
                results.append((True, None, f"✓ {config_path.name} is valid"))
        except Exception as e:
            results.append((False, None, f"✗ {config_path.name}: {str(e)}"))
            all_valid = False

    # Print results
    print("\n" + "="*60)
    print("ECCO Configuration Validation Results")
    print("="*60 + "\n")

    for success, config_data, message in results:
        print(message)
        if config_data and args.with_defaults:
            import json
            print("\nConfiguration with defaults applied:")
            print(json.dumps(config_data, indent=2, default=str))
            print()

    print("="*60)
    if all_valid:
        print("All configuration files are valid! ✓")
    else:
        print("Some configuration files failed validation ✗")
    print("="*60 + "\n")

    sys.exit(0 if all_valid else 1)


if __name__ == '__main__':
    main()
