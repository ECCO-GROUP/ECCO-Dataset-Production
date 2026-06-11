#!/usr/bin/env python3
"""Verify that all variables in groupings have corresponding metadata entries.

This utility validates that every variable referenced in ECCO dataset groupings
files has a corresponding entry in the variable metadata JSON files. This catches
situations where a grouping references a variable name that doesn't have metadata
defined, which would cause failures during dataset production.

The validator:
- Loads all groupings from a groupings JSON file
- Extracts all variable names from the 'fields' entries
- Loads variable metadata from one or more metadata JSON files
- Checks that each variable has a metadata entry
- Reports any missing metadata entries with details

Typical Usage::

    # Check native grid groupings and metadata
    python verify_existence_of_variable_metadata_defined_in_groupings.py \\
        --groupings groupings_for_native_datasets.json \\
        --metadata variable_metadata.json

    # Check with multiple metadata files (e.g., including geometry metadata)
    python verify_existence_of_variable_metadata_defined_in_groupings.py \\
        --groupings groupings_for_latlon_datasets.json \\
        --metadata variable_metadata_for_latlon_datasets.json \\
                   geometry_metadata_for_latlon_datasets.json

    # Check latlon with verbose output
    python verify_existence_of_variable_metadata_defined_in_groupings.py \\
        --groupings groupings_for_latlon_datasets.json \\
        --metadata variable_metadata_for_latlon_datasets.json \\
                   geometry_metadata_for_latlon_datasets.json \\
        --verbose

Common Use Cases:
    - Pre-production validation before generating datasets
    - Verify metadata completeness after adding new groupings
    - Check consistency between V4r4, V4r5, V4r6 configurations
    - Catch typos in grouping 'fields' entries
    - Ensure all variables are documented before release

Expected JSON Structure:

    Groupings file (list of grouping dictionaries):
    [
        {
            "filename": "SEA_SURFACE_HEIGHT",
            "name": "Sea Surface Height",
            "fields": "SSH, SSHIBC, SSHNOIBC",
            "dimension": "2D",
            "frequency": "AVG_MON"
        },
        ...
    ]

    Variable metadata file (list of variable dictionaries):
    [
        {
            "name": "SSH",
            "long_name": "Sea Surface Height",
            "units": "m",
            ...
        },
        ...
    ]

Output:
    Prints report showing:
    - Number of groupings processed
    - Number of unique variables found
    - Any missing metadata entries (with grouping context)
    - Success message if all variables have metadata

Exit Codes:
    0 - All variables have metadata entries
    1 - One or more variables missing metadata entries

"""

import argparse
import json
import sys
from collections import defaultdict


def load_json_file(filepath):
    """Load and parse a JSON file.

    Args:
        filepath (str): Path to JSON file.

    Returns:
        list or dict: Parsed JSON data.

    Raises:
        FileNotFoundError: If file doesn't exist.
        json.JSONDecodeError: If file is not valid JSON.
    """
    with open(filepath, 'r') as f:
        return json.load(f)


def extract_variables_from_groupings(groupings):
    """Extract all unique variable names from groupings.

    Args:
        groupings (list): List of grouping dictionaries with 'fields' key.

    Returns:
        dict: Mapping of variable_name -> list of grouping names that use it.
    """
    variable_to_groupings = defaultdict(list)

    for grouping in groupings:
        grouping_name = grouping.get('filename', grouping.get('name', 'Unknown'))
        fields_str = grouping.get('fields', '')

        # Parse comma-separated variable names
        if fields_str:
            variables = [v.strip() for v in fields_str.split(',') if v.strip()]
            for var in variables:
                variable_to_groupings[var].append(grouping_name)

    return variable_to_groupings


def load_all_metadata(metadata_files):
    """Load variable metadata from one or more JSON files.

    Args:
        metadata_files (list): List of paths to variable metadata JSON files.

    Returns:
        set: Set of variable names that have metadata entries.
    """
    all_variable_names = set()

    for metadata_file in metadata_files:
        metadata = load_json_file(metadata_file)

        if not isinstance(metadata, list):
            print(f"Warning: {metadata_file} is not a list, skipping")
            continue

        for entry in metadata:
            var_name = entry.get('name')
            if var_name:
                all_variable_names.add(var_name)

    return all_variable_names


def verify_metadata_exists(groupings_file, metadata_files, verbose=False):
    """Verify that all variables in groupings have metadata entries.

    Args:
        groupings_file (str): Path to groupings JSON file.
        metadata_files (list): List of paths to variable metadata JSON files.
        verbose (bool): If True, print detailed information.

    Returns:
        bool: True if all variables have metadata, False otherwise.
    """
    print("=" * 80)
    print("ECCO Variable Metadata Validation")
    print("=" * 80)

    # Load groupings
    print(f"\nLoading groupings from: {groupings_file}")
    try:
        groupings = load_json_file(groupings_file)
    except FileNotFoundError:
        print(f"Error: Groupings file not found: {groupings_file}")
        return False
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in groupings file: {e}")
        return False

    if not isinstance(groupings, list):
        print(f"Error: Groupings file must contain a JSON list")
        return False

    print(f"  Found {len(groupings)} groupings")

    # Extract variables from groupings
    variable_to_groupings = extract_variables_from_groupings(groupings)
    unique_variables = set(variable_to_groupings.keys())
    print(f"  Found {len(unique_variables)} unique variables across all groupings")

    # Load metadata
    print(f"\nLoading metadata from {len(metadata_files)} file(s):")
    for mf in metadata_files:
        print(f"  - {mf}")

    all_metadata_names = set()
    for metadata_file in metadata_files:
        try:
            metadata_names = load_all_metadata([metadata_file])
            all_metadata_names.update(metadata_names)
            print(f"    Loaded {len(metadata_names)} entries from {metadata_file}")
        except FileNotFoundError:
            print(f"  Error: Metadata file not found: {metadata_file}")
            return False
        except json.JSONDecodeError as e:
            print(f"  Error: Invalid JSON in metadata file {metadata_file}: {e}")
            return False

    print(f"  Total unique metadata entries: {len(all_metadata_names)}")

    # Find missing metadata
    missing_variables = unique_variables - all_metadata_names

    print("\n" + "=" * 80)
    print("VALIDATION RESULTS")
    print("=" * 80)

    if not missing_variables:
        print("\n✅ SUCCESS: All variables have metadata entries!")
        print(f"\n  Variables checked: {len(unique_variables)}")
        print(f"  Metadata entries: {len(all_metadata_names)}")
        print(f"  Missing metadata: 0")

        if verbose:
            print("\nAll variables:")
            for var in sorted(unique_variables):
                grouping_list = ', '.join(variable_to_groupings[var])
                print(f"  ✓ {var:<20} (used in: {grouping_list})")

        return True
    else:
        print(f"\n❌ FAILURE: {len(missing_variables)} variable(s) missing metadata entries\n")
        print("Missing variables:")
        print("-" * 80)

        for var in sorted(missing_variables):
            groupings_using_var = variable_to_groupings[var]
            print(f"\n  Variable: {var}")
            print(f"    Used in {len(groupings_using_var)} grouping(s):")
            for grouping_name in groupings_using_var:
                print(f"      - {grouping_name}")

        print("\n" + "-" * 80)
        print(f"\nSummary:")
        print(f"  Variables checked: {len(unique_variables)}")
        print(f"  Variables with metadata: {len(unique_variables) - len(missing_variables)}")
        print(f"  Variables missing metadata: {len(missing_variables)}")

        if verbose:
            print(f"\nVariables with metadata ({len(unique_variables) - len(missing_variables)}):")
            found_variables = unique_variables - missing_variables
            for var in sorted(found_variables):
                print(f"  ✓ {var}")

        return False


def main():
    parser = argparse.ArgumentParser(
        description="Verify that all variables in groupings have corresponding metadata entries.",
        epilog="""
Examples:
  # Check native grid configuration
  %(prog)s \\
    --groupings groupings_for_native_datasets.json \\
    --metadata variable_metadata.json

  # Check latlon with multiple metadata sources
  %(prog)s \\
    --groupings groupings_for_latlon_datasets.json \\
    --metadata variable_metadata_for_latlon_datasets.json \\
               geometry_metadata_for_latlon_datasets.json

  # Verbose output showing all variables
  %(prog)s \\
    --groupings groupings_for_native_datasets.json \\
    --metadata variable_metadata.json \\
    --verbose

The script checks that every variable listed in the groupings 'fields'
has a corresponding entry in the variable metadata file(s).
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument(
        '--groupings',
        required=True,
        help='Path to groupings JSON file (e.g., groupings_for_native_datasets.json)'
    )
    parser.add_argument(
        '--metadata',
        nargs='+',
        required=True,
        help='Path(s) to variable metadata JSON file(s). Multiple files can be specified.'
    )
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Print detailed information about all variables'
    )

    args = parser.parse_args()

    # Run validation
    success = verify_metadata_exists(
        args.groupings,
        args.metadata,
        args.verbose
    )

    # Exit with appropriate code
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()