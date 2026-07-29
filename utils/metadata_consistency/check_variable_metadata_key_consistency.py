"""Check key consistency across ECCO variable metadata entries.

This utility validates that all entries in an ECCO variable metadata JSON file
have consistent keys (attributes). It's used to catch metadata files where some
variables are missing required attributes like 'long_name', 'units', 'coverage_content_type', etc.

The checker:
- Computes the union of all keys across all entries
- Checks each entry against this reference set
- Reports entries that are missing any keys (except explicitly optional ones)
- Allows certain keys (like 'standard_name') to be optional

Typical Usage::

    # Check with default optional keys ('standard_name', 'internal note')
    python check_variable_metadata_key_consistency.py \\
        variable_metadata.json

    # Specify custom optional keys
    python check_variable_metadata_key_consistency.py \\
        variable_metadata.json \\
        --optional standard_name comment internal_note

    # Require all keys (no optional keys)
    python check_variable_metadata_key_consistency.py \\
        variable_metadata.json \\
        --optional

Common Use Cases:
    - Validate metadata files after manual editing
    - Check consistency across V4r4, V4r5, V4r6 metadata versions
    - Verify all required CF-1.8 attributes are present
    - Ensure new variable additions follow existing patterns

Expected JSON Structure:
    The input file must be a JSON list where each element is a variable
    metadata dictionary with keys like:
    - name: variable identifier (e.g., "SSH", "THETA")
    - long_name: CF-compliant long name
    - units: CF-compliant units
    - coverage_content_type: CF-compliant content type
    - standard_name: (optional) CF standard name if one exists

Output:
    Prints each entry with missing keys, or a success message if all entries
    are consistent.

"""

import json
import argparse

def check_keys_consistency(json_file, optional_keys=None):
    """Check that all entries in a JSON metadata file have consistent keys.

    Computes the union of all keys present across all entries in the file,
    then checks that each entry has all of these keys (except those marked
    as optional). Reports any entries with missing required keys.

    Args:
        json_file (str): Path to JSON file containing list of variable metadata
            dictionaries.
        optional_keys (list or None): Keys that are allowed to be absent from
            some entries. If None, defaults to ['standard_name', 'internal note'].
            Pass an empty list to require all keys.

    Returns:
        None. Prints validation results to stdout.

    Raises:
        ValueError: If the JSON file does not contain a list at the top level.
        FileNotFoundError: If json_file does not exist.
        json.JSONDecodeError: If json_file is not valid JSON.

    """
    # Set default optional keys
    if optional_keys is None:
        optional_keys = set(['standard_name', 'internal note'])
    else:
        optional_keys = set(optional_keys)

    # Load the JSON file
    with open(json_file, 'r') as f:
        data = json.load(f)

    if not isinstance(data, list):
        raise ValueError("Expected a list of JSON objects at the top level.")

    # Build reference set: union of all keys across all entries
    # This ensures we catch entries missing keys that appear elsewhere
    reference_keys = set(data[0].keys())

    for entry in data:
        reference_keys |= set(entry.keys())

    all_good = True

    # Check each entry against the reference set
    for i, entry in enumerate(data):
        entry_keys = set(entry.keys())
        # Missing = keys in reference but not in this entry (excluding optional)
        missing = reference_keys - entry_keys - optional_keys

        if missing:
            all_good = False
            # Use 'name' field if present for better readability
            name_str = str(entry.get("name", "???")).rjust(10)
            print(f"Inconsistent keys in entry {str(i).zfill(3)}: {name_str}  Missing keys: {missing}")

    if all_good:
        print("✅ All entries have consistent keys.")

    print("Check complete.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Check that all entries in an ECCO variable metadata JSON file have consistent keys.",
        epilog="""
Examples:
  # Check with default optional keys
  %(prog)s variable_metadata.json

  # Specify custom optional keys
  %(prog)s variable_metadata.json --optional standard_name comment

  # Require all keys (no optional keys allowed)
  %(prog)s variable_metadata.json --optional

Default optional keys: 'standard_name', 'internal note'
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "json_file",
        help="Path to JSON file containing list of variable metadata dictionaries."
    )
    parser.add_argument(
        "--optional",
        nargs="*",
        metavar="KEY",
        help="Keys that are allowed to be missing from some entries. "
             "Overrides defaults. Pass no arguments to require all keys."
    )
    args = parser.parse_args()

    check_keys_consistency(args.json_file, optional_keys=args.optional)

