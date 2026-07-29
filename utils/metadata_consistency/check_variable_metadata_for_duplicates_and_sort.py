"""Check for duplicate variables and sort ECCO metadata files.

This utility validates ECCO variable metadata JSON files by:
1. Detecting duplicate variable names (same 'name' field appearing multiple times)
2. Listing all unique variables found in the file
3. Sorting the metadata entries alphabetically by variable name (case-insensitive)
4. Writing the sorted metadata to an output file

The tool is useful for:
- Quality assurance after manual metadata edits
- Detecting accidentally duplicated entries
- Maintaining consistent alphabetical ordering across metadata files
- Creating clean, sorted metadata files for version control

Typical Usage::

    # Check for duplicates and create sorted output
    python check_variable_metadata_for_duplicates_and_sort.py \\
        variable_metadata.json \\
        variable_metadata_sorted.json

    # Process V4r4 native metadata
    python check_variable_metadata_for_duplicates_and_sort.py \\
        ~/ECCO-v4-Configurations/ECCOv4r4/metadata/variable_metadata.json \\
        variable_metadata_v4r4_sorted.json

Expected Input Format:
    JSON list where each element is a variable metadata dictionary with at least
    a 'name' field:
    [
        {"name": "SSH", "units": "m", "long_name": "Sea Surface Height", ...},
        {"name": "THETA", "units": "degree_C", "long_name": "Potential Temperature", ...},
        ...
    ]

Output:
    - Console report showing:
      * Count and list of unique variables
      * Warnings for any duplicates found (with occurrence count)
      * Success message with output filename
    - Sorted JSON file written to the specified output path

Common Use Cases:
    - Validate metadata after manual edits or merges
    - Detect copy-paste errors that created duplicates
    - Standardize metadata file ordering across ECCO versions
    - Prepare metadata for version control commits (consistent ordering reduces diffs)

"""

import json
import argparse
import sys
from collections import Counter

def sort_and_check_metadata(input_file, output_file):
    """Check for duplicate variable names and write sorted metadata to output file.

    The function performs five main operations:
    1. Loads and validates the input JSON file (must be a list)
    2. Extracts all variable names (strips whitespace)
    3. Detects duplicates using Counter
    4. Sorts entries alphabetically by name (case-insensitive)
    5. Writes sorted metadata to output file with indentation

    The sorting is case-insensitive (so 'SSH' comes before 'salt') but preserves
    the original case in the output.

    Args:
        input_file (str): Path to input variable_metadata.json file.
        output_file (str): Path where sorted metadata will be written.

    Returns:
        None. Prints report to stdout and writes sorted JSON to output_file.

    Raises:
        SystemExit: If input file is not a JSON list (exits with status 1).
        Prints error messages for FileNotFoundError or JSONDecodeError but
        does not raise exceptions.

    """
    try:
        # Load and validate the JSON data
        with open(input_file, 'r') as f:
            data = json.load(f)

        if not isinstance(data, list):
            print(f"Error: {input_file} must contain a JSON list of objects.")
            sys.exit(1)

        # Step 1: Extract and clean variable names
        # Strip whitespace to ensure accurate duplicate detection
        raw_names = [item.get('name', '').strip() for item in data if 'name' in item]

        # Step 2: Count occurrences to detect duplicates
        name_counts = Counter(raw_names)
        duplicates = [name for name, count in name_counts.items() if count > 1]

        # Step 3: Get unique names and sort (case-insensitive) for display
        unique_names = sorted(list(set(raw_names)), key=lambda s: s.lower())

        # Print report header
        print("=" * 50)
        print(f"REPORT FOR: {input_file}")
        print("=" * 50)

        # Print list of unique variable names
        print(f"FOUND {len(unique_names)} UNIQUE VARIABLES:")
        for name in unique_names:
            print(f"  • {name}")

        print("-" * 50)

        # Print duplicate warnings (if any)
        if duplicates:
            print(f"⚠️  WARNING: FOUND {len(duplicates)} DUPLICATE ENTRIES:")
            for dup in duplicates:
                print(f"   - {dup} (appears {name_counts[dup]} times)")
        else:
            print("✅ No duplicate variable names found.")

        # Step 4: Sort the full metadata list (case-insensitive by 'name')
        sorted_data = sorted(data, key=lambda x: x.get('name', '').strip().lower())

        # Step 5: Write sorted metadata to output file (indented for readability)
        with open(output_file, 'w') as f:
            json.dump(sorted_data, f, indent=3)

        print("-" * 50)
        print(f"SUCCESS: Sorted metadata saved to '{output_file}'")
        print("=" * 50)

    except FileNotFoundError:
        print(f"Error: The file '{input_file}' was not found.")
    except json.JSONDecodeError:
        print(f"Error: Failed to decode JSON. Check the syntax of '{input_file}'.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Check ECCO variable metadata for duplicates, list unique variables, and create sorted output.",
        epilog="""
Examples:
  # Check and sort variable metadata
  %(prog)s variable_metadata.json variable_metadata_sorted.json

  # Process V4r4 native metadata
  %(prog)s \\
    ~/ECCO-v4-Configurations/ECCOv4r4/metadata/variable_metadata.json \\
    variable_metadata_v4r4_sorted.json

The tool detects duplicate variable names, lists all unique variables, and
writes a sorted version (alphabetically by name, case-insensitive) to the
output file.
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    # Positional arguments
    parser.add_argument(
        "input",
        help="Path to input variable_metadata.json file (must be a JSON list)."
    )
    parser.add_argument(
        "output",
        help="Path where sorted metadata will be written (JSON format with indent=3)."
    )

    args = parser.parse_args()

    sort_and_check_metadata(args.input, args.output)