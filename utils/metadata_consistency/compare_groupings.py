"""Compare ECCO metadata grouping files.

This utility compares two JSON grouping files (e.g.,
ECCOv4r4_groupings_for_native_datasets.json or
ECCOv4r4_groupings_for_latlon_datasets.json) and reports differences in:

- Fields (variables) included in each grouping
- Dataset descriptions
- Other grouping properties (dimension, frequency, etc.)

Groupings are matched by their 'name' field. The comparison highlights:
- Groups that appear in only one file
- Differences in variable lists between matching groups
- Changes to dataset descriptions (with unified diff output)
- Changes to any other metadata fields

Typical Usage::

    # Compare two versions of native groupings
    python compare_groupings.py \\
        ECCOv4r4_groupings_for_native_datasets.json \\
        ECCOv4r5_groupings_for_native_datasets.json

    # Compare native vs latlon groupings for the same version
    python compare_groupings.py \\
        ECCOv4r4_groupings_for_native_datasets.json \\
        ECCOv4r4_groupings_for_latlon_datasets.json

Output Format:
    For each grouping, the script prints:
    - Full list of fields in each file
    - Fields unique to each file
    - Unified diff of dataset descriptions
    - Differences in other metadata properties

"""

import json
import argparse
from pathlib import Path
import difflib

def compare_json_files(file1_path, file2_path):
    """Compare two JSON grouping files and report all differences.

    Groups are matched by their 'name' field. For each group, the function
    compares:
    - 'fields': comma-separated list of variable names (order-independent)
    - 'dataset_description': free-text description (shows unified diff)
    - All other keys: dimension, frequency, filename, etc.

    Groups that appear in only one file are also reported.

    Args:
        file1_path (Path): Path to first JSON grouping file.
        file2_path (Path): Path to second JSON grouping file.

    Returns:
        None. Prints formatted comparison output to stdout.

    Raises:
        Prints error messages for FileNotFoundError or JSONDecodeError
        but does not raise exceptions.

    """
    try:
        with open(file1_path, 'r') as f:
            data1 = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Error reading {file1_path}: {e}")
        return

    try:
        with open(file2_path, 'r') as f:
            data2 = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Error reading {file2_path}: {e}")
        return

    # Index groups by 'name' field for efficient lookup
    groups1 = {item['name']: item for item in data1}
    groups2 = {item['name']: item for item in data2}

    # Get union of all group names from both files
    all_group_names = sorted(list(set(groups1.keys()) | set(groups2.keys())))

    print(f"Comparing files:\n  - {file1_path.name}\n  - {file2_path.name}\n")

    # Compare each group
    for name in all_group_names:
        print(f"--- Group: {name} ---")
        in_file1 = name in groups1
        in_file2 = name in groups2

        if in_file1 and not in_file2:
            print(f"  - Only in {file1_path.name}")
            print("\n\n")
            continue
        if not in_file1 and in_file2:
            print(f"  - Only in {file2_path.name}")
            print("\n\n")
            continue

        group1 = groups1[name]
        group2 = groups2[name]

        # Compare fields (comma-separated variable names, order-independent)
        fields1_raw = group1.get('fields', '')
        fields2_raw = group2.get('fields', '')
        
        fields1_list = sorted([field.strip() for field in fields1_raw.split(',') if field.strip()])
        fields2_list = sorted([field.strip() for field in fields2_raw.split(',') if field.strip()])

        fields1 = set(fields1_list)
        fields2 = set(fields2_list)

        print(f"  - Fields in {file1_path.name}: {', '.join(fields1_list)}")
        print(f"  - Fields in {file2_path.name}: {', '.join(fields2_list)}")

        if fields1 != fields2:
            print("  - Field differences:")
            if fields1 - fields2:
                print(f"    - Only in {file1_path.name}: {', '.join(sorted(list(fields1 - fields2)))}")
            if fields2 - fields1:
                print(f"    - Only in {file2_path.name}: {', '.join(sorted(list(fields2 - fields1)))}")

        # Compare dataset_description (show unified diff for long text)
        desc1 = group1.get('dataset_description', '')
        desc2 = group2.get('dataset_description', '')
        if desc1 != desc2:
            print("  - Dataset Description differences:")
            diff = difflib.unified_diff(
                desc1.splitlines(),
                desc2.splitlines(),
                fromfile=file1_path.name,
                tofile=file2_path.name,
                lineterm='',
            )
            # Skip unified diff header lines (---, +++)
            for _ in range(2):
                next(diff, None)

            for line in diff:
                print(f"    {line}")

        # Compare all other metadata keys (dimension, frequency, filename, etc.)
        all_keys = set(group1.keys()) | set(group2.keys())
        other_keys = all_keys - {'name', 'fields', 'dataset_description'}

        for key in sorted(list(other_keys)):
            val1 = group1.get(key)
            val2 = group2.get(key)
            if val1 != val2:
                print(f"  - Difference in '{key}':")
                print(f"    - {file1_path.name}: {val1}")
                print(f"    - {file2_path.name}: {val2}")
        
        print("-" * (len(name) + 14))
        print("\n\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Compare two ECCO metadata grouping JSON files.",
        epilog="""
Examples:
  # Compare two versions
  %(prog)s ECCOv4r4_groupings_for_native_datasets.json \\
           ECCOv4r5_groupings_for_native_datasets.json

  # Compare native vs latlon
  %(prog)s ECCOv4r4_groupings_for_native_datasets.json \\
           ECCOv4r4_groupings_for_latlon_datasets.json
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "file1",
        type=Path,
        help="Path to the first JSON grouping file."
    )
    parser.add_argument(
        "file2",
        type=Path,
        help="Path to the second JSON grouping file."
    )

    args = parser.parse_args()

    if not args.file1.is_file():
        print(f"Error: File not found at {args.file1}")
    elif not args.file2.is_file():
        print(f"Error: File not found at {args.file2}")
    else:
        compare_json_files(args.file1, args.file2)
