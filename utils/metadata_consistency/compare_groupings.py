
import json
import argparse
from pathlib import Path
import difflib

def compare_json_files(file1_path, file2_path):
    """
    Compares two JSON grouping files, highlighting differences in fields
    and dataset descriptions for each group identified by the 'name' field.
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

    groups1 = {item['name']: item for item in data1}
    groups2 = {item['name']: item for item in data2}

    all_group_names = sorted(list(set(groups1.keys()) | set(groups2.keys())))

    print(f"Comparing files:\n  - {file1_path.name}\n  - {file2_path.name}\n")

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

        # Compare fields
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

        # Compare dataset_description
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
            # Skip header
            for _ in range(2):
                next(diff, None)
            
            for line in diff:
                print(f"    {line}")

        # Compare other keys
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
        description="Compare two JSON grouping files."
    )
    parser.add_argument(
        "file1",
        type=Path,
        help="Path to the first JSON file."
    )
    parser.add_argument(
        "file2",
        type=Path,
        help="Path to the second JSON file."
    )

    args = parser.parse_args()

    if not args.file1.is_file():
        print(f"Error: File not found at {args.file1}")
    elif not args.file2.is_file():
        print(f"Error: File not found at {args.file2}")
    else:
        compare_json_files(args.file1, args.file2)
