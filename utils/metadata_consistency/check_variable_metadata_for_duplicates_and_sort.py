import json
import argparse
import sys
from collections import Counter

def sort_and_check_metadata(input_file, output_file):
    try:
        # Load the JSON data
        with open(input_file, 'r') as f:
            data = json.load(f)

        if not isinstance(data, list):
            print(f"Error: {input_file} must contain a JSON list of objects.")
            sys.exit(1)

        # 1. Extract and Clean Names
        # We strip whitespace to ensure accuracy
        raw_names = [item.get('name', '').strip() for item in data if 'name' in item]
        
        # 2. Check for duplicates
        name_counts = Counter(raw_names)
        duplicates = [name for name, count in name_counts.items() if count > 1]

        # 3. Get unique names and sort them for the printout
        unique_names = sorted(list(set(raw_names)), key=lambda s: s.lower())

        print("=" * 50)
        print(f"REPORT FOR: {input_file}")
        print("=" * 50)

        # Print Unique Names List
        print(f"FOUND {len(unique_names)} UNIQUE VARIABLES:")
        for name in unique_names:
            print(f"  • {name}")
        
        print("-" * 50)

        # Print Duplicate Warnings
        if duplicates:
            print(f"⚠️  WARNING: FOUND {len(duplicates)} DUPLICATE ENTRIES:")
            for dup in duplicates:
                print(f"   - {dup} (appears {name_counts[dup]} times)")
        else:
            print("✅ No duplicate variable names found.")

        # 4. Sort the full dictionary list for the JSON file
        sorted_data = sorted(data, key=lambda x: x.get('name', '').strip().lower())

        # 5. Write to the output file
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
    parser = argparse.ArgumentParser(description="Sort ECCO metadata, check for duplicates, and list unique variables.")
    
    # Positional arguments
    parser.add_argument("input", help="The path to the original variable_metadata.json file")
    parser.add_argument("output", help="The filename/path for the sorted output file")

    args = parser.parse_args()

    sort_and_check_metadata(args.input, args.output)