import json
import argparse

def check_keys_consistency(json_file, optional_keys=None):
    # Set default optional keys
    if optional_keys is None:
        optional_keys = set(['standard_name', 'internal note'])
    else:
        optional_keys = set(optional_keys)

    # Read the JSON file
    with open(json_file, 'r') as f:
        data = json.load(f)

    if not isinstance(data, list):
        raise ValueError("Expected a list of JSON objects at the top level.")

    # Start the reference set of keys from the first entry
    reference_keys = set(data[0].keys())

    # Accumulate union of all keys across entries
    for entry in data:
        reference_keys |= set(entry.keys())

    all_good = True

    # Loop through entries and check for missing keys
    for i, entry in enumerate(data):
        entry_keys = set(entry.keys())
        missing = reference_keys - entry_keys - optional_keys

        if missing:
            all_good = False
            name_str = str(entry.get("name", "???")).rjust(10)
            print(f"Inconsistent keys in entry {str(i).zfill(3)}: {name_str}  Missing keys: {missing}")

    if all_good:
        print("✅ All entries have consistent keys.")

    print("Check complete.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Check for consistent keys in a list of JSON objects.")
    parser.add_argument("json_file", help="Path to the JSON file")
    parser.add_argument("--optional", nargs="*", help="Optional keys to exclude from missing check")
    args = parser.parse_args()

    check_keys_consistency(args.json_file, optional_keys=args.optional)

