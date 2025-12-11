import json
import argparse
from pathlib import Path
import glob

def main():
    parser = argparse.ArgumentParser(description="Load all JSONs in a directory and split entries into separate files.")
    parser.add_argument("--input_dir", required=True, help="Directory containing input JSON files")
    parser.add_argument("--output_dir", required=True, help="Directory to write split JSON files")
    parser.add_argument("--runid", required=True, help="RUNID to include in output filenames")

    args = parser.parse_args()
    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    runid = args.runid

    output_dir.mkdir(parents=True, exist_ok=True)

    combined_entries = []

    for filepath in sorted(input_dir.glob("*.json")):
        with open(filepath, "r") as f:
            data = json.load(f)
            if isinstance(data, list):
                combined_entries.extend(data)
            else:
                combined_entries.append(data)

    total = len(combined_entries)
    num_digits = len(str(total))

    for i, entry in enumerate(combined_entries):
        filename = f"redo_{runid}_{i:0{num_digits}d}.json"
        with open(output_dir / filename, "w") as out:
            json.dump(entry, out, indent=2)

    print(f"Loaded {total} entries from {input_dir}, wrote to {output_dir}")

if __name__ == "__main__":
    main()

