import argparse
import json
import glob
import os
import random
import math
from pprint import pprint

def load_all_json_entries(input_dir):
    combined = []
    json_files = glob.glob(os.path.join(input_dir, "*.json"))

    print('input directory contains n json files n=', len(json_files))
    pprint(json_files)
    
    for file_path in json_files:
        with open(file_path, 'r') as f:
            try:
                data = json.load(f)
                if isinstance(data, list):
                    combined.extend(data)
                else:
                    combined.append(data)
            except json.JSONDecodeError as e:
                print(f"Skipping {file_path}: {e}")

    print('combined task list contains n tasks, n=', len(combined))
    
    return combined

def split_and_save_chunks(data, n_chunks, output_dir):
    random.shuffle(data)

    all_chunks_saved = 0
    num_chunk_files = 0

    chunk_size = math.ceil(len(data) / n_chunks)
    for i in range(n_chunks):
        chunk = data[i * chunk_size : (i + 1) * chunk_size]

        if len(chunk) > 0:
            out_path = os.path.join(output_dir, f"chunk_{i+1}.json")
            with open(out_path, 'w') as f:
                json.dump(chunk, f, indent=2)
            print(f"Wrote {len(chunk)} entries to {out_path}")
            all_chunks_saved += len(chunk)
            num_chunk_files += 1
        else:
            print(f"Skipped chunk {i} because zero length")

    print(f"Saved a total of {all_chunks_saved} across {num_chunk_files} files")

def main():
    parser = argparse.ArgumentParser(description="Combine, shuffle, and split JSON entries.")
    parser.add_argument("--input_dir", required=True, help="Directory containing input JSON files")
    parser.add_argument("--output_dir", required=True, help="Directory to write output chunks")
    parser.add_argument("--n_chunks", required=True, type=int, help="Number of output chunks to create")
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)
    data = load_all_json_entries(args.input_dir)
    print(f"Loaded {len(data)} total JSON entries from {args.input_dir}")

    if len(data) == 0:
        print("No data found to process.")
        return

    split_and_save_chunks(data, args.n_chunks, args.output_dir)

if __name__ == "__main__":
    main()

