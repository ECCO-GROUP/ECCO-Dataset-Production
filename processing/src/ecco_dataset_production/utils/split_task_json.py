import json
import math
import argparse

def split_json(input_file, num_files, output_base):
    """
    Splits a JSON file into multiple smaller JSON files.
    
    Arguments:
    - input_file (str): Path to the input JSON file to split.
    - num_files (int): Number of output JSON files to create.
    - output_base (str): Base name for the output files. Each file will be named 
                         as `output_base_001.json`, `output_base_002.json`, etc.
    
    The script reads a JSON file, divides it into `num_files` parts, and writes 
    each part into a new JSON file. Each new file will contain a roughly equal 
    number of entries from the original JSON file.
    """
    
    # Load the original JSON file
    with open(input_file, 'r') as infile:
        data = json.load(infile)

    # Total number of entries in the input JSON
    n = len(data)

    # Calculate the number of entries per file (ceiling division to ensure all data is covered)
    entries_per_file = math.ceil(n / num_files)

    # Split the data and write to new JSON files
    for i in range(num_files):
        start_index = i * entries_per_file
        end_index = start_index + entries_per_file

        # Create a subset of the data for the current split
        subset = data[start_index:end_index]

        # Output filename with zero-padded numbers (e.g., output_base_001.json)
        output_filename = f'{output_base}_{i+1:03}.json'

        # Write the subset to the output file
        with open(output_filename, 'w') as outfile:
            json.dump(subset, outfile, indent=4)

    print(f"Split into {num_files} files.")

# Set up argument parsing
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Split a JSON file into multiple smaller files.'
    )
    
    # Positional argument for input file
    parser.add_argument('input_file', type=str, 
                        help='Path to the input JSON file to be split.')
    
    # Positional argument for number of files to split into
    parser.add_argument('num_files', type=int, 
                        help='Number of output JSON files to create.')
    
    # Positional argument for output base name
    parser.add_argument('output_base', type=str, 
                        help='Base name for the output files (e.g., "output" will result in "output_001.json", "output_002.json", etc.)')
    
    # Parse the command-line arguments
    args = parser.parse_args()
    
    # Call the function to split the JSON
    split_json(args.input_file, args.num_files, args.output_base)

