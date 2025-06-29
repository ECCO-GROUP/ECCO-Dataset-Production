
# This program adds default empty strings
# to variable metadata entries if they
# are missing. The defaults are all empty
# string with the exception of 
# coverage_content_type, which is modelResult

# if entries are missing the really important
# entries:
# 'name'
# 'long name'
# 'units'
# 'grid_dimension'
# 'grid_location'

# then you've got bigger problems 
 

import json
import sys
from collections import OrderedDict

# Desired key order (based on your example)
ordered_keys = [
    "name",
    "long_name",
    "units",
    "comments_1",
    "comments_2",
    "direction",
    "GCMD_keywords",
    "grid_dimension",
    "grid_location",
    "coverage_content_type"
]

# Default values for missing fields
default_fields = {
    "name": "",
    "long_name": "",
    "units": "",
    "comments_1": "",
    "comments_2": "",
    "direction": "",
    "GCMD_keywords": "",
    "grid_dimension": "",
    "grid_location": "",
    "coverage_content_type": "modelResult"
}

# Usage
if len(sys.argv) != 3:
    print("Usage: python update_json_fields_ordered.py input.json output.json")
    sys.exit(1)

input_file = sys.argv[1]
output_file = sys.argv[2]

# Load input
with open(input_file, "r") as f:
    data = json.load(f)

# Construct entries with required key order and defaults
new_data = []
for entry in data:
    new_entry = OrderedDict()
    for key in ordered_keys:
        new_entry[key] = entry.get(key, default_fields[key])

    new_data.append(new_entry)

# Save output with pretty formatting
with open(output_file, "w") as f:
    json.dump(new_data, f, indent=3)

