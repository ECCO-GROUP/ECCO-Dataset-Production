import json
import re
import os
import argparse


# 2026-04-26. 
# Ian Fenty
# co-written with copilot after many iterations regarding how to best handle the various unit formats in the available_diagnostics log file and variable metadata json file. 

def normalize_units(unit_string):
    """
    Converts unit strings with '/' to use negative exponents and combines terms.
    e.g., 'm/s' becomes 'm s-1'
    e.g., '1/m^2' becomes 'm-2'
    e.g., 'degC' becomes 'degree_C'
    e.g., 'kg/m^2/s' becomes 'kg m-2 s-1'
    e.g., 'm m2' becomes 'm3'
    Removes '^' symbols.
    Replaces '.' with a space.
    'fraction' and 'count' become '1'.
    'degK' becomes 'degree_K'.
    Preserves unit order and case.
    """
    original_unit_string = unit_string.strip()
    if original_unit_string.lower() in ['fraction', 'count']:
        return '1'

    unit_string = original_unit_string.replace('.', ' ')
    unit_string = unit_string.replace('degC', 'degree_C')
    unit_string = unit_string.replace('degK', 'degree_K')
    unit_string = unit_string.replace('^', '')

    parts = unit_string.split('/')
    
    unit_powers = {}

    # Process numerator
    if parts[0] != '1':
        # This regex now handles negative exponents, e.g., s-1
        numerator_components = re.findall(r'([a-zA-Z_]+)(-?\d*)', parts[0])
        for part, power in numerator_components:
            power = int(power) if power else 1
            unit_powers[part] = unit_powers.get(part, 0) + power

    # Process denominators
    if len(parts) > 1:
        for denominator in parts[1:]:
            denominator = denominator.strip()
            den_components = re.findall(r'([a-zA-Z_]+)(-?\d*)', denominator)
            for part, power in den_components:
                power = int(power) if power else 1
                unit_powers[part] = unit_powers.get(part, 0) - power

    # Format the final string, preserving order of first appearance
    result_parts = []
    
    # Get all unique units in their original order of appearance
    all_units_in_order = []
    all_unit_components = re.findall(r'([a-zA-Z_]+)', unit_string)
    for unit in all_unit_components:
        if unit not in all_units_in_order:
            all_units_in_order.append(unit)

    non_zero_power_exists = False
    for unit in all_units_in_order:
        power = unit_powers.get(unit, 0)
        if power != 0:
            non_zero_power_exists = True
        if power == 0:
            continue
        if power == 1:
            result_parts.append(unit)
        else:
            result_parts.append(f"{unit}{power}")
    
    if not non_zero_power_exists and unit_powers:
        return '1'
            
    return " ".join(result_parts)


def parse_diagnostics_log(file_path):
    """
    Parses the available_diagnostics.log file to extract variable names and units.
    """
    diagnostics_units = {}
    with open(file_path, 'r') as f:
        for line in f:
            if '|' in line and 'DIAG_NAME' not in line and '-------' not in line:
                parts = [p.strip() for p in line.split('|')]
                if len(parts) > 5:
                    diag_name = parts[1]
                    units = parts[5]
                    if diag_name and units:
                        diagnostics_units[diag_name] = units
    return diagnostics_units

def parse_variable_metadata(file_path):
    """
    Parses the variable_metadata.json file.
    """
    with open(file_path, 'r') as f:
        metadata_list = json.load(f)
    
    metadata_dict = {}
    for item in metadata_list:
        metadata_dict[item['name']] = item
    return metadata_dict

def main(diagnostics_log_path, metadata_path):
    """
    Main function to compare units.
    """
    diagnostics_units = parse_diagnostics_log(diagnostics_log_path)
    variable_metadata = parse_variable_metadata(metadata_path)

    mismatches = []
    
    print(f"{'Variable':<15} | {'Meta Units':<25} | {'Diag Units (Orig)':<25} | {'Diag Units (Reconciled)'}")
    print("-" * 90)

    for var_name, meta_info in variable_metadata.items():
        meta_unit = meta_info.get('units', 'Not Found')
        diag_unit_orig = diagnostics_units.get(var_name, 'Not Found')
        
        if diag_unit_orig != 'Not Found':
            reconciled_unit = normalize_units(diag_unit_orig)
        else:
            reconciled_unit = 'Not Found'

        print(f"{var_name:<15} | {meta_unit:<25} | {diag_unit_orig:<25} | {reconciled_unit}")

        # Still check for mismatches
        if diag_unit_orig != 'Not Found':
            # Special case for '-' or '---' which means dimensionless
            if meta_unit in ['-', '---'] and reconciled_unit.strip() in ['-', '---', '']:
                 continue

            if reconciled_unit.replace(" ", "") != meta_unit.replace(" ", ""):
                mismatches.append({
                    'variable': var_name,
                    'diagnostics_log_unit': diag_unit_orig,
                    'normalized_diagnostics_log_unit': reconciled_unit,
                    'variable_metadata_unit': meta_unit
                })

    if mismatches:
        print("\nUnit mismatches found:")
        for mismatch in mismatches:
            print(f"  Variable: {mismatch['variable']}")
            print(f"    'available_diagnostics.log' unit: '{mismatch['diagnostics_log_unit']}' (normalized: '{mismatch['normalized_diagnostics_log_unit']}')")
            print(f"    'variable_metadata.json' unit:    '{mismatch['variable_metadata_unit']}'")
            print("-" * 20)
    else:
        print("\nNo unit mismatches found.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Validate units between available_diagnostics.log and variable_metadata.json")
    parser.add_argument("diagnostics_log", help="Path to available_diagnostics.log")
    parser.add_argument("variable_metadata", help="Path to variable_metadata.json")
    args = parser.parse_args()
    main(args.diagnostics_log, args.variable_metadata)
