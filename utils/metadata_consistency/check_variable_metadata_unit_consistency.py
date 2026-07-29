"""Check unit consistency between MITgcm diagnostics and ECCO metadata.

This utility validates that units in ECCO variable metadata JSON files match
the units reported by the MITgcm model in the available_diagnostics.log file.
Since units can be formatted differently in these two sources, the tool normalizes
both representations before comparing.

Unit Normalization:
    The tool handles various unit format differences:
    - Fraction notation: 'm/s' → 'm s-1'
    - Multiple denominators: 'kg/m^2/s' → 'kg m-2 s-1'
    - Exponent notation: removes '^' symbols
    - Temperature: 'degC' → 'degree_C', 'degK' → 'degree_K'
    - Dimensionless: 'fraction', 'count' → '1'
    - Compound units: 'm m2' → 'm3' (combines powers)
    - Dot notation: 'm.s' → 'm s'

Typical Usage::

    # Compare units between diagnostics log and metadata
    python check_variable_metadata_unit_consistency.py \\
        /path/to/available_diagnostics.log \\
        /path/to/variable_metadata.json

    # Example with ECCO V4r4 files
    python check_variable_metadata_unit_consistency.py \\
        ~/ECCO-v4-Configurations/ECCOv4r4/available_diagnostics.000000.log \\
        ~/ECCO-v4-Configurations/ECCOv4r4/metadata/variable_metadata.json

Expected Input Files:

    1. available_diagnostics.log (from MITgcm):
       Pipe-delimited table with columns including:
       - DIAG_NAME: variable name
       - UNITS: unit string

    2. variable_metadata.json:
       JSON list where each entry has:
       - name: variable identifier
       - units: CF-compliant unit string

Output:
    - Comparison table showing all variables with their units from both sources
    - List of mismatches (after normalization)
    - Pass/fail summary

Common Use Cases:
    - Validate metadata after model code changes
    - Check for unit discrepancies before dataset production
    - Verify CF-compliant unit conversions are correct
    - Quality assurance for new ECCO releases

Authors:
    Ian Fenty (2026-04-26), co-written with GitHub Copilot after many
    iterations on handling various unit format edge cases.

"""

import json
import re
import os
import argparse 

def normalize_units(unit_string):
    """Normalize unit strings to a canonical format for comparison.

    Converts various unit notations to a standardized form using negative
    exponents, combines terms with the same base, and handles special cases
    like temperature units and dimensionless quantities.

    The normalization process:
    1. Handles dimensionless: 'fraction', 'count' → '1'
    2. Temperature: 'degC' → 'degree_C', 'degK' → 'degree_K'
    3. Removes '^' symbols from exponents
    4. Replaces '.' with spaces
    5. Converts fraction notation: 'm/s' → 'm s-1'
    6. Combines powers of same unit: 'm m2' → 'm3'
    7. Preserves original unit order and case

    Args:
        unit_string (str): Unit string in any supported format.

    Returns:
        str: Normalized unit string with negative exponents and combined terms,
            or '1' for dimensionless quantities.

    Examples:
        >>> normalize_units('m/s')
        'm s-1'
        >>> normalize_units('1/m^2')
        'm-2'
        >>> normalize_units('kg/m^2/s')
        'kg m-2 s-1'
        >>> normalize_units('m m2')
        'm3'
        >>> normalize_units('degC')
        'degree_C'
        >>> normalize_units('fraction')
        '1'

    """
    original_unit_string = unit_string.strip()

    # Handle dimensionless quantities
    if original_unit_string.lower() in ['fraction', 'count']:
        return '1'

    # Pre-processing: normalize common variations
    unit_string = original_unit_string.replace('.', ' ')
    unit_string = unit_string.replace('degC', 'degree_C')
    unit_string = unit_string.replace('degK', 'degree_K')
    unit_string = unit_string.replace('^', '')  # Remove caret symbols

    # Split on '/' to separate numerator from denominators
    parts = unit_string.split('/')

    # Dictionary to accumulate net power for each unit
    unit_powers = {}

    # Process numerator (first part)
    if parts[0] != '1':
        # Extract unit names and their exponents (handles negative exponents like s-1)
        numerator_components = re.findall(r'([a-zA-Z_]+)(-?\d*)', parts[0])
        for part, power in numerator_components:
            power = int(power) if power else 1
            unit_powers[part] = unit_powers.get(part, 0) + power

    # Process denominators (all parts after first '/')
    if len(parts) > 1:
        for denominator in parts[1:]:
            denominator = denominator.strip()
            den_components = re.findall(r'([a-zA-Z_]+)(-?\d*)', denominator)
            for part, power in den_components:
                power = int(power) if power else 1
                # Subtract power because this unit is in denominator
                unit_powers[part] = unit_powers.get(part, 0) - power

    # Build output string, preserving original order of appearance
    result_parts = []

    # Extract all unique unit names in their order of first appearance
    all_units_in_order = []
    all_unit_components = re.findall(r'([a-zA-Z_]+)', unit_string)
    for unit in all_unit_components:
        if unit not in all_units_in_order:
            all_units_in_order.append(unit)

    # Build result: only include units with non-zero net power
    non_zero_power_exists = False
    for unit in all_units_in_order:
        power = unit_powers.get(unit, 0)
        if power != 0:
            non_zero_power_exists = True
        if power == 0:
            continue  # Units that cancel out
        if power == 1:
            result_parts.append(unit)
        else:
            result_parts.append(f"{unit}{power}")

    # If all powers canceled to zero, this is dimensionless
    if not non_zero_power_exists and unit_powers:
        return '1'

    return " ".join(result_parts)


def parse_diagnostics_log(file_path):
    """Parse MITgcm available_diagnostics.log file to extract variable units.

    The diagnostics log file is a pipe-delimited table produced by MITgcm that
    lists all available diagnostic variables and their metadata. This function
    extracts the DIAG_NAME and UNITS columns.

    Args:
        file_path (str): Path to available_diagnostics.log file.

    Returns:
        dict: Mapping of variable name (str) → unit string (str).
            Variables not found in the file are not included.

    """
    diagnostics_units = {}
    with open(file_path, 'r') as f:
        for line in f:
            # Skip header, separator, and non-data lines
            if '|' in line and 'DIAG_NAME' not in line and '-------' not in line:
                parts = [p.strip() for p in line.split('|')]
                # Format: | DIAG_NAME | LEVS | MATE | CODE | UNITS | TITLE |
                if len(parts) > 5:
                    diag_name = parts[1]  # Column index 1: DIAG_NAME
                    units = parts[5]       # Column index 5: UNITS
                    if diag_name and units:
                        diagnostics_units[diag_name] = units
    return diagnostics_units

def parse_variable_metadata(file_path):
    """Parse ECCO variable metadata JSON file to extract variable units.

    The variable metadata file is a JSON list of dictionaries, each containing
    metadata for one ECCO variable (name, long_name, units, etc.). This function
    indexes the list by variable name for easy lookup.

    Args:
        file_path (str): Path to variable_metadata.json file.

    Returns:
        dict: Mapping of variable name (str) → complete metadata dict.
            Each metadata dict includes keys like 'name', 'units', 'long_name', etc.

    """
    with open(file_path, 'r') as f:
        metadata_list = json.load(f)
    
    metadata_dict = {}
    for item in metadata_list:
        metadata_dict[item['name']] = item
    return metadata_dict

def main(diagnostics_log_path, metadata_path):
    """Compare units between MITgcm diagnostics log and ECCO variable metadata.

    For each variable in the metadata file:
    1. Look up its units in both files
    2. Normalize the diagnostics log units
    3. Compare normalized units with metadata units (whitespace-insensitive)
    4. Print comparison table and report mismatches

    Args:
        diagnostics_log_path (str): Path to MITgcm available_diagnostics.log file.
        metadata_path (str): Path to ECCO variable_metadata.json file.

    Returns:
        None. Prints comparison table and mismatch report to stdout.

    """
    # Load both data sources
    diagnostics_units = parse_diagnostics_log(diagnostics_log_path)
    variable_metadata = parse_variable_metadata(metadata_path)

    mismatches = []

    # Print comparison table header
    print(f"{'Variable':<15} | {'Meta Units':<25} | {'Diag Units (Orig)':<25} | {'Diag Units (Reconciled)'}")
    print("-" * 90)

    # Compare units for each variable in the metadata
    for var_name, meta_info in variable_metadata.items():
        meta_unit = meta_info.get('units', 'Not Found')
        diag_unit_orig = diagnostics_units.get(var_name, 'Not Found')

        # Normalize diagnostics log units for comparison
        if diag_unit_orig != 'Not Found':
            reconciled_unit = normalize_units(diag_unit_orig)
        else:
            reconciled_unit = 'Not Found'

        print(f"{var_name:<15} | {meta_unit:<25} | {diag_unit_orig:<25} | {reconciled_unit}")

        # Check for mismatches after normalization
        if diag_unit_orig != 'Not Found':
            # Special case: '-' or '---' represents dimensionless in either format
            if meta_unit in ['-', '---'] and reconciled_unit.strip() in ['-', '---', '']:
                 continue

            # Compare whitespace-insensitive (allow 'm s-1' == 'ms-1')
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
    parser = argparse.ArgumentParser(
        description="Validate unit consistency between MITgcm diagnostics log and ECCO variable metadata.",
        epilog="""
Examples:
  # Compare units for V4r4
  %(prog)s \\
    ~/ECCO-v4-Configurations/ECCOv4r4/available_diagnostics.000000.log \\
    ~/ECCO-v4-Configurations/ECCOv4r4/metadata/variable_metadata.json

  # Compare for current working directory
  %(prog)s ./available_diagnostics.log ./variable_metadata.json

The tool normalizes unit strings before comparison to handle format differences
like 'm/s' vs 'm s-1', 'degC' vs 'degree_C', etc.
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "diagnostics_log",
        help="Path to MITgcm available_diagnostics.log file (pipe-delimited table)."
    )
    parser.add_argument(
        "variable_metadata",
        help="Path to ECCO variable_metadata.json file (JSON list of variable dicts)."
    )
    args = parser.parse_args()
    main(args.diagnostics_log, args.variable_metadata)
