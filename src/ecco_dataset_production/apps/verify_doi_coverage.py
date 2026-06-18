#!/usr/bin/env python3
"""Verify that all dataset groupings have corresponding DOI entries.

This tool checks that every grouping defined in the metadata JSON files
(groupings_for_native_datasets.json, groupings_for_latlon_datasets.json,
groupings_for_1D_datasets.json) has at least one corresponding entry in
the PO.DAAC dataset CSV file with DOI information.
"""

import argparse
import csv
import glob
import json
import logging
import os
import sys
from collections import defaultdict


# Initialize root logger
logging.basicConfig(format='%(levelname)-10s %(message)s')


def load_groupings(metadata_dir):
    """Load all groupings from JSON files in metadata directory.

    Args:
        metadata_dir (str): Path to directory containing groupings JSON files.

    Returns:
        dict: Dictionary mapping grouping type to list of groupings.
              Keys are '1D', 'native', 'latlon'.
              Each grouping is a dict with 'filename', 'name', etc.
    """
    log = logging.getLogger('verify_doi')

    groupings_files = {
        '1D': 'groupings_for_1D_datasets.json',
        'native': 'groupings_for_native_datasets.json',
        'latlon': 'groupings_for_latlon_datasets.json'
    }

    all_groupings = {}

    for group_type, filename in groupings_files.items():
        filepath = os.path.join(metadata_dir, filename)
        if not os.path.exists(filepath):
            log.warning('Groupings file not found: %s', filepath)
            all_groupings[group_type] = []
            continue

        try:
            with open(filepath, 'r') as f:
                groupings = json.load(f)
            all_groupings[group_type] = groupings
            log.info('Loaded %d %s groupings from %s',
                    len(groupings), group_type, filename)
        except Exception as e:
            log.error('Error loading %s: %s', filepath, e)
            all_groupings[group_type] = []

    return all_groupings


def load_doi_csv(csv_path):
    """Load DOI information from CSV file.

    Args:
        csv_path (str): Path to CSV file with columns including DATASET.FILENAME
                       and DATASET.PERSISTENT_ID.

    Returns:
        dict: Dictionary mapping filename prefix to list of CSV row dicts.
    """
    log = logging.getLogger('verify_doi')

    if not os.path.exists(csv_path):
        log.error('CSV file not found: %s', csv_path)
        return {}

    # Map of filename prefix -> list of matching rows
    doi_entries = defaultdict(list)

    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)

            # Check for required column
            if 'DATASET.FILENAME' not in reader.fieldnames:
                log.error('CSV missing required column: DATASET.FILENAME')
                log.error('Available columns: %s', ', '.join(reader.fieldnames))
                return {}

            for row in reader:
                filename = row.get('DATASET.FILENAME', '').strip()
                if filename:
                    doi_entries[filename] = row

        log.info('Loaded %d DOI entries from %s', len(doi_entries), csv_path)

    except Exception as e:
        log.error('Error loading CSV %s: %s', csv_path, e)
        return {}

    return doi_entries


def check_doi_uniqueness(doi_entries, log_level='INFO'):
    """Check that all DOIs in the CSV are unique.

    Args:
        doi_entries (dict): Dictionary of DOI CSV entries by filename.
        log_level (str): Logging level.

    Returns:
        tuple: (duplicate_count, duplicate_details)
               duplicate_details is dict mapping DOI -> list of filenames
    """
    log = logging.getLogger('verify_doi')
    log.setLevel(log_level)

    # Map DOI -> list of filenames using that DOI
    doi_to_files = defaultdict(list)

    for filename, row in doi_entries.items():
        doi = row.get('DATASET.PERSISTENT_ID', '').strip()
        if doi:
            doi_to_files[doi].append(filename)

    # Find duplicates
    duplicates = {doi: files for doi, files in doi_to_files.items() if len(files) > 1}

    log.info('')
    log.info('='*70)
    log.info('DOI UNIQUENESS CHECK')
    log.info('='*70)

    if duplicates:
        log.error('Found %d duplicate DOIs:', len(duplicates))
        log.error('')
        for doi, files in duplicates.items():
            log.error('DOI: %s', doi)
            log.error('  Used by %d datasets:', len(files))
            for filename in files:
                log.error('    - %s', filename)
            log.error('')
    else:
        log.info('All %d DOIs are unique', len(doi_to_files))

    log.info('='*70)

    return len(duplicates), duplicates


def expand_groupings_by_frequency(groupings):
    """Expand groupings into individual dataset expectations based on frequency.

    Args:
        groupings (dict): Dictionary of groupings by type.

    Returns:
        list: List of expected dataset dicts with keys:
              'type', 'name', 'filename', 'frequency_code', 'frequency_pattern'
    """
    # Map frequency codes to filename patterns
    frequency_map = {
        'AVG_DAY': 'day_mean',
        'AVG_MON': 'mon_mean',
        'SNAP': 'snap'
    }

    expected_datasets = []

    for group_type in ['1D', 'native', 'latlon']:
        group_list = groupings.get(group_type, [])

        for grouping in group_list:
            grouping_filename = grouping.get('filename', '')
            grouping_name = grouping.get('name', 'UNKNOWN')

            if not grouping_filename:
                # Missing filename field
                expected_datasets.append({
                    'type': group_type,
                    'name': grouping_name,
                    'filename': 'MISSING',
                    'frequency_code': None,
                    'frequency_pattern': None,
                    'search_pattern': '',
                    'reason': 'No filename field in grouping'
                })
                continue

            # Get frequency field
            frequency_str = grouping.get('frequency', '')
            if not frequency_str:
                # No frequency field - treat as single dataset
                expected_datasets.append({
                    'type': group_type,
                    'name': grouping_name,
                    'filename': grouping_filename,
                    'frequency_code': None,
                    'frequency_pattern': None,
                    'search_pattern': grouping_filename
                })
                continue

            # Parse comma-separated frequencies
            frequencies = [f.strip() for f in frequency_str.split(',')]

            for freq_code in frequencies:
                # Handle time-invariant datasets specially
                # Check for both underscore and hyphen versions
                if freq_code.upper().replace('-', '_') == 'TIME_INVARIANT':
                    # Time-invariant files don't have date strings
                    # Pattern: {filename}_ECCO_{version}_{grid}.nc
                    # Example: OCEAN_MIXING_COEFFS_ECCO_V4r6_native_llc0090.nc
                    expected_datasets.append({
                        'type': group_type,
                        'name': grouping_name,
                        'filename': grouping_filename,
                        'frequency_code': freq_code,
                        'frequency_pattern': None,  # No pattern for time-invariant
                        'search_pattern': f"{grouping_filename}_ECCO_"
                    })
                else:
                    # Time-varying datasets have date strings
                    # Pattern: {filename}_{freq_pattern}_{date}_ECCO_{version}_{grid}.nc
                    freq_pattern = frequency_map.get(freq_code, freq_code.lower())
                    expected_datasets.append({
                        'type': group_type,
                        'name': grouping_name,
                        'filename': grouping_filename,
                        'frequency_code': freq_code,
                        'frequency_pattern': freq_pattern,
                        'search_pattern': f"{grouping_filename}_{freq_pattern}_"
                    })

    return expected_datasets


def verify_reverse_coverage(groupings, doi_entries, log_level='INFO'):
    """Verify that all CSV entries correspond to a grouping definition.

    Args:
        groupings (dict): Dictionary of groupings by type.
        doi_entries (dict): Dictionary of DOI CSV entries by filename.
        log_level (str): Logging level.

    Returns:
        tuple: (orphan_count, orphan_details)
               orphan_details is list of CSV filenames without groupings
    """
    log = logging.getLogger('verify_doi')
    log.setLevel(log_level)

    # Known exceptions: files that should be in CSV but don't have groupings
    # These are typically metadata files rather than data products
    known_exceptions = [
        'GRID_GEOMETRY_',  # Grid geometry files (native and latlon)
    ]

    # Build list of all grouping filename prefixes
    grouping_prefixes = set()
    for group_type in ['1D', 'native', 'latlon']:
        group_list = groupings.get(group_type, [])
        for grouping in group_list:
            grouping_filename = grouping.get('filename', '')
            if grouping_filename:
                grouping_prefixes.add(grouping_filename)

    log.info('')
    log.info('='*70)
    log.info('REVERSE CHECK: CSV ENTRIES → GROUPINGS')
    log.info('='*70)

    # Check each CSV entry
    orphans = []
    exceptions_found = []
    for csv_filename in doi_entries.keys():
        # Check if this is a known exception
        is_exception = any(csv_filename.startswith(exc) for exc in known_exceptions)
        if is_exception:
            exceptions_found.append(csv_filename)
            log.debug('CSV entry is known exception (no grouping required): %s', csv_filename)
            continue

        # Check if this filename starts with any known grouping prefix
        matched = False
        for prefix in grouping_prefixes:
            if csv_filename.startswith(prefix):
                matched = True
                break

        if not matched:
            orphans.append(csv_filename)
            log.warning('CSV entry has no grouping: %s', csv_filename)

    if exceptions_found:
        log.info('Found %d known exceptions (e.g., GRID_GEOMETRY files)', len(exceptions_found))

    if orphans:
        log.error('')
        log.error('Found %d CSV entries without corresponding groupings', len(orphans))
    else:
        log.info('All %d CSV entries correspond to groupings or known exceptions',
                len(doi_entries) - len(exceptions_found))

    log.info('='*70)

    return len(orphans), orphans


def verify_coverage(groupings, doi_entries, log_level='INFO'):
    """Verify that all groupings have corresponding DOI entries.

    Args:
        groupings (dict): Dictionary of groupings by type.
        doi_entries (dict): Dictionary of DOI CSV entries by filename.
        log_level (str): Logging level.

    Returns:
        tuple: (missing_count, total_count, missing_details)
               missing_details is list of dicts with grouping info
    """
    log = logging.getLogger('verify_doi')
    log.setLevel(log_level)

    # Expand groupings by frequency
    expected_datasets = expand_groupings_by_frequency(groupings)

    log.info('')
    log.info('='*70)
    log.info('EXPECTED DATASETS (GROUPINGS × FREQUENCIES)')
    log.info('='*70)

    # Group by type for display
    by_type = {'1D': [], 'native': [], 'latlon': []}
    for ds in expected_datasets:
        by_type[ds['type']].append(ds)

    for group_type in ['1D', 'native', 'latlon']:
        if by_type[group_type]:
            log.info('')
            log.info('[%s] - %d expected datasets:', group_type.upper(), len(by_type[group_type]))
            for ds in by_type[group_type]:
                if ds['frequency_code']:
                    log.info('  - %s [%s]', ds['filename'], ds['frequency_code'])
                else:
                    log.info('  - %s', ds['filename'])

    # Now check each expected dataset
    missing = []
    found = 0

    log.info('')
    log.info('='*70)
    log.info('CHECKING DOI COVERAGE')
    log.info('='*70)

    for expected in expected_datasets:
        search_pattern = expected['search_pattern']

        # Handle missing filename
        if not search_pattern:
            log.warning('%s [%s] -> Missing filename in grouping',
                       expected['name'], expected['type'])
            missing.append(expected)
            continue

        # Find matching entries in CSV
        matches = [csv_fn for csv_fn in doi_entries.keys()
                  if search_pattern in csv_fn]

        if matches:
            found += 1
            log.debug('%s -> found %d DOI entries',
                     search_pattern, len(matches))
        else:
            freq_label = f" [{expected['frequency_code']}]" if expected['frequency_code'] else ""
            log.warning('%s%s [%s] -> NO DOI entries found',
                       expected['filename'], freq_label, expected['type'])
            missing.append(expected)

    log.info('')
    log.info('='*70)
    log.info('VERIFICATION SUMMARY')
    log.info('='*70)
    log.info('Total expected datasets: %d', len(expected_datasets))
    log.info('  - With DOI entries: %d', found)
    log.info('  - Missing DOI entries: %d', len(missing))
    log.info('='*70)

    return len(missing), len(expected_datasets), missing


def main():
    """Command-line entry point."""
    parser = argparse.ArgumentParser(
        description="""Verify that all dataset groupings have corresponding DOI entries.

        This tool checks that every grouping defined in the groupings JSON files
        has at least one matching entry in the PO.DAAC dataset CSV file.""",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Example:
  edp_verify_doi_coverage \\
      --metadata-dir "/path/to/ECCO-v4-Configurations/ECCOv4 Release 6/metadata" \\
      --csv-file "/path/to/metadata/PODAAC_dataset_table_V4r6_prelim.csv"
        """)

    parser.add_argument('-m', '--metadata-dir', required=True,
        help='Path to directory containing groupings JSON files')

    parser.add_argument('-c', '--csv-file', required=True,
        help='Path to CSV file with DOI information (must have DATASET.FILENAME column)')

    parser.add_argument('-l', '--log',
        dest='log_level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        default='INFO',
        help='Set logging level (default: %(default)s)')

    parser.add_argument('-v', '--verbose',
        action='store_true',
        help='Show detailed missing entries (equivalent to --log DEBUG)')

    parser.add_argument('-q', '--quiet',
        action='store_true',
        help='Only show summary and errors (equivalent to --log WARNING)')

    args = parser.parse_args()

    # Set log level
    if args.verbose:
        log_level = 'DEBUG'
    elif args.quiet:
        log_level = 'WARNING'
    else:
        log_level = args.log_level

    log = logging.getLogger('verify_doi')
    log.setLevel(log_level)

    # Load data
    log.info('Loading groupings from %s', args.metadata_dir)
    groupings = load_groupings(args.metadata_dir)

    log.info('Loading DOI CSV from %s', args.csv_file)
    doi_entries = load_doi_csv(args.csv_file)

    if not doi_entries:
        log.error('Failed to load DOI entries. Aborting.')
        return 1

    # Check DOI uniqueness
    duplicate_count, duplicate_details = check_doi_uniqueness(doi_entries, log_level)

    # Check reverse: CSV entries → groupings
    orphan_count, orphan_details = verify_reverse_coverage(groupings, doi_entries, log_level)

    # Verify coverage: groupings → CSV entries
    missing_count, total_count, missing_details = verify_coverage(
        groupings, doi_entries, log_level)

    # Report missing entries
    if missing_details:
        log.warning('')
        log.warning('MISSING DOI ENTRIES:')
        log.warning('-'*70)
        for entry in missing_details:
            freq_label = f" [{entry['frequency_code']}]" if entry.get('frequency_code') else ""
            log.warning('[%s] %s%s', entry['type'].upper(), entry['filename'], freq_label)
            log.warning('    Name: %s', entry['name'])
            # Always show search pattern if it exists (even for time-invariant)
            if entry.get('search_pattern'):
                log.warning('    Search pattern: %s', entry['search_pattern'])
            reason = entry.get('reason', 'No matching entries in CSV')
            log.warning('    Reason: %s', reason)
            log.warning('')

    # Report orphaned CSV entries
    if orphan_details:
        log.warning('')
        log.warning('ORPHANED CSV ENTRIES (no corresponding grouping):')
        log.warning('-'*70)
        for csv_filename in orphan_details:
            log.warning('  - %s', csv_filename)
        log.warning('')

    # Exit code - fail if duplicates, missing entries, or orphaned entries
    has_errors = False
    if duplicate_count > 0:
        log.error('VERIFICATION FAILED: %d duplicate DOIs found', duplicate_count)
        has_errors = True

    if orphan_count > 0:
        log.error('VERIFICATION FAILED: %d CSV entries without corresponding groupings',
                 orphan_count)
        has_errors = True

    if missing_count > 0:
        log.error('VERIFICATION FAILED: %d expected datasets missing DOI entries',
                 missing_count)
        has_errors = True

    if has_errors:
        return 1
    else:
        log.info('VERIFICATION PASSED: All checks successful')
        log.info('  - All DOIs are unique')
        log.info('  - All CSV entries have corresponding groupings')
        log.info('  - All groupings have DOI entries')
        return 0


if __name__ == '__main__':
    sys.exit(main())
