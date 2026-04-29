import argparse
import json
import boto3
from typing import Set

## usage example
## python validate_ecco_variables_across_sources.py --desc /Users/ifenty/git_repo_others/ECCO-v4-Configurations/ECCOv4\ Release\ 6/metadata/variable_metadata.json --groups /Users/ifenty/git_repo_others/ECCO-v4-Configurations/ECCOv4\ Release\ 6/metadata/groupings_for_native_datasets.json --s3-bucket ecco-tmp --s3-prefixes ecco-results/V4r6/diags_monthly/



def get_vars_from_descriptions(filepath: str) -> Set[str]:
    """Source 1: Extract 'name' from a list of variable dictionaries."""
    with open(filepath, 'r') as f:
        data = json.load(f)
        return {item['name'].strip() for item in data if 'name' in item}

def get_vars_from_groups(filepath: str) -> Set[str]:
    """Source 2: Extract variables from a comma-separated 'fields' string."""
    with open(filepath, 'r') as f:
        data = json.load(f)
        all_vars = set()
        for group in data:
            if 'fields' in group:
                fields_list = [v.strip() for v in group['fields'].split(',')]
                all_vars.update(fields_list)
        return {v for v in all_vars if v}

def get_vars_from_s3(bucket: str, base_prefixes: list, profile: str = None) -> Set[str]:
    """
    Source 3: Crawl specific ECCO S3 prefixes and strip suffixes.
    Handles '_snap' for inst and '_mon_mean' for monthly.
    """
    session = boto3.Session(profile_name=profile) if profile else boto3.Session()
    s3 = session.client('s3')
    found_vars = set()
    
    for prefix in base_prefixes:
        # Use Delimiter='/' to get 'CommonPrefixes' (folders) 
        # This is significantly faster than listing every file.
        paginator = s3.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter='/'):
            for folder in page.get('CommonPrefixes', []):
                # Folder looks like: 'ecco-results/V4r6/diags_inst/ETAN_snap/'
                folder_path = folder.get('Prefix').rstrip('/')
                folder_name = folder_path.split('/')[-1]
                
                # Strip the specific suffixes to get the raw variable name
                var_name = folder_name.replace('_snap', '').replace('_mon_mean', '')
                found_vars.add(var_name)
                
    return found_vars

def run_validation(sources: list):
    if not sources:
        print("No sources selected for validation. Use --desc, --groups, or --s3 flags.")
        return

    # 2. Master Set
    master_list = set().union(*(s[1] for s in sources))
    
    print(f"Master variable list compiled with {len(master_list)} unique variables.\n")

    # 3. Report
    print(f"{'Source':<20} | {'Count':<6} | {'Status'}")
    print("-" * 45)

    for name, var_set in sources:
        missing = master_list - var_set
        status = "✅ OK" if not missing else f"❌ Missing {len(missing)}"
        print(f"{name:<20} | {len(var_set):<6} | {status}")
        if missing:
            for var in sorted(missing):
                found_in = [s_name for s_name, s_set in sources if var in s_set]
                print(f"   - {var:<20} (found in: {', '.join(found_in)})")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Validate ECCO variables across different metadata sources.")
    parser.add_argument('--desc', help='Path to descriptions.json')
    parser.add_argument('--groups', help='Path to groups.json')
    parser.add_argument('--s3-bucket', help='S3 Bucket name (e.g., ecco-tmp)')
    parser.add_argument('--s3-prefixes', nargs='+', help='S3 prefixes to crawl', 
                        default=['ecco-results/V4r6/diags_inst/', 'ecco-results/V4r6/diags_monthly/'])
    # default profile should be saml-pub, but allowing override for flexibility
    parser.add_argument('--profile', default='saml-pub', 
                        help='AWS profile name (default: %(default)s)')

    args = parser.parse_args()
    active_sources = []

    if args.desc:
        print(f"Loading variables from {args.desc}...")
        active_sources.append(("Descriptions JSON", get_vars_from_descriptions(args.desc)))
    
    if args.groups:
        print(f"Loading variables from {args.groups}...")
        active_sources.append(("Groups JSON", get_vars_from_groups(args.groups)))

    if args.s3_bucket:
        profile_str = f" using profile '{args.profile}'" if args.profile else ""
        print(f"Crawling S3 bucket '{args.s3_bucket}'{profile_str}...")
        active_sources.append(("S3 Folders", get_vars_from_s3(args.s3_bucket, args.s3_prefixes, profile=args.profile)))

    print("-" * 45)
    run_validation(active_sources)
