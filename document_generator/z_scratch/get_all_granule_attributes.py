import netCDF4 as nc
import glob
from pathlib import Path

granule_parent_dir = "/Users/brucel/ecco/yip/ECCO-Dataset-Production/document_generator/files_general/resource_files/version_specific/v4r4/output_and_granules/granules"

def list_files_pathlib(directory_path='.'):
    files = [str(p) for p in Path(directory_path).rglob('*.nc') if p.is_file()]
    return files

all_granule_paths = list_files_pathlib(granule_parent_dir)

global_attrs = set()
non_global_attrs = set()

for granule_path in all_granule_paths:
    with nc.Dataset(granule_path, 'r') as dataset:
        global_attrs.update(dataset.ncattrs())
        for variable_name in dataset.variables.keys():
            non_global_attrs.update(dataset.variables[variable_name].ncattrs())
