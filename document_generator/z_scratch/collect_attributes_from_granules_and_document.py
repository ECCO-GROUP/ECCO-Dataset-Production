import glob
from pathlib import Path
import xarray as xr

granule_parent_dir = "/Users/brucel/ecco/yip/ECCO-Dataset-Production/document_generator/files_general/resource_files/version_specific/v4r4/output_and_granules/granules"

def list_files_pathlib(directory_path='.'):
    files = [str(p) for p in Path(directory_path).rglob('*.nc') if p.is_file()]
    return files

all_granule_paths = list_files_pathlib(granule_parent_dir)

global_attrs_granules = set()
non_global_attrs_granules = set()

for granule_path in all_granule_paths:
        dataset = xr.open_dataset(granule_path)
        global_attrs_granules.update(list(dataset.attrs.keys()))
        for var in dataset.data_vars:
            non_global_attrs_granules.update([s.strip() for s in list(dataset[var].attrs.keys())])
        for var in dataset.coords:
            non_global_attrs_granules.update([s.strip() for s in list(dataset[var].attrs.keys())])
        for var in dataset.dims:
            non_global_attrs_granules.update([s.strip() for s in list(dataset[var].attrs.keys())])

global_attr_tex_path = "/Users/brucel/ecco/yip/ECCO-Dataset-Production/document_generator/z_scratch/GlobalAttribute_tableV1.tex"
non_global_attr_tex_path = "/Users/brucel/ecco/yip/ECCO-Dataset-Production/document_generator/z_scratch/Coordinates_Dimensions_Variables_Attribute_tablev1.tex"

global_attrs_document = set()
non_global_attrs_document = set()

with open(global_attr_tex_path) as file:
    for line in file:
        if line.strip()[0] != "\\":
            global_attrs_document.add(line.split("&")[0].replace('\\','').strip())

with open(non_global_attr_tex_path) as file:
    for line in file:
        if line.strip()[0] != "\\":
            non_global_attrs_document.add(line.split("&")[0].replace('\\','').strip())



global_attr_tex_path_new = "/Users/brucel/ecco/yip/ECCO-Dataset-Production/document_generator/files_general/resource_files/version_specific/v4r4/input_and_templates/latex/components/global_attributes_table.tex"
non_global_attr_tex_path_new = "/Users/brucel/ecco/yip/ECCO-Dataset-Production/document_generator/files_general/resource_files/version_specific/v4r4/output_and_granules/output/latex/variable_attributes.tex"

global_attrs_document_new = set()
non_global_attrs_document_new = set()

with open(global_attr_tex_path_new) as file:
    for line in file:
        if line.strip()[0] != "\\" and line.strip()[0] != ">":
            global_attrs_document_new.add(line.split("&")[0].replace('\\','').strip())

with open(non_global_attr_tex_path_new) as file:
    for line in file:
        #if line.strip()[0] != "\\":
        if line.strip() != "":
            if line.strip()[0] != "\\":
                non_global_attrs_document_new.add(line.split("&")[0].replace('\\','').strip())



global_attrs_ecco_exclusive = set(["author", "coordinates_comment", "product_name", "product_time_coverage_end", "product_time_coverage_start"])
non_global_attrs_ecco_exclusive = set(["c_grid_axis_shift", "coordinate", "direction", "mate", "swap_dim"])

print()
print("global_attrs_document - global_attrs_document_new")
print(sorted(list(global_attrs_document - global_attrs_document_new)))

print()
print("global_attrs_document_new - global_attrs_document")
print(sorted(list(global_attrs_document_new - global_attrs_document)))

print()
print("global_attrs_document - global_attrs_document_new - global_attrs_ecco_exclusive")
print(sorted(list(global_attrs_document - global_attrs_document_new - global_attrs_ecco_exclusive)))

print("-------")

print()
print("non_global_attrs_document - non_global_attrs_document_new")
print(sorted(list(non_global_attrs_document - non_global_attrs_document_new)))

print()
print("non_global_attrs_document_new - non_global_attrs_document")
print(sorted(list(non_global_attrs_document_new - non_global_attrs_document)))

print()
print("non_global_attrs_document - non_global_attrs_document_new - non_global_attrs_ecco_exclusive")
print(sorted(list(non_global_attrs_document - non_global_attrs_document_new - non_global_attrs_ecco_exclusive)))



global_attrs_document_new = sorted(list(global_attrs_document_new))
non_global_attrs_document_new = sorted(list(non_global_attrs_document_new))

global_attrs_granules = sorted(list(global_attrs_granules)) 
global_attrs_document = sorted(list(global_attrs_document))
non_global_attrs_granules = sorted(list(non_global_attrs_granules)) 
non_global_attrs_document = sorted(list(non_global_attrs_document))

#print(global_attrs_granules == global_attrs_document)
#print(non_global_attrs_granules == non_global_attrs_document)


'''
print()
print("global_attrs_granules - global_attrs_document")
print(global_attrs_granules - global_attrs_document)

print()
print("global_attrs_document - global_attrs_granules")
print(global_attrs_document - global_attrs_granules)

print()
print("non_global_attrs_granules - non_global_attrs_document")
print(non_global_attrs_granules - non_global_attrs_document)

print()
print("non_global_attrs_document - non_global_attrs_granules")
print(non_global_attrs_document - non_global_attrs_granules)
'''



