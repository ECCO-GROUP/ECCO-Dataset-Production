import re
import glob
from pathlib import Path
import xarray as xr

print()
print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
print("NOTE: All attribute names have been set to lowercase, for consistent comparison.  We should probably to re-capitalize attribute names according to official standards")
print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
print()



granule_parent_dir = "/Users/brucel/ecco/yip/ECCO-Dataset-Production/document_generator/files_general/resource_files/version_specific/v4r4/output_and_granules/granules"

def list_files_pathlib(directory_path='.'):
    files = [str(p) for p in Path(directory_path).rglob('*.nc') if p.is_file()]
    return files

all_granule_paths = list_files_pathlib(granule_parent_dir)

global_attrs_granules = set()
non_global_attrs_granules = set()

for granule_path in all_granule_paths:
        dataset = xr.open_dataset(granule_path)
        global_attrs_granules.update([el.lower() for el in list(dataset.attrs.keys())])
        for var in dataset.data_vars:
            non_global_attrs_granules.update([s.strip().lower() for s in list(dataset[var].attrs.keys())])
        for var in dataset.coords:
            non_global_attrs_granules.update([s.strip().lower() for s in list(dataset[var].attrs.keys())])
        for var in dataset.dims:
            non_global_attrs_granules.update([s.strip().lower() for s in list(dataset[var].attrs.keys())])

global_attrs_repoExampleDoc_tex_path = "/Users/brucel/ecco/yip/ECCO-Dataset-Production/document_generator/z_scratch/GlobalAttribute_tableV1.tex"
non_global_attrs_repoExampleDoc_tex_path = "/Users/brucel/ecco/yip/ECCO-Dataset-Production/document_generator/z_scratch/Coordinates_Dimensions_Variables_Attribute_tablev1.tex"

global_attrs_repoExampleDoc = set()
non_global_attrs_repoExampleDoc = set()

with open(global_attrs_repoExampleDoc_tex_path) as file:
    for line in file:
        if line.strip()[0] != "\\" or "".join(line.strip()[:2]) == r"\_":
            global_attrs_repoExampleDoc.add(line.split("&")[0].replace('\\','').strip().lower())

with open(non_global_attrs_repoExampleDoc_tex_path) as file:
    for line in file:
        if line.strip()[0] != "\\" or "".join(line.strip()[:2]) == r"\_":
            non_global_attrs_repoExampleDoc.add(line.split("&")[0].replace('\\','').strip().lower())



#global_attrs_jsonDoc_tex_path = "/Users/brucel/ecco/yip/ECCO-Dataset-Production/document_generator/files_general/resource_files/version_specific/v4r4/input_and_templates/latex/components/global_attributes_table.tex"
global_attrs_jsonDoc_tex_path = "/Users/brucel/ecco/yip/ECCO-Dataset-Production/document_generator/files_general/resource_files/version_specific/v4r4/output_and_granules/output/latex/global_attributes.tex"
non_global_attrs_jsonDoc_tex_path = "/Users/brucel/ecco/yip/ECCO-Dataset-Production/document_generator/files_general/resource_files/version_specific/v4r4/output_and_granules/output/latex/variable_attributes.tex"

global_attrs_jsonDoc = set()
non_global_attrs_jsonDoc = set()

with open(global_attrs_jsonDoc_tex_path) as file:
    for line in file:
        if line.strip() != "":
            if line.strip()[0] != "\\" or "".join(line.strip()[:2]) == r"\_":
            #if "cyan" in line.strip():
                global_attrs_jsonDoc.add(line.split("&")[0].replace('\\','').strip().lower())

with open(non_global_attrs_jsonDoc_tex_path) as file:
    for line in file:
        if line.strip() != "":
            if line.strip()[0] != "\\" or "".join(line.strip()[:2]) == r"\_":
            #if "violet" in line.strip():
                non_global_attrs_jsonDoc.add(line.split("&")[0].replace('\\','').strip().lower())




attr_web_file_path = "/Users/brucel/ecco/yip/ECCO-Dataset-Production/document_generator/z_scratch/from_web/attrs_from_web_global.txt"

with open(attr_web_file_path) as file:
    monster_table_line = file.readline()

global_non_global_split_string = "Appendix E"

global_non_global = monster_table_line.split(global_non_global_split_string)

# Note that there is one "erronious" row within one of the global data cells, containing the string "UMM-Var"  ; filter that out of the results below
row_split_string = "<tr><td>" 
name_split_string = "</td>"

global_table_rows = global_non_global[0].lower().split(row_split_string)
#global_table_rows = global_non_global[0].split(row_split_string)
# first element is all the stuff before the first row of interest
global_table_rows = global_table_rows[1:]

global_names = [row.split(name_split_string)[0] for row in global_table_rows]
erronious_name = "UMM-Var".lower()
#erronious_name = "UMM-Var"
global_names.remove(erronious_name)

# Add another funky hack to deal with 1-off website weirdness
# Ok actually what happened, is that in the 1-off case, instead of <tr><td>, there's <tr><td rowspan="2">
row_span_string = 'rowspan="2">'
for row in global_table_rows:
    if row_span_string in row:
        global_names.append(row.split(row_span_string)[1].split(name_split_string)[0])



non_global_table_rows = global_non_global[1].lower().split(row_split_string)
#non_global_table_rows = global_non_global[1].split(row_split_string)
# first element is all the stuff before the first row of interest
non_global_table_rows = non_global_table_rows[1:]
name_split_string = "</td>"
non_global_names = [row.split(name_split_string)[0] for row in non_global_table_rows]

bad_strings = ["</p>","<p>"]

for ii in range(len(global_names)):
    global_names[ii] = global_names[ii].replace("<p>","")
    global_names[ii] = global_names[ii].replace("</p>","")

for ii in range(len(non_global_names)):
    non_global_names[ii] = non_global_names[ii].replace("<p>","")
    non_global_names[ii] = non_global_names[ii].replace("</p>","")

global_names_no_paren = []
global_names_paren = []
non_global_names_no_paren = []
non_global_names_paren = []

for el in global_names:
    parts = [part.strip() for part in re.split(r'[()]', el) if part.strip()]
    global_names_no_paren.append(parts[0])
    if len(parts) > 1:
        global_names_paren.append(parts[1])


for el in non_global_names:
    parts = [part.strip() for part in re.split(r'[()]', el) if part.strip()]
    non_global_names_no_paren.append(parts[0])
    if len(parts) > 1:
        non_global_names_paren.append(parts[1])

global_attrs_web_no_paren = set(global_names_no_paren)
global_attrs_web_paren = set(global_names_paren)
non_global_attrs_web_no_paren = set(non_global_names_no_paren)
non_global_attrs_web_paren = set(non_global_names_paren)

global_attrs_web = set(global_names)
non_global_attrs_web = set(non_global_names)



global_attrs_eccoExclusive = set(["author", "coordinates_comment", "product_name", "product_time_coverage_end", "product_time_coverage_start"])
non_global_attrs_eccoExclusive = set(["c_grid_axis_shift", "coordinate", "direction", "mate", "swap_dim"])

global_attrs_web_probablyNotRelevantToECCO = set(['dataprogress', 'datasetlanguage', 'datasetquality', 'enddirection', 'endlatitude', 'equatorcrossingdate', 'equatorcrossinglongitude', 'equatorcrossingtime', 'fovresolution', 'inputdataproducts', 'inputdataproductversion', 'instrument', 'numberoforbits', 'orbitnumber', 'pge_endtime', 'pge_name', 'pge_starttime', 'pgeversion', 'processingenvironment', 'productiondatetime', 'spatialcompletenessdefinition', 'spatialcompletenessratio', 'startdirection', 'startlatitude', 'startorbit', 'stoporbit', 'validationdata'])




if global_attrs_granules == global_attrs_repoExampleDoc:
    print()
    print()
    print("------------")
    print("------------")
    print("NOTE: global attributes are the same for the downloaded granules and the repoExampleDoc, so granules will not be compared to other sources")
    print("------------")
    print("------------")
    print()
    print()
else:
    print()
    print()
    print("global_attrs_granules - global_attrs_repoExampleDoc")
    print(sorted(list(global_attrs_granules - global_attrs_repoExampleDoc)))
    print()
    print()
    print("global_attrs_repoExampleDoc - global_attrs_granules")
    print(sorted(list(global_attrs_repoExampleDoc - global_attrs_granules)))
    print()
    print()
if non_global_attrs_granules == non_global_attrs_repoExampleDoc:
    print()
    print()
    print("------------")
    print("------------")
    print("NOTE: non-global attributes are the same for the downloaded granules and the repoExampleDoc, so granules will not be compared to other sources")
    print("------------")
    print("------------")
    print()
    print()
else:
    print()
    print()
    print("non_global_attrs_granules - non_global_attrs_repoExampleDoc")
    print(sorted(list(non_global_attrs_granules - non_global_attrs_repoExampleDoc)))
    print()
    print()
    print("non_global_attrs_repoExampleDoc - non_global_attrs_granules")
    print(sorted(list(non_global_attrs_repoExampleDoc - non_global_attrs_granules)))
    print()
    print()









print("------------------------------------")
print("------------------------------------")
print("------------------------------------")
print("------------------------------------")
print("------------------------------------")
print("global attributes")
print("------------------------------------")
print("------------------------------------")
print("------------------------------------")
print("------------------------------------")
print("------------------------------------")
print()
print()

print()
print("--------------------------------------------------------------------------")
print("------------------------ repoExampleDoc subtractions ---------------------")
print("--------------------------------------------------------------------------")
print()

print()
print(">>>>>>global_attrs_repoExampleDoc - global_attrs_jsonDoc")
print()
print(sorted(list(global_attrs_repoExampleDoc - global_attrs_jsonDoc)))
print()

'''
print()
print(">>>>>>global_attrs_repoExampleDoc - global_attrs_jsonDoc - global_attrs_eccoExclusive")
print()
print(sorted(list(global_attrs_repoExampleDoc - global_attrs_jsonDoc - global_attrs_eccoExclusive)))
print()
'''

print()
print(">>>>>>global_attrs_repoExampleDoc - global_attrs_web_no_paren")
print()
print(sorted(list(global_attrs_repoExampleDoc - global_attrs_web_no_paren)))
print()

print()
print(">>>>>>global_attrs_repoExampleDoc - global_attrs_web_no_paren - global_attrs_eccoExclusive")
print()
print(sorted(list(global_attrs_repoExampleDoc - global_attrs_web_no_paren - global_attrs_eccoExclusive)))
print()


'''
print()
print(">>>>>>global_attrs_repoExampleDoc - global_attrs_jsonDoc  - global_attrs_web_no_paren")
print()
print(sorted(list(global_attrs_repoExampleDoc - global_attrs_jsonDoc  - global_attrs_web_no_paren)))
print()

print()
print(">>>>>>global_attrs_repoExampleDoc - global_attrs_jsonDoc  - global_attrs_web_no_paren - global_attrs_eccoExclusive")
print()
print(sorted(list(global_attrs_repoExampleDoc - global_attrs_jsonDoc  - global_attrs_web_no_paren - global_attrs_eccoExclusive)))
print()
'''

print()
print()
print()
print("-------------------------------------------------------------------")
print("------------------------ jsonDoc subtractions ---------------------")
print("-------------------------------------------------------------------")
print()

print()
print(">>>>>>global_attrs_jsonDoc - global_attrs_repoExampleDoc")
print()
print(sorted(list(global_attrs_jsonDoc - global_attrs_repoExampleDoc)))
print()

print()
print(">>>>>>global_attrs_jsonDoc - global_attrs_repoExampleDoc - global_attrs_eccoExclusive")
print()
print(sorted(list(global_attrs_jsonDoc - global_attrs_repoExampleDoc - global_attrs_eccoExclusive)))
print()

print()
print(">>>>>>global_attrs_jsonDoc - global_attrs_web_no_paren")
print()
print(sorted(list(global_attrs_jsonDoc - global_attrs_web_no_paren)))
print()

print()
print(">>>>>>global_attrs_jsonDoc - global_attrs_web_no_paren - global_attrs_eccoExclusive")
print()
print(sorted(list(global_attrs_jsonDoc - global_attrs_web_no_paren - global_attrs_eccoExclusive)))
print()

print()
print(">>>>>>global_attrs_jsonDoc - global_attrs_repoExampleDoc - global_attrs_web_no_paren")
print()
print(sorted(list(global_attrs_jsonDoc - global_attrs_repoExampleDoc - global_attrs_web_no_paren)))
print()

print()
print(">>>>>>global_attrs_jsonDoc - global_attrs_repoExampleDoc - global_attrs_web_no_paren - global_attrs_eccoExclusive")
print()
print(sorted(list(global_attrs_jsonDoc - global_attrs_repoExampleDoc - global_attrs_web_no_paren - global_attrs_eccoExclusive)))
print()

print()
print()
print()
print("------------------------------------------------------------------------")
print("------------------------ web_no_paren subtractions ---------------------")
print("------------------------------------------------------------------------")
print()


print()
print(">>>>>>global_attrs_web_no_paren - global_attrs_repoExampleDoc - global_attrs_web_probablyNotRelevantToECCO")
print()
print(sorted(list(global_attrs_web_no_paren - global_attrs_repoExampleDoc - global_attrs_web_probablyNotRelevantToECCO)))
print()

print()
print(">>>>>>global_attrs_web_no_paren - global_attrs_jsonDoc - global_attrs_web_probablyNotRelevantToECCO")
print()
print(sorted(list(global_attrs_web_no_paren - global_attrs_jsonDoc - global_attrs_web_probablyNotRelevantToECCO)))
print()

print()
print(">>>>>>global_attrs_web_no_paren - global_attrs_jsonDoc - global_attrs_repoExampleDoc - global_attrs_web_probablyNotRelevantToECCO")
print()
print(sorted(list(global_attrs_web_no_paren - global_attrs_jsonDoc - global_attrs_repoExampleDoc - global_attrs_web_probablyNotRelevantToECCO)))
print()

print()
print()
print()
print("IGNORE↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓???????????")
print(">>>>>>global_attrs_web_no_paren - global_attrs_repoExampleDoc")
print(sorted(list(global_attrs_web_no_paren - global_attrs_repoExampleDoc)))
print()
print(">>>>>>global_attrs_web_no_paren - global_attrs_jsonDoc")
print(sorted(list(global_attrs_web_no_paren - global_attrs_jsonDoc)))
print()
print(">>>>>>global_attrs_web_no_paren - global_attrs_jsonDoc - global_attrs_repoExampleDoc")
print(sorted(list(global_attrs_web_no_paren - global_attrs_jsonDoc - global_attrs_repoExampleDoc)))
print("IGNORE↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑???????????")
print()


print()
print()
print()
print("-------------------------------------------------------------------------")
print("------------------------ eccoExclusive subtractions ---------------------")
print("-------------------------------------------------------------------------")
print()

print()
print(">>>>>>global_attrs_eccoExclusive - global_attrs_web_no_paren")
print(sorted(list(global_attrs_eccoExclusive - global_attrs_web_no_paren)))
print()
print(">>>>>>global_attrs_eccoExclusive - global_attrs_repoExampleDoc")
print(sorted(list(global_attrs_eccoExclusive - global_attrs_repoExampleDoc)))
print()
print(">>>>>>global_attrs_eccoExclusive - global_attrs_jsonDoc")
print(sorted(list(global_attrs_eccoExclusive - global_attrs_jsonDoc)))
print()




print()
print()
print()
print("------------------------------------")
print("------------------------------------")
print("------------------------------------")
print("------------------------------------")
print("------------------------------------")
print("non-global attributes")
print("------------------------------------")
print("------------------------------------")
print("------------------------------------")
print("------------------------------------")
print("------------------------------------")
print()
print()


print()
print("--------------------------------------------------------------------------")
print("------------------------ repoExampleDoc subtractions ---------------------")
print("--------------------------------------------------------------------------")
print()

print()
print(">>>>>>non_global_attrs_repoExampleDoc - non_global_attrs_jsonDoc")
print()
print(sorted(list(non_global_attrs_repoExampleDoc - non_global_attrs_jsonDoc)))
print()

print()
print(">>>>>>non_global_attrs_repoExampleDoc - non_global_attrs_jsonDoc - non_global_attrs_eccoExclusive")
print()
print(sorted(list(non_global_attrs_repoExampleDoc - non_global_attrs_jsonDoc - non_global_attrs_eccoExclusive)))
print()

print()
print(">>>>>>non_global_attrs_repoExampleDoc - non_global_attrs_web_no_paren")
print()
print(sorted(list(non_global_attrs_repoExampleDoc - non_global_attrs_web_no_paren)))
print()

print()
print(">>>>>>non_global_attrs_repoExampleDoc - non_global_attrs_web_no_paren - non_global_attrs_eccoExclusive")
print()
print(sorted(list(non_global_attrs_repoExampleDoc - non_global_attrs_web_no_paren - non_global_attrs_eccoExclusive)))
print()

print()
print(">>>>>>non_global_attrs_repoExampleDoc - non_global_attrs_jsonDoc  - non_global_attrs_web_no_paren")
print()
print(sorted(list(non_global_attrs_repoExampleDoc - non_global_attrs_jsonDoc  - non_global_attrs_web_no_paren)))
print()

print()
print(">>>>>>non_global_attrs_repoExampleDoc - non_global_attrs_jsonDoc  - non_global_attrs_web_no_paren - non_global_attrs_eccoExclusive")
print()
print(sorted(list(non_global_attrs_repoExampleDoc - non_global_attrs_jsonDoc  - non_global_attrs_web_no_paren - non_global_attrs_eccoExclusive)))
print()

print()
print()
print()
print("-------------------------------------------------------------------")
print("------------------------ jsonDoc subtractions ---------------------")
print("-------------------------------------------------------------------")
print()

print()
print(">>>>>>non_global_attrs_jsonDoc - non_global_attrs_repoExampleDoc")
print()
print(sorted(list(non_global_attrs_jsonDoc - non_global_attrs_repoExampleDoc)))
print()

print()
print(">>>>>>non_global_attrs_jsonDoc - non_global_attrs_repoExampleDoc - non_global_attrs_eccoExclusive")
print()
print(sorted(list(non_global_attrs_jsonDoc - non_global_attrs_repoExampleDoc - non_global_attrs_eccoExclusive)))
print()

print()
print(">>>>>>non_global_attrs_jsonDoc - non_global_attrs_web_no_paren")
print()
print(sorted(list(non_global_attrs_jsonDoc - non_global_attrs_web_no_paren)))
print()

print()
print(">>>>>>non_global_attrs_jsonDoc - non_global_attrs_web_no_paren - non_global_attrs_eccoExclusive")
print()
print(sorted(list(non_global_attrs_jsonDoc - non_global_attrs_web_no_paren - non_global_attrs_eccoExclusive)))
print()

print()
print(">>>>>>non_global_attrs_jsonDoc - non_global_attrs_repoExampleDoc - non_global_attrs_web_no_paren")
print()
print(sorted(list(non_global_attrs_jsonDoc - non_global_attrs_repoExampleDoc - non_global_attrs_web_no_paren)))
print()

print()
print(">>>>>>non_global_attrs_jsonDoc - non_global_attrs_repoExampleDoc - non_global_attrs_web_no_paren - non_global_attrs_eccoExclusive")
print()
print(sorted(list(non_global_attrs_jsonDoc - non_global_attrs_repoExampleDoc - non_global_attrs_web_no_paren - non_global_attrs_eccoExclusive)))
print()


print()
print()
print()
print("------------------------------------------------------------------------")
print("------------------------ web_no_paren subtractions ---------------------")
print("------------------------------------------------------------------------")
print()

print()
print(">>>>>>non_global_attrs_web_no_paren - non_global_attrs_repoExampleDoc")
print()
print(sorted(list(non_global_attrs_web_no_paren - non_global_attrs_repoExampleDoc)))
print()

print()
print(">>>>>>non_global_attrs_web_no_paren - non_global_attrs_jsonDoc")
print()
print(sorted(list(non_global_attrs_web_no_paren - non_global_attrs_jsonDoc)))
print()

print()
print(">>>>>>non_global_attrs_web_no_paren - non_global_attrs_jsonDoc - non_global_attrs_repoExampleDoc")
print()
print(sorted(list(non_global_attrs_web_no_paren - non_global_attrs_jsonDoc - non_global_attrs_repoExampleDoc)))
print()
print()
print()

print()
print("-------------------------------------------------------------------------")
print("------------------------ eccoExclusive subtractions ---------------------")
print("-------------------------------------------------------------------------")
print()

print()
print(">>>>>>non_global_attrs_eccoExclusive - non_global_attrs_web_no_paren")
print(sorted(list(non_global_attrs_eccoExclusive - non_global_attrs_web_no_paren)))
print()
print(">>>>>>non_global_attrs_eccoExclusive - non_global_attrs_repoExampleDoc")
print(sorted(list(non_global_attrs_eccoExclusive - non_global_attrs_repoExampleDoc)))
print()
print(">>>>>>non_global_attrs_eccoExclusive - non_global_attrs_jsonDoc")
print(sorted(list(non_global_attrs_eccoExclusive - non_global_attrs_jsonDoc)))
print()
print()
print()


print()
print()
print()
print("SUPPLEMENTARY INFO:")
print()
print()
print()
print()

print()
print()
print("NOTE: other than the attributes 'conventions' and 'sorted', the 'paren' set of attributes from the web is disjoint from that from the granules/repoExampleDoc (see the following '==' comparison):")
print()
print('sorted(list(global_attrs_web_paren - global_attrs_repoExampleDoc - global_attrs_web_probablyNotRelevantToECCO)) == sorted(list(global_attrs_web_paren - {\'conventions\', \'source\'}))')
print()
print(sorted(list(global_attrs_web_paren - global_attrs_repoExampleDoc - global_attrs_web_probablyNotRelevantToECCO)) == sorted(list(global_attrs_web_paren - {'conventions', 'source'})))
print()
print()

print()
print()
print()
print()
print("global attributes from the EARTHDATA website (before separating primary and secondary (ie surrounded by parentheses) names):")
print()
print(sorted(global_names))
print()
print()

print()
print()
print()
print()
print("global_attrs_web_no_paren:")
print()
print(sorted(list(global_attrs_web_no_paren)))
print()
print()

print()
print()
print()
print()
print("global_attrs_web_paren:")
print()
print(sorted(list(global_attrs_web_paren)))
print()
print()

print()
print()
print()
print()
print("global_attrs_web_probablyNotRelevantToECCO:")
print()
print(sorted(list(global_attrs_web_probablyNotRelevantToECCO)))
print()
print()

print()
print()
print()
print()
print("global_attrs_eccoExclusive:")
print()
print(sorted(list(global_attrs_eccoExclusive)))
print()

print()
print()
print()
print()
print("non_global_attrs_eccoExclusive:")
print()
print(sorted(list(non_global_attrs_eccoExclusive)))
print()


print()
print()
print()
print()
print()




