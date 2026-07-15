attr_file_path = "/Users/brucel/ecco/yip/ECCO-Dataset-Production/document_generator/z_scratch/from_web/attrs_from_web_global.txt"

with open(attr_file_path) as file:
    monster_table_line = file.readline()

global_non_global_split_string = "Appendix E"

global_non_global = monster_table_line.split(global_non_global_split_string)

#category_split_string = ">D."
#categories = monster_table_line.split(category_split_string)

# Note that there is one "erronious" row within one of the global data cells, containing the string "UMM-Var"  ; filter that out of the results below
row_split_string = "<tr><td>" 
name_split_string = "</td>"

global_table_rows = global_non_global[0].split(row_split_string)
# first element is all the stuff before the first row of interest
global_table_rows = global_table_rows[1:]
global_names = [row.split(name_split_string)[0] for row in global_table_rows]
erronious_name = "UMM-Var"
global_names.remove(erronious_name)

non_global_table_rows = global_non_global[1].split(row_split_string)
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

