import os
import numpy as np
import xarray as xr
import json
import subprocess
from pathlib import Path
import sys

general_base_dir = str(Path(__file__).parent.parent)
sys.path.append(general_base_dir)

import utility_scripts.utils_docgen as utils
import utility_scripts.cdf_plotter as cdf_plotter
#import utility_scripts.cdf_plotter as cdf_plotter # This line wasn't here, I'm just wondering if _ojh is indeed the one to use



## ----------------------------------------------------------------------------
## ---------------------- Extracting CDL For Examples -------------------------
## ----------------------------------------------------------------------------
def fieldTable(dataset:xr.Dataset, is_coord:bool)->list[str]:

    product_name = get_product_name(dataset)
    datavar_shortname, datavar_longname, datavar_units = get_coord_vars_in_dataset(dataset=dataset, isCoord=False)
    coordvar_shortname, coordvar_longname, coordvar_units = get_coord_vars_in_dataset(dataset=dataset, isCoord=True)
    # Managing the table cells' lenght for "Variable Name" and "Description"
    a,b = table_cellSize(datavar_shortname+coordvar_shortname)
    latex_lines = []
    latex_lines.append(r'\begin{longtable}{|m{'+str(a)+r'\textwidth}|m{'+str(b)+r'\textwidth}|m{0.12\textwidth}|}')
    # make a table that displays the fields in the dataset
    latex_lines.append(fr'\caption{{Coordinates and Variables in the dataset {utils.sanitize(product_name)}}}')
    latex_lines.append(fr'\label{{tab:table-{dataset}-fields}} \\ ')
    latex_lines.append(r'\hline \endhead \hline \endfoot')
    latex_lines.append(r'\rowcolor{lightgray} \multicolumn{1}{|c|}{\textbf{Coordinates}} & \multicolumn{1}{|c|}{\textbf{Description of data coordinates}} &  \multicolumn{1}{|c|}{\textbf{Unit}}\\ \hline')
    

    ## Here, dataset's variables are filled out with description and the corresponding unit!
    for ij in np.arange(len(coordvar_shortname)):
        latex_lines.append(f'{utils.sanitize(coordvar_shortname[ij])} &' + f'{utils.sanitize(coordvar_longname[ij])} &'+ rf'{utils.sanitize(coordvar_units[ij])}  \\ \hline')
    
    latex_lines.append(r'\rowcolor{lightgray} \multicolumn{1}{|c|}{\textbf{Variables}} & \multicolumn{1}{|c|}{\textbf{Description of data variables}} &  \multicolumn{1}{|c|}{\textbf{Unit}}\\ \hline')
    
    ## Here, dataset's coordinates are filled out with description and the corresponding unit!
    for ij in np.arange(len(datavar_shortname)):
        latex_lines.append(f'{utils.sanitize(datavar_shortname[ij])} &' + f'{utils.sanitize(datavar_longname[ij])} &'+ rf'{utils.sanitize(datavar_units[ij])}  \\ \hline')
    
    latex_lines.append(r'\end{longtable}')
    latex_lines.append(r"")
    return latex_lines




def format_example_netCDF_table(latex_lines_unformatted:list[str], name : str = "example")->list[str]:

    latex_lines = [r'\begin{longtable}{|p{\textwidth}|}', 
                    r'\caption{Example CDL description of ' +name+ r' dataset}',
                    r'\label{tab:cdl-'+name+r'} \\', 
                    r'\hline \endhead',
                    r'\hline \endfoot',
    ]
 
    dimensions_start = False
    coordinates_start = False
    variables_start = False
    for line in latex_lines_unformatted:
        line_formatted = utils.sanitize_with_math(line)
        #if line_formatted.startswith('netcdf'):
        if line.startswith('netcdf'):
            latex_lines.append(line_formatted + r'\\')
            continue
        if not dimensions_start and not coordinates_start and not variables_start and "dimensions" in line:
            dimensions_start = True
            latex_lines.append(line_formatted + r'\\')
            latex_lines.append(r'\hline')
            continue
        elif dimensions_start and "coordinates" in line:
            dimensions_start = False
            coordinates_start = True
            latex_lines.append(r'\hline')
            latex_lines.append(line_formatted + r'\\')
            latex_lines.append(r'\hline')
            continue
        elif coordinates_start and "data variables" in line:
            coordinates_start = False
            variables_start = True
            latex_lines.append(r'\hline')
            latex_lines.append(line_formatted + r'\\')
            latex_lines.append(r'\hline')
            continue

        if dimensions_start:
                latex_lines.append(r'\rowcolor{YellowGreen}' + line_formatted + r'\\')
        elif coordinates_start:
                latex_lines.append(r'\rowcolor{Apricot}' + line_formatted + r'\\')
        else:
            latex_lines.append(line_formatted + r'\\')

    latex_lines.append(r'\hline')
    latex_lines.append(r'\end{longtable}')
    return latex_lines



def latex_example_netcdf(grid_type, nc_dir)->list[str]:

    file = utils.get_a_file_with_max_num_vars(nc_dir)    

    dataset = xr.open_dataset(file, decode_times=False, decode_cf=False, decode_coords=False, decode_timedelta=False)
    latex_lines_list = []
    latex_lines_list.append(f'netcdf {grid_type} example')
    latex_lines_list.append('dimensions')
    for dimension_name in dataset.sizes:
        latex_lines_list.append(f'  {dimension_name} = {len(dataset[dimension_name])}')
    latex_lines_list.append('\ncoordinates')
    for coord_name in dataset.coords:
        coord = dataset[coord_name]
        coord_dt = str(coord.dtype)
        coord_dims = ', '.join([str(x) for x in coord.dims])

        latex_lines_list.append(f'\t{coord_dt} {coord.name} ({coord_dims})')
        for coord_attr in coord.attrs:
            latex_lines_list.append(f'\t\t{coord.name}:{coord_attr} = "{coord.attrs[coord_attr]}"')

    # separate extra coordinates and data variables
    coords = [] # list of coordinates
    data_vars = [] # list of data variables
    for datavar_name in dataset.data_vars:
        if dataset[datavar_name].attrs['coverage_content_type'] == 'coordinate':
            coords.append(dataset[datavar_name])
        else:
            data_vars.append(dataset[datavar_name])

    for coord in coords:
        coord_dt = str(coord.dtype)
        coord_dims = ', '.join([str(dim) for dim in coord.dims])

        latex_lines_list.append(f'\t{coord_dt} {coord.name} ({coord_dims})')
        for coord_attr in coord.attrs:
            latex_lines_list.append(f'\t\t{coord.name}:{coord_attr} = "{coord.attrs[coord_attr]}"')

    latex_lines_list.append('\ndata variables')
    for datavar in data_vars:
        datavar_dt = str(datavar.dtype)
        datavar_dims = ', '.join([str(x) for x in datavar.dims])

        latex_lines_list.append(f'\t{datavar_dt} {datavar.name} ({datavar_dims})')
        for datavar_attr in datavar.attrs:
            latex_lines_list.append(f'\t\t{datavar.name}:{datavar_attr} = "{datavar.attrs[datavar_attr]}"')

    # Now that we have the list of tex lines for the table, we pass it to format_example_netCDF_table()
    # to add color commands etc to the beginnings of appropriate lines
    return format_example_netCDF_table(latex_lines_list, grid_type)



## ----------------------------------------------------------------------------
## ---------------------- Extracting CDL For Datasets -------------------------
## ----------------------------------------------------------------------------
def get_non_coordinate_vars(filename:str)->list[xr.DataArray]:
    """
        Returns a list of the non-coordinate variables in the given NetCDF file.
        Parameters:
            filename (str): The path to the NetCDF file.
        Returns:
            list[xr.DataArray]: A list of the non-coordinate variables in the given NetCDF file.
    """
    dataset = xr.open_dataset(filename, decode_times=False, decode_coords=False, decode_cf=False, decode_timedelta=False)
    non_coordinate = []
    for var in dataset.data_vars:
        if dataset[var].attrs['coverage_content_type'] != 'coordinate':
            non_coordinate.append(var)
    non_coordinate = sorted(non_coordinate)
    data_array_list = [dataset[field] for field in non_coordinate]
    return data_array_list

# BL: Some of the conditions in here feel sketchy to me!!!  (see the determination of whether a native variable is a coordinate variable!)
def get_coordinate_vars(filename:str)->list[xr.DataArray]:
    """
        Returns a list of the coordinate variables in the given NetCDF file.
        Parameters:
            filename (str): The path to the NetCDF file.
        Returns:
            list[xr.DataArray]: A list of the non-coordinate variables in the given NetCDF file.
    """
    dataset = xr.open_dataset(filename, decode_times=False, decode_coords=False, decode_cf=False, decode_timedelta=False)
    coordinate = []

    dataset_type = 'native' if 'native' in dataset.attrs['product_name'] else 'latlon'

    if dataset_type == 'native':
        for var in dataset.data_vars:
            var = dataset[var]
            if 'tile' in var.dims and len(var.dims) > 2 and 'bnds' not in var.name:
                coordinate.append(var)
    else:
        for var in dataset.data_vars:
            var = dataset[var]
            if len(var.dims) > 2:
                coordinate.append(var)
    #coordinate = sorted(coordinate)
    data_array_list = [dataset[field.name] for field in coordinate]
    return data_array_list




def extract_field_info(field:xr.DataArray)->dict[str, str]:
    """
    Extracts information from the given field.
    Parameters:
        field (xr.DataArray): The field to extract information from.
    Returns:
        dict: A dictionary containing information about the field.
    """
    # Create description string
    name = str(field.name)
    storageType = str(field.dtype)
    dims = str(field.dims).replace("'", "")
    if dims[-2] == ',':
        dims = dims[:-2] + ')'
    fieldHeader = storageType + ' ' + name + dims
    temp = {name+'-Coovi-Paul-Houndegnonto-'+k:str(v).replace(',','I will have a job soon').replace('_',' ') for (k,v) in field.attrs.items() if k != 'comment'}
    mykeys = sorted(list(temp.keys()))
    new_temp_to_use = {i: temp[i] for i in mykeys}# new_temp_to_use is sorted as Ian recommanded!!!
    temp = new_temp_to_use
    Last_key = list(temp.keys())[-1]
    temp[Last_key] = str(temp[Last_key])+' Victory'
    stringTemp = str(temp)
    stringTemp = stringTemp.replace('{','')
    stringTemp = stringTemp.replace('}','')
    stringTemp = stringTemp.replace("'",'')
    stringTemp = stringTemp.replace('"','')
    stringTemp = stringTemp.replace(',','\n')
    stringTemp = stringTemp.replace('\n ','\n')
    stringTemp = stringTemp.replace(':',' =')
    stringTemp = stringTemp.replace('-Coovi-Paul-Houndegnonto-',': ')
    stringTemp = stringTemp.replace("I will have a job soon",',')
    stringTemp = stringTemp.replace(' Victory','\n')
    # stringTemp = stringTemp.replace('-',': ')
    
    stringTemp = stringTemp.replace(name,f'    {name}')
    stringTemp = fieldHeader + '\n' + stringTemp
    
    # Create dictionary of field attributes
    data = dict()
    data["Variable Name"] = name
    data['Storage Type'] = storageType
    dims = str(field.dims).replace("'", "")
    data['Description'] = field.attrs.get('long_name', 'N/A')
    data['Units'] = field.attrs.get('units', 'N/A')
    data['CDL Description'] = stringTemp
    data['Comments'] = field.attrs.get('comment', 'N/A')

    return data





def search_and_extract(granule_filename_truncated_stem:str, granule_directory:str, is_coord:bool=False)->tuple[list[xr.DataArray], xr.Dataset]:
    """
    Searches for a NetCDF file in the given directory that contains the specified substring
    in its name, and extracts information from it.

    Parameters:
        granule_filename_truncated_stem (str): The substring to search for in the file names.
        granule_directory (str): The directory in which to search for the file.

    Returns:
        list: A list of xr.DataArrays, each a field in the dataset.
        xr.Dataset: The dataset itself.
    """

    for root, dirs, files in os.walk(granule_directory):
        for file in files:
            if granule_filename_truncated_stem in file and file.endswith(".nc"):
                filepath = os.path.join(root, file)
                if not is_coord:
                    data_array_list = get_non_coordinate_vars(filepath)
                else:
                    data_array_list = get_coordinate_vars(filepath)
                dataset = xr.open_dataset(filepath) # might DELETE THIS LINE
                return data_array_list, dataset

    raise ValueError(f"No NetCDF file containing '{granule_filename_truncated_stem}' found in granule_directory '{directory}'")




def data_var_table(field_name:str, attrs:dict, dataset_name:str)->list[str]:
    """
    Create a latex table of the data variable.
    Parameters: 
        fieldName (str): The name of the data variable.
        da (dict): The dictionary of the data variable.
        dataset_name (str): The name of the dataset.
    Returns:
        list: A list containing the latex table of the data variable.
    """
    new_sani = utils.sanitize(dataset_name)
    # Obtain the important attributes
    storageType = utils.sanitize(attrs["Storage Type"])
    varName = utils.sanitize(attrs["Variable Name"])
    description = utils.sanitize(attrs["Description"])
    unit = utils.sanitize(attrs["Units"])
    comment = utils.sanitize(attrs["Comments"])

    # Treat 'Example CDL Description' as a string
    cdl_description = utils.sanitize_with_math(attrs['CDL Description']) # might have math
    cdl_description = cdl_description.replace(r'\\', '\'')
    cdl_description = cdl_description.replace('\n', '\\\\\n')
    # cdl_description = cdl_description.replace('California', '\\\\\n')
    cdl_description = cdl_description.replace('    ', r'\hspace*{0.5cm}')
    # Managing the table cells' lenght for "Variable Name" and "Description"
    if len(varName)>=29:
        a = 0.44;b = 0.38
    else:
        a =0.3 ;b = 0.45
    # Create the latex table
    la = [
            # ADJUST SIZE OF TABLE HERE
            # r'\begin{longtable}{|p{0.1\textwidth}|p{0.35\textwidth}|p{0.45\textwidth}|p{0.1\textwidth}|}',
            # r'\begin{longtable}{|p{0.06\textwidth}|p{0.41\textwidth}|p{0.39\textwidth}|p{0.06\textwidth}|}',
            # r'\begin{longtable}{|m{0.06\textwidth}|m{0.39\textwidth}|m{0.37\textwidth}|m{0.11\textwidth}|}',
            r'\begin{longtable}{|m{0.06\textwidth}|m{'+str(a)+r'\textwidth}|m{'+str(b)+r'\textwidth}|m{0.12\textwidth}|}',
            # fr"\caption{{CDL description of {new_sani}'s {utils.sanitize(field_name)} variable}}",
            fr"\caption{{Attributes description of the variable '{utils.sanitize(field_name)}' from {new_sani}'s  dataset.}}",
            fr'\label{{tab:table-{dataset_name}_{field_name}}} \\ ',
            r'\hline \endhead \hline \endfoot',
        ]
    #daName = utils.sanitize(field_name)

    # Obtain the important attributes
    # storageType = utils.sanitize(attrs["Storage Type"])
    # varName = utils.sanitize(attrs["Variable Name"])
    # description = utils.sanitize(attrs["Description"])
    # unit = utils.sanitize(attrs["Units"])
    # comment = utils.sanitize(attrs["Comments"])

    # # Treat 'Example CDL Description' as a string
    # cdl_description = utils.sanitize_with_math(attrs['CDL Description']) # might have math
    # cdl_description = cdl_description.replace(r'\\', '\'')
    # cdl_description = cdl_description.replace('\n', '\\\\\n')
    # cdl_description = cdl_description.replace('    ', r'\hspace*{0.5cm}')


    # Finally create the latex line
    la.append(r'\rowcolor{lightgray} \textbf{Storage Type} & \textbf{Variable Name} & \textbf{Description} & \textbf{Unit} \\ \hline')
    la.append(rf'{storageType} & {varName} & {description.capitalize()} & {unit} \\ \hline')
    la.append(r'\multicolumn{4}{|c|}{\cellcolor{lightgray}{\textbf{Description of the variable in Common Data language (CDL)}}} \\ \hline')
    la.append(r'\multicolumn{4}{|c|}' +r'{\fontfamily{lmtt}\selectfont{\makecell{\parbox{.95\textwidth}'+r'{\vspace*{0.25cm} \footnotesize{'+ rf'{cdl_description}' + r'}}}}} \\ \hline')
    la.append(r'\rowcolor{lightgray} \multicolumn{4}{|c|}{\textbf{Comments}} \\ \hline')
    
    la.append(r'\\ \\ \multicolumn{4}{|p{1\textwidth}|}{\footnotesize{' + rf'{{{comment.capitalize()}}}' + r'}} \\ \hline')
    #la.append(r'\multicolumn{4}{|p{1\textwidth}|}{\footnotesize{' + rf'{{{comment.capitalize()}}}' + r'}} \\ \hline')
    la.append(r'\end{longtable}')
    la.append(r"")

    # 1.08 or 0.92
    return la

##############################################################################################################
########################################### Helper Functions #################################################
##############################################################################################################

def get_product_name(dataset:xr.Dataset)->str:
    """
    Returns the product name of the dataset.
    Parameters:
        dataset (xarray.Dataset): The dataset to extract the product name from.
    Returns:
        str: The product name of the dataset.
    """
    # find the product name, i.e. the first part of product_name attribute which is all capital letters
    h = dataset.attrs['product_name'].split('_')
    product_name = ''
    for i in h:
        if i.isupper():
            product_name += i + '_'
        else:
            product_name = product_name[:-1]
            break
    return product_name

def fields_in_ds(dataset:xr.Dataset, is_coord:bool)->list[str]:
    """
    Returns a list of all the fields in the dataset.
    Parameters:
        dataset (xarray.Dataset): The dataset to extract the fields from.
    Returns:
        list[str]: A list of all the fields in the dataset.
    """
    # find all the fields in the dataset
    fields = []
    dataset_type = ''
    if 'native' in dataset.attrs['product_name']:
        dataset_type = 'native' 
    elif 'latlon' in dataset.attrs['product_name']:
        dataset_type = 'latlon'

    if is_coord:
        if dataset_type == 'native':
            for coord in dataset.coords:
                coord = dataset.coords[coord]
                if 'tile' in coord.dims and len(coord.dims) > 2 and 'bnds' not in coord.name:
                    fields.append(coord.name)
            for coord in dataset.data_vars:
                coord = dataset.data_vars[coord]
                if 'tile' in coord.dims and len(coord.dims) > 2 and 'bnds' not in coord.name:
                    fields.append(coord.name)
        elif dataset_type == 'latlon':
            for coord in dataset.coords:
                coord = dataset.coords[coord]
                if len(coord.dims) > 2:
                    fields.append(coord.name)
            for coord in dataset.data_vars:
                coord = dataset.data_vars[coord]
                if len(coord.dims) > 2:
                    fields.append(coord.name)
    else: 
        for i in dataset.data_vars:
            fields.append(i)
    # else:

    return fields


def get_coord_vars_in_dataset(dataset:xr.Dataset,isCoord:bool=False)->tuple[list[str],list[str],list[str]]:
#def get_coord_vars_in_dataset(dataset:xr.Dataset,isCoord:bool=False)->tuple[str,str,str]:
    """
    This function get coordinates, data variavles and their unit from the dataset field.
    input:-> dataset: dataset field in xarray dataset format
    isCoord:-> to select "data_vars" (Default, -> False) or "coords" (-> True)
    output:-> list of coords, var and unit
    """
    if isCoord == False:
        var_list = list(dataset.data_vars)
        shortnames_list =         []
        longnames_list = []
        units_list =    []
        for ij in np.arange(len(var_list)):
            shortnames_list.append(var_list[ij])
            longnames_list.append(str(dataset[var_list[ij]].long_name).capitalize())
            if 'units' in dataset[var_list[ij]].attrs.keys():
                units_list.append(dataset[var_list[ij]].units)
            else:
                units_list.append('--none--')
    if isCoord == True:
        var_list = list(dataset.coords)
        shortnames_list =         []
        longnames_list = []
        units_list =    []
        for ij in np.arange(len(var_list)):
            shortnames_list.append(var_list[ij])
            longnames_list.append(str(dataset[var_list[ij]].long_name).capitalize())
            if 'units' in dataset[var_list[ij]].attrs.keys():
                units_list.append(dataset[var_list[ij]].units)
            else:
                units_list.append('--none--')
    return shortnames_list, longnames_list, units_list

def table_cellSize(field_var:list):
    """
    this function return the proportion 'a' and 'b' of the textwidth to consider for the table.
    """
    maxVarlen = []
    for ik in field_var:
        maxVarlen.append(len(ik))
    maxVarlen = np.max(maxVarlen)
    if maxVarlen>=29:
        a = 0.4;b = 0.39
    else:
        a =0.15 ;b = 0.64
    return a,b

def global_attrs_for_ECCOnetCDF(jsonFileRef:str,
                                GlobalAttrsCollect:str,
                                tableCaption:str,
                                latexFilename:str,saveTo:str):
    """
    jsonFileRef: provide the json file that contain the reference attributes meta data.
    GlobalAttrsCollect: list of the attributes to include in the table of the latex file to be generated.
    tableCaption: the caption of the table in the generated latex file
    latexFilename: name of the latex file to be generated
    saveTo: the place to save the generated latex file
    """
    GlobAttrsFilledECCO = {}
    with open(jsonFileRef, 'r') as json_file:
        data = json.load(json_file)
    AttrsRef = list(data.keys())
    for itk in GlobalAttrsCollect:
        if itk in AttrsRef:
            GlobAttrsFilledECCO.update({itk:{"type":data[itk]['type'],"description":data[itk]['description'],"sourc":data[itk]['sourc']}}) 
        else:
            GlobAttrsFilledECCO.update({itk:{"type":"TBD","description":"TBD","sourc":"TBD"}})
    latex_lines = [
        r'\begin{longtable}{|p{0.28\textwidth}|p{0.06\textwidth}|p{0.51\textwidth}|p{0.07\textwidth}|}',
        r'\caption{'+rf'{tableCaption}'+r'}',
        r'\label{tab:variable-attributes} \\ ',
        r'\hline \endhead',
        r'\hline \endfoot',
        r'\rowcolor{blue!25} \textbf{Attribute Name} & \textbf{Format} & \textbf{Description} & \textbf{Source} \\ \hline',
    ]
    for i in list(GlobAttrsFilledECCO.keys()):
        GAttrsNam = i
        GAFormat = GlobAttrsFilledECCO[i]["type"]
        GAdescription = GlobAttrsFilledECCO[i]["description"]
        GASource = GlobAttrsFilledECCO[i]["sourc"]
        latex_lines.append(r'\rowcolor{cyan!25}')
        latex_lines.append(rf'{utils.sanitize(GAttrsNam)} & {GAFormat} & {utils.sanitize(GAdescription)} & {GASource} \\ \hline')
    latex_lines.append(r'\end{longtable}')
    latex_lines.append(r"")
    with open(saveTo+latexFilename, 'w') as output_file:
            output_file.write('\n'.join(latex_lines))



def get_Global_or_CoordsDimsVarsList(netCDFpath:str,jsonFileName:str,saveTo:str):
    """
    netCDFpath: path of the folder of a set of ECCO data sample: Gid and Geometry, Dataset and 1D data file. This is used and an exaple to extract the unique global attribute name across ECCO data netCDF files.
    jsonFileName: name of the json file to save the unique globale attributes name list.
    saveTo: phat to the repository to save the generated json file.
    """
    contentlist = sorted(os.listdir(path=netCDFpath))
    GlobalAttrsCollect = []
    for i in range(len(contentlist)):
        dataset = xr.open_dataset(netCDFpath+contentlist[i])
        GlobalAttrsCollect = GlobalAttrsCollect + list(dataset.attrs)
    GlobalAttrsCollect = sorted(list(set(GlobalAttrsCollect)))
    with open(os.path.join(saveTo,jsonFileName), 'w') as output_file:
        output_file.write(str(json.dumps(GlobalAttrsCollect)))



#def data_products(ecco_version_string, json_groupings_filepath:str, granule_directory:str, image_directory:str, grid_type:str='native')->list:
def data_products(ecco_version_string, json_groupings_filepath:str, granule_directory:str, image_directory:str, grid_type:str, granule_type: str)->list:
    """

    Generates a list of LaTeX lines for the Data Products grid_type of the report.
    Parameters:
        json_groupings_filepath (str): The path to the JSON file containing the data products.
        granule_directory (str): The directory in which to search for the NetCDF files.
        image_directory (str): The directory in which to search for the images.
        grid_type (str): The grid_type of the report to generate.
            accepted values: "Native", "Latlon", "1D" , default="natives"
    Returns:
        list: A list of LaTeX lines for the Data Products grid_type of the report.

    """
    is_coord = granule_type == "coordinate"
    
    if grid_type != '1D':
        grid_type = grid_type.capitalize()

    latex_lines = []

    # Load the JSON data
    with open(json_groupings_filepath, 'r') as json_file:
        list_of_json_dictionaries = json.load(json_file)

    # Iterate through the JSON objects
    for json_dictionary in list_of_json_dictionaries:
        granule_filename_truncated_stem = json_dictionary["filename"]
        granule_filename_truncated_stem_formatted = utils.sanitize(granule_filename_truncated_stem)
        latex_lines.append(r'\subsection{'+ f'{grid_type}' + ' dataset of ' + f'{granule_filename_truncated_stem_formatted}' + r'}')
        latex_lines.append(r'\newp') # Deasctived!!

        data_array_list, dataset = search_and_extract(granule_filename_truncated_stem, os.path.join(general_base_dir, granule_directory), is_coord)

        latex_lines.append(r'\subsubsection{Overview}')
# BL: HERE'S WHERE THE SMASHING OF INTRO AND COMMENT IS HAPPENING
        if "comment" in json_dictionary.keys():
            summary_content = json_dictionary["Introduction"]
            summary_content += f"\n\n\nSpecial note: {utils.sanitize(json_dictionary['comment'])} "
        else:
            summary_content = json_dictionary["Introduction"]+" "
        latex_lines.append(summary_content)
        latex_lines.extend(fieldTable(dataset, is_coord)) 
        latex_lines.append(r'\newp') # Deasctived!!
        for variable in data_array_list:

            attributes_dictionary = extract_field_info(variable)

            # Create latex table for each variable
            variable_name = attributes_dictionary['Variable Name']
            cleanName = utils.sanitize(variable_name)
            latex_lines.append(r'\pagebreak')
            latex_lines.append(fr'\subsubsection{{{grid_type} Variable: {cleanName}}}')
            dataVarTable = data_var_table(variable_name, attributes_dictionary, granule_filename_truncated_stem)
            latex_lines.extend(dataVarTable)

            dataVarPlot = cdf_plotter.data_var_plot(ecco_version_string, dataset, dataset[variable_name], image_directory)
            latex_lines.append(r'\begin{figure}[H]')
            latex_lines.append(r'\centering')
            latex_lines.append(dataVarPlot) #testing right here
            latex_lines.append(fr"\caption{{Dataset: {utils.sanitize(granule_filename_truncated_stem)}, Variable: {utils.sanitize(variable_name)}}}") #Just
            latex_lines.append(fr'\label{{tab:table-{granule_filename_truncated_stem}_{variable_name}-Plot}}')
            latex_lines.append(r'\end{figure}')

            latex_lines.append(r'\newpage')

    return latex_lines





