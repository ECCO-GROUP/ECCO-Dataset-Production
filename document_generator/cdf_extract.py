import os
import xarray as xr
import subprocess
import utils

## ----------------------------------------------------------------------------
## ---------------------- Extracting CDL For Examples -------------------------
## ----------------------------------------------------------------------------
def fieldTable(ds:xr.Dataset, is_coord:bool)->list[str]:

    dataset = get_product_name(ds)
    fields = fields_in_ds(ds, is_coord)
    ll = []
    ll.append(r'\begin{longtable}{|p{0.1\textwidth}|p{0.5\textwidth}|}')
    # make a table that displays the fields in the dataset
    ll.append(fr'\caption{{Variables in the dataset {utils.sanitize(dataset)}}}')
    ll.append(fr'\label{{tab:table-{dataset}-fields}} \\ ')
    ll.append(r'\hline \endhead \hline \endfoot')

    
    ll.append(r'\rowcolor{lightgray} \textbf{Dataset:} & \textbf{'+f'{utils.sanitize(dataset)}'+r'} \\ \hline')
    for field in fields:
        ll.append(r'Field: &' + f'{utils.sanitize(field)}'+r' \\ \hline')

    ll.append(r'\end{longtable}')
    ll.append(r"")
    return ll




def formatList(cdl:list[str], name : str = "example")->list[str]:

    latex_lines = [r'\begin{longtable}{|p{\textwidth}|}', 
                    r'\caption{Example CDL description of ' +name+ r' dataset}',
                    r'\label{tab:cdl-'+name+r'} \\', 
                    r'\hline \endhead',
                    r'\hline \endfoot',

    ]
 
    dimensions_start = False
    coordinates_start = False
    variables_start = False
    independent_vars = []
    for line in cdl:
        new_line = utils.sanitize_with_math(line)
        if line.startswith('netcdf'):
            latex_lines.append(new_line + r'\\')
            continue
        if not dimensions_start and not coordinates_start and not variables_start and "dimensions" in line:
            dimensions_start = True
            latex_lines.append(new_line + r'\\')
            latex_lines.append(r'\hline')
            #latex_lines.append(r'\rowcolor{YellowGreen}')
            continue
        elif dimensions_start and "coordinates" in line:
            dimensions_start = False
            coordinates_start = True
            latex_lines.append(r'\hline')
            latex_lines.append(new_line + r'\\')
            latex_lines.append(r'\hline')
            #latex_lines.append(r'\rowcolor{YellowGreen}')
            continue
        elif coordinates_start and "data variables" in line:
            coordinates_start = False
            variables_start = True
            latex_lines.append(r'\hline')
            latex_lines.append(new_line + r'\\')
            latex_lines.append(r'\hline')
            #latex_lines.append(r'\rowcolor{Apricot}')
            continue

        if dimensions_start:
                latex_lines.append(r'\rowcolor{YellowGreen}' + new_line + r'\\')
                independent_vars.append(utils.get_substring(new_line))
        elif coordinates_start:
                data_var = utils.get_substring(line)
                latex_lines.append(r'\rowcolor{Apricot}' + new_line + r'\\')
                # if "\t\t" not in line and data_var + "(" + data_var + ")" not in line:
                #     variables_start = False
                #     latex_lines.append(r'\hline')
                #     latex_lines.append(new_line + r'\\')
                # else:
                #     latex_lines.append(r'\rowcolor{Apricot}' + new_line + r'\\')
        else:
            latex_lines.append(new_line + r'\\')

    latex_lines.append(r'\hline')
    latex_lines.append(r'\end{longtable}')
    return latex_lines



def latex_example_netcdf(fileType)->list[str]:
    if fileType == 'native':
        file = 'granule_datasets/natives/OCEAN_3D_SALINITY_FLUX_day_mean_2017-12-29_ECCO_V4r4_native_llc0090.nc'
    elif fileType == 'latlon':
        file = 'granule_datasets/latlon/OCEAN_AND_ICE_SURFACE_HEAT_FLUX_day_mean_2017-12-29_ECCO_V4r4_latlon_0p50deg.nc'
    else:
        file = 'granule_datasets/oneD/GLOBAL_MEAN_ATM_SURFACE_PRES_snap_ECCO_V4r4_1D.nc'

    ds = xr.open_dataset(file, decode_times=False, decode_cf=False, decode_coords=False, decode_timedelta=False)
    ll = []
    ll.append(f'netcdf {fileType} example')
    ll.append('dimensions')
    for d in ds.dims:
        ll.append(f'  {d} = {len(ds[d])}')
    ll.append('\ncoordinates')
    for c in ds.coords:
        c = ds[c]
        c_dt = str(c.dtype)
        c_dims = ', '.join([str(x) for x in c.dims])

        ll.append(f'\t{c_dt} {c.name} ({c_dims})')
        for c_attr in c.attrs:
            ll.append(f'\t\t{c.name}:{c_attr} = "{c.attrs[c_attr]}"')


    # separate extra coordinates and data variables
    coords = [] # list of coordinates
    data_vars = [] # list of data variables
    for dv in ds.data_vars:
        if ds[dv].attrs['coverage_content_type'] == 'coordinate':
            coords.append(ds[dv])
        else:
            data_vars.append(ds[dv])


    for coord in coords:
        c_dt = str(coord.dtype)
        c_dims = ', '.join([str(x) for x in coord.dims])

        ll.append(f'\t{c_dt} {coord.name} ({c_dims})')
        for c_attr in coord.attrs:
            ll.append(f'\t\t{coord.name}:{c_attr} = "{coord.attrs[c_attr]}"')

    ll.append('\ndata variables')
    for dv in data_vars:
        dv_dt = str(dv.dtype)
        dv_dims = ', '.join([str(x) for x in dv.dims])

        ll.append(f'\t{dv_dt} {dv.name} ({dv_dims})')
        for dv_attr in dv.attrs:
            ll.append(f'\t\t{dv.name}:{dv_attr} = "{dv.attrs[dv_attr]}"')

    
    return formatList(ll, fileType)



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
    ds = xr.open_dataset(filename, decode_times=False, decode_coords=False, decode_cf=False, decode_timedelta=False)
    non_coordinate = []
    for var in ds.data_vars:
        if ds[var].attrs['coverage_content_type'] != 'coordinate':
            non_coordinate.append(var)
    non_coordinate = sorted(non_coordinate)
    fields = [ds[field] for field in non_coordinate]
    return fields

def get_coordinate_vars(filename:str)->list[xr.DataArray]:
    """
        Returns a list of the non-coordinate variables in the given NetCDF file.
        Parameters:
            filename (str): The path to the NetCDF file.
        Returns:
            list[xr.DataArray]: A list of the non-coordinate variables in the given NetCDF file.
    """
    ds = xr.open_dataset(filename, decode_times=False, decode_coords=False, decode_cf=False, decode_timedelta=False)
    coordinate = []

    ds_type = 'native' if 'native' in ds.attrs['product_name'] else 'latlon'

    if ds_type == 'native':
        for var in ds.data_vars:
            var = ds[var]
            if 'tile' in var.dims and len(var.dims) > 2 and 'bnds' not in var.name:
                coordinate.append(var)
    else:
        for var in ds.data_vars:
            var = ds[var]
            if len(var.dims) > 2:
                coordinate.append(var)
    #coordinate = sorted(coordinate)
    fields = [ds[field.name] for field in coordinate]
    return fields




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
    temp = {name+'-'+k:v for (k,v) in field.attrs.items() if k != 'comment'}
    stringTemp = str(temp)
    stringTemp = stringTemp.replace('{','')
    stringTemp = stringTemp.replace('}','')
    stringTemp = stringTemp.replace("'",'')
    stringTemp = stringTemp.replace(',','\n')
    stringTemp = stringTemp.replace('\n ','\n')
    stringTemp = stringTemp.replace(':',' =')
    stringTemp = stringTemp.replace('-',': ')
    
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





def search_and_extract(substring:str, directory:str="granule_datasets/natives/", get_coords:bool=False)->tuple[list[xr.DataArray], xr.Dataset]:
    """
    Searches for a NetCDF file in the given directory that contains the specified substring
    in its name, and extracts information from it.

    Parameters:
        substring (str): The substring to search for in the file names.
        directory (str): The directory in which to search for the file.

    Returns:
        list: A list of xr.DataArrays, each a field in the dataset.
        #list: A list of dictionaries, each containing information about one variable in the dataset.
        xr.Dataset: The dataset itself.
    """

    for root, dirs, files in os.walk(directory):
        for file in files:
            if substring in file and file.endswith(".nc"):
                filepath = os.path.join(root, file)
                if not get_coords:
                    fields = get_non_coordinate_vars(filepath)
                else:
                    fields = get_coordinate_vars(filepath)
                ds = xr.open_dataset(filepath) # might DELETE THIS LINE
                return fields, ds

    raise ValueError(f"No NetCDF file containing '{substring}' found in directory '{directory}'")




def data_var_table(field_name:str, attrs:dict, ds_name:str)->list[str]:
    """
    Create a latex table of the data variable.
    Parameters: 
        fieldName (str): The name of the data variable.
        da (dict): The dictionary of the data variable.
        ds_name (str): The name of the dataset.
    Returns:
        list: A list containing the latex table of the data variable.
    """
    new_sani = utils.sanitize(ds_name)
    # Create the latex table
    la = [
            # ADJUST SIZE OF TABLE HERE
            # r'\begin{longtable}{|p{0.1\textwidth}|p{0.35\textwidth}|p{0.45\textwidth}|p{0.1\textwidth}|}',
            # r'\begin{longtable}{|p{0.06\textwidth}|p{0.41\textwidth}|p{0.39\textwidth}|p{0.06\textwidth}|}',
            r'\begin{longtable}{|m{0.06\textwidth}|m{0.41\textwidth}|m{0.39\textwidth}|m{0.06\textwidth}|}',
            fr"\caption{{CDL description of {new_sani}'s {utils.sanitize(field_name)} variable}}",
            fr'\label{{tab:table-{ds_name}_{field_name}}} \\ ',
            r'\hline \endhead \hline \endfoot',
        ]
    #daName = utils.sanitize(field_name)

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
    cdl_description = cdl_description.replace('    ', r'\hspace*{0.5cm}')


    # Finally create the latex line
    la.append(r'\rowcolor{lightgray} \textbf{Storage Type} & \textbf{Variable Name} & \textbf{Description} & \textbf{Unit} \\ \hline')
    la.append(rf'{storageType} & {varName} & {description} & {unit} \\ \hline')
    la.append(r'\rowcolor{lightgray}  \multicolumn{4}{|p{1.00\textwidth}|}{\textbf{CDL Description}} \\ \hline')
    la.append(r'\multicolumn{4}{|p{1.00\textwidth}|}' +r'{\makecell{\parbox{1\textwidth}'+ rf'{{{cdl_description}}}' + r'}} \\ \hline')
    la.append(r'\rowcolor{lightgray} \multicolumn{4}{|p{1.00\textwidth}|}{\textbf{Comments}} \\ \hline')
    la.append(r'\multicolumn{4}{|p{1\textwidth}|}' + rf'{{{comment}}}' + r' \\ \hline')
    la.append(r'\end{longtable}')
    la.append(r"")

    # 1.08 or 0.92
    return la

##############################################################################################################
########################################### Helper Functions #################################################
##############################################################################################################

def get_product_name(ds:xr.Dataset)->str:
    """
    Returns the product name of the dataset.
    Parameters:
        ds (xarray.Dataset): The dataset to extract the product name from.
    Returns:
        str: The product name of the dataset.
    """
    # find the product name, i.e. the first part of product_name attribute which is all capital letters
    h = ds.attrs['product_name'].split('_')
    product_name = ''
    for i in h:
        if i.isupper():
            product_name += i + '_'
        else:
            product_name = product_name[:-1]
            break
    return product_name

def fields_in_ds(ds:xr.Dataset, is_coord:bool)->list[str]:
    """
    Returns a list of all the fields in the dataset.
    Parameters:
        ds (xarray.Dataset): The dataset to extract the fields from.
    Returns:
        list[str]: A list of all the fields in the dataset.
    """
    # find all the fields in the dataset
    fields = []
    ds_type = ''
    if 'native' in ds.attrs['product_name']:
        ds_type = 'native' 
    elif 'latlon' in ds.attrs['product_name']:
        ds_type = 'latlon'

    if is_coord:
        if ds_type == 'native':
            for coord in ds.coords:
                coord = ds.coords[coord]
                if 'tile' in coord.dims and len(coord.dims) > 2 and 'bnds' not in coord.name:
                    fields.append(coord.name)
            for coord in ds.data_vars:
                coord = ds.data_vars[coord]
                if 'tile' in coord.dims and len(coord.dims) > 2 and 'bnds' not in coord.name:
                    fields.append(coord.name)
        elif ds_type == 'latlon':
            for coord in ds.coords:
                coord = ds.coords[coord]
                if len(coord.dims) > 2:
                    fields.append(coord.name)
            for coord in ds.data_vars:
                coord = ds.data_vars[coord]
                if len(coord.dims) > 2:
                    fields.append(coord.name)
    else: 
        for i in ds.data_vars:
            fields.append(i)
    # else:

    return fields
