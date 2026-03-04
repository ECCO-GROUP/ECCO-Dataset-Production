import xarray as xa
import utils as u
import subprocess
import re

def get_non_coord_vars(ds_grid : xa.Dataset)-> dict:
    """Returns a dictionary of non-coordinate variables of the Dataset"""
    non_coord_vars = {key: value for key, value in ds_grid.variables.items() if key not in ds_grid.coords}
    return non_coord_vars


def readVarAttr(varName: str, var : xa.Variable) -> dict:
    """
    Read a netCDF file and extract variables.

    Parameters:
    var (str): The variable name of the netCDF object.

    Returns:
    list: A list containing the dictionaries of the data variables, with their attributes of the netCDF file.
    """
    # Add a prefix to the start of each key name
    var.attrs = {varName + " " + (key[len(varName)+1:] if key.startswith(varName + " ") else key): value for key, value in var.attrs.items()}

    # Create a dictionary to store the variables
    d = dict()
    # Extract the variables
    d["Variable Name"] = varName
    d["Storage Type Definition"] = var.dtype
    d["Description"] = var.attrs.get(f'{varName} description', 'N/A')
    d["Units"] = var.attrs.get(f'{varName} units', 'N/A')
    d["Comment"] = var.attrs.get(f'{varName} comment', 'N/A')
    d["tag"] = str(var.dtype) + " "+ varName + " " + str(var.dims).replace("\'", "")
    for attr, value in var.attrs.items():
        d[attr] = value
        
    return d


def readAllVarAttrs(ds_grid : xa.Dataset) -> list:
    """
    Read a netCDF file and extract variables.

    Parameters:
    ds_grid (str): The Dataset object of the netCDF file.

    Returns:
    list: A list containing the dictionaries of the data variables, with their attributes of the netCDF file.
    """
    # create non-coordinate variables dictionary
    vars = get_non_coord_vars(ds_grid)
    # Create a dictionary to store the variables
    a = list()
    # Extract the variables
    for var in vars:
        #a.append(var) # name such EXFatemp, EXFewind, etc. do not think i will need it
        
        a.append(readVarAttr(var, vars[var]))
        
    return a

#-------------------------------------------------------------------------------------------------
# All of below were made for the grouping tasks in the native_sets.py, 1D.py, and latlong.py file
#-------------------------------------------------------------------------------------------------

def process_dict_items(data: dict, sanitation_func) -> list:
    """
    Process the items of a dictionary using a sanitation function.

    Parameters:
        data (dict): The dictionary to process.
        sanitation_func (function): The sanitation function to use.
    Returns:
        list: A list of the processed items.
    """
    results = []
    for key, value in data.items():
        sanitized_value = sanitation_func(value)
        results.append(sanitized_value)
    return results



def data_var_table(fieldName, da:dict, ds_name:str)->list:
    """
    Create a latex table of the data variable.
    Parameters: 
        fieldName (str): The name of the data variable.
        da (dict): The dictionary of the data variable.
        ds_name (str): The name of the dataset.
    Returns:
        list: A list containing the latex table of the data variable.
    """
    new_sani = u.sanitize(ds_name)
    # Create the latex table
    la = [
            r'\begin{longtable}{|p{0.1\textwidth}|p{0.35\textwidth}|p{0.45\textwidth}|p{0.1\textwidth}|}',
            fr"\caption{{CDL example description of {new_sani}\'s {u.sanitize(fieldName)} variable}}",
            fr'\label{{tab:table-{ds_name}_{fieldName}}} \\',
            r'\hline \endhead \hline \endfoot',
    ]
    daName = u.sanitize(fieldName)

    # Obtain the important attributes
    storageType = u.sanitize(str(da["Storage Type Definition"]))
    varName = u.sanitize(da["Variable Name"])
    description = u.sanitize(da["Description"])
    unit = u.sanitize(da["Units"])
    comment = u.sanitize(da["Comment"])

    # Treat 'Example CDL Description' as a string
    cdl_description = u.sanitize_with_math(da['Example CDL Description']) # might have math
    cdl_description = cdl_description.replace(r'\\', '\'')
    cdl_description = cdl_description.replace('\n', '\\\\\n')


    # Finally create the latex line

    la.append(r'\rowcolor{lightgray} \textbf{Storage Type} & \textbf{Variable Name} & \textbf{Description} & \textbf{Unit} \\ \hline')
    la.append(rf'{storageType} & {varName} & {description} & {unit} \\ \hline')
    la.append(r'\rowcolor{lightgray}  \multicolumn{4}{|p{1.08\textwidth}|}{\textbf{CDL Description}} \\ \hline')
    la.append(r'\multicolumn{4}{|p{1.08\textwidth}|}' +r'{\makecell{\parbox{1.08\textwidth}'+ rf'{{{cdl_description}}}' + r'}} \\ \hline')
    la.append(r'\rowcolor{lightgray} \multicolumn{4}{|p{1.08\textwidth}|}{\textbf{Comments}} \\ \hline')
    la.append(r'\multicolumn{4}{|p{1.08\textwidth}|}' + rf'{{{comment}}}' + r' \\ \hline')
    la.append(r'\end{longtable}')
    la.append(r"")

    
    return la







def compute_ds_dict(varName: str, var : xa.DataArray) -> dict:
    """
    Read a netCDF imported xArray DataArray and extract variables and their attributes.

    Parameters:
    var (str): The variable name of the netCDF DataArray.

    Returns:
    dict: A dict containing the dictionaries of the data variables, with their attributes of the netCDF file.
    """
    # Add a prefix to the start of each key name
    #var.attrs = {varName + " " + (key[len(varName)+1:] if key.startswith(varName + " ") else key): value for key, value in var.attrs.items()}

    # Create a dictionary to store the variables
    d = dict()
    attrDict = dict()
    attrDict["tag"] = str(var.dtype) + " "+ varName + " " + str(var.dims).replace("\'", "") + ' ;'
    for attr, value in var.attrs.items():
        #attrDict[attr] = varName + ':' + attr + ' = ' + value
        formatted_value = f'"{value}"' if isinstance(value, str) else value
        attrDict[attr] = varName + ':' + attr + ' = ' + str(formatted_value) + ' ;'


    # Extract the variables
    d["Variable Name"] = varName
    d["Storage Type Definition"] = str(var.dtype)
    d["Description"] = var.attrs.get('long_name', 'N/A')
    d["Units"] = var.attrs.get(f'units', 'N/A')
    d['Example CDL Description'] = attrDict
    d["Comment"] = var.attrs.get(f'comment', 'N/A')
    return d



def read_data_vars(ds : xa.Dataset) -> dict:
    """
    Reads an xArray Dataset and produces a dict of the data variables / attributes from each DataArray in the Dataset.

    Parameters:
    ds (xarray.Dataset): The dataset name of the netCDF object.

    Returns:
    dict: A dict containing the dictionaries of the data variables, with their attributes of the netCDF file.
    """
    ds_dict = dict()
    for vars in ds.data_vars:
        varsStr = str(vars)
        ds_dict[vars] = compute_ds_dict(varsStr, ds.data_vars[varsStr])

    return ds_dict




#----------------------------------------------------------------------------------------
# Generate example CDL description
#----------------------------------------------------------------------------------------
def generate_CDL(original_nc_path : str, new_nc_path : str) -> str:
    """
    Generate a CDL description of a netCDF file.
    """
    # # Define the paths to your original and new netCDF files.
    # original_nc_path = 'granule_datasets/natives/OCEAN_TEMPERATURE_SALINITY_day_mean_2017-12-29_ECCO_V4r4_native_llc0090.nc'
    # new_nc_path = 'granule_datasets/examples/example_native.nc'

    # Run the nccopy command.
    nccopy_command = f'nccopy {original_nc_path} {new_nc_path}'
    subprocess.run(nccopy_command, shell=True, check=True)

    # Run the ncdump command and capture its output.
    ncdump_command = f'ncdump -h {new_nc_path}'
    ncdump_result = subprocess.run(ncdump_command, shell=True, check=True, capture_output=True, text=True)

    return ncdump_result.stdout



def cdl_to_latex(cdl_string, name : str = "example"):
    lines = cdl_string.split('\n')

    latex_lines = [r'\begin{longtable}{|p{\textwidth}|}', 
                    r'\caption{Example CDL description of ' +name+ r' dataset}',
                    r'\label{tab:cdl-'+name+r'} \\', 
                    r'\hline \endhead',
                    r'\hline \endfoot',

    ]
    variables_start = False
    dimensions_start = False
    independent_vars = []

    for line in lines:
        new_line = u.sanitize_with_math(line)
        if line.startswith('netcdf'):
            latex_lines.append(new_line + r'\\')
            continue
        if "dimensions:" in line:
            dimensions_start = True
            latex_lines.append(new_line + r'\\')
            latex_lines.append(r'\hline')
            #latex_lines.append(r'\rowcolor{YellowGreen}')
            continue
        elif "variables:" in line:
            dimensions_start = False
            variables_start = True
            latex_lines.append(r'\hline')
            latex_lines.append(new_line + r'\\')
            latex_lines.append(r'\hline')
            #latex_lines.append(r'\rowcolor{Apricot}')
            continue

        if dimensions_start:
                latex_lines.append(r'\rowcolor{YellowGreen}' + new_line + r'\\')
                independent_vars.append(u.get_substring(new_line))
        elif variables_start:
                data_var = u.get_substring(line)
                if "\t\t" not in line and data_var + "(" + data_var + ")" not in line:
                    variables_start = False
                    latex_lines.append(r'\hline')
                    latex_lines.append(new_line + r'\\')
                else:
                    latex_lines.append(r'\rowcolor{Apricot}' + new_line + r'\\')
        else:
            latex_lines.append(new_line + r'\\')

    latex_lines.append(r'\hline')
    latex_lines.append(r'\end{longtable}')
    return latex_lines
