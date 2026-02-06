# BL: SEE COMMENT AROUND LINE 46
import utils
import json
import cdf_extract
import images.Figure_for_PODAAC.cdf_plotter_ojh as cdf_plotter

def data_products(filePath:str, directory:str, imageDirectory:str, section:str='native')->list:
    """

    Generates a list of LaTeX lines for the Data Products section of the report.
    Parameters:
        filePath (str): The path to the JSON file containing the data products.
        directory (str): The directory in which to search for the NetCDF files.
        imageDirectory (str): The directory in which to search for the images.
        section (str): The section of the report to generate.
            accepted values: "Native", "Latlon", "1D" , default="natives"
    Returns:
        list: A list of LaTeX lines for the Data Products section of the report.

    """
    is_coord = False
    if 'Coordinate' in section:
        is_coord = True
    if section != '1D':
        section = section.capitalize()

    latex_lines = []

    # Load the JSON data
    with open(filePath, 'r') as json_file:
        list_of_json_dicts = json.load(json_file)

    # Iterate through the JSON objects
    for item in list_of_json_dicts:
        filename = item["filename"]
        filename_formatted = utils.sanitize(filename)
        if "coordinates" in section:
            complementText = " "
        else:
            complementText = ' dataset of '
        latex_lines.append(r'\subsection{'+ f'{section}' + complementText + f'{filename_formatted}' + r'}')
        latex_lines.append(r'\newp') # Deasctived!!

        data_array_list, dataset = cdf_extract.search_and_extract(filename, directory, is_coord)

        latex_lines.append(r'\subsubsection{Overview}')
# BL: HERE'S WHERE THE SMASHING OF INTRO AND COMMENT IS HAPPENING
        if "comment" in item.keys():
            summary_content = item["Introduction"]+' '+utils.sanitize(item["comment"])+" "
        else:
            summary_content = item["Introduction"]+" "
        latex_lines.append(summary_content)
        latex_lines.extend(cdf_extract.fieldTable(dataset, is_coord)) #<== Modified!!! in order to remove the table that contain a list of variable per dataset.
        latex_lines.append(r'\newp') # Deasctived!!
        for field in data_array_list:
            attrs = cdf_extract.extract_field_info(field)

            # Create latex table for each variable
            fieldName = attrs['Variable Name']
            cleanName = utils.sanitize(fieldName)
            latex_lines.append(r'\pagebreak') # Page break -- added ## <= is this utils? => yes, but I remove it to have continious fluent paging
            latex_lines.append(fr'\subsubsection{{{section} Variable: {cleanName}}}')
            dataVarTable = cdf_extract.data_var_table(fieldName, attrs, filename)
            latex_lines.extend(dataVarTable)

            # Create latex plot for each variable
            dataVarPlot = cdf_plotter.data_var_plot(dataset, dataset[fieldName], imageDirectory, True, is_coord)
            latex_lines.append(r'\begin{figure}[H]')
            latex_lines.append(r'\centering')
            latex_lines.append(dataVarPlot) #testing right here
            latex_lines.append(fr"\caption{{Dataset: {utils.sanitize(filename)}, Variable: {utils.sanitize(fieldName)}}}") #Just
            latex_lines.append(fr'\label{{tab:table-{filename}_{fieldName}-Plot}}')
            latex_lines.append(r'\end{figure}')
            latex_lines.append(r'\newpage')

    return latex_lines

