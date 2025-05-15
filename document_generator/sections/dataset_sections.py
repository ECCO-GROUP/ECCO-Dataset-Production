#import cdf_reader as cr
import utils as s
import json
import cdf_extract
# import cdf_plotter
# import sys
# sys.path.append(r'../images/Figure_for_PODAAC')
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
    
    l = []

    # Load the JSON data
    with open(filePath, 'r') as json_file:
        data = json.load(json_file)


    #coord_list = create_coord_section(data, filePath, directory, imageDirectory, section)
    # Iterate through the JSON objects
    for item in data:
        filename = item["filename"]
        netCDF_ds = s.sanitize(filename)
        # l.append(r'\pagebreak') # Page break -- added ## <= is this utils? => yes, but I remove it to have continious paging
        # l.append(r'\subsection{'+ f'{section}' + ' NetCDF '+ f'{netCDF_ds}' + r'}')
        if "coordinates" in section:
            complementText = " "
        else:
            complementText = ' dataset of '
        # new page breaker addded here on May 14
        # l.append(r'\pagebreak')
        l.append(r'\subsection{'+ f'{section}' + complementText + f'{netCDF_ds}' + r'}')
        # l.append(r'\par\vspace{0.5cm}') # activated!! 
        l.append(r'\newp') # Deasctived!!
        
        fields, ds = cdf_extract.search_and_extract(filename, directory, is_coord)

        # l.append(r'\subsubsection{Overview of '+ f'{netCDF_ds}' + r' dataset content}')
        l.append(r'\subsubsection{Overview}')
        if "comment" in item.keys():
            summary_content = item["Introduction"]+' '+s.sanitize(item["comment"])+" "
        else:
            summary_content = item["Introduction"]+" "
        l.append(summary_content)
        # insert table function for each field in ds here ! 
        l.extend(cdf_extract.fieldTable(ds, is_coord)) #<== Modified!!! in order to remove the table that contain a list of variable per dataset.
        l.append(r'\newp') # Deasctived!!
        for field in fields:
            attrs = cdf_extract.extract_field_info(field)
            #l.extend(newLines)

            # Create latex table for each variable
            fieldName = attrs['Variable Name']
            cleanName = s.sanitize(fieldName)
            l.append(r'\pagebreak') # Page break -- added ## <= is this utils? => yes, but I remove it to have continious fluent paging
            l.append(fr'\subsubsection{{{section} Variable: {cleanName}}}')
            dataVarTable = cdf_extract.data_var_table(fieldName, attrs, filename)
            l.extend(dataVarTable)    

            # Create latex plot for each variable
            dataVarPlot = cdf_plotter.data_var_plot(ds, ds[fieldName], imageDirectory, True, is_coord)
            l.append(r'\begin{figure}[H]')
            l.append(r'\centering')
            l.append(dataVarPlot) #testing right here
            # l.append(fr"\caption{{\\Dataset: {s.sanitize(filename)}\\Variable: {s.sanitize(fieldName)}}}") #Just 
            l.append(fr"\caption{{Dataset: {s.sanitize(filename)}, Variable: {s.sanitize(fieldName)}}}") #Just 
            l.append(fr'\label{{tab:table-{filename}_{fieldName}-Plot}}')
            l.append(r'\end{figure}')
            l.append(r'\newpage')
        # if is_coord:
        #     break

    return l


############################################################################################################
#                                   Helper functions                
############################################################################################################
# def create_coord_section(data, filePath:str, directory:str, imageDirectory:str, section:str)->list[str]:
#     # Iterate through the JSON objects
#     for item in data:
#         filename = item["filename"]
#         netCDF_ds = s.sanitize(filename)
#         l.append(r'\pagebreak') # Page break -- added
#         l.append(r'\subsection{'+ f'{section}' + ' NetCDF '+ f'{netCDF_ds}' + r'}')
#         #l.append(r'\par\vspace{0.5cm}')
#         l.append(r'\newp')
        
#         fields, ds = cdf_extract.search_and_extract(filename, directory)
#         # insert table function for each field in ds here ! 
#         l.extend(cdf_extract.fieldTable(ds))
#         for field in fields:
#             attrs = cdf_extract.extract_field_info(field)
#             #l.extend(newLines)

#             # Create latex table for each variable
#             fieldName = attrs['Variable Name']
#             cleanName = s.sanitize(fieldName)
#             l.append(r'\pagebreak') # Page break -- added 
#             l.append(fr'\subsubsection{{{section} Variable {cleanName}}}')
#             dataVarTable = cdf_extract.data_var_table(fieldName, attrs, filename)
#             l.extend(dataVarTable)    

#             # Create latex plot for each variable
#             dataVarPlot = cdf_plotter.data_var_plot(ds, ds[fieldName], imageDirectory)
#             l.append(r'\begin{figure}[H]')
#             l.append(r'\centering')
#             l.append(dataVarPlot) #testing right here
#             l.append(fr"\caption{{\\Dataset: {s.sanitize(filename)}\\Variable: {s.sanitize(fieldName)}}}") #Just 
#             l.append(fr'\label{{tab:table-{filename}_{fieldName}-Plot}}')
#             l.append(r'\end{figure}')
#     return l