#import cdf_reader
import readJSON
import cdf_extract
import sections.dataset_sections as ds_s
import argparse

def write_data_attributes_tables():
    """
        This function writes the data product tables to the latex document.
    """
    global_lines = [
        r'% Table 8-1 Mandatory global attributes for GDS 2.0 netCDF data files',
        r'\begin{longtable}{|p{0.276\textwidth}|p{0.092\textwidth}|p{0.46\textwidth}|p{0.092\textwidth}|}',
        r'\caption{Mandatory global attributes for GDS 2.0 netCDF data files}',
        r'\label{tab:global-attributes} \\ ',
        r'\hline \endhead',
        r'\hline \endfoot',
        r'\rowcolor{lightgray} \textbf{Global Attribute Name} & \textbf{Type} & \textbf{Description} & \textbf{Source} \\ \hline',
    ]   
    global_vars = readJSON.obtain_json_data('data/global_attributes.json')
    global_lines.extend(readJSON.establish_table(global_vars))
    global_lines.append(r'\end{longtable}')
    with open('document/latex/data_product/global_attributes.tex', 'w') as output_file:
        output_file.write('\n'.join(global_lines)) 

    var_attr_lines = [
        r'% Table 8-2 Variable attributes for GDS 2.0 netCDF data files',
        r'\begin{longtable}{|p{0.168\textwidth}|p{0.20\textwidth}|p{0.46\textwidth}|p{0.092\textwidth}|}',
        r'\caption{Table 8-2. Variable attributes for GDS 2.0 netCDF data files}',
        r'\label{tab:variable-attributes} \\ ',
        r'\hline \endhead',
        r'\hline \endfoot',
        r'\rowcolor{lightgray} \textbf{Variable Attribute Name} & \textbf{Format} & \textbf{Description} & \textbf{Source} \\ \hline',
    ]
    variable_vars = readJSON.obtain_json_data('data/variable_attributes.json')
    var_attr_lines.extend(readJSON.establish_table(variable_vars))
    var_attr_lines.append(r'\end{longtable}')
    with open('document/latex/data_product/variable_attributes.tex', 'w') as output_file:
        output_file.write('\n'.join(var_attr_lines))


    example_native_lines = cdf_extract.latex_example_netcdf('native')
    with open('document/latex/data_product/example_native_table.tex', 'w') as output_file:
        output_file.write('\n'.join(example_native_lines))
    

    example_latlon_lines = cdf_extract.latex_example_netcdf('latlon')
    with open('document/latex/data_product/example_latlon_table.tex', 'w') as output_file:
        output_file.write('\n'.join(example_latlon_lines))

    example_1D_lines = cdf_extract.latex_example_netcdf('1D')
    with open('document/latex/data_product/example_oneD_table.tex', 'w') as output_file:
        output_file.write('\n'.join(example_1D_lines))








def write_datasets(dataset_type:str)->None:
    native_coords_groupings = 'granule_datasets/native_coords.json'
    native_groupings_json = 'granule_datasets/ECCOv4r4_groupings_for_native_datasets.json'
    latlon_coords_groupings = 'granule_datasets/latlon_coords.json'
    latlon_groupings_json = 'granule_datasets/ECCOv4r4_groupings_for_latlon_datasets.json'
    oneD_groupings_json = 'granule_datasets/ECCOv4r4_groupings_for_1D_datasets.json'

    native_coords_dir = 'granule_datasets/natives_coords/'
    native_ds_dir = 'granule_datasets/natives/'
    latlon_coords_dir = 'granule_datasets/latlon_coords/'
    latlon_ds_dir = 'granule_datasets/latlon/'
    oneD_ds_dir = 'granule_datasets/oneD/'

    native_coords_images_dir = 'images/plots/native_plots_coords/'
    native_images_dir = 'images/plots/native_plots/'
    latlon_coords_images_dir = 'images/plots/latlon_plots_coords/'
    latlon_images_dir = 'images/plots/latlon_plots/'
    oneD_images_dir = 'images/plots/oneD_plots/'

    if dataset_type == 'native':
        
        native_coord_ds_lines = ds_s.data_products(native_coords_groupings, native_coords_dir,
                                                   native_coords_images_dir, dataset_type + " Coordinates")
        with open('document/latex/dataset/native_coords_dataset_tables.tex', 'w') as output_file:
            output_file.write('\n'.join(native_coord_ds_lines))


        native_ds_lines = ds_s.data_products(native_groupings_json, native_ds_dir,
                                            native_images_dir, dataset_type)
        with open('document/latex/dataset/native_dataset_tables.tex', 'w') as output_file:
            output_file.write('\n'.join(native_ds_lines))
            #output_file.write('example of native dataset table\n')

    elif dataset_type == 'latlon':
        latlon_coord_ds_lines = ds_s.data_products(latlon_coords_groupings, latlon_coords_dir,
                                                   latlon_coords_images_dir, dataset_type) #+ " Coordinates"
        with open('document/latex/dataset/latlon_coords_dataset_tables.tex', 'w') as output_file:
            output_file.write('\n'.join(latlon_coord_ds_lines))



        latlon_ds_lines = ds_s.data_products(latlon_groupings_json, latlon_ds_dir,
                                            latlon_images_dir, dataset_type)
        with open('document/latex/dataset/latlon_dataset_tables.tex', 'w') as output_file:
            output_file.write('\n'.join(latlon_ds_lines))
            #output_file.write('example of latlon dataset table\n')

    elif dataset_type == '1D':
        oneD_ds_lines = ds_s.data_products(oneD_groupings_json, oneD_ds_dir,
                                        oneD_images_dir, dataset_type)
        with open('document/latex/dataset/oneD_dataset_tables.tex', 'w') as output_file:
            output_file.write('\n'.join(oneD_ds_lines))
    
    else:
        print(f"Invalid dataset type: {dataset_type}. Please select from 'Native', 'Latlon', '1D'.")


if __name__ == '__main__':
    """
        This script generates the LaTeX outline for the dataset.
    """
    parser = argparse.ArgumentParser(description='Write datasets to latex')
    parser.add_argument('--type', required=True, type=str,
                        help="Type of the dataset to write. Should be one of 'Native', 'Latlon', '1D'.")
    args = parser.parse_args()
    
    write_datasets(args.type)

# usage: main.py [-h] --type TYPE
# python sections/latex_outline.py --type 1D















# def generate_outline()->str:
#     latex_lines = []
#     latex_lines.extend(generate_first_page())

#     return '\n'.join(latex_lines)


# def generate_first_page() -> list:
#     latex_lines = [
#         r'\documentclass[letterpaper]{article}',

#         r'\input{latex/preamble.tex}',

#         r'\begin{document}',

#         r'\input{latex/front_pages.tex}',
#         r'\input{latex/doc_records.tex}',
#         r'\input{latex/ECCO_team.tex}',
#         r'\input{latex/executive_summary.tex}'
#         r'\input{latex/TOC.tex}',
#         r'\input{latex/LOF.tex}',
#         r'\input{latex/app_ref_docs.tex}',
#         r'\input{latex/acry_abbrev_list.tex}', 
#         r'\input{latex/document_conventions.tex}',
#         r'\input{latex/scope_content_doc.tex}', 
#         r'\input{latex/ECCO_overview.tex}',  
#         r'\input{latex/filenames_conventions}',
#         r'\input{latex/data_product/data_product_file_struct.tex}',

#         r'\input{latex/dataset/native_dataset.tex}',
#         r'\input{latex/dataset/latlon_dataset.tex}',
#         r'\input{latex/dataset/oneD_dataset.tex}',


#         r'\input{latex/metadata_specification.tex}',
#         r'\input{latex/doc_management_policies.tex}',
#         r'\input{latex/closing_statement.tex}',

#         r'\end{document}',
#     ]
#     return latex_lines



# def create_third_page()->list:
#     latex_lines = [
#         r'\pagebreak',
#         r"",
#         r'\begin{center}',
# 	    r'\vspace*{2cm}',

#  	    r'{\Large The Recommended',
#         r'GHRSST Data Specification (GDS)}\\[2cm]',
# 	    r'{\LARGE \textbf{GDS 2.0 Technical Specifications}}\\[2cm]'

# 	    r'{\large Compiled by\\'
#         r'the GHRSST International Science Team 2010,\\'
#         r'reviewed by DAS-TAG 2011.}\\[2cm]',

# 	    r'{\large Published by the International \\GHRSST Project Office\\',
#         r'Department of Meteorology,\\',
#         r'University of Reading,\\',
#         r'Reading\\',
#         r'United Kingdom}\\[2cm]',
#         r'{\large Tel +44 (0) 118 3785579\\',
#         r'Fax +44 (0) 118 3785576\\',
#         r'E-mail: ghrsst-po@nceo.ac.uk}\\[2cm]',
#         r'\end{center}',
#     ]
#     return latex_lines


# def create_doc_approval()->list:
#     l = [
#         r'\newpage',
#         r'\textbf{\Large Document Approval Record}\par\vspace{1cm}',
#         r'This document has been approved for release only when signed and dated signatures are present for',
#         r'the entities listed below. Documents may be digitally signed. \par \vspace{1cm}',
#         r'\begin{tabular}{|p{1.5cm}|>{\raggedright}p{2.5cm}|>{\raggedright}p{3cm}|p{4cm}|p{1.5cm}|}',
#         r'\hline \rowcolor{lightgray}',
#         r'Role & Name & Representing Entity & Signature(s) & Date(s) \\',
#         r'\hline',
#         r'Book Captains & Kenneth Casey and Craig Donion & GHRSST Science Team & insert image & tbd \\',
#         r'\hline',
#         r'GHRSST Project Office & Andrea KaiserWeiss & GHRSST Quality Assurance and Revision Control &insert image & tbd \\',
#         r'\hline',
#         r'GHRSST GDS 2.0 Internal Review Board & Edward M Armstrong & Data Assembly and Systems Technical Advisory Group (DAS-TAG) & insert image & tbd \\',
#         r'\hline',
#         r'GDS 2.0 External Review Board & Anne O\'Carroll & ll GHRSST External Review Board & insert image & tbd \\',
#         r'\hline',
#         r'GHRSST Advisory Council & Jacob Hoyer & GHRSST Advisory Council & insert image & tbd \\',
#         r'\hline',
#         r'\end{tabular}',
#     ]
#     return l

# def create_doc_history_changeRecord()->list:
#     l = [
#         r'\newpage',
#         r'\textbf{\large Document History} \par \vspace{1.5cm}',
#         r'\begin{tabular}{|p{2.5cm}|>{\raggedright}p{5.5cm}|p{3cm}|p{2cm}|}',
#         r'\hline \rowcolor{lightgray}',
#         r'Author & Version description & Version number & Date of Revision\\',
#         r'\hline',
#         r'K Casey & edits based on external review and inputs from the GHRSST team & v2.006 & 27 September 2010 \\',
#         r'\hline',
#         r'A Kaiser-Weiss & Release version & v2.007 & 1 October 2010 \\',
#         r'\hline',
#         r'Ed Armstrong & GDS2.0 reviewed by DAS-TAG 2011 & v2 rev 4 & 6th November 2011 \\',
#         r'\hline',
#         r'Ed Armstrong & GDS2.0 release 5 & v2 rev 5 & 9th October 2012 \\',
#         r'\hline',
#         r'\end{tabular}',
#         r'\par \vspace{3cm}',
#         r'\textbf{\Large Document Change Record} \par \vspace{1.5cm}',
#         r'\begin{tabular}{|p{2.5cm}|>{\raggedright}p{5.5cm}|p{3.5cm}|p{2cm}|}',
#         r'\hline \rowcolor{lightgray}',
#         r'Author & Reason for Change & Pages/paragraphs Changed & Date of Revision\\',
#         r'\hline',
#         r'E. Armstrong & Updates based on external review and DAS-TAG summary report to GHRSST-12 & Multiple & 28 Sep 2011 \\',
#         r'\hline',
#         r'A Kaiser-Weiss & Links updated, minor typos removed & 1-7, 37, 50, 104 & 29 Sep 2011 \\',
#         r'\hline',
#         r'Ed Armstrong & Updated based on final DAS-TAG mini review & CF comment attribute added to all variable examples; full example, L2P CDL revised; variable l2p\_flags clarified ; SSES clarified as Sensor Specific Error Statistic & 6 Nov 2011 \\',
#         r'\hline',
#         r'Ed Armstrong & g Minor updates & Minor changes and additions to metadata attributes. Mostly table 8.2. Other minor changes. & 9 October 2012 \\',
#         r'\hline',
#         r'\end{tabular}',
#     ]
#     return l

# def create_science_team()->list:
#     l = [
#         r"\pagebreak",
#         r"\section{The GHRSST Science Team 2010/11}",
#         r"\begin{tabular}{ l  l }",
#         r"O Arino & European Space Agency, Italy\\",
#         r"E Armstrong & NASA/JPL, USA\\",
#         r"I Barton & CSIRO Marine Research, Australia\\",
#         r"H Beggs & Bureau of Meteorology, Melbourne Australia\\",
#         r"A Bingham & NASA/JPL, USA\\",
#         r"K S Casey & NOAA/NODC, USA\\",
#         r"S Castro & University of Colorado, USA\\",
#         r"M Chin & NASA/JPL, USA\\",
#         r"G Corlett & University of Leicester, UK\\",
#         r"P Cornillon & University of Rhode Island, USA\\",
#         r"C J Donlon & (Chair) European Space Agency, The Netherlands\\",
#         r"S Eastwood &  Met.no, Norway\\",
#         r"\end{tabular}"
#     ]
#     return l

# def create_executive_sum()->list:
#     l = [
#         r'\pagebreak'
#         r'\section{Executive Summary}',
#         r'\par \vspace{0.5cm}',
#         r'A new generation of integrated Sea Surface Temperature (SST) data products are being provided by the Group for High Resolution Sea Surface Temperature (GHRSST). L2 products are provided by a variety of data providers in a common format. L3 and L4 products combine, in near-real time, various SST data products from several different satellite sensors and in situ observations and maintain fine spatial and temporal resolution needed by SST inputs to a variety of ocean and atmosphere applications in the operational and scientific communities. Other GHRSST products provide diagnostic data sets and global multi-product ensemble analysis products. Retrospective reanalysis products are provided in a non real time critical offline manner. All GHRSST products have a standard format, include uncertainty estimates for each measurement, and are served to the international user community free of charge through a variety of data transport mechanisms and access points that are collectively referred to as the GHRSST Regional/Global Task Sharing (R/GTS) framework. ',
#         r'\par \vspace{0.5cm}',
#         r'\noindent The GHRSST Data Specification (GDS) Version 2.0 is a technical specification of GHRSST products and services. It consists of a technical specification document (this volume) and a separate Interface Control Document (ICD). The GDS technical documents are supported by a User Manual and a complete description of the GHRSST ISO-19115-2 metadata model. GDS-2.0 represents a consensus opinion of the GHRSST international community on how to optimally combine satellite and in situ SST data streams within the R/GTS. The GDS also provides guidance on how data providers might implement SST processing chains that contribute to the R/GTS.',
#         r'\par \vspace{0.5cm}',
#         r'\noindent This document first provides an overview of GHRSST followed by detailed technical specifications of the adopted file naming specification and supporting definitions and conventions used throughout GHRSST and the technical specifications for all GHRSST Level 2P, Level 3, Level 4, and GHRSST Multi-Product Ensemble data products. In addition, the GDS 2.0 Technical Specification provides controlled code tables and best practices for identifying sources of SST and ancillary data that are used within GHRSST data files.',
#         r'\par \vspace{0.5cm}',
#         r'\noindent The GDS document has been developed for data providers who wish to produce any level of GHRSST data product and for all users wishing to fully understand GHRSST product conventions, GHRSST data file contents, GHRSST and Climate Forecast definitions for SST, and other useful information. For a complete discussion and access to data products and services see https://www.ghrsst.org, which is a central portal for all GHRSST activities.'
#     ]
#     return l


# def table_of_contents()->list:
#     l = [
#         r'\tableofcontents',
#     ]
#     return l


# def list_of_figures_and_tables()->list:
#     #Here is how to use figures and tables in latex
#     # \begin{figure}
#     # \centering
#     # \includegraphics[width=0.5\textwidth]{figure.png}
#     # \caption{Caption text}
#     # \label{fig:figure_label}
#     # \end{figure}

#     # \listoffigures


#     # \begin{table}
#     # \centering
#     # \begin{tabular}{|c|c|}
#     #     \hline
#     #     Column 1 & Column 2 \\
#     #     \hline
#     #     Row 1 & 1 \\
#     #     Row 2 & 2 \\
#     #     \hline
#     # \end{tabular}
#     # \caption{Caption text}
#     # \label{tab:table_label}
#     # \end{table}

#     # \listoftables


# #     \begin{table}[ht]
# #   \centering
# #   \begin{tabular}{|c|c|}
# #     \hline
# #     Column 1 & Column 2 \\
# #     \hline
# #     Row 1, Column 1 & Row 1, Column 2 \\
# #     Row 2, Column 1 & Row 2, Column 2 \\
# #     \hline
# #   \end{tabular}
# #   \caption{My Table Caption}
# #   \label{tab:mytable}
# # \end{table}



#     l = [
#         r'\pagebreak',
#         r'\section{Figures in this document}',
#         r'\par \vspace{0.5cm}',
#         r'',
#         r'\listoffigures',
#         r'\listoftables',
#         r'\section{Tables in this document}',
#         r'\par \vspace{0.5cm}',
#     ]
#     return l

