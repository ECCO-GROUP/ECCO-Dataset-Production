# ECCO Dataset Document Generator

_everything is being ran when current working directory (cwd) is the `/document_generator` folder_

## 0. Prerequsuites to run generator through **`main.py`**

main files used are: `latex_outline.py` and `dataset_sections.py`:

1. **JSON Configuration Files:**

    - The script relies on several JSON files located in the `granule_datasets/` directory to define dataset groupings and coordinate information. These files must exist and be correctly formatted:
        - `granule_datasets/native_coords.json`
        - `granule_datasets/ECCOv4r4_groupings_for_native_datasets.json`
        - `granule_datasets/latlon_coords.json`
        - `granule_datasets/ECCOv4r4_groupings_for_latlon_datasets.json`
        - `granule_datasets/ECCOv4r4_groupings_for_1D_datasets.json`
    - The structure of these JSON files must match what the `data_products` function in `dataset_sections.py` expects (a list of dictionaries, each containing a `"filename"` key, at a minimum).

2. **Directory Structure:**

    - the granule netcdf datasets need to be downloaded from site that is hosting them
    - The script expects a specific directory structure for NetCDF files and generated images:

        - `granule_datasets/natives_coords/` (Native coordinate datasets)
        - `granule_datasets/natives/` (Native datasets)
        - `granule_datasets/latlon_coords/` (LatLon coordinate datasets)
        - `granule_datasets/latlon/` (LatLon datasets)
        - `granule_datasets/oneD/` (1D datasets)

        - `images/plots/native_plots_coords/` (Native coordinate plots)
        - `images/plots/native_plots/` (Native plots)
        - `images/plots/latlon_plots_coords/` (LatLon coordinate plots)
        - `images/plots/latlon_plots/` (LatLon plots)
        - `images/plots/oneD_plots/` (1D plots)

    - These directories must exist, or the script will likely throw errors when trying to read NetCDF files or save plots.
    - They is actually instructions on how to download granules in `granule_datasets/download_instructions.txt` right after following those instructions, you can run `granule_datasets/download_granules.py` to download the datasets!

3. **Attributes Table:**
    - Also modify `data/global_attributes.json` and `data/variable_attributes` to your liking, the data will be parsed and utilized in the code

-   **`dataset_sections.py`:** Orchestrates the process, calling functions from `cdf_extract` and `cdf_plotter` and assembling the LaTeX code for each data product.
-   **`latex_outline.py`:** Calls `dataset_sections.py` and writes the final LaTeX code to `.tex` files in the `document/latex/dataset/` and `document/latex/data_product/` directories.

## 1. obtain dependencies

**Important:** This project requires Python 3.11 (specifically 3.11.10). Using a different Python version may lead to compatibility issues, especially with `ecco_v4_py`.

**Choose your preferred package manager:** You can use either `pip` or `conda` to manage the project's dependencies. Follow the instructions below for your chosen method.

**Create virtual env** using pip or conda, but again make sure you are using python version 3.11 (3.11.10).

the following is how i installed the dependencies with pip, if you are not going to use pip and/or do not want to install mapping binaries with brew on your mac, then follow instructions on https://ecco-v4-python-tutorial.readthedocs.io/Installing_Python_and_Python_Packages.html to install `ecco_v4_py` package (and skip step 1)

1. **Install mapping binaries (macOS only):** install the mapping binaries using Homebrew _before_ installing the Python packages.

```bash
brew update
brew install geos
brew install proj
```

2. create a `requirements.txt` file in the `/document_generator` folder and add following packages
    - xarray
    - matplotlib
    - numpy
    - cartopy
    - cmocean
    - ecco_v4_py

**requirements.txt**

```txt
bokeh==3.6.3
bottleneck==1.4.2
cachetools==5.5.2
cartopy==0.24.1
certifi==2025.1.31
cftime==1.6.4.post1
click==8.1.8
cloudpickle==3.1.1
cmocean==4.0.3
configobj==5.0.9
contourpy==1.3.1
cycler==0.12.1
dask==2025.2.0
distributed==2025.2.0
donfig==0.8.1.post1
ecco-v4-py==1.6.0
fonttools==4.56.0
fsspec==2025.2.0
future==1.0.0
importlib-metadata==8.6.1
jinja2==3.1.5
kiwisolver==1.4.8
locket==1.0.0
lz4==4.4.3
markupsafe==3.0.2
matplotlib==3.10.0
msgpack==1.1.0
netcdf4==1.7.2
numpy==2.2.3
packaging==24.2
pandas==2.2.3
partd==1.4.2
pillow==11.1.0
platformdirs==4.3.6
psutil==7.0.0
pyarrow==19.0.1
pykdtree==1.4.1
pyparsing==3.2.1
pyproj==3.7.1
pyresample==1.32.0
pyshp==2.3.1
python-dateutil==2.9.0.post0
pytz==2025.1
pyyaml==6.0.2
shapely==2.0.7
six==1.17.0
sortedcontainers==2.4.0
tblib==3.0.0
toolz==1.0.0
tornado==6.4.2
tzdata==2025.1
urllib3==2.3.0
xarray==2025.1.2
xgcm==0.8.1
xmitgcm==0.5.2
xyzservices==2025.1.0
zict==3.0.0
zipp==3.21.0
```

1. install python dependencies `pip install -r requirements.txt` or `conda install --file requirements.txt`
2. create a .gitignore with following if you want to make it convenient to ignore files from git

```.gitignore
.venv
.vscode/
.ipynb_checkpoints/
__pycache__/
*.pyc
images/plots/
*.nc
*.aux
*.lof
*.log
*.lot
*.synctex.gz
*.toc
*.DS_Store
```

## 2. run document generator

1. as stated earlier, make sure to
    1. Modify `data/global_attributes.json` and `data/variable_attributes` to your liking, the data will be parsed and utilized in the code
    2. Download ecco_datasets manually and separate by groups (latlon, latlon_coords, natives, natives_coords, and oneD ), save them in `granule_datasets/`
        - recall: they is instructions on how to download granules in `granule_datasets/download_instructions.txt` right after following those instructions, you can run `granule_datasets/download_granules.py` to download the datasets!
2. in main.py uncomment the three write_dataset code to generate all the plot data(it takes a minute), just need to do it once! so comment those lines after so you don't have to wait a minute again

```python
import sections.latex_outline as outline
def main():
    outline.write_data_attributes_tables()


    outline.write_datasets('native')  # <----------------
    outline.write_datasets('latlon')  # <----------------
    outline.write_datasets('1D')      # <----------------



main()
```

## Document Classification

This table explains which files in the `document/` directory are automatically generated from `main.py` --> `sections/latex_outline.py` and which require manual input.

### Summary

-   **Automatically Generated:** Files in `document/latex/data_product/*` and `document/latex/dataset/*` directories
-   **Manually Modified:** Various `.tex` files containing document structure and content
-   **Final Compilation:** All files are compiled into `document/final_doc.tex`

### Detailed Classification

| File Directory                    | Classification          | Description                        |
| --------------------------------- | ----------------------- | ---------------------------------- |
| latex/data_product/\*             | Automatically Generated | Contains files generated from code |
| latex/dataset/\*                  | Automatically Generated | Contains files generated from code |
| latex/acry_abbrev_list.tex        | Manually Modified       | List of acronyms and abbreviations |
| latex/app_ref_docs.tex            | Manually Modified       | Appendix reference documents       |
| latex/closing_statement.tex       | Manually Modified       | Closing statement for the document |
| latex/doc_changes.tex             | Manually Modified       | Document change history            |
| latex/doc_management_policies.tex | Manually Modified       | Document management policies       |
| latex/doc_records.tex             | Manually Modified       | Document records                   |
| latex/document_conventions.tex    | Manually Modified       | Document conventions               |
| latex/ECCO_overview.tex           | Manually Modified       | ECCO system overview               |
| latex/ECCO_team.tex               | Manually Modified       | ECCO team information              |
| latex/executive_summary.tex       | Manually Modified       | Executive summary                  |
| latex/filenames_conventions.tex   | Manually Modified       | File naming conventions            |
| latex/front_pages.tex             | Manually Modified       | Front pages of the document        |
| latex/LOF.tex                     | Manually Modified       | List of figures                    |
| latex/metadata_specification.tex  | Manually Modified       | Metadata specifications            |
| latex/preamble.tex                | Manually Modified       | LaTeX preamble                     |
| latex/scope_content_doc.tex       | Manually Modified       | Document scope and content         |
| latex/TOC.tex                     | Manually Modified       | Table of contents                  |
| **final_doc.tex**                 | Compilation Output      | Final compiled document            |

## what do certain files do?

### what does `latex_outline.py` do?

`latex_outline.py` generates LaTeX code for documenting datasets. It creates tables for:

1.  **Global and Variable Attributes:** Reads attribute definitions from JSON files and outputs LaTeX `longtable` environments to `document/latex/data_product/`.
2.  **Example NetCDF Structures:** Uses `cdf_extract.latex_example_netcdf()` to generate LaTeX tables representing example NetCDF structures (native, latlon, 1D) in `document/latex/data_product/`.
3.  **Dataset Descriptions:** Uses `sections.dataset_sections.data_products()` to generate LaTeX tables describing datasets (native, latlon, 1D) based on JSON groupings and image paths, outputting to `document/latex/dataset/`.

**Command-Line Usage:**

```bash
python latex_outline.py --type [native|latlon|1D]
```

### what does `dataset_sections.py` do?

`dataset_sections.py` creates LaTeX code to document datasets. It reads dataset info from a JSON file and generates:

-   **Sections & Subsections:** Organizes documentation by dataset and variable.
-   **Tables:** Uses `cdf_extract` to create tables describing dataset fields and variable attributes.
-   **Plots:** Uses `cdf_plotter` to generate and include plots of each variable.

**Key Function:**

-   `data_products(filePath, directory, imageDirectory, section)`: Takes a JSON file path, directories, and dataset type (`section`) as input. It returns a list of LaTeX lines that form the documentation section.

### what does `cdf_plotter.py` do ?

This script plots data variables from a NetCDF file. It uses command-line arguments to specify the file, the variable to plot, and various plot options.

**Basic Usage:**

To run the script, use the following command in your terminal:

```bash
python cdf_plotter.py --file <path_to_netcdf_file> --field <variable_name>
```
