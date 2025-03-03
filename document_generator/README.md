<h1 align="center">
User guide for document generator for ECCO v4r4 Datasets
</h1>

<h1>1. Overview</h1>

<div align="justify">
The present document aims to provide a step-by-step path on how to use the document generator tool for producing documentation for ECCO v4r4 dataset. It is an update of the previous version by Jose Gonzalez. The document generator is written in python scripts to generate a Latex file that produces a PDF file providing documentation on ECCO v4r4 data sets. This python language based tool is a combination of i) independent and ii) dependent packages. In the following, guidance will be provided on i) and ii) script set configuration, and how to run the tool with success.
</div>

<div align="justify">
  
For the python environment setting and external package dependence for the present tool, please, refer to [this document](/document_generator/guid_doc_Jose_Gonzalez/instructions.md/). Note that the external packages required to successfully run “document generator” can be found in requirements.txt ([here](/requirements.txt/)). Python 3.11 (specifically 3.11.10) is required. Using a different Python version may lead to compatibility issues, especially with the `ecco_v4_py` package which is indispensable for the project (more details on `ecco_v4_py` can be found [here](https://ecco-v4-python-tutorial.readthedocs.io/Installing_Python_and_Python_Packages.html) ). It is recommended to run the present project in a separate python environment so, the user will need to create its own environment with regard to the external dependency (confer [here](/requirements.txt/)).

</div>

On Mac OS, the following softwares is required along with the python environment setting: [GEOS](https://libgeos.org/) and [PROJ](https://proj.org/en/stable/). They can be installed using [Homebrew](https://brew.sh/) as follow:

```bash
brew update
brew install geos
brew install proj
```

Preferably, install `geos` and `proj` before setting the python environment. To install the python package dependencies, one of the followings methods can be used:

- with pip

```bash
pip install -r requirements.txt
```

- with conda

```bash
conda install --file requirements.txt
```

<h1>2. Folder configuration</h1>

<h1>3. Independent Internal package</h1>

<h2>3.1 download_granules.py</h2>
<div align="justify">

This script (in `/document_generator/granule_datasets/`) aims to download a sample of ECCO data sets that will be documented by the document generator. The local environment settings in order to make `download_granules.py` works can be found in [/granule_datasets/download_instructions.txt](/granule_datasets/download_instructions.txt/). It will mainly target native datasets ([native.txt](/granule_datasets/native.txt)), native coordinates ([native_coords.txt](/granule_datasets/native_coords.txt)), coordinates in longitude and latitude format ([latlon.txt](/granule_datasets/latlon.txt)) and global mean time series of certain variables ([oneD.txt](/granule_datasets/oneD.txt/)). Each of the aforementioned files is manually edited with the appropriate links where the datasets are hosted on the [Earthdata platform](https://search.earthdata.nasa.gov/search).

</div>

<h2>3.2 utils.py</h2>
<div align="justify">

This script (in `/document_generator/`) looks for the occurrence of any reserved characters by replacing them by their equivalent in Latex format, whether inside a mathematics environment denoted by ‘`$`’ or outside (`utils.sanitize` and `utils.sanitize_with_math`). It does the same for URLs in the text (`utils.sanitize_with_url`). It also searches and gets out the content between a pair of parentheses (`utils.get_substring`). `utils.add_to_line` customizes Latex lines styles and `utils.get_ds_title` gets out the targeted dataset title.

</div>

<h1>4. Inter-dependent Internal scripts and packages</h1>
<h2>4.1 readJSON.py</h2>
<div align="justify">

This script (in `/document/generator/`) depends on `utils.py` described above. It contains 6 sub-functions that read JSON files and customize table format to be generated in the Latex file for the document generator (`readJSON.obtain_json_data`, `readJSON.obtain_keys`, `readJSON.verify_columns`, `readJSON.establish_table`, `readJSON.set_table`, and `readJSON.main`).

</div>

<h2>4.2 cdf_reader.py</h2>
<div align="justify">

</div>

<h2>4.3 cdf_extract.py</h2>
<div align="justify">

</div>

<h2>4.4 cdf_plotter.py</h2>
<div align="justify">

</div>

<h2>4.5 sections</h2>
<div align="justify">

</div>
