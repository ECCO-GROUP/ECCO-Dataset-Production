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

<h1>2. How to run the documewnt generator?</h1>
<div align="justify">
  
If the Python environment is properly configured, you are now ready to start. In this section, you will learn how to run the program. We will provide you with a step-by-step guide to running the document generator program. The first time you do this, everything needs to be done one by one. Follow the order indicated below.

</div>
<h2>2.1 Clone the github repository</h1>
<div align="justify">

The document generator program is available on the [ECCO-group](https://github.com/ECCO-GROUP) GitHub space in the repository named [ECCO-Dataset-Production](https://github.com/ECCO-GROUP/ECCO-Dataset-Production). You will need to fork the repository to your personal GitHub account, assuming you have one. Then, clone your forked repository to your local machine. This could be a laptop, virtual machine, or cloud-based computer. In its current version, the program is fully configured to generate a user-friendly document for the ECCO V4r4 Dataset Catalog and Variable Compendium. To clone the program to your local machine, use the following command:

</div>

```bash
git clone <repository-url>
```
Replace `<repository-url>` with the URL of your forked repository. For example:

```bash
git clone https://github.com/YOUR_GITHUB_USERNAME/ECCO-Dataset-Production.git
```
Note that this clone process should be done in your desired local folder as you wish.

<h2>2.2 Dataset version specification</h1>

By default, the program comes with the ECCO dataset specification for version 4, release 4 (v4r4), and is ready to run after the dataset sample has been collected. You can specify the ECCO dataset version for which you want to generate the document. To do this, you will need to run the initial Python script: `data_version.py`. This program is located in `document_generator/granule_datasets` (Don't worry about the folder structure. You will know more about it in the following!). The program will ask you to specify the version number (which should be an integer, such as `4`) and the release information, such as `4`, `4.1`, or `4.1.a`, depending on your target. Let's assume you are targeting ECCO v4r5 (version 4 release 5), the program will display the following on your screen:

```python
python data_version.py
Insert the dataset version number you're willing 
 to generate the document for. Note, only 
 the number is required ...:  4
Insert the dataset release number you're willing 
 to generate the document for. Note, only 
 the number is required ...:  5

Folder need to be created for v4r5
Folder is now created for v4r5. You're all set! Go for it!
```
By the end of the program excecution, a `json` file `data_version.json` is generated and store all provided information in the following structure:

```json
{"version": "4", "release": "5", "dataset_version": "v4r5"}
```
This `json` file is used by `cdf_extract.get_dataset_version()` to handle folder specification for dataset sample collection and figures saving directory for the finale document generation (Don't worry about the package `cdf_extract`. You will know more about it in the following!). Note that by the end, a folder `v4r5` is created in `granule_datasets`. By default, it is `v4r4` that contains all needed datasets samples for the document.

<h2>2.2 Download the targeted dataset sample</h1>
<div align="justify">

This script (in `/document_generator/granule_datasets/`) aims to download a sample of ECCO data sets that will be documented by the document generator. The local environment settings in order to make `download_granules.py` works can be found in [/granule_datasets/download_instructions.txt](/granule_datasets/download_instructions.txt/). If it is yur first time to download data from NASA Earthdata platform, you have to create an account, and then run the following `bash` script (works for Mac OS and Linux distrbution. If other OS, go ask Google!):

```bash
  > cd ~
  > touch .netrc
  > echo "machine urs.earthdata.nasa.gov login uid_goes_here password password_goes_here" > .netrc
  > chmod 0600 .netrc
  > touch .urs_cookies
```

</div>

<h3>2.2.1 Text files setting for datasets sample collection</h3>

Runing this script will aneble you to download your targeted ECCO datasets samples. Before that, you need to edit three `.txt` files: `native.txt`, `latlon.txt`, `oneD.txt`, `natives_coords.txt` and `latlon_coords.txt`. These `text` files contain a list of links of datasets samples to be downloaded. They are used by `download_granules.py` based on `wget` method to download all needed datasets samples. By default, these text are already created for the ECCO v4r4 datasets samples. Indeed, each of them is manually edited with the appropriate links where the datasets are hosted on the [Earthdata platform](https://search.earthdata.nasa.gov/search).

| Text file | Role |
|----------|----------|
| native.txt       | This file contains all links to ECCO v4r4 dataset samples in native lat-lon-cap 90 (llc90) coordinates (tiles) format.|
| latlon.txt       | This file contains all links to ECCO v4r4 dataset samples in regular 0.05 latitude-longitude grid coordinates format.|
| oneD.txt         | This file contains all links to ECCO v4r4 dataset samples of global mean time series of certain variables.|
| native_coords.txt| This file contains a link to ECCO v4r4 dataset native lat-lon-cap 90 (llc90) coordinates and grid geometry data.|
| latlon_coords.txt| This file contains a link to ECCO v4r4 dataset 0.5 lat-lon coordinates and grid geometry data.|

*Note:* By default, all of these text files already exist in `v4r4` folder with all needed links. If you are targeting another version of ECCO datasets, you will have to creat all of these text files in the appropriate data version folder. See bellow an example for `native.txt` file:

```txt
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/ECCO_L4_ATM_STATE_LLC0090GRID_DAILY_V4R4/ATM_SURFACE_TEMP_HUM_WIND_PRES_day_mean_2017-12-29_ECCO_V4r4_native_llc0090.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/ECCO_L4_MIXED_LAYER_DEPTH_LLC0090GRID_DAILY_V4R4/OCEAN_MIXED_LAYER_DEPTH_day_mean_2017-12-29_ECCO_V4r4_native_llc0090.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/ECCO_L4_STRESS_LLC0090GRID_DAILY_V4R4/OCEAN_AND_ICE_SURFACE_STRESS_day_mean_2017-12-29_ECCO_V4r4_native_llc0090.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/ECCO_L4_SEA_ICE_CONC_THICKNESS_LLC0090GRID_DAILY_V4R4/SEA_ICE_CONC_THICKNESS_day_mean_2017-12-29_ECCO_V4r4_native_llc0090.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/ECCO_L4_SEA_ICE_VELOCITY_LLC0090GRID_DAILY_V4R4/SEA_ICE_VELOCITY_day_mean_2017-12-29_ECCO_V4r4_native_llc0090.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/ECCO_L4_SEA_ICE_HORIZ_VOLUME_FLUX_LLC0090GRID_DAILY_V4R4/SEA_ICE_HORIZ_VOLUME_FLUX_day_mean_2017-12-29_ECCO_V4r4_native_llc0090.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/ECCO_L4_OCEAN_BOLUS_STREAMFUNCTION_LLC0090GRID_DAILY_V4R4/OCEAN_BOLUS_STREAMFUNCTION_day_mean_2017-12-29_ECCO_V4r4_native_llc0090.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/ECCO_L4_OCEAN_3D_VOLUME_FLUX_LLC0090GRID_DAILY_V4R4/OCEAN_3D_VOLUME_FLUX_day_mean_2017-12-29_ECCO_V4r4_native_llc0090.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/ECCO_L4_OCEAN_3D_SALINITY_FLUX_LLC0090GRID_DAILY_V4R4/OCEAN_3D_SALINITY_FLUX_day_mean_2017-12-29_ECCO_V4r4_native_llc0090.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/ECCO_L4_SEA_ICE_SALT_PLUME_FLUX_LLC0090GRID_DAILY_V4R4/SEA_ICE_SALT_PLUME_FLUX_day_mean_2017-12-29_ECCO_V4r4_native_llc0090.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/ECCO_L4_OCEAN_3D_TEMPERATURE_FLUX_LLC0090GRID_DAILY_V4R4/OCEAN_3D_TEMPERATURE_FLUX_day_mean_2017-12-29_ECCO_V4r4_native_llc0090.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/ECCO_L4_OBP_LLC0090GRID_DAILY_V4R4/OCEAN_BOTTOM_PRESSURE_day_mean_2017-12-29_ECCO_V4r4_native_llc0090.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/ECCO_L4_HEAT_FLUX_LLC0090GRID_DAILY_V4R4/OCEAN_AND_ICE_SURFACE_HEAT_FLUX_day_mean_2017-12-29_ECCO_V4r4_native_llc0090.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/ECCO_L4_FRESH_FLUX_LLC0090GRID_DAILY_V4R4/OCEAN_AND_ICE_SURFACE_FW_FLUX_day_mean_2017-12-29_ECCO_V4r4_native_llc0090.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/ECCO_L4_TEMP_SALINITY_LLC0090GRID_DAILY_V4R4/OCEAN_TEMPERATURE_SALINITY_day_mean_2017-12-29_ECCO_V4r4_native_llc0090.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/ECCO_L4_DENS_STRAT_PRESS_LLC0090GRID_DAILY_V4R4/OCEAN_DENS_STRAT_PRESS_day_mean_2017-12-29_ECCO_V4r4_native_llc0090.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/ECCO_L4_BOLUS_LLC0090GRID_DAILY_V4R4/OCEAN_BOLUS_VELOCITY_day_mean_2017-12-29_ECCO_V4r4_native_llc0090.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/ECCO_L4_OCEAN_VEL_LLC0090GRID_DAILY_V4R4/OCEAN_VELOCITY_day_mean_2017-12-29_ECCO_V4r4_native_llc0090.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/ECCO_L4_OCEAN_3D_MOMENTUM_TEND_LLC0090GRID_DAILY_V4R4/OCEAN_3D_MOMENTUM_TEND_day_mean_2017-12-29_ECCO_V4r4_native_llc0090.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/ECCO_L4_OCEAN_3D_MIX_COEFFS_LLC0090GRID_V4R4/OCEAN_3D_MIXING_COEFFS_ECCO_V4r4_native_llc0090.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/ECCO_L4_SSH_LLC0090GRID_DAILY_V4R4/SEA_SURFACE_HEIGHT_day_mean_2017-12-29_ECCO_V4r4_native_llc0090.nc
```

<h3>2.2.1 Runing "download_granules.py"</h3>

Now, it is time to run `download_granules.py`. To do so, go to the folder `document_generator/granule_datasets` and run the script as follow:

```bash
>> python download_granules.py
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

This script (in `/document_generator/`) reads the netCDF file of each targeted dataset with a set of functions to generate Latex lines for table building for each variable per netCDF file. See `cdf_reader.get_non_coord_vars`, `cdf_reader.readVarAttr`, `cdf_reader.readAllVarAttrs`, `cdf_reader.process_dict_items`, `cdf_reader.data_var_table`, `cdf_reader.compute_ds_dict`, `cdf_reader.read_data_vars`, `cdf_reader.generate_CDL` and `cdf_reader.cdl_to_latex`.

</div>

<h2>4.3 cdf_extract.py</h2>
<div align="justify">

</div>

<h2>4.4 cdf_plotter.py</h2>
<div align="justify">

</div>

<h2>4.5 sections</h2>
<div align="justify">

<h1>5. JSON and TXT files configurations</h1>

<h2>5.1 JSON files</h2>
<div align="justify">

</div>

<h2>5.1.1 global_attrs_GDS20r5.json</h2>
<div align="justify">

</div>

<h2>5.2 TXT files</h2>
<div align="justify">

</div>
