<h1 align="center">
ECCO Document Generator user instructions
</h1>

<h2 align="left">
Setup:
</h2>

Step 1: Clone the "ECCO-Dataset-Production" github repo (the parent folder of this project) to your local machine.

Step 2: Install the "pdflatex" program on your computer (installation instructions will vary depending on your operating system).

Step 3: Install the following python packages, if they are not currently installed:
- pyyaml
- xarray
- requests
- Pillow
- matplotlib
- cartopy
- cmocean
- xgcm
- xmitgcm
- pyresample
- netCDF4

Step 4: Specify the urls of the remotely hosted granules you'd like to document in the "granules\_to\_download.txt" file (relative path printed below, where "V#r#" must be changed to reflect the version of ECCO you're documenting (i.e. V4r6)):
 - .../ECCO-Dataset-Production/document\_generator/files\_general/resource\_files/version\_specific/V#r#/input\_and\_templates/granules\_to\_download/granules\_to\_download.txt

Step 5: Modify the "config\_user.yaml" file (relative path printed below) to reflect the version of ECCO you're documenting, the grid types of the variables you're documenting, overwrite options, and latex compilation options:
 - .../ECCO-Dataset-Production/document\_generator/files\_general/resource\_files/config\_user\_ModifyMe/config\_user.yaml


<h2 align="left">
Running the code:
</h2>

Step 1: Download granules via:
 - python .../ECCO-Dataset-Production/document\_generator/src/document\_generator/apps/step1\_download\_granules.py

Step 2: Generate the document components (figures and latex table files)
 - python .../ECCO-Dataset-Production/document\_generator/src/document\_generator/apps/step2\_generate\_compendium\_sub\_components.py

Step 3: Compile the compendium, resulting in the pdf file ".../ECCO-Dataset-Production/document\_generator/files\_general/compendium\_compilation\_output\_files/ECCO\_Dataset\_Catalog\_and\_Variable\_Compendium.pdf"
 - python /Users/brucel/ecco/yip/ECCO-Dataset-Production/document\_generator/src/document\_generator/apps/step3\_compile\_compendium.py
