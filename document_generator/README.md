<h1 align="center">
User instructions for the ECCO Document Generator 
</h1>

<br>

Note: All local paths printed here are in reference to the location of the "/ECCO-Dataset-Production/document\_generator" directory on your local machine (see Step 1 in the "Before running the code" section below) (i.e. "/Users/your\_user\_name/ECCO-Dataset-Production/document\_generator")

<h2 align="left">
Setup - Before running the main document generator scripts:
</h2>

Step 1: If you haven't done so already, clone the "ECCO-Dataset-Production" git repo (the parent folder of this project) to your local machine (i.e. "https://github.com/ECCO-GROUP/ECCO-Dataset-Production")

Step 2: If you haven't done so already, clone the "ECCOv#-py" git repo to your local machine (where "#" is the version of ECCO used to produce the granules you're documenting (i.e. "https://github.com/ECCO-GROUP/ECCOv4-py")).  Once you have done this, in the first block of code in "/document\_generator/src/document\_generator/utils/cdf\_plotter.py", modify the argument of the first call to "sys.path.append()" to be the location of your cloned ECCOv#-py repo (i.e. sys.path.append('/Users/your\_user\_name/ECCOv4-py')).  

Step 3: If you haven't already, install the program "pdflatex" onto your local machine (installation instructions vary depending on operating system).

Step 4: Make sure you've installed the following Python packages: 
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

Step 5: If you need to download granules to be described in the compendium, create or edit the file "granules\_to\_download.txt" ("/files\_general/resource\_files/version\_specific/V#r#/input\_and\_templates/granules\_to\_download/granules\_to\_download.txt", where "V#r#" must be changed to reflect the version of ECCO you're documenting (i.e. V4r6)) by specifying the urls of the remotely hosted granules to download. 

Step 6: Modify the "config\_user.yaml" file ("/files\_general/resource\_files/config\_user\_ModifyMe/config\_user.yaml", further instructions provided within the file) to reflect:
 - the version of ECCO used to generate the granules you're documenting
 - the grid types of the granules you're documenting
 - overwrite options 
 - latex compilation options 
 
<br>
<br>

<h2 align="left">
Running the main scripts:
</h2>

<h5>
(From the "/ECCO-Dataset-Production/document\_generator" directory, run the commands listed in the following steps (each step is a single python command line call)):
</h5>
---


Step 1: If required, download granules via:
 - python src/document\_generator/apps/step1\_download\_granules.py

Step 2: Generate the compendium components (figures and latex table files)
 - python src/document\_generator/apps/step2\_generate\_compendium\_sub\_components.py

Step 3: Compile the compendium, resulting in the pdf file "/files\_general/compendium\_compilation\_output\_files/ECCO\_Dataset\_Catalog\_and\_Variable\_Compendium.pdf"
 - python src/document\_generator/apps/step3\_compile\_compendium.py
