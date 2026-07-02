<h1 align="center">
ECCO Document Generator user instructions
</h1>

<br>

Note: All local paths printed here are in reference to the location of the "/ECCO-Dataset-Production/document\_generator" directory on your local machine (see Step 1 in the "Before running the code" section below) (i.e. "/Users/your\_user\_name/ECCO-Dataset-Production/document\_generator")

<h2 align="left">
Before running the code:
</h2>

Step 1: If you haven't done so already, clone the "ECCO-Dataset-Production" git repo (the parent folder of this project) to your local machine (i.e. "https://github.com/ECCO-GROUP/ECCO-Dataset-Production")

Step 2: If you haven't done so already, clone the "ECCOv#-py" git repo to your local machine (where "#" is the version of ECCO used to produce the granules you're documenting (i.e. "https://github.com/ECCO-GROUP/ECCOv4-py")).  Once you have done this, in the first block of code in "/document\_generator/src/document\_generator/utils/cdf\_plotter.py", modify the argument of the first call to "sys.path.append()" to be the location of your cloned ECCOv#-py repo (i.e. sys.path.append('/Users/your\_user\_name/ECCOv4-py')).  

Step 3: Install the program "pdflatex" onto your local machine (installation instructions vary depending on operating system).

Step 4: Install the following Python packages:
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

Step 5: In the file "granules\_to\_download.txt" ("/files\_general/resource\_files/version\_specific/V#r#/input\_and\_templates/granules\_to\_download/granules\_to\_download.txt", where "V#r#" must be changed to reflect the version of ECCO you're documenting (i.e. V4r6)), specify the urls of the remotely hosted granules to download and describe in the compendium (further instructions are provided within the file).

Step 6: Modify the "config\_user.yaml" file ("/files\_general/resource\_files/config\_user\_ModifyMe/config\_user.yaml", further instructions provided within the file) to reflect:
 - the version of ECCO used to generate the granules you're documenting
 - the grid types of the granules you're documenting
 - overwrite options 
 - latex compilation options 
 
<br>
<br>

<h2 align="left">
Running the code:
</h2>

Step 1: Download granules via:
 - python src/document\_generator/apps/step1\_download\_granules.py

Step 2: Generate the compendium components (figures and latex table files)
 - python src/document\_generator/apps/step2\_generate\_compendium\_sub\_components.py

Step 3: Compile the compendium, resulting in the pdf file "/files\_general/compendium\_compilation\_output\_files/ECCO\_Dataset\_Catalog\_and\_Variable\_Compendium.pdf"
 - python src/document\_generator/apps/step3\_compile\_compendium.py
