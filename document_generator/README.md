<h1 align="center">
User instructions for the ECCO Document Generator 
</h1>

<br>

Notes:
 - All local paths printed here are in reference to the location of the "/ECCO-Dataset-Production/document\_generator" directory on your local machine (see Step 1 in the "Setup" section below) (i.e. "/Users/your\_user\_name/ECCO-Dataset-Production/document\_generator")
 - Where you see the "#" symbol in the text below, it represents a number that you must substitute according to the version of ECCO used to generate the granules you're documenting.  For example, for ECCO version 4 release 6, "ECCOv#-py" becomes "ECCOv4-py", and "V#r#" becomes "V4r6".



<h2 align="left">
Setup - Before running the main document generator scripts:
</h2>

Step 1: If you haven't done so already, clone the "ECCO-Dataset-Production" git repo (the parent folder of this project) to your local machine (i.e. "https://github.com/ECCO-GROUP/ECCO-Dataset-Production")

Step 2: If you haven't done so already, clone the "ECCOv#-py" git repo to your local machine (i.e. "https://github.com/ECCO-GROUP/ECCOv4-py").  Once you have done this, in the first block of code in "/document\_generator/src/document\_generator/utils/cdf\_plotter.py", modify the argument of the first call to "sys.path.append()" to be the location of your cloned ECCOv#-py repo (i.e. sys.path.append('/Users/your\_user\_name/ECCOv4-py')).  

Step 3: If you haven't already, install the program "pdflatex" onto your local machine (installation instructions will vary depending on your operating system).

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

Step 5: Modify the "config\_user.yaml" file ("/files\_general/resource\_files/config\_user\_ModifyMe/config\_user.yaml", further instructions provided within the file) to reflect:
 - the version ("V#r#") of ECCO used to generate the granules you're documenting (i.e. "V4r4", "V4r6", etc..)
 - the grid types (e.g. native, latlon, 1D) used by the granules you're documenting
 - example granules names - specify 1 for each grid type used by your granules
 - overwrite options ('url\_list\_overwrite\_switch', 'granule\_download\_overwrite\_switch', 'figure\_generation\_overwrite\_switch')
 - latex compilation options ('num\_pdflatex\_calls')

Step 6: If necessary (i.e. if this is your first time running the code for a given ECCO version, or you'd like to generate a new "granules\_to\_download.txt" file (see Step 8) (possible when "url\_list\_overwrite\_switch" is set to "True" in "config\_user.yaml")), run "step0\_generate\_preliminary\_file\_tree.py".  This generates the granule directory structure you'll need if using your own granules (see Step 7), along with optionally creating a fresh version of "granules\_to\_download.txt", with the old one being renamed according to current UTC time. 

Step 7 (if using any granules already present on your machine): To document local granules, either create symbolic links to them (recommended) in, or move them into, the following directories, according to their type (grid type and content type (i.e. variable vs coordinate)):
 - native coordinate (ie geometry) granule directory: files\_general/resource\_files/version\_specific/V#r#/output\_and\_granules/granules/coordinate\_granules/granules\_native/  
 - latlon coordinate (ie geometry) granule directory: files\_general/resource\_files/version\_specific/V#r#/output\_and\_granules/granules/coordinate\_granules/granules\_latlon/  
 - native variable granules directory: files\_general/resource\_files/version\_specific/V#r#/output\_and\_granules/granules/variable\_granules/granules\_native/
 - latlon variable granules directory: files\_general/resource\_files/version\_specific/V#r#/output\_and\_granules/granules/variable\_granules/granules\_latlon/
 - 1D variable granules directory: files\_general/resource\_files/version\_specific/V#r#/output\_and\_granules/granules/variable\_granules/granules\_1D/

Step 8 (if downloading any granules for the compendium): To prepare to download granules for the compendium, edit the file "granules\_to\_download.txt" by specifying the urls of the remotely hosted granules to download (further instructions provided in the file; full file path: "/files\_general/resource\_files/version\_specific/V#r#/input\_and\_templates/granules\_to\_download/granules\_to\_download.txt"). 

 
<br>
<br>

<h2 align="left">
Running the main scripts:
</h2>

<h5>
(Note: these steps assume you're in the "/ECCO-Dataset-Production/document_generator" directory):
</h5>

---

Step 0: If documenting a specific version (V#r#) of ECCO for the first time, wanting to re-create the granule directory structure (no overwriting will occur if the stucture already exists), or wanting a fresh copy of "granules\_to\_download.txt" to edit (with the prevoius version being automatically renamed according to current UTC time, which preserves it and prevents it from being used (only "granules\_to\_download.txt" is used)), execute:
 - python src/document\_generator/apps/step0\_generate\_preliminary\_file\_tree.py

Step 1: If needed, download granules (note that this script reads only the file "granules\_to\_download.txt", and not any renamed versions) via:
 - python src/document\_generator/apps/step1\_download\_granules.py

Step 2: Generate the compendium components via:
 - python src/document\_generator/apps/step2\_generate\_compendium\_sub\_components.py

Step 3: Compile the compendium via:
 - python src/document\_generator/apps/step3\_compile\_compendium.py

The compendium should then be found here: "/files\_general/compendium\_compilation\_output\_files/ECCO\_Dataset\_Catalog\_and\_Variable\_Compendium.pdf"
