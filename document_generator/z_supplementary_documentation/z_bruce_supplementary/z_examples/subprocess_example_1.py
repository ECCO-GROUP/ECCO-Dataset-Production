# Thanks gweogle

import subprocess

import glob


var = "/Users/brucel/ecco/yip/ECCO-Dataset-Production/document_generator/z_document_files/v4r4/granule_datasets/coordinate_files/coordinate_files_latlon/GRID_GEOMETRY_ECCO_V4r4_latlon_0p50deg.nc"
#dir = "/Users/brucel/ecco/yip/ECCO-Dataset-Production/document_generator/z_document_files/v4r4/granule_datasets/granule_files/granules_latlon"

cmd1 = ["ncdump", "-h", var] 
cmd2 = ["grep", "long_name"]
cmd3 = ["wc", "-l"]


p1 = subprocess.Popen(cmd1, stdout=subprocess.PIPE, text=True)
p2 = subprocess.Popen(cmd2, stdin=p1.stdout, stdout=subprocess.PIPE, text=True)
p3 = subprocess.Popen(cmd3, stdin=p2.stdout, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)


p2.stdout.close()
p1.stdout.close()

stdout, stderr = p3.communicate()

if stdout:
    print("STDOUT Output:")
    print(stdout)
if stderr:
    print("STDERR Output:")
    print(stderr)
print(f"Return code: {p3.returncode}")


