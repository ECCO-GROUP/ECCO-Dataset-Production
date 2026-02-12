# Thanks gweogle

import subprocess

import glob


num_vars_per_file_list = []
num_vars_max = 0



dir = "/Users/brucel/ecco/yip/ECCO-Dataset-Production/document_generator/z_document_files/v4r4/granule_datasets/granule_files/granules_latlon"

nc_files = glob.glob(f"{dir}/*")
    
for nc_file in nc_files:
    
    cmd1 = ["ncdump", "-h", nc_file] 
    cmd2 = ["grep", "long_name"]
    cmd3 = ["wc", "-l"]


    p1 = subprocess.Popen(cmd1, stdout=subprocess.PIPE, text=True)
    p2 = subprocess.Popen(cmd2, stdin=p1.stdout, stdout=subprocess.PIPE, text=True)
    p3 = subprocess.Popen(cmd3, stdin=p2.stdout, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)


    p2.stdout.close()
    p1.stdout.close()

    stdout, stderr = p3.communicate()

    if stdout:
        num_vars = int(stdout)
        num_vars_per_file_list.append(num_vars)
        num_vars_max = num_vars if num_vars > num_vars_max else num_vars_max

        print(num_vars)

    if stderr:
        print("STDERR Output:")
        print(stderr)
    #print(f"Return code: {p3.returncode}")

print(f"{num_vars_max}, {nc_files[num_vars_per_file_list.index(num_vars_max)]}")
