import argparse 
import os
import sys


def file_exists(file_path):
    if not os.path.exists(file_path):
        print(f"Error: The file {file_path} does not exist.")
        sys.exit(1)

if __name__ == '__main__':
    user_input = input('Have you created the prerequisite files in your home directory?\n'
                       'They can be referred to in the download_insturctions.txt in this directory. (y/n): ')
    if user_input.lower() != 'y':
        print("Please create the prerequisite file and run the script again.")
        sys.exit(0)


    parser = argparse.ArgumentParser(description='Download granules for a given dataset type.')
    parser.add_argument('--type', required=False, type=str,
                        help="Type of the dataset to write. Should be one of 'native', 'latlon', '1D','coords', 'all'.", 
                        choices=['native', 'latlon', '1D','coords', 'all'],
                        default='all')
    # parser.add_argument('--to', required=False, type=str,
    parser.add_argument('--to', required=False, type=str,
                        help="What directory will the files will be saved", 
                        default='here')
    args = parser.parse_args()

    root_dir = os.path.dirname(os.path.realpath(__file__)) # <= this line alow to get the current path, "/document_generator/granule_datasets/"

    if args.to == 'here':
        dataset_dir = root_dir
    else:
        dataset_dir = os.path.join(root_dir, args.to)
    
    os.makedirs(dataset_dir, exist_ok=True)#<= this line create the folder where the granuls_datasets will be saved whether it exists or not!

    if args.type == 'native':
        natives_txt = os.path.join(dataset_dir, 'natives.txt')
        file_exists(natives_txt)#<= with check if 'natives.txt' existe, if 'yes', it means the links for natives granules are well setup.

        os.system(f'mkdir -p {dataset_dir}/natives')#<= create the folder where to save native granules datasts
        os.system(f'wget --no-verbose --no-clobber --continue -i {natives_txt} -P {dataset_dir}/natives/')#<= download with 'wget' tool
    
    elif args.type == 'latlon':
        latlon_txt = os.path.join(dataset_dir, 'latlon.txt')
        file_exists(latlon_txt)

        os.system(f'mkdir -p {dataset_dir}/latlon')
        os.system(f'wget --no-verbose --no-clobber --continue -i {latlon_txt} -P {dataset_dir}/latlon/')

    elif args.type == '1D':
        oneD_txt = os.path.join(dataset_dir, 'oneD.txt')
        file_exists(oneD_txt)

        os.system(f'mkdir -p {dataset_dir}/oneD')
        os.system(f'wget --no-verbose --no-clobber --continue -i {oneD_txt} -P {dataset_dir}/oneD/')

    elif args.type == 'coords':
        nat_coords_txt = os.path.join(dataset_dir, 'natives_coords.txt')
        file_exists(nat_coords_txt)

        os.system(f'mkdir -p {dataset_dir}/natives_coords')
        os.system(f'wget --no-verbose --no-clobber --continue -i {nat_coords_txt} -P {dataset_dir}/natives_coords/')

        ll_coords_txt = os.path.join(dataset_dir, 'latlon_coords.txt')
        file_exists(ll_coords_txt)

        os.system(f'mkdir -p {dataset_dir}/latlon_coords')
        os.system(f'wget --no-verbose --no-clobber --continue -i {ll_coords_txt} -P {dataset_dir}/latlon_coords/')
    
    else: # args.type == 'all'
        natives_txt = os.path.join(dataset_dir, 'natives.txt')
        file_exists(natives_txt)
        os.system(f'mkdir -p {dataset_dir}/natives')
        os.system(f'wget --no-verbose --no-clobber --continue -i {natives_txt} -P {dataset_dir}/natives/')
        latlon_txt = os.path.join(dataset_dir, 'latlon.txt')
        file_exists(latlon_txt)
        os.system(f'mkdir -p {dataset_dir}/latlon')
        os.system(f'wget --no-verbose --no-clobber --continue -i {latlon_txt} -P {dataset_dir}/latlon/')

        oneD_txt = os.path.join(dataset_dir, 'oneD.txt')
        file_exists(oneD_txt)
        os.system(f'mkdir -p {dataset_dir}/oneD')
        os.system(f'wget --no-verbose --no-clobber --continue -i {oneD_txt} -P {dataset_dir}/oneD/')
    
        nat_coords_txt = os.path.join(dataset_dir, 'natives_coords.txt')
        file_exists(nat_coords_txt)
        os.system(f'mkdir -p {dataset_dir}/natives_coords')
        os.system(f'wget --no-verbose --no-clobber --continue -i {nat_coords_txt} -P {dataset_dir}/natives_coords/')

        ll_coords_txt = os.path.join(dataset_dir, 'latlon_coords.txt')
        file_exists(ll_coords_txt)
        os.system(f'mkdir -p {dataset_dir}/latlon_coords')
        os.system(f'wget --no-verbose --no-clobber --continue -i {ll_coords_txt} -P {dataset_dir}/latlon_coords/')


#------
