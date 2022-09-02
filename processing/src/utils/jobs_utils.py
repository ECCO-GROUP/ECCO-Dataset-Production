"""
ECCO Dataset Production jobs utilities

Author: Duncan Bark

Contains functions for creating, calculating, and running jobs for processing.

"""

import sys
from pathlib import Path
from collections import defaultdict

# Local imports
main_path = Path(__file__).parent.parent.parent.resolve()
sys.path.append(f'{main_path / "src"}')
sys.path.append(f'{main_path / "src" / "utils"}')
from print_utils import printc
import file_utils as file_utils
import lambda_utils as lambda_utils
from ecco_gen_for_podaac_cloud import generate_netcdfs


# ==========================================================================================================================
# CALCULATE ALL JOBS
# ==========================================================================================================================
def calculate_all_jobs(groupings):
    """
    Create lists of all jobs to execute according to the groupings metadata files. Executed only if there is a line
    in jobs.txt that is just 'all'

    Args:
        groupings (dict): Dictionary of groupings. Key = name of grouping (i.e. 'latlon', 'native'), Value = grouping dict

    Returns:
        all_jobs (list): List of all jobs to execute (i.e. [[0,latlon,AVG_MON,all], [1,latlon,AVG_MON,all], ...])
    """
    # Go through each grouping and create a list of jobs based on each dataset, and the frequencies it supports
    jobs = defaultdict(list)
    for product_type, groupings in groupings.items():
        for i, grouping in enumerate(groupings):
            freqs = grouping['frequency'].split(', ')
            for freq in freqs:
                if freq == 'TI':
                    printc(f'\tTime-invariant groupings not currently tested/supported. Skipping', 'red')
                    continue
                if grouping['dimension'] == '1D':
                    jobs[f'1D'].append([i, product_type, freq, 'all'])
                if grouping['dimension'] == '2D':
                    jobs[f'2D_{product_type}'].append([i, product_type, freq, 'all'])
                if grouping['dimension'] == '3D':
                    jobs[f'3D_{product_type}'].append([i, product_type, freq, 'all'])

    # Create all_jobs by appending jobs in order of longest to shortest
    all_jobs = []
    all_jobs.extend(jobs['3D_native'])
    all_jobs.extend(jobs['3D_latlon'])
    all_jobs.extend(jobs['2D_native'])
    all_jobs.extend(jobs['2D_latlon'])
    all_jobs.extend(jobs['1D'])

    return all_jobs


# ==========================================================================================================================
# CREATE JOBS
# ==========================================================================================================================
def create_jobs(groupings, jobs_filename):
    """
    Create lists of all jobs to execute according to the groupings metadata files

    Args:
        groupings (dict): Dictionary of groupings. Key = name of grouping (i.e. 'latlon', 'native'), Value = grouping dict
        jobs_filename (str): String name to save the jobs file as

    Returns:
        None
    """
    all_jobs = []
    raw_jobs = {}
    menu = {}

    # Create menu, where menu is a collection of all products, their datasets and dimensions, and their supported frequencies
    # menu = {product: {freq: [(grouping_num, dimension, product, freq, dataset_name)]}}
    # i.e. {'latlon': {'AVG_DAY': [(0, 2D, 'latlon', 'AVG_DAY', 'dynamic sea surface height')]}}
    for grouping in groupings.values():
        for i, group in enumerate(grouping):
            product = group['product']
            dimension = group['dimension']
            if product not in raw_jobs:
                raw_jobs[product] = {}
            if product not in menu:
                menu[product] = defaultdict(list)
            if dimension not in raw_jobs[product]:
                raw_jobs[product][dimension] = defaultdict(list)
            frequencies = group['frequency'].split(', ')
            for freq in frequencies:
                menu[product][freq].append((i, dimension, product, freq, group['name']))
    

    # ========== <Product prompt> =================================================================
    # Prompt the user for which products they want to view. Options are 1D, latlon, and/or native
    product_order = ['1D', 'latlon', 'native']
    print(f'\nPRODUCT_TYPE')
    for i, product in enumerate(product_order):
        print(f'\t{i} -- {product}')
    product_input = input(f'Please select what products to view: ')
    user_continue, products_to_view = __selected_options_helper(product_input, 
                                                                product_order, 
                                                                'You selected the following products to view: ', 
                                                                '')
    if user_continue == 'n' or products_to_view == []:
        printc(f'Exiting', 'red')
        sys.exit()
    # ========== </Product prompt> ================================================================


    # ========== <Freqs, Datasets, and Timesteps> =================================================
    # Prompt user for which frequencies to view, for each product they selected previously
    frequency_order = ['SNAP', 'AVG_DAY', 'AVG_MON', 'TI']
    for product in products_to_view:
        print(f'\n{product}')
        product_freq_raw = menu[product].keys()
        product_freqs = []
        for freq in frequency_order:
            if freq in product_freq_raw:
                product_freqs.append(freq)
        print(f'\tOUTPUT_FREQUENCY')
        for i, freq in enumerate(product_freqs):
            print(f'\t\t{i} -- {freq}')
        frequency_input = input(f'\tPlease select what frequencies to view: ')
        user_continue, frequencies_to_view = __selected_options_helper(frequency_input, 
                                                                       product_freqs, 
                                                                       'You selected the following frequencies to view: ', 
                                                                       '\t')
        if user_continue == 'n' or frequencies_to_view == []:
            printc(f'Exiting', 'red')
            sys.exit()

        # Prompt user for which datasets to process, for each frequency they selected previously
        for freq in frequencies_to_view:
            datasets = menu[product][freq]
            print(f'\n\t\t{freq}')
            for i, ds in enumerate(datasets):
                print(f'\t\t\t{i} -- {ds[-1]} ({ds[1]})')
            datasets_input = input(f'\t\tPlease select which datasets you would like process: ')
            user_continue, datasets_to_process = __selected_options_helper(datasets_input, 
                                                                           datasets, 
                                                                           'You selected the following datasets to process: ', 
                                                                           '\t\t')
            if user_continue == 'n' or datasets_to_process == []:
                printc(f'Exiting', 'red')
                sys.exit()

            # Prompt user for which timesteps to process, for each dataset they selected previously
            # Timesteps can be entered as follows:
            # - X (i.e. 5) -> Processes X many timesteps from model start time
            # - X-Y (i.e. 10-15) -> Processes timesteps X to Y indices (exclusive) from model start time
            # - all -> Processes all available timesteps
            # - X, Y, W-Z (i.e. 5, 10, 200-300) -> Processes X from start, Y from start, and W to Z timestep indices. All separate jobs
            print(f'\n\t\t\tDATASETS')
            for dataset in datasets_to_process:
                print(f'\t\t\t\t{dataset}')
                timesteps = []
                timesteps_input = input(f'\t\t\tPlease enter what timesteps to process for the above dataset: ')
                raw_input = timesteps_input.lower().strip().replace(' ', '').split(',')
                for timestep_input in raw_input:
                    if '-' in timestep_input:
                        timestep_input = timestep_input.split('-')
                        timesteps.append([ts for ts in range(int(timestep_input[0]), int(timestep_input[1]))])
                    else:
                        timesteps.append(timestep_input)

                grouping_number = dataset[0]
                for timesteps_for_job in timesteps:
                    all_jobs.append([grouping_number, product, freq, timesteps_for_job])
    # ========== </Freqs, Datasets, and Timesteps> ================================================


    # ========== <Old Technique (less granular)> ==================================================
    # for product in product_order:
    #     for frequency in frequency_order:
    #         cur_options = menu[product][frequency]
    #         print(f'\n{menu_opt}')
    #         for i, option in enumerate(cur_options):
    #             print(f'\t{i} -- {option[-1]}')
    #         selected_options = input(f'Please select which datasets you would like process: ')


    #         if user_continue == 'y':
    #             for selected_option in selected_options:
    #                 group_num = cur_options[selected_option][0]
    #                 dim = cur_options[selected_option][1]
    #                 product = cur_options[selected_option][2]
    #                 freq = cur_options[selected_option][3]
    #                 raw_jobs[product][dim][freq].append(group_num)
    #         else:
    #             printc(f'Exiting', 'red')
    #             sys.exit()

    # all_jobs = []
    # for product, dim_groups in raw_jobs.items():
    #     for _, freq_groups in dim_groups.items():
    #         for frequency, groupings in freq_groups.items():
    #             for grouping in groupings:
    #                 all_jobs.append([grouping, product, frequency, 'all'])
    # ========== </Old technique (less granule) ===================================================


    # ========== <Save new created jobs> ==========================================================
    jobs_filename = 'jobs.txt'
    jobs_path = main_path / 'configs' / jobs_filename
    print(f'\nWriting to {jobs_path} with selected jobs\n')
    with open(jobs_path, 'w') as jobs_file:
        for job in all_jobs:
            if job[3] == 'all': job[3] = f"'{job[3]}'"
            jobs_string = f"[{job[0]},'{job[1]}','{job[2]}',{job[3]}]\n"
            jobs_file.write(jobs_string)
    # ========== </Save new created jobs> =========================================================

    return


def __selected_options_helper(user_input, 
                              list_of_options, 
                              selection_string, 
                              tabs):
    """
    Helps create jobs to present user with their selected options and confirm it's what they want

    Args:
        user_input (str): Raw user input from prompt
        list_of_options (list): List of options presented to the user
        selection_string (str): String to print with the options the user selected
        tabs (str): String containing some number of '\t' characters. Used for nice spacing on prints

    Returns:
        (user_continue, selected_options) (tuple):
            user_continue (str): Cleaned user input from selection confirmation prompt
            selected_options (list): List of options selected by the user 
    """
    # parse user input
    user_options = user_input.lower().strip().replace(' ', '').split(',')
    if user_options != ['']:
        # if user entered 'all', set selected_options_num to the range of the number of options available
        if user_options == ['all']:
            selected_options_num = range(len(list_of_options))
        # otherwise, set selected_options_num to the options the user entered
        else:
            selected_options_num = [int(opt) for opt in user_options]
    else:
        selected_options_num = []

    print(f'\n{tabs}{selection_string}')

    # for the num selected in selected_options_num, get the options from list_of_options
    selected_options = []
    if selected_options_num == []:
        print(f'\t{tabs}NONE')
    else:
        for selected_option_num in sorted(selected_options_num):
            # if the num entered is not valid, remove it from selceted_options
            if selected_option_num >= len(list_of_options) or selected_option_num < 0:
                print(f'\t{tabs}{selected_option_num} -- NOT A VALID OPTION. REMOVED')
                selected_options.remove(selected_option_num)
            # otherwise, add the option selected to selected_options
            else:
                selected_options.append(list_of_options[selected_option_num])
                print(f'\t{tabs}{selected_option_num} -- {list_of_options[selected_option_num]}')

    # prompt user to confirm selections
    user_continue = input(f'{tabs}Is this correct? (y/n) ').lower().strip()

    return user_continue, selected_options


# ==========================================================================================================================
# RUN JOBS
# ==========================================================================================================================
def run_job(job, 
            groupings_for_datasets, 
            dict_key_args, 
            product_generation_config, 
            aws_config, 
            s3=None, 
            lambda_client=None, 
            job_logs=None, 
            credentials=None):
    """
    Run the job provided either locally or via AWS Lambda

    Args:
        job (list): List (grouping_num, product_type, output_frequency, time_steps_to_process) for the current job
        groupings_for_dataset (dict): Dictionary of groupings. Key = name of grouping (i.e. 'latlon', 'native'), Value = grouping dict
        dict_key_args (dict): Dictionary of command line arguments to master_scipt.py
        product_generation_config (dict): Dictionary of product_generation_config.yaml config file
        aws_config (dict): Dictionary of aws_config.yaml config file
        s3 (optional, botocore.client.S3): boto3 client object for AWS S3
        lambda_client (optional, botocore.client.Lambda): boto3 client object for AWS Lambda
        job_logs (optional, dict): Dictionary containing information of each job and processing overall
        credentials (optional, dict): Dictionary containaing credentials information for AWS

    Returns:
        (num_jobs, job_logs, status) (tuple):
            num_jobs (int): Number of jobs submitted to Lambda or completed locally
            job_logs (dict): Dictionary containing information of each job and processing overall
            status (str): String that is either "SUCCESS" or "ERROR {error message}"
    """
    status = 'SUCCESS'
    num_jobs = 0

    # Check that option values are passed for lambda processing
    if dict_key_args['use_lambda']:
        if s3 == None or lambda_client == None or job_logs == None or credentials == None:
            status = f'ERROR Cannot create lambda jobs, unusable values for: s3, lambda_client, job_logs, or credentials'
            return (num_jobs, job_logs, status)

    # Get field time steps and field files
    (grouping_to_process, product_type, output_freq_code, time_steps_to_process) = job

    # Get values from args and configs
    use_lambda = dict_key_args['use_lambda']
    use_S3 = dict_key_args['use_S3'] or use_lambda

    if use_S3:
        source_bucket = aws_config['source_bucket']
        function_name_prefix = aws_config['function_name_prefix']
        source_bucket_folder_name = aws_config['bucket_subfolder']

    # Get grouping dictionary for job's product type
    curr_grouping = groupings_for_datasets[product_type][grouping_to_process]
        
    if output_freq_code == 'AVG_DAY':
        freq_folder = 'diags_daily'
        period_suffix = 'day_mean'
    elif output_freq_code == 'AVG_MON':
        freq_folder = 'diags_monthly'
        period_suffix = 'mon_mean'
    elif output_freq_code == 'SNAPSHOT':
        freq_folder = 'diags_inst'
        period_suffix = 'day_inst'
    else:
        status = f'ERROR valid options are AVG_DAY, AVG_MON, SNAPSHOT, you provided {output_freq_code}'
        return (num_jobs, job_logs, status)

    fields = curr_grouping['fields'].split(', ')
    dimension = curr_grouping['dimension']
    

    # ========== <Get files/timesteps> ============================================================
    if use_S3:
        s3_dir_prefix = f'{source_bucket_folder_name}/{freq_folder}'

        # Get files and timesteps using S3
        file_time_steps, status = file_utils.get_files_time_steps(fields, 
                                                                  period_suffix, 
                                                                  time_steps_to_process, 
                                                                  freq_folder, 
                                                                  use_S3, 
                                                                  curr_grouping,
                                                                  s3_dir_prefix=s3_dir_prefix, 
                                                                  source_bucket=source_bucket,
                                                                  derived_bucket=aws_config['derived_bucket'])
    else:
        # Get files and timesteps from a local directory
        file_time_steps, status = file_utils.get_files_time_steps(fields, 
                                                                  period_suffix, 
                                                                  time_steps_to_process, 
                                                                  freq_folder, 
                                                                  use_S3, 
                                                                  curr_grouping,
                                                                  model_output_dir=product_generation_config['model_output_dir'])

    # Check if status is not "SUCCESS" or if no timesteps were found
    if status != 'SUCCESS':
        status = f'ERROR grouping: {grouping_to_process}, product_type: {product_type}, output_freq_code: {output_freq_code}, time_steps_to_process: {time_steps_to_process}, Error: {status}'
        return (num_jobs, job_logs, status)
    else:
        field_files, time_steps = file_time_steps
        if len(time_steps) == 0:
            if use_S3:
                status = f'ERROR No files found in bucket {source_bucket} for fields {fields}'
            else:
                status = f'ERROR No files found in local directory {product_generation_config["model_output_dir"]} for fields {fields}, for time steps: {time_steps_to_process}'
            return (num_jobs, job_logs, status)
    # ========== </Get files/timesteps> ===========================================================


    # ========== <Start job> ======================================================================
    if use_lambda:
        num_jobs += lambda_utils.invoke_lambda(lambda_client, 
                                               job_logs, 
                                               time_steps, 
                                               dict_key_args, 
                                               product_generation_config, 
                                               aws_config, 
                                               job, 
                                               function_name_prefix, 
                                               dimension, 
                                               field_files, 
                                               credentials)
    else:
        # Call local generate_netcdfs function
        # Note: You can update this to utilize parallel processing
        # if you mimic the lambda functionality of batches and creating
        # separate payloads and function calls for each batch.
        payload = {
            'grouping_to_process': grouping_to_process,
            'product_type': product_type,
            'output_freq_code': output_freq_code,
            'time_steps_to_process': time_steps,
            'field_files': field_files,
            'product_generation_config': product_generation_config,
            'aws_config': aws_config,
            'use_S3': use_S3,
            'use_lambda': use_lambda,
            'credentials': credentials
        }

        generate_netcdfs(payload)
        num_jobs += 1
    # ========== </Start job> =====================================================================
    
    return (num_jobs, job_logs, status)