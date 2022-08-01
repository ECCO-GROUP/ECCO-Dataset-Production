import sys
from pathlib import Path
from collections import defaultdict

# Local imports
main_path = Path(__file__).parent.parent.resolve()
sys.path.append(f'{main_path / "src"}')
sys.path.append(f'{main_path / "src" / "utils"}')
import file_utils as file_utils
import lambda_utils as lambda_utils
from ecco_gen_for_podaac_cloud import generate_netcdfs


# ==========================================================================================================================
# CALCULATE ALL JOBS
# ==========================================================================================================================
def calculate_all_jobs(groupings):
    # all_groupings = {'latlon':latlon_groupings, 'native':native_groupings}
    jobs = defaultdict(list)
    for product_type, groupings in groupings.items():
        for i, grouping in enumerate(groupings):
            freqs = grouping['frequency'].split(', ')
            for freq in freqs:
                if freq == 'TI':
                    print(f'Time-invariant groupings not currently tested/supported. Exiting')
                    sys.exit()
                if grouping['dimension'] == '1D':
                    jobs[f'1D'].append([i, product_type, freq, 'all'])
                if grouping['dimension'] == '2D':
                    jobs[f'2D_{product_type}'].append([i, product_type, freq, 'all'])
                if grouping['dimension'] == '3D':
                    jobs[f'3D_{product_type}'].append([i, product_type, freq, 'all'])

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
def selected_options_helper(user_input, list_of_options, selection_string, tabs):
    user_options = user_input.lower().strip().replace(' ', '').split(',')
    if user_options != ['']:
        if user_options == ['all']:
            selected_options_num = range(len(list_of_options))
        else:
            selected_options_num = [int(opt) for opt in user_options]
    else:
        selected_options_num = []

    # print(f'\nYou selected the following datasets to process for frequency "{menu_opt}": ')
    print(f'\n{tabs}{selection_string}')

    selected_options = []
    if selected_options_num == []:
        print(f'\t{tabs}NONE')
    else:
        for selected_option_num in sorted(selected_options_num):
            if selected_option_num >= len(list_of_options) or selected_option_num < 0:
                print(f'\t{tabs}{selected_option_num} -- NOT A VALID OPTION. REMOVED')
                selected_options.remove(selected_option_num)
            else:
                selected_options.append(list_of_options[selected_option_num])
                print(f'\t{tabs}{selected_option_num} -- {list_of_options[selected_option_num]}')
    user_continue = input(f'{tabs}Is this correct? (y/n) ').lower().strip()
    return user_continue, selected_options


def create_jobs(groupings):
    all_jobs = []
    raw_jobs = {}
    menu = {}
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
    
    product_order = ['1D', 'latlon', 'native']
    print(f'\nPRODUCT_TYPE')
    for i, product in enumerate(product_order):
        print(f'\t{i} -- {product}')
    product_input = input(f'Please select what products to view: ')
    user_continue, products_to_view = selected_options_helper(product_input, product_order, 'You selected the following products to view: ', '')
    if user_continue == 'n' or products_to_view == []:
        print(f'Exiting')
        sys.exit()

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
        user_continue, frequencies_to_view = selected_options_helper(frequency_input, product_freqs, 'You selected the following frequencies to view: ', '\t')
        if user_continue == 'n' or frequencies_to_view == []:
            print(f'Exiting')
            sys.exit()

        for freq in frequencies_to_view:
            datasets = menu[product][freq]
            print(f'\n\t\t{freq}')
            for i, ds in enumerate(datasets):
                print(f'\t\t\t{i} -- {ds[-1]}')
            datasets_input = input(f'\t\tPlease select which datasets you would like process: ')
            user_continue, datasets_to_process = selected_options_helper(datasets_input, datasets, 'You selected the following datasets to process: ', '\t\t')
            if user_continue == 'n' or datasets_to_process == []:
                print(f'Exiting')
                sys.exit()

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
    #             print(f'Exiting')
    #             sys.exit()

    # all_jobs = []
    # for product, dim_groups in raw_jobs.items():
    #     for _, freq_groups in dim_groups.items():
    #         for frequency, groupings in freq_groups.items():
    #             for grouping in groupings:
    #                 all_jobs.append([grouping, product, frequency, 'all'])

    jobs_filename = 'created_jobs.txt'
    jobs_path = Path('/Users/bark/Documents/ECCO_GROUP/ECCO-Dataset-Production/aws/configs') / jobs_filename
    print(f'\nWriting to {jobs_path} with selected jobs\n')
    with open(jobs_path, 'w') as jobs_file:
        for job in all_jobs:
            # jobs_string = f'{job[0]},{job[1]},{job[2]},{job[3]}\n'
            if job[3] == 'all': job[3] = f"'{job[3]}'"
            jobs_string = f"[{job[0]},'{job[1]}','{job[2]}',{job[3]}]\n"
            jobs_file.write(jobs_string)

    return jobs_filename


# ==========================================================================================================================
# RUN JOBS
# ==========================================================================================================================
def run_job(job, groupings_for_datasets, dict_key_args, product_generation_config, aws_config, debug_mode, s3=None, lambda_client=None, job_logs=None, credentials=None):
    status = 'SUCCESS'
    num_jobs = 0

    # Check that option values are passed for lambda processing
    if dict_key_args['use_lambda']:
        if s3 == None or lambda_client == None or job_logs == None or credentials == None:
            status = f'ERROR Cannot create lambda jobs, unusable values for: s3, lambda_client, job_logs, or credentials'
            return (num_jobs, job_logs, status)

    # Get field time steps and field files
    (grouping_to_process, product_type, output_freq_code, num_time_steps_to_process) = job

    # Get values from args and configs
    local = not dict_key_args['use_cloud']
    use_lambda = dict_key_args['use_lambda']
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
        # print('valid options are AVG_DAY, AVG_MON, SNAPSHOT')
        # print(f'you provided {output_freq_code}. Skipping job')
        status = f'ERROR valid options are AVG_DAY, AVG_MON, SNAPSHOT, you provided {output_freq_code}'
        return (num_jobs, job_logs, status)

    fields = curr_grouping['fields'].split(', ')
    dimension = curr_grouping['dimension']
    filename = curr_grouping['filename']
    
    if not local:
        s3_dir_prefix = f'{source_bucket_folder_name}/{freq_folder}'
        
        file_time_steps, status = file_utils.get_files_time_steps_s3(s3, 
                                                                        fields, 
                                                                        s3_dir_prefix, 
                                                                        period_suffix, 
                                                                        source_bucket, 
                                                                        num_time_steps_to_process)
        if status == 'SKIP':
            status = f'ERROR grouping: {grouping_to_process}, product_type: {product_type}, output_freq_code: {output_freq_code}, num_time_steps_to_process: {num_time_steps_to_process}'
            return (num_jobs, job_logs, status)
        else:
            field_files, field_time_steps, time_steps = file_time_steps
            if len(time_steps) == 0:
                status = f'ERROR No files found in bucket {source_bucket} for fields {fields}'
                return (num_jobs, job_logs, status)

    else:
        file_time_steps, status = file_utils.get_files_time_steps_local(fields, 
                                                                        grouping_to_process, 
                                                                        product_generation_config, 
                                                                        freq_folder, 
                                                                        period_suffix, 
                                                                        num_time_steps_to_process, 
                                                                        product_type, 
                                                                        output_freq_code)
        if status == 'SKIP':
            status = f'ERROR grouping: {grouping_to_process}, product_type: {product_type}, output_freq_code: {output_freq_code}, num_time_steps_to_process: {num_time_steps_to_process}'
            return (num_jobs, job_logs, status)
        else:
            field_files, field_time_steps, time_steps = file_time_steps
            if len(time_steps) == 0:
                status = f'ERROR No files found in local directory {product_generation_config["model_output_dir"]} for fields {fields}'
                return (num_jobs, job_logs, status)

    # **********
    # CREATE LAMBDA REQUEST FOR EACH "JOB"
    # **********
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
                                                credentials,
                                                debug_mode)
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
            'aws_metadata': aws_config,
            'debug_mode': debug_mode,
            'local': local,
            'use_lambda': use_lambda,
            'credentials': credentials,
            'use_workers_to_download': product_generation_config['use_workers_to_download']
        }

        generate_netcdfs(payload)
    
    return (num_jobs, job_logs, status)