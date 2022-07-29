import sys
from pathlib import Path
from collections import defaultdict


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