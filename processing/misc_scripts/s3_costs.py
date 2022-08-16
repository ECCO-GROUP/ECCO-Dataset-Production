green = '\033[92m'
cyan = '\033[96m'
endc = '\033[0m'

amt_tb = float(input('Enter the amount of data to store (in TB): '))
amt_gb = amt_tb * 1000

# Storage Costs
store_s3_standard = {50: 0.023, 450: 0.022, 500: 0.021, 'min': 0}
store_s3_intelligent_freq = {50: 0.023, 450: 0.022, 500: 0.021, 'min': 0}
s3_intelligent_obj = (0.0025, 1000)
store_s3_intelligent_infreq = {'def': 0.0125, 'min': 0}
store_s3_intelligent_arch_inst = {'def': 0.004, 'min': 0}
store_s3_intelligent_async_archive_access = {'def': 0.0036, 'min': 0}
store_s3_intelligent_async_deep_archive = {'def': 0.00099, 'min': 0}
store_s3_standard_infreq = {'def': 0.0125, 'min': 1}
store_s3_one_zone_infreq = {'def': 0.01, 'min': 1}
store_s3_glacier_instant = {'def': 0.004, 'min': 3}
store_s3_glacier_flex = {'def': 0.0036, 'min': 3}
store_s3_glacier_deep_arch = {'def': 0.00099, 'min': 6}

all_options = [store_s3_standard,
                store_s3_intelligent_freq,
                store_s3_intelligent_infreq,
                store_s3_intelligent_arch_inst,
                store_s3_intelligent_async_archive_access,
                store_s3_intelligent_async_deep_archive,
                store_s3_standard_infreq,
                store_s3_one_zone_infreq,
                store_s3_glacier_instant,
                store_s3_glacier_flex,
                store_s3_glacier_deep_arch,]

all_options_names = ['Standard',
                        'Intelligent Frequent Access',
                        'Intelligent Infrequent Access',
                        'Intelligent Archive Instant Access',
                        'Intelligent Archive Async Access',
                        'Intelligent Deep Archive Async Access',
                        'Standard Infrequent Access',
                        'One Zone Infrequent Access',
                        'Glacier Instant Retrieval',
                        'Glacier Flexible Retrieval',
                        'Glacier Deep Archive']

print()
for i, option in enumerate(all_options):
    done_tiering = False
    tmp_amt_tb = amt_tb
    total_cost = 0
    for opt_key, opt_value in option.items():
        if opt_key != 'def' and opt_key != 'min':
            if tmp_amt_tb >= opt_key:
                total_cost += (opt_key * 1000) * opt_value
                tmp_amt_tb -= opt_key
            elif not done_tiering:
                total_cost += (tmp_amt_tb * 1000) * opt_value
                done_tiering = True
        elif opt_key == 'def':
            total_cost += (tmp_amt_tb * 1000) * opt_value
        elif opt_key == 'min':
            min_cost = opt_value * total_cost
    if 'Intelligent' in all_options_names[i]:
        print(f'{green}{all_options_names[i]}{endc} storage cost for {amt_tb}TB per month: {cyan}${total_cost}{endc} (with {cyan}${min_cost}{endc} minimum cost). Intelligent tiering costs {cyan}${s3_intelligent_obj[0]}{endc} per {s3_intelligent_obj[1]} requests')
    else:
        print(f'{green}{all_options_names[i]}{endc} storage cost for {amt_tb}TB per month: {cyan}${total_cost}{endc} (with {cyan}${min_cost}{endc} minimum cost)')


# Retrieval Costs
ret_s3_standard = 0
ret_s3_intelligent = 0
ret_s3_intelligent_archive_access_expedited = 0.03
ret_s3_standard_infreq = 0.01
ret_s3_one_zone_infreq = 0.01
ret_s3_glacier_instant = 0.03
ret_s3_glacier_flex_exped = 0.03
ret_s3_glacier_flex_standard = 0.01
ret_s3_glacier_flex_bulk = 0
ret_s3_glacier_deep_arch_standard = 0.02
ret_s3_glacier_deep_arch_bulk = 0.0025

all_options = [ret_s3_standard,
                ret_s3_intelligent,
                ret_s3_intelligent_archive_access_expedited,
                ret_s3_standard_infreq,
                ret_s3_one_zone_infreq,
                ret_s3_glacier_instant,
                ret_s3_glacier_flex_exped,
                ret_s3_glacier_flex_standard,
                ret_s3_glacier_flex_bulk,
                ret_s3_glacier_deep_arch_standard,
                ret_s3_glacier_deep_arch_bulk]

all_options_names = ['Standard',
                        'Intelligent (all)',
                        'Intelligent Archive Access Expedited',
                        'Standard Infrequent Access',
                        'One Zone Infrequent Access',
                        'Glacier Instant Retrieval',
                        'Glacier Flexible Expedited',
                        'Glacier Flexible Standard',
                        'Glacier Flexible Bulk',
                        'Glacier Deep Archive Standard',
                        'Glacier Deep Archive Bulk']

print()
for i, option in enumerate(all_options):
    cost = amt_gb * option
    print(f'{green}{all_options_names[i]}{endc} retrieval cost for {amt_tb}TB per month: {cyan}${cost}{endc}')