from collections import defaultdict
import os
import json
import argparse
import numpy as np
from pathlib import Path
import matplotlib.pyplot as plt
from scipy.optimize import curve_fit
import matplotlib.patches as mpatches

def create_parser():
    parser = argparse.ArgumentParser()

    parser.add_argument('--test_batch', default=False, action='store_true',
                        help='Use logs in the "/test_batch/" subdirectory of "/logs/"')

    parser.add_argument('--best_fit', default=False, action='store_true',
                        help='Try to make an exponential best fit. WARNING: May or may not work')

    parser.add_argument('--single_memory', default=False, action='store_true',
                        help='Plot time and cost for a single memory value. Assumes the only memory value present')

    parser.add_argument('--normalize', default=False, action='store_true',
                        help='Normalize time and cost values by vertical level')
    return parser

# Parse command line arguments
parser = create_parser()
args = parser.parse_args()
dict_key_args = {key: value for key, value in args._get_kwargs()} 

if dict_key_args['test_batch']:
    logs_dir = Path(__file__).parent.resolve() / 'logs' / 'test_batch'
else:
    logs_dir = Path(__file__).parent.resolve() / 'logs'

log_files = [logs_dir / lf for lf in os.listdir(logs_dir) if 'FINAL' in lf and ':' in lf]

# Get data from log files
memory_data_all = {}
all_total_memories_dict = {}
all_total_times_dict = {}
all_total_costs_dict = {}
all_indiv_memories_dict = {}
all_indiv_times_dict = {}
all_indiv_costs_dict = {}
for log_file in log_files:
    with open(log_file, 'r') as lf:
        lf_json = json.load(lf)

    example_log = lf_json[list(lf_json.keys())[-1]]
    curr_mem = example_log['report'][0]['Memory Size (MB)']
    if 'dimension' not in example_log['data']:
        curr_dim = '2D'
    else:
        curr_dim = example_log['data']['dimension']

    if curr_dim not in memory_data_all:
        memory_data_all[curr_dim] = {}
    if curr_mem not in memory_data_all[curr_dim]:
        memory_data_all[curr_dim][curr_mem] = defaultdict(list)


    normalization = 1
    total_normalization = 1
    if dict_key_args['normalize']:
        if curr_dim == '3D':
            normalization = 150
            total_normalization = 10 * normalization
        else:
            normalization = 9
            total_normalization = 5 * normalization

    for k, i in lf_json.items():
        if k == 'Master Script Total Time (s)':
            memory_data_all[curr_dim][curr_mem]['Master Script Total Time (s)'].append(i)
        elif k == 'Cost Information':
            memory_data_all[curr_dim][curr_mem]['Total Billed Time (s)'].append(i[f'{curr_mem} MB Total Billed Time (s)'] / total_normalization)
            memory_data_all[curr_dim][curr_mem]['Total Cost ($)'].append(i[f'Total Cost'] / total_normalization)

            if curr_dim not in all_total_memories_dict:
                all_total_times_dict[curr_dim] = defaultdict(list)
                all_total_costs_dict[curr_dim] = defaultdict(list)
                all_total_memories_dict[curr_dim] = defaultdict(int)
            all_total_times_dict[curr_dim][curr_mem].append(i[f'{curr_mem} MB Total Billed Time (s)'] / total_normalization)
            all_total_costs_dict[curr_dim][curr_mem].append(i[f'Total Cost'] / total_normalization)
            all_total_memories_dict[curr_dim][curr_mem] += 1
        else:
            if curr_dim not in all_indiv_memories_dict:
                all_indiv_memories_dict[curr_dim] = defaultdict(int)
                all_indiv_times_dict[curr_dim] = defaultdict(list)
                all_indiv_costs_dict[curr_dim] = defaultdict(list)

            memory_data_all[curr_dim][curr_mem]['Individual Time (s)'].append(i['report'][0]['Billed Duration (s)'] / normalization)
            memory_data_all[curr_dim][curr_mem]['Individual Cost ($)'].append(i['report'][0]['Cost Estimate (USD)'] / normalization)
            memory_data_all[curr_dim][curr_mem]['Max Memory Used (MB)'].append(i['report'][0]['Max Memory Used (MB)'])
            if 'Init Duration (s)' in i['report'][0]:
                memory_data_all[curr_dim][curr_mem]['Init Duration (s)'].append(i['report'][0]['Init Duration (s)'])
            else:
                memory_data_all[curr_dim][curr_mem]['Init Duration (s)'].append(-1)
            memory_data_all[curr_dim][curr_mem]['TOTAL DURATION'].append(i['extra']['Duration (s)']['TOTAL'])
            memory_data_all[curr_dim][curr_mem]['SCRIPT DURATION'].append(i['extra']['Duration (s)']['SCRIPT'])

            # Due to issue in lambda code, and logging (TODO: Fix), need to subtract out the I/O values from both TOTAL and SCRIPT
            dl_t = i['extra']['Duration (s)']['DOWNLOAD']
            nc_t = i['extra']['Duration (s)']['NETCDF']
            up_t = i['extra']['Duration (s)']['UPLOAD']
            memory_data_all[curr_dim][curr_mem]['TOTAL DURATION'][-1] -= (dl_t + nc_t + up_t)
            memory_data_all[curr_dim][curr_mem]['SCRIPT DURATION'][-1] -= (dl_t + nc_t + up_t)
            memory_data_all[curr_dim][curr_mem]['TOTAL DURATION'][-1] /= normalization
            memory_data_all[curr_dim][curr_mem]['SCRIPT DURATION'][-1] /= normalization

            all_indiv_memories_dict[curr_dim][curr_mem] += 1
            all_indiv_times_dict[curr_dim][curr_mem].append(i['report'][0]['Billed Duration (s)'] / normalization)
            all_indiv_costs_dict[curr_dim][curr_mem].append(i['report'][0]['Cost Estimate (USD)'] / normalization)

for curr_dim in memory_data_all.keys():
    # Restructure data
    include_10240 = False
    memory_data = {}
    all_total_memories = []
    all_total_times = []
    all_total_costs = []
    all_indiv_memories = []
    all_indiv_times = []
    all_indiv_costs = []
    all_indiv_total_duration = []
    all_indiv_script_duration = []
    for mem, num_mem in sorted(all_total_memories_dict[curr_dim].items()):
        if not include_10240 and mem == 10240:
            continue
        memory_data[mem] = memory_data_all[curr_dim][mem]
        all_total_memories.extend([mem for _ in range(num_mem)])
        all_total_times.extend(all_total_times_dict[curr_dim][mem])
        all_total_costs.extend(all_total_costs_dict[curr_dim][mem])
        all_indiv_memories.extend([mem for _ in range(all_indiv_memories_dict[curr_dim][mem])])
        all_indiv_times.extend(all_indiv_times_dict[curr_dim][mem])
        all_indiv_costs.extend(all_indiv_costs_dict[curr_dim][mem])

        all_indiv_total_duration.extend(memory_data[mem]['TOTAL DURATION'])
        all_indiv_script_duration.extend(memory_data[mem]['SCRIPT DURATION'])
    total_script_duration_diff = list(np.subtract(all_indiv_total_duration, all_indiv_script_duration))

    if dict_key_args['best_fit']:
        # best fits
        # exponential technique
        def time_func(x, a, x0, tau, c):
            return a * np.exp(-(x-x0) / tau) + c
        def cost_func(x, a, x0, tau, c):
            return a * np.exp((x-x0) / tau) + c

        # exponential fit best guesses
        if dict_key_args['normalize']:
            if curr_dim == '2D':
                p0_total_time = np.array([100, 1, 100, 1.5])
                p0_total_cost = np.array([.0001, .0001, 2000, 0.0003])
                p0_indiv_time = np.array([100, 1, 100, 1.5])
                p0_indiv_cost = np.array([.0002, 1, 1000, 0.00005])
                p0_indiv_total_dur = np.array([100, 1, 100, 1.5])
                p0_indiv_script_dur = np.array([100, 1, 100, 1.5])
                p0_indiv_diff_dur = np.array([100, 1, 100, 0])
            elif curr_dim == '3D':
                p0_total_time = np.array([100, 1, 100, 1.5])
                p0_total_cost = np.array([.00001, .0001, 2000, 0.0003])
                p0_indiv_time = np.array([100, 1, 100, 1.5])
                p0_indiv_cost = np.array([.0001, 1, 1000, 0.00003])
                p0_indiv_total_dur = np.array([100, 1, 100, 1.5])
                p0_indiv_script_dur = np.array([100, 1, 100, 1.5])
                p0_indiv_diff_dur = np.array([100, 1, 100, 0.1])  
        else:
            if curr_dim == '2D':
                p0_total_time = np.array([600, 1, 500, 80])
                p0_total_cost = np.array([.0001, .0001, 2000, 0.002])
                p0_indiv_time = np.array([200, 1, 500, 15])
                p0_indiv_cost = np.array([.00002, 1, 2000, 0.00045])
                p0_indiv_total_dur = np.array([200, 1, 500, 15])
                p0_indiv_script_dur = np.array([200, 1, 500, 14])
                p0_indiv_diff_dur = np.array([200, 1, 5000, 0])
            elif curr_dim == '3D':
                p0_total_time = np.array([1000, 1, 1000, 1600])
                p0_total_cost = np.array([.01, .0001, 2000, 0.05])
                p0_indiv_time = np.array([800, 1, 500, 150])
                p0_indiv_cost = np.array([.0001, 1, 1000, 0.005])
                p0_indiv_total_dur = np.array([200, 1, 300, 170])
                p0_indiv_script_dur = np.array([500, 1, 500, 150])
                p0_indiv_diff_dur = np.array([500, 1, 5000, 0])

        # polyfit degress
        deg_time = 2
        deg_cost = 2

        # total fitting ===================================================================================
        total_mem_fit = np.linspace(sorted(all_total_memories)[0], sorted(all_total_memories)[-1], 100)
        all_total_memories = all_total_memories
        all_total_times = all_total_times
        all_total_costs = all_total_costs

        # total_time
        # print(curr_dim)
        total_time_p0_fit = time_func(total_mem_fit, *p0_total_time)
        popt, pcov = curve_fit(time_func, all_total_memories, all_total_times, p0=p0_total_time)
        total_time_exp_fit = time_func(total_mem_fit, *popt)

        # total_mem_fit_hat = np.log10(total_mem_fit)
        # total_time_fit_z = np.polyfit(all_total_memories, all_total_times, deg_time)
        # total_time_fit = np.poly1d(total_time_fit_z)

        # total_cost
        # print(curr_dim)
        total_cost_p0_fit = cost_func(total_mem_fit, *p0_total_cost)
        popt, pcov = curve_fit(cost_func, all_total_memories, all_total_costs, p0=p0_total_cost)
        total_cost_exp_fit = cost_func(total_mem_fit, *popt)
        # total_cost_fit_z = np.polyfit(all_total_memories, all_total_costs, deg_cost)
        # total_cost_fit = np.poly1d(total_cost_fit_z)


        # individual fitting ==============================================================================
        indiv_mem_fit = np.linspace(sorted(all_indiv_memories)[0], sorted(all_indiv_memories)[-1], 100)
        all_indiv_memories = all_indiv_memories
        all_indiv_times = all_indiv_times
        all_indiv_costs = all_indiv_costs

        # indiv_time
        # print(curr_dim)
        indiv_time_p0_fit = time_func(indiv_mem_fit, *p0_indiv_time)
        popt, pcov = curve_fit(time_func, all_indiv_memories, all_indiv_times, p0=p0_indiv_time)
        indiv_time_exp_fit = time_func(indiv_mem_fit, *popt)

        # indiv_mem_fit_hat = np.log10(indiv_mem_fit)
        # indiv_time_fit_z = np.polyfit(all_indiv_memories, all_indiv_times, deg_time)
        # indiv_time_fit = np.poly1d(indiv_time_fit_z)

        # indiv_cost
        # print(curr_dim)
        indiv_cost_p0_fit = cost_func(indiv_mem_fit, *p0_indiv_cost)
        popt, pcov = curve_fit(cost_func, all_indiv_memories, all_indiv_costs, p0=p0_indiv_cost)
        indiv_cost_exp_fit = cost_func(indiv_mem_fit, *popt)
        # indiv_cost_fit_z = np.polyfit(all_indiv_memories, all_indiv_costs, deg_cost)
        # indiv_cost_fit = np.poly1d(indiv_cost_fit_z)

        # indiv total duration
        # print(curr_dim)
        indiv_total_duration_p0_fit = time_func(indiv_mem_fit, *p0_indiv_total_dur)
        popt, pcov = curve_fit(time_func, all_indiv_memories, all_indiv_total_duration, p0=p0_indiv_total_dur)
        indiv_total_duration_exp_fit = time_func(indiv_mem_fit, *popt)

        # indiv script duration
        # print(curr_dim)
        indiv_script_duration_p0_fit = time_func(indiv_mem_fit, *p0_indiv_script_dur)
        popt, pcov = curve_fit(time_func, all_indiv_memories, all_indiv_script_duration, p0=p0_indiv_script_dur)
        indiv_script_duration_exp_fit = time_func(indiv_mem_fit, *popt)

        # indiv total/script difference duration
        # print(curr_dim)
        indiv_duration_diff_p0_fit = time_func(indiv_mem_fit, *p0_indiv_diff_dur)
        popt, pcov = curve_fit(time_func, all_indiv_memories, total_script_duration_diff, p0=p0_indiv_diff_dur)
        indiv_duration_diff_exp_fit = time_func(indiv_mem_fit, *popt)

    # plotting ========================================================================================
    fig1, ((ax1, ax2)) = plt.subplots(1, 2, figsize=(14,6))

    if dict_key_args['single_memory']:
        values = memory_data[list(memory_data.keys())[0]]
        ax1.grid()
        ax2.grid()
        ax1.scatter(values['Total Cost ($)'], values['Total Billed Time (s)'])
        ax1.axvline(np.mean(values['Total Cost ($)']), c='tomato')
        ax1.axhline(np.mean(values['Total Billed Time (s)']), c='forestgreen')

        ax2.scatter(values['Individual Cost ($)'], values['Individual Time (s)'])
        ax2.axvline(np.mean(values['Individual Cost ($)']), c='tomato')
        ax2.axhline(np.mean(values['Individual Time (s)']), c='forestgreen')
    else:
        ax1_t = ax1.twinx()
        ax2_t = ax2.twinx()
        for k, i in sorted(memory_data.items()):
            ax1.axvline(k, c='black', alpha=0.5, ls='--', label=f'{k} MB')
            # ax1.text(k, 440, f'{k} MB', c='black', rotation='vertical', va='bottom')
            ax2.axvline(k, c='black', alpha=0.5, ls='--', label=f'{k} MB')
            # ax2.text(k, 117, f'{k} MB', c='black', rotation='vertical', va='bottom')
            for n, j in i.items():
                # if curr_dim == '3D':
                #     j = np.divide(j, 150)
                #     j_tot = np.divide(j, 10)
                # else:
                #     j = np.divide(j, 9)
                #     j_tot = np.divide(j, 5)
                if n == 'Total Billed Time (s)':
                # if n == 'Master Script Total Time (s)':
                    ax1.plot([k]*len(j), j, 'o', c='forestgreen', label=f'{k} MB')
                elif n == 'Total Cost ($)':
                    ax1_t.plot([k]*len(j), j, 'x', c='tomato', label=f'{k} MB')
                elif n == 'Individual Time (s)':
                    ax2.plot([k]*len(j), j, 'o', c='forestgreen')
                elif n == 'Individual Cost ($)':
                    ax2_t.plot([k]*len(j), j, 'x', c='tomato')

    if dict_key_args['best_fit']:
        # plot exponential fits
        ax1.plot(total_mem_fit, total_time_exp_fit, '-', c='forestgreen')
        ax1_t.plot(total_mem_fit, total_cost_exp_fit, '-', c='tomato')
        ax2.plot(indiv_mem_fit, indiv_time_exp_fit, '-', c='forestgreen')
        ax2_t.plot(indiv_mem_fit, indiv_cost_exp_fit, '-', c='tomato')

        # plot exponential inital guesses
        # ax1.plot(total_mem_fit, total_time_p0_fit, '--', c='green')
        # ax1_t.plot(total_mem_fit, total_cost_p0_fit, '--', c='red')
        # ax2.plot(indiv_mem_fit, indiv_time_p0_fit, '--', c='green')
        # ax2_t.plot(indiv_mem_fit, indiv_cost_p0_fit, '--', c='red')

        # plot polyfit best fits
        # ax1.plot(total_mem_fit, total_time_fit(total_mem_fit), c='forestgreen')
        # ax1_t.plot(total_mem_fit, total_cost_fit(total_mem_fit), c='tomato')
        # ax2.plot(indiv_mem_fit, indiv_time_fit(indiv_mem_fit), c='forestgreen')
        # ax2_t.plot(indiv_mem_fit, indiv_cost_fit(indiv_mem_fit), c='tomato')

    if dict_key_args['single_memory']:
        # plot labels and information
        ax1.set_title('Total Billed Time (s)\nvs\nTotal Cost ($)')
        ax1.set_ylabel('Time (s)')
        ax1.set_xlabel('Cost ($)')
        ax2.set_title('Indiv Job Billed Time (s)\nvs\nIndiv Job Cost ($)')
        ax2.set_ylabel('Time (s)')
        ax2.set_xlabel('Cost ($)')
    else:
        # plot labels and information
        ax1.set_title(f'Total Billed Time (s) & Total Cost ($)\nvs\nMemory Allocated (MB)\n({curr_dim}, Normalized by # of Vertical Levels)')
        ax1.set_ylabel('Time (s)', c='forestgreen')
        ax1.set_xlabel('Memory (MB)')
        ax1_t.set_ylabel('Cost ($)', c='tomato')
        ax2.set_title(f'Indiv Job Billed Time (s) & Indiv Job Cost ($)\nvs\nMemory Allocated (MB)\n({curr_dim}, Normalized by # of Vertical Levels)')
        ax2.set_ylabel('Time (s)', c='forestgreen')
        ax2.set_xlabel('Memory (MB)')
        ax2_t.set_ylabel('Cost ($)', c='tomato')

        # adjust ticks
        min_memory = min(memory_data.keys())
        max_memory = max(memory_data.keys())
        memory_range = max_memory - min_memory
        min_memory_plot = int(round((min_memory - 0.1*memory_range)/100)) * 100
        max_memory_plot = int(round((max_memory + 0.1*memory_range)/100)) * 100
        if include_10240:
            memory_ticks = np.arange(0, max_memory_plot, 500)
        else:
            memory_ticks = np.arange(0, max_memory_plot, 250)
        ax1.set_xticks(memory_ticks)
        ax1.tick_params(axis='x', which='major', direction='out', bottom=True, length=5, labelrotation=80)
        ax1.tick_params(axis='y', which='major', direction='out', bottom=True, length=5, colors='forestgreen')
        ax1_t.tick_params(axis='y', which='major', direction='out', bottom=True, length=5, colors='tomato')
        ax2.set_xticks(memory_ticks)
        ax2.tick_params(axis='x', which='major', direction='out', bottom=True, length=5, labelrotation=80)
        ax2.tick_params(axis='y', which='major', direction='out', bottom=True, length=5, colors='forestgreen')
        ax2_t.tick_params(axis='y', which='major', direction='out', bottom=True, length=5, colors='tomato')

        ax1.set_xlim(min_memory_plot)
        ax2.set_xlim(min_memory_plot)

    # plt.show()
    fig1.tight_layout()
    fig1.savefig(f'./logs/log_plots/{curr_dim}_log_plots.png', dpi=200)

    # setup plots for "zoomed"
    if include_10240:
        ax1.set_xlim(0, 3500)
        ax2.set_xlim(0, 3500)

        fig1.tight_layout()
        fig1.savefig(f'./logs/log_plots/{curr_dim}_log_plots_zoom.png', dpi=200)


    # plot script and total duration for each memory
    fig2, (ax3) = plt.subplots(1, 1, figsize=(7,6))

    if dict_key_args['single_memory']:
        values = memory_data[list(memory_data.keys())[0]]
        ax3.grid()
        ax3.scatter(values['TOTAL DURATION'], values['SCRIPT DURATION'], c='red')
        ax3.scatter(values['TOTAL DURATION'], values['Individual Time (s)'], c='green')
        ax3.scatter(values['TOTAL DURATION'], total_script_duration_diff, c='purple')

        ax3.axhline(0, ls='--', c='black', alpha=0.5)

        red_patch = mpatches.Patch(color='red', label='Script duration (s)')
        green_patch = mpatches.Patch(color='green', label='Billed duration (s)')
        purple_patch = mpatches.Patch(color='purple', label='I/O duration (s)')
        plt.legend(handles=[red_patch, purple_patch, green_patch])

        ax3.set_title('Code Duration Values (s) vs Memory (MB)')
        ax3.set_ylabel('Time (s)')
        ax3.set_xlabel('Total duration (s)')
    else:
        for k, i in sorted(memory_data.items()):
            ax3.axvline(k, c='black', alpha=0.5, ls='--', label=f'{k} MB')
            for n, j in i.items():
                # if curr_dim == '3D':
                #     j = np.divide(j, 150)
                # else:
                #     j = np.divide(j, 9)
                if n == 'TOTAL DURATION':
                    ax3.plot([k]*len(j), j, 'o', c='blue', label=f'{k} MB')
                elif n == 'SCRIPT DURATION':
                    ax3.plot([k]*len(j), j, 'o', c='red', label=f'{k} MB')
                elif n == 'Individual Time (s)':
                    ax3.plot([k]*len(j), j, 'o', c='green')
        # if curr_dim == '3D':
        #     norm_dur = np.divide(total_script_duration_diff, 150)
        # else:
        #     norm_dur = np.divide(total_script_duration_diff, 9)
        ax3.plot(all_indiv_memories, total_script_duration_diff, 'x', c='purple')

        ax3.axhline(0, ls='--', c='black', alpha=0.5)

        red_patch = mpatches.Patch(color='red', label='Script duration (s)')
        blue_patch = mpatches.Patch(color='blue', label='Total duration (s)')
        green_patch = mpatches.Patch(color='green', label='Billed duration (s)')
        purple_patch = mpatches.Patch(color='purple', label='I/O duration (s)')
        plt.legend(handles=[blue_patch, red_patch, purple_patch, green_patch])

        ax3.set_title(f'Code Duration Values (s) vs Memory (MB)\n({curr_dim}, Normalized by # of Vertical Levels)')
        ax3.set_ylabel('Time (s)')
        ax3.set_xlabel('Memory (MB)')

    if dict_key_args['best_fit']:
        # plot exponential fits
        ax3.plot(indiv_mem_fit, indiv_total_duration_exp_fit, '-', c='blue')
        ax3.plot(indiv_mem_fit, indiv_script_duration_exp_fit, '-', c='red')
        ax3.plot(indiv_mem_fit, indiv_time_exp_fit, '-', c='green')
        ax3.plot(indiv_mem_fit, indiv_duration_diff_exp_fit, '-', c='purple')

        # plot exponential inital guesses
        # ax3.plot(total_mem_fit, indiv_total_duration_p0_fit, '--', c='blue')
        # ax3.plot(total_mem_fit, indiv_script_duration_p0_fit, '--', c='red')
        # ax3.plot(indiv_mem_fit, indiv_time_p0_fit, '--', c='green')
        # ax3.plot(indiv_mem_fit, indiv_duration_diff_p0_fit, '--', c='purple')

    fig2.savefig(f'./logs/log_plots/{curr_dim}_log_plots_duration.png', dpi=200)