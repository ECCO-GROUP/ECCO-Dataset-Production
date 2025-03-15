import ecco_v4_py as ecco
import matplotlib.colors
import matplotlib.pyplot as plt
from mpl_toolkits.axisartist.axislines import AxesZero #<= to plot x-axis and y-axis direction
import xarray as xr
import numpy as np
import os
import utils
import cartopy.crs as ccrs
import copy
import cmocean
import cdf_plotter_ojh

# Function to plot example figure per dataset
def plot_fig_for_PODAAC(dataType="natives"):
    # Dataset source reading
    if dataType =="natives":
        data_source = "../../granule_datasets/natives/"
    elif dataType == "latlon":
        data_source = "../../granule_datasets/latlon/"
    elif dataType == "oneD":
        data_source = "../../granule_datasets/oneD/"
    else:
        print("Noway! Make your choice in {'natives', 'latlon', 'oneD'}")
            
    # Getting the content of selected dataset type source folder
    DataSet_list = sorted(os.listdir(path=data_source))
    # Getting the na,e of each dataset selected without the extension ".nc"
    # This is to save the plotted figure with the selected dataset name
    DataSet_name = []
    for i in DataSet_list:
        DataSet_name.append(i[:-3])
    # Selection of dataset to plot per variable
    # /!\ Testing for the first file#ij=0
    for ij in np.arange(len(DataSet_name)):
        DataSet_input = data_source+DataSet_list[ij]
        # For xr.Dataset
        DS = xr.open_dataset(DataSet_input)
        # For variable list
        DataSet_var_list = sorted(DS.data_vars)
        # /!\ testing with the first varible# ijk = 0
        for ijk in np.arange(len(DataSet_var_list)):
            if dataType =="natives":
                # plotting with "plot_native"
                cdf_plotter_ojh.plot_native(ds=DS,field=DS[DataSet_var_list[ijk]], output_dir="./",show_colorbar=True)
            elif dataType == "latlon":
                cdf_plotter_ojh.plot_latlon(ds=DS, field=DS[DataSet_var_list[ijk]], directory='./')
            elif dataType == "oneD":
                cdf_plotter_ojh.plot_oneD(ds=DS, field=DS[DataSet_var_list[ijk]], directory='./')
            else:
                print("Noway! Make your choice in {'natives', 'latlon', 'oneD'}")
            fig_path = 'Figs/'+DataSet_name[ij]+"_var_"+DataSet_var_list[ijk]+"_pic.png"
            plt.savefig(fig_path, dpi=300, facecolor='w', bbox_inches='tight', pad_inches = 0.05)
            plt.close('all')

# Plotting figures per dataset:
## Figures will be save in Figs/ from the current folder
## Plotting "natives" datasets
plot_fig_for_PODAAC(dataType="natives")
## Plotting "latlon" datasets
plot_fig_for_PODAAC(dataType="latlon")
## Plotting "oneD" datasets
plot_fig_for_PODAAC(dataType="oneD")