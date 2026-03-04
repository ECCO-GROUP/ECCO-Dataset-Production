# BL: Should add option to overwrite_switch even if file exists, ie if newer granule is downloaded....?


# BL:  I need to fix this rigid spaghetti pile
# BL: The plotting feels very awkward to me, but perhaps it's fine/normal to do things like this.


import sys
import matplotlib.colors
import matplotlib.pyplot as plt
from mpl_toolkits.axisartist.axislines import AxesZero #<= to plot x-axis and y-axis direction
import xarray as xr
import numpy as np
import cartopy.crs as ccrs
import os
import copy
import cmocean
import argparse
from pathlib import Path
from PIL import Image

base_dir = str(Path(__file__).parent.parent)
sys.path.append(base_dir)

import utility_scripts.utils_general as utils
import utility_scripts.cdf_extract as cdf_extract



sys.path.append('/Users/brucel/ECCOv4-py')
import ecco_v4_py as ecco



#-------------------------------------------------------------------------------------------------
#----------------------------------------- Plotting Functions ------------------------------------






# This currently does NOT have an option to re-plot existing figures
#def data_var_plot(ecco_version_string, dataset:xr.Dataset, data_array:xr.DataArray, image_directory:str, overwrite_switch:bool=False)->str:
def data_var_plot (config_dictionary, dataset, data_array, image_directory):


            #dataVarPlot = cdf_plotter.data_var_plot(config_dictionary["ecco_version_string"], dataset, dataset[variable_name], image_directory, config_dictionary['overwrite_switch'], config_dictionary['thumbnail_size'])


    """
    Plot the data variable and save the plot.

    Parameters
    ----------
    ecco_version_string : str
        The string describing the version of ECCO being used (i.e. "v4r4")
    dataset : xr.Dataset
        The dataset that contains the data_array to plot.
    data_array : xr.DataArray
        The data_array (data variable) to plot.
    image_directory : str
        The directory where the plot should be saved.

    Returns
    -------
    str
        The LaTeX command to include the saved figure.
    """
   
    # I don't love this
    figure_path  = os.path.join(image_directory, utils.get_ds_title(dataset).replace(',', ''), str(data_array.name).replace(' ', '_') + '.png')
    
    figure_path_Path_object = Path(figure_path)

    if not figure_path_Path_object.exists():

        figure_path_Path_object.parent.mkdir(parents=True, exist_ok=True)
        
        #print(f"figure path: {figure_path}")

        if 'native' in dataset.attrs['product_name']:
            plot_native(dataset, data_array, figure_path)
            #plot_native(dataset, data_array, image_directory)
        elif 'latlon' in dataset.attrs['product_name']:
            plot_latlon(dataset, data_array, figure_path)
            #plot_latlon(dataset, data_array, image_directory)
        elif '1D' in dataset.attrs['product_name']:
            plot_oneD(dataset, data_array, figure_path)
            #plot_oneD(dataset, data_array, image_directory)
       
        thumbnail_output_path = f"{'.'.join(figure_path.split('.')[:-1])}{config_dictionary['thumbnail_path_modifier_string']}.{figure_path.split('.')[-1]}" 
        thumbnail_size_tuple = (config_dictionary["thumbnail_size"], config_dictionary["thumbnail_size"])

        try:
            with Image.open(figure_path) as image:
                image.thumbnail(thumbnail_size_tuple, resample=Image.Resampling.BOX)
                image.save(thumbnail_output_path, format='PNG')
                
        except IOError as e:
            print(f"Error generating thumbnail: {e}")

    return r'\includegraphics[scale=0.55]{' + f'{figure_path}' + r'}'  









def plot_native(dataset:xr.Dataset, field:xr.DataArray, figure_path:str)->None:
#def plot_native(dataset:xr.Dataset, field:xr.DataArray, imageDirectory:str)->None:
    """
    Create a plot in native projection.

    Parameters
    ----------
    ds : xr.Dataset
        The dataset that contains the field to plot.
    field : xr.DataArray
        The field (data variable) to plot.
    directory : str
        The directory where the plot should be saved.
    show_colorbar : bool, optional
        Whether to show the colorbar. Default is True.
    show_coords : bool, optional
        Whether to show the coordinates. Default is False.
    """

    show_colorbar = True

    #-Files name setting-#
    product_name = dataset.product_name
    #--------------------#

    product_type = 'Native'
    tmp_plt = field
    if 'time' in field.dims:
        tmp_plt = field.isel(time=0)

    # GENERALLY PLOT THE K=0 (OR K_L=0) LAYER EXCEPT
    # FOR WVEL AND DRHODR BECAUSE THEIR VALUES ARE
    # 0 OR NAN AT THE SURFACE
    target_k = 0
    if 'WVEL' in field.name or 'DRHO' in field.name:
        target_k = 1
    if 'k_l' in field.dims:
        tmp_plt = tmp_plt.isel(k_l=target_k)
    elif 'k' in field.dims:
        tmp_plt = tmp_plt.isel(k=target_k)
    # verticale level
    verti_level = target_k+1
    #print(target_k)
    # find reasonable color limit for the plot
    cmin = np.nanmin(tmp_plt)
    cmax = np.nanmax(tmp_plt)
    # default
    cmap = copy.copy(plt.get_cmap('jet'))
    cmap.set_bad(color='dimgray')#, alpha=0

    shortname_tmp = dataset.metadata_link.split('ShortName=')[1]
    # create function for editing cmap, cmin and cmax
    cmap, cmin, cmax = cal_cmin_cmax(cmap, cmin, cmax, shortname_tmp, product_type)

    if dataset.attrs["product_name"].startswith('SEA_ICE'):
        #plt.figure(figsize=(12,6))
        if field.name == 'SIhsnow':
            cmin = 0
            cmax = 0.3

        ax1_subplot_grid = [1, 2, 1]
        fig, ax1, p, cbar1, new_grid_lon_centers_out, new_grid_lat_centers_out,\
            data_latlon_projection_out, gl = ecco.plot_proj_to_latlon_grid(dataset.XC, \
                                    dataset.YC, \
                                    tmp_plt, \
                                    plot_type = 'pcolormesh', \
                                    dx=2,\
                                    dy=2, \
                                    lat_lim=45,\
                                    projection_type = 'stereo',\
                                    show_colorbar=show_colorbar, \
                                    less_output = True,\
                                    cmap=cmap, \
                                    subplot_grid=ax1_subplot_grid,\
                                    cmin=cmin,\
                                    cmax=cmax
                                          )

        if show_colorbar:
            cax1 = cbar1.ax  # cax is now the `Axes` object for the colorbar
            bbox1 = cax1.get_position()  # this gets the current position of the colorbar
            ax1pos = ax1.get_position()
            new_bbox1 = [bbox1.x0, ax1pos.y0, bbox1.width, ax1pos.height]
            cax1.set_position(new_bbox1)  # and apply the new position to the `Axes`
            cbar1.set_label(tmp_plt.attrs['units'])


        ax2_subplot_grid = [1, 2, 2]
        fig, ax2, p, cbar2, new_grid_lon_centers_out, new_grid_lat_centers_out,\
            data_latlon_projection_out, gl = ecco.plot_proj_to_latlon_grid(dataset.XC, \
                                    dataset.YC, \
                                    tmp_plt, \
                                    plot_type = 'pcolormesh', \
                                    dx=2,\
                                    dy=2, \
                                    lat_lim=-65,\
                                    projection_type = 'stereo',\
                                    show_colorbar=show_colorbar, \
                                    less_output = True,\
                                    cmap=cmap, \
                                    subplot_grid=ax2_subplot_grid,\
                                    cmin=cmin,\
                                    cmax=cmax
                                    )
        if show_colorbar:
            cax2 = cbar2.ax
            bbox2 = cax2.get_position()
            ax2pos = ax2.get_position()
            new_bbox2 = [bbox2.x0, ax2pos.y0, bbox2.width, ax2pos.height]
            cax2.set_position(new_bbox2)
            cbar2.set_label(tmp_plt.attrs['units'])

        # title settings
        fig = plt.gcf()  # get the current figure
        fig.set_size_inches(12, 6)
        if 'time' in field.dims:
            fig.suptitle(str(field.name)+" (Daily Mean)",ha='center',x=.45,y=.90,weight='bold', fontsize=16)
            fig.text(s=field.attrs["long_name"]+"\n (vertical level = "+str(verti_level)+")", x=0.45, y=.82, fontsize=12, ha='center', va='center')
            plt.figtext(0.45, 0.13,
                            s=product_name,
                            wrap=True,horizontalalignment='center', fontsize=11)#'Example fied '+str(field.time.values[0])[:10]+',\n '
        else:
            fig.suptitle(str(field.name),ha='center',x=.45,y=.90,weight='bold', fontsize=16)
            fig.text(s=field.attrs["long_name"]+"\n (vertical level = "+str(verti_level)+")", x=0.45, y=.82, fontsize=12, ha='center', va='center')
            plt.figtext(0.45, 0.13,
                            s=product_name,
                            wrap=True,horizontalalignment='center', fontsize=11)#'Example fied :\n '+

    else:
        if 'units' in tmp_plt.attrs.keys():
            label = tmp_plt.attrs['units']
            show_label = True
        else:
            label = ''
            show_label = False

        #print(tmp_plt.long_name)

        fig, cur_arr = ecco.plot_tiles(tmp_plt,cmin=cmin,cmax=cmax, fig_num=0, cmap=cmap, \
                                    show_colorbar=show_colorbar, show_tile_labels= False, fig_size=8, cbar_label=label, show_cbar_label=show_label) #fig_size=8 was original
        if show_colorbar:
            cbar_ax = fig.axes[-1]
            bbox = cbar_ax.get_position()
            axpos = fig.axes[-2].get_position()
            new_bbox = [bbox.x0, axpos.y0, bbox.width, axpos.height * 5]
            cbar_ax.set_position(new_bbox)

        #OJH: Adding tiile' x-axis and y-axis direction guide on the plot
        # left, bottom, width, height = [0.5, 0.23, 0.25, 0.25]#<= to small x-axis and y-axis as inset
        left, bottom, width, height = [0.1, 0.08, 0.75, 0.8]#<= to x-axis and y-axis around the entire figure.
        ax_ojh = fig.add_axes([left, bottom, width, height],axes_class=AxesZero)
        for direction in ["xzero", "yzero"]:
            ## adds arrows at the ends of each axis
            ax_ojh.axis[direction].set_axisline_style("-|>")
            ## adds X and Y-axis from the origin
            ax_ojh.axis[direction].set_visible(True)
        ## hides borders
        for direction in ["left", "right", "bottom", "top"]:
            ax_ojh.axis[direction].set_visible(False)
        ax_ojh.set_xticklabels('',fontsize=24);ax_ojh.set_yticklabels('',fontsize=24)
        ax_ojh.set_xlabel('tiles x-axis',fontsize=24);ax_ojh.set_ylabel('tiles y-axis',fontsize=24)
        ax_ojh.set_facecolor('none')#<= to x-axis and y-axis background color fully transparent.

        # title settings
        if 'time' in field.dims:
            # fig.suptitle(f'{field.name}: {field.attrs["long_name"]}\n{str(field.time.values[0])[:10]}\n ', wrap=True, fontsize='x-large')
            fig.suptitle(str(field.name)+" (Daily Mean)",ha='center',x=.5,y=.968,weight='bold', fontsize=16)
            fig.text(s=field.attrs["long_name"]+"\n (vertical level = "+str(verti_level)+") \n", x=0.5, y=.9, fontsize=12, ha='center', va='center')
            plt.figtext(0.5, -0.02,
                            s=product_name,
                            wrap=True,horizontalalignment='center', fontsize=11)#'Example fied '+str(field.time.values[0])[:10]+',\n '

        else:
            # fig.suptitle(f'{field.name}: \n{field.attrs["long_name"]}\n ', wrap=True, fontsize='x-large')
            fig.suptitle(str(field.name),ha='center',x=.5,y=.968,weight='bold', fontsize=16)
            fig.text(s=field.attrs["long_name"]+"\n (vertical level = "+str(verti_level)+") \n", x=0.5, y=.9, fontsize=12, ha='center', va='center')
            plt.figtext(0.5, -0.02,
                        s=product_name,
                        wrap=True,horizontalalignment='center', fontsize=11)#'Example fied: \n '+

    dpi = 300
    plt.savefig(figure_path, dpi=dpi, facecolor='w', bbox_inches='tight', pad_inches = 0.05)
    plt.close('all')




def plot_latlon(dataset:xr.Dataset, field:xr.DataArray, figure_path:str)->None:
    """
    Create a plot in latlon projection.

    Parameters
    ----------
    ds : xr.Dataset
        The dataset that contains the field to plot.
    field : xr.DataArray
        The field (data variable) to plot.
    product_name : str
        The Name of dataset that is selected with '.nc' at the end.
    """

    show_colorbar = True

    #-Files name setting-#
    product_name = dataset.product_name
    #--------------------#
    tmp_plt = field
    if 'time' in field.dims:
        tmp_plt = field.isel(time=0)

    target_k = 0
    if 'WVEL' in field.name or 'DRHO' in field.name:
        target_k = 1
    if 'Z' in field.dims:
        tmp_plt = tmp_plt.isel(Z=target_k)
    # verticale level
    verti_level = target_k+1
    cmin = np.nanmin(tmp_plt)
    cmax = np.nanmax(tmp_plt)
    # default
    cmap = copy.copy(plt.get_cmap('jet'))

    shortname_tmp = dataset.metadata_link.split('ShortName=')[1]
    # create function for editing cmap, cmin and cmax
    cmap, cmin, cmax = cal_cmin_cmax(cmap, cmin, cmax, shortname_tmp, 'Latlon')

    if 'units' in field.attrs.keys():
        cbarLabel = field.attrs['units']
    else:
        cbarLabel = None
    if dataset.attrs['product_name'].startswith('SEA_ICE'):

        fig = plt.gcf()
        fig.set_size_inches(12, 6)

        ax1 = plt.subplot(1,2,1, projection=ccrs.NorthPolarStereo())
        ecco.plot_pstereo(dataset.longitude, dataset.latitude, tmp_plt, 4326, cmap=cmap,cmin=cmin, cmax=cmax,\
                           show_colorbar=show_colorbar, colorbar_label=cbarLabel, ax=ax1, lat_lim=45)

        if show_colorbar:
            cax1 = fig.axes[-1]
            bbox1 = cax1.get_position()
            ax1pos = ax1.get_position()
            new_bbox1 = [bbox1.x0, ax1pos.y0, bbox1.width, ax1pos.height]
            cax1.set_position(new_bbox1)

        ax2 = plt.subplot(1,2,2, projection=ccrs.SouthPolarStereo())
        ecco.plot_pstereo(dataset.longitude, dataset.latitude, tmp_plt, 4326, cmap=cmap, cmin=cmin, cmax=cmax,\
                           show_colorbar=show_colorbar, colorbar_label=cbarLabel,ax=ax2, lat_lim=-65)
        if show_colorbar:
            cax2 = fig.axes[-1]
            bbox2 = cax2.get_position()
            ax2pos = ax2.get_position()
            new_bbox2 = [bbox2.x0, ax2pos.y0, bbox2.width, ax2pos.height]
            cax2.set_position(new_bbox2)

        # plt.suptitle(f'{field.name}: {str(field.time.values[0])[:10]}\n{field.attrs["long_name"]}\n', wrap=True, fontsize='x-large')
        fig.suptitle(str(field.name)+" (Daily Mean)",ha='center',x=.45,y=.91,weight='bold', fontsize=16)
        fig.text(s=field.attrs["long_name"]+"\n (vertical level = "+str(verti_level)+")", x=0.45, y=.82, fontsize=12, ha='center', va='center')
        plt.figtext(0.45, 0.1,
                    s=product_name,
                    wrap=True,horizontalalignment='center', fontsize=11)#'Example fied '+str(field.time.values[0])[:10]+',\n '+
    elif field.name=='drF':
        # plotting only this particular case of var from latlon grid geometry!
        fig = plt.gcf()  # get the current figure
        fig.set_size_inches(8, 9)
        plt.plot(field,dataset.Z)
        plt.ylim(-6000.1,50);plt.xlim(0.490)
        plt.yticks(-np.arange(0,6000.1,500))
        plt.xticks(np.arange(0,490.1,50))
        plt.xlabel(field.long_name.capitalize()+ " [m]",wrap=True)
        plt.ylabel("Depth of the grid cell center [m]")

        fig.suptitle(str(field.name),ha='center',x=.5,y=.95,weight='bold', fontsize=16)
        plt.title(field.attrs["long_name"]+"\n", wrap=True, fontsize=12)
        # fig.text(s=field.attrs["long_name"]+"\n", x=0.45, y=.82, fontsize=12, ha='center', va='center')
        plt.figtext(0.45, 0.025,
                    s=product_name,
                    wrap=True,horizontalalignment='center', fontsize=11)
    else:

        fig = plt.gcf()  # get the current figure
        fig.set_size_inches(12, 6)
        ax = plt.subplot(1,1,1,
                            projection=ccrs.Robinson(central_longitude=200))
        # the plot (p), the gridlines (gl), and the colorbar (cbar).
        p, gl, cbar = ecco.plot_global(dataset.longitude, dataset.latitude, tmp_plt, data_epsg_code=4326, cmin=cmin, cmax=cmax, ax=ax,cmap=cmap, \
                                        show_colorbar=show_colorbar, colorbar_label=cbarLabel)
        if show_colorbar:
            cax = cbar.ax
            bbox = cax.get_position()
            axpos = ax.get_position()
            new_bbox = [bbox.x0, axpos.y0, bbox.width, axpos.height]
            cax.set_position(new_bbox)

        if 'time' in field.dims:
            # plt.suptitle(f'{field.name}: {str(field.time.values[0])[:10]}\n{field.attrs["long_name"]}', wrap=True, fontsize='x-large')
            plt.suptitle(str(field.name)+" (Daily Mean)",ha='center',x=.45,y=.91,weight='bold', fontsize=16)
            plt.title(field.attrs["long_name"]+"\n (vertical level = "+str(verti_level)+")", wrap=True, fontsize=12)
            plt.figtext(0.45, 0.1,
                        s=str(field.time.values[0])[:10]+',\n '+product_name,
                        wrap=True,horizontalalignment='center', fontsize=11)#'Example fied '+
        else:
            # plt.suptitle(f'{field.name}: \n{field.attrs["long_name"]}', wrap=True, fontsize='x-large')
            plt.suptitle(str(field.name),ha='center',x=.45,y=.91,weight='bold', fontsize=16)
            # plt.title(field.attrs["long_name"]+"\n", wrap=True, fontsize=12)
            plt.title(field.attrs["long_name"]+"\n (vertical level = "+str(verti_level)+")", wrap=True, fontsize=12)
            plt.figtext(0.45, 0.1,
                        s=product_name,
                        wrap=True,horizontalalignment='center', fontsize=11)#'Example fied: \n '+

    dpi = 300
    plt.savefig(figure_path, dpi=dpi, facecolor='w', bbox_inches='tight', pad_inches = 0.05)
    plt.close('all')



def plot_oneD(dataset:xr.Dataset, field:xr.DataArray, figure_path:str)->None:
    """
    Create a plot in 1D projection.

    Parameters
    ----------
    ds : xr.Dataset
        The dataset that contains the field to plot.
    field : xr.DataArray
        The field (data variable) to plot.
    product_name : str
        The Name of dataset that is selected with '.nc' at the end.
    """
    #-Files name setting-#
    product_name = dataset.product_name
    #--------------------#
    dataset[field.name].plot() # type: ignore
    fig = plt.gcf()
    fig.set_size_inches(12, 6)
    # plt.suptitle(f'{field.name}: {str(field.time.values[0])[:10]}\n{field.attrs["long_name"]}', wrap=True, fontsize='x-large')
    # plt.suptitle(str(field.name),ha='center',x=.45,y=.91,weight='bold', fontsize=16)
    plt.title(field.attrs["long_name"]+"\n", wrap=True,weight='bold', fontsize=16)# fontsize=12)
    plt.figtext(0.45, -0.05,
                s=product_name,
                wrap=True,horizontalalignment='center', fontsize=11) #'Example fied: '+

    dpi = 300
    plt.savefig(figure_path, dpi=dpi, facecolor='w', bbox_inches='tight', pad_inches = 0.05)
    plt.close('all')



def plot_datasetPicEg(dataset:xr.Dataset,save_to:str):
    Dims_box = list(dataset.dims)
    Var_box  = list(dataset.data_vars)
    # Var selection
    var_sel = 0
    tmp_plt = dataset[Var_box[var_sel]]
    if 'time' in Dims_box:
        tmp_plt = dataset[Var_box[var_sel]].isel(time=0)
    # GENERALLY PLOT THE K=0 (OR K_L=0) LAYER EXCEPT
    # FOR WVEL AND DRHODR BECAUSE THEIR VALUES ARE
    # 0 OR NAN AT THE SURFACE
    target_k = 0
    if 'WVEL' in tmp_plt.name or 'DRHO' in tmp_plt.name:
        target_k = 1
    if 'k_l' in tmp_plt.dims:
        tmp_plt = tmp_plt.isel(k_l=target_k)
    elif 'k' in tmp_plt.dims:
        tmp_plt = tmp_plt.isel(k=target_k)
    elif 'Z' in tmp_plt.dims:
        tmp_plt = tmp_plt.isel(Z=target_k)
    # find reasonable color limit for the plot
    cmin = np.nanmin(tmp_plt)
    cmax = np.nanmax(tmp_plt)
    # default
    cmap = copy.copy(plt.get_cmap('jet'))
    #PLOTTING PART#
    fig = plt.gcf()
    fig.set_size_inches(12, 6)
    if 'tile' in Dims_box:
        cmap.set_bad(color='dimgray')#<= to change the NaN values by a unique color
        ecco.plot_tiles(tmp_plt,cmin=cmin,cmax=cmax, fig_num=0, cmap=cmap,
                        show_colorbar=False, show_tile_labels= False,
                        fig_size=8, cbar_label=False, show_cbar_label=False)
    else:
        ax = plt.subplot(1,1,1,projection=ccrs.Robinson(central_longitude=200))
        # the plot (p), the gridlines (gl), and the colorbar (cbar).
        p, gl, cbar = ecco.plot_global(dataset.longitude, dataset.latitude, tmp_plt, data_epsg_code=4326,
                                       cmin=cmin, cmax=cmax, ax=ax,cmap=cmap,
                                       show_colorbar=False, colorbar_label=False)
#         ax.add_feature(cfeature.LAND)
    #------ getting Dataset name and building the saving path for the figure -----#
    FILEname = dataset.metadata_link.split('ShortName=')[1]+'.png'
    fig_path = os.path.join(save_to, FILEname)
    #------ SAVING-----#
    plt.savefig(fig_path, dpi=300, facecolor='w', bbox_inches='tight', pad_inches = 0.05)
    plt.close('all')

############################################################################################################
#                                   Helper functions
############################################################################################################
def even_cax(cmin:float, cmax:float, fac:float=1.0)->tuple[float, float]:
    """
    Make the color axis symmetric about zero.
    Parameters:
    -----------
        cmin : float | The minimum value of the color axis.
        cmax : float | The maximum value of the color axis.
        fac : float, optional | The factor by which to multiply the color axis boundataset. Default is 1.0.
    Returns:
    --------
        (cmin:float, cmax:float) : tuple of float | The new cmin and cmax values.
    """

    tmp = np.max([np.abs(cmin), np.abs(cmax)])
    cmin = -tmp*fac
    cmax =  tmp*fac
    return cmin, cmax

def cal_cmin_cmax(cmap:matplotlib.colors.LinearSegmentedColormap,
                  cmin:float, cmax:float,
                  shortname_tmp:str, product_type:str):
        """
        Create a plot in native projection.

        Parameters
        ----------
        cmap : matplotlib.colors.LinearSegmentedColormap
            The colormap to use for the plot.
        cmin : float
            The minimum value of the color axis.
        cmax : float
            The maximum value of the color axis.
        shortname_tmp : str
            The shortname of the data variable.
        product_type : str
            The product type of the dataset.

        Returns
        -------
        (cmap, cmin, cmax) : tuple of matplotlib.colors.LinearSegmentedColormap, float, float
            The colormap, cmin, and cmax values.
        """

        if ('STRESS' in shortname_tmp) or \
           ('FLUX' in shortname_tmp) or\
           ('VEL' in shortname_tmp) or\
           ('TEND' in shortname_tmp) or\
           ('BOLUS' in shortname_tmp):

            fac = 0.8
            if ('FRESH_FLUX' in shortname_tmp) or\
                ('MOMENTUM_TEND' in shortname_tmp):
                fac = 0.25

            cmin, cmax= even_cax(cmin, cmax, fac)
            cmap = copy.copy(cmocean.cm.balance)#plt.get_cmap('bwr'))

        elif ('TEMP_SALINITY' in shortname_tmp):
            cmap = copy.copy(cmocean.cm.thermal)

        elif ('MIXED_LAYER' in shortname_tmp):

            cmap = copy.copy(cmocean.cm.deep)
            cmin = 0
            cmax = 250

        elif ('SEA_ICE_CONC' in shortname_tmp):

            cmap = copy.copy(cmocean.cm.ice)
            cmin = 0
            cmax = 1

        elif 'DENS_STRAT' in shortname_tmp:
            cmap = copy.copy(cmocean.cm.dense)

        if product_type == 'Native':
            cmap.set_bad(color='dimgray')#,alpha=0

        return cmap, cmin, cmax




def compute_cmin_cmax(data, factor=1.5):
    """
    Compute cmin and cmax values (color range) for visualization based on IQR method.

    Parameters
    ----------
    data : ndarray or xarray.DataArray
        The data for which to compute cmin and cmax.
    factor : float, optional
        The multiplier for the IQR. Default is 1.5, corresponding to the 1.5xIQR rule.

    Returns
    -------
    (cmin:float, cmax:float) : tuple of float
        The computed cmin and cmax values.
    """
    # If data is an xarray.DataArray, convert to numpy array
    if isinstance(data, xr.DataArray):
        data = data.values

    # Compute quartiles and IQR
    Q1 = np.nanpercentile(data, 25)
    Q3 = np.nanpercentile(data, 75)
    IQR = Q3 - Q1

    # Compute bounds for cmin and cmax
    lower_bound = Q1 - factor * IQR
    upper_bound = Q3 + factor * IQR

    # Compute cmin and cmax, ensuring they fall within the data range
    cmin = max(np.nanmin(data), lower_bound) # type: ignore
    cmax = min(np.nanmax(data), upper_bound) # type: ignore

    return cmin, cmax


if __name__ == '__main__':
    """
    Plot a (or all) data variable.

    Parameters
    ----------
    --file : str
        The path to the NetCDF file.
    --field : str
        The name of the data variable to plot.
    --directory : str, optional
        The directory in which to save the plot. Default is None.
    --cbar : str, optional
        Whether to show the colorbar. Default is True.
    --coords : str, optional
        Whether to show the coordinates. Default is False.
    """
    parser = argparse.ArgumentParser(description='Plot a data variable.')
    parser.add_argument('--file', required=True, type=str, help="The path to the NetCDF file.")
    parser.add_argument('--field', required=True, type=str, help="The name of the data variable to plot.")
    parser.add_argument('--directory', required=False, type=str, help="The directory in which to save the plot.")
    parser.add_argument('--cbar', required=False, type=str, help="Whether to show the colorbar. Default is True.", default=None)
    parser.add_argument('--coords', required=False, type=str, help="Whether to show the coordinates. Default is False.", default=None)

    args = parser.parse_args()
    file = args.file
    field= args.field
    directory = args.directory

    cbar = True
    if args.cbar == 'False':
        cbar = False

    coords = False
    if args.coords == 'True':
        coords = True

    #cbar = True if args.cbar is None or 'True' else False
    #coords = False if args.coords is None or 'False' else True

    ##print(f'{file} {field} {directory} {cbar} {coords}')
    ds = xr.open_dataset(args.file)

    ##print(list(dataset.coords))
    if args.field == 'all':
        if coordataset:
            fields = list(dataset.coords)
        else:
            fields = list(dataset.data_vars)
    else:
        fields = [args.field]

    # find file type
    if 'native' in file:
        type = 'native'
    elif 'latlon' in file:
        type = 'latlon'
    else:
        type = 'oneD'

    ##print(fields)
    for fi, f in enumerate(fields):
        ##print(f'{fi} {f}')
        field = dataset[f]

        if directory is None:
            directory = 'images/plots/' + type + '_plots/'


        data_var_plot(ds, field, directory, cbar, coords)
