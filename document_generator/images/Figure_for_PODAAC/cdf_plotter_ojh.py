import ecco_v4_py as ecco
import matplotlib.colors
import matplotlib.pyplot as plt
from mpl_toolkits.axisartist.axislines import AxesZero #<= to plot x-axis and y-axis direction
import xarray as xr
import numpy as np
import utils
import cartopy.crs as ccrs
import os
import copy
import cmocean
import argparse

#-------------------------------------------------------------------------------------------------
#----------------------------------------- Plotting Functions ------------------------------------
def data_var_plot(ds:xr.Dataset, field:xr.DataArray, directory:str='none', colorbar:bool=True, coords:bool=False)->str:
    """
    Plot the data variable and save the plot.

    Parameters
    ----------
    ds : xr.Dataset
        The dataset that contains the field to plot.
    field : xr.DataArray
        The field (data variable) to plot.
    directory : str
        The directory where the plot should be saved.

    Returns
    -------
    str
        The LaTeX command to include the saved figure.
    """
    # TESTING  :  current progress 
    #print(f'Dataset: {ds.attrs["product_name"]}, Field: {field.name}, Shape: {field.shape}')
    fig = plt.figure(figsize=(12,6))
    #plt.rcParams['font.size'] = 5
    if 'native' in ds.attrs['product_name']:
        if directory == 'none' and not coords:
            directory = 'images/plots/native_plots/'
        elif directory == 'none' and coords:
            directory = 'images/plots/native_plots_coords/'
        if coords and 'tile' in field.dims and len(field.dims) > 2 and 'bnds' not in field.name:
            figure_path  = os.path.join(directory + utils.get_ds_title(ds).replace(',', ''), str(field.name).replace(' ', '_') + '.png')
            TrueFalse = os.path.exists(figure_path)
            if TrueFalse is False:
                plot_native(ds, field, directory, colorbar, coords)
                address = save_plt(fig, directory + utils.get_ds_title(ds).replace(',', ''), str(field.name).replace(' ', '_') + '.png')
            else:
                address = '../'+figure_path[figure_path.find('images'):]
        elif coords:
            plt.close('all')
            return 'skipped'
        else:
            figure_path  = os.path.join(directory + utils.get_ds_title(ds).replace(',', ''), str(field.name).replace(' ', '_') + '.png')
            TrueFalse = os.path.exists(figure_path)
            if TrueFalse is False:
                plot_native(ds, field, directory, colorbar, coords)
                address = save_plt(fig, directory + utils.get_ds_title(ds).replace(',', ''), str(field.name).replace(' ', '_') + '.png')
            else:
                address = '../'+figure_path[figure_path.find('images'):]
    elif 'latlon' in ds.attrs['product_name']:
        if directory == 'none' and not coords:
            directory = 'images/plots/latlon_plots/'
        elif directory == 'none' and coords:
            directory = 'images/plots/latlon_plots_coords/'
        if coords and len(field.dims) > 2:
            figure_path  = os.path.join(directory + utils.get_ds_title(ds).replace(',', ''), str(field.name).replace(' ', '_') + '.png')
            TrueFalse = os.path.exists(figure_path)
            if TrueFalse is False:
                plot_latlon(ds, field, directory, colorbar, coords)
                address = save_plt(fig, directory + utils.get_ds_title(ds).replace(',', ''), str(field.name).replace(' ', '_') + '.png')
            else:
                address = '../'+figure_path[figure_path.find('images'):]
        elif coords:
            plt.close('all')
            return 'skipped'
        else:
            figure_path  = os.path.join(directory + utils.get_ds_title(ds).replace(',', ''), str(field.name).replace(' ', '_') + '.png')
            TrueFalse = os.path.exists(figure_path)
            if TrueFalse is False:
                plot_latlon(ds, field, directory, colorbar, coords)
                address = save_plt(fig, directory + utils.get_ds_title(ds).replace(',', ''), str(field.name).replace(' ', '_') + '.png')
            else:
                address = '../'+figure_path[figure_path.find('images'):]
    else:
        figure_path  = os.path.join(directory + utils.get_ds_title(ds).replace(',', ''), str(field.name).replace(' ', '_') + '.png')
        TrueFalse = os.path.exists(figure_path)
        if TrueFalse is False:
            plot_oneD(ds, field, directory)
            address = save_plt(fig, directory + utils.get_ds_title(ds).replace(',', ''), str(field.name).replace(' ', '_') + '.png')
        else:
            address = '../'+figure_path[figure_path.find('images'):]
    print(address) # TESTING----------------------
    return r'\includegraphics[scale=0.55]{' + f'{address}' + r'}'#width=\textwidth



def plot_native(ds:xr.Dataset, field:xr.DataArray, dataseteName:str, show_colorbar:bool=True, is_coord:bool=False)->None:
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
    #-Files name setting-#
    dataseteName = ds.product_name
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

    print(target_k)
    # find reasonable color limit for the plot
    cmin = np.nanmin(tmp_plt)
    cmax = np.nanmax(tmp_plt)
    # default
    cmap = copy.copy(plt.get_cmap('jet'))
    cmap.set_bad(color='dimgray')#, alpha=0 

    shortname_tmp = ds.metadata_link.split('ShortName=')[1]
    # create function for editing cmap, cmin and cmax
    cmap, cmin, cmax = cal_cmin_cmax(cmap, cmin, cmax, shortname_tmp, product_type)

    if ds.attrs["product_name"].startswith('SEA_ICE'):
        #plt.figure(figsize=(12,6))
        if field.name == 'SIhsnow':
            cmin = 0
            cmax = 0.3

        ax1_subplot_grid = [1, 2, 1]
        fig, ax1, p, cbar1, new_grid_lon_centers_out, new_grid_lat_centers_out,\
            data_latlon_projection_out, gl = ecco.plot_proj_to_latlon_grid(ds.XC, \
                                    ds.YC, \
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
            data_latlon_projection_out, gl = ecco.plot_proj_to_latlon_grid(ds.XC, \
                                    ds.YC, \
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
            # if not is_coord:
            #     cbar2.set_label(tmp_plt.attrs['units'])
            #else:
                #cbar2.set_label('Axis:'+ field.attrs['axis'])
        
        # title settings
        fig = plt.gcf()  # get the current figure
        fig.set_size_inches(12, 6)
        if 'time' in field.dims:
            fig.suptitle(str(field.name)+" (Daily Mean)",ha='center',x=.45,y=.90,weight='bold', fontsize=16)
            fig.text(s=field.attrs["long_name"]+"\n", x=0.45, y=.82, fontsize=12, ha='center', va='center')
            plt.figtext(0.45, 0.13, 
                            s=dataseteName, 
                            wrap=True,horizontalalignment='center', fontsize=11)#'Example fied '+str(field.time.values[0])[:10]+',\n '
        else:
            fig.suptitle(str(field.name),ha='center',x=.45,y=.90,weight='bold', fontsize=16)
            fig.text(s=field.attrs["long_name"]+"\n", x=0.45, y=.82, fontsize=12, ha='center', va='center')
            plt.figtext(0.45, 0.13, 
                            s=dataseteName, 
                            wrap=True,horizontalalignment='center', fontsize=11)#'Example fied :\n '+

    else:
        if 'units' in tmp_plt.attrs.keys():
            label = tmp_plt.attrs['units']
            show_label = True
        else:
            label = ''
            show_label = False

        # fig, cur_arr = ecco.plot_tiles(tmp_plt,cmin=cmin,cmax=cmax, fig_num=1, cmap=cmap, \
        #                             show_colorbar=show_colorbar, show_tile_labels= False, fig_size=8, cbar_label=label, show_cbar_label=True) #fig_size=8 was original
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
            fig.text(s=field.attrs["long_name"]+"\n", x=0.5, y=.9, fontsize=12, ha='center', va='center')
            plt.figtext(0.5, -0.02, 
                            s=dataseteName, 
                            wrap=True,horizontalalignment='center', fontsize=11)#'Example fied '+str(field.time.values[0])[:10]+',\n '

        else:
            # fig.suptitle(f'{field.name}: \n{field.attrs["long_name"]}\n ', wrap=True, fontsize='x-large')
            fig.suptitle(str(field.name),ha='center',x=.5,y=.968,weight='bold', fontsize=16)
            fig.text(s=field.attrs["long_name"]+"\n", x=0.5, y=.9, fontsize=12, ha='center', va='center')
            plt.figtext(0.5, -0.02, 
                        s=dataseteName, 
                        wrap=True,horizontalalignment='center', fontsize=11)#'Example fied: \n '+




def plot_latlon(ds:xr.Dataset, field:xr.DataArray, dataseteName:str, show_colorbar:bool=True, is_coord:bool=False)->None:
    """
    Create a plot in latlon projection.

    Parameters
    ----------
    ds : xr.Dataset
        The dataset that contains the field to plot.
    field : xr.DataArray
        The field (data variable) to plot.
    dataseteName : str
        The Name of dataset that is selected with '.nc' at the end.
    """
    #-Files name setting-#
    dataseteName = ds.product_name
    #--------------------#
    tmp_plt = field
    if 'time' in field.dims:
        tmp_plt = field.isel(time=0)
    
    target_k = 0
    if 'WVEL' in field.name or 'DRHO' in field.name:
        target_k = 1
    if 'Z' in field.dims:
        tmp_plt = tmp_plt.isel(Z=target_k)
    
    cmin = np.nanmin(tmp_plt)
    cmax = np.nanmax(tmp_plt)
    # default
    cmap = copy.copy(plt.get_cmap('jet'))

    shortname_tmp = ds.metadata_link.split('ShortName=')[1]
    # create function for editing cmap, cmin and cmax
    cmap, cmin, cmax = cal_cmin_cmax(cmap, cmin, cmax, shortname_tmp, 'Latlon')

    if 'units' in field.attrs.keys():
        cbarLabel = field.attrs['units']
    else:
        cbarLabel = None
    if ds.attrs['product_name'].startswith('SEA_ICE'):
        
        fig = plt.gcf() 
        fig.set_size_inches(12, 6)
        
        ax1 = plt.subplot(1,2,1, projection=ccrs.NorthPolarStereo())
        ecco.plot_pstereo(ds.longitude, ds.latitude, tmp_plt, 4326, cmap=cmap,cmin=cmin, cmax=cmax,\
                           show_colorbar=show_colorbar, colorbar_label=cbarLabel, ax=ax1, lat_lim=45)
        
        if show_colorbar:
            cax1 = fig.axes[-1]  
            bbox1 = cax1.get_position() 
            ax1pos = ax1.get_position()
            new_bbox1 = [bbox1.x0, ax1pos.y0, bbox1.width, ax1pos.height]
            cax1.set_position(new_bbox1) 

        ax2 = plt.subplot(1,2,2, projection=ccrs.SouthPolarStereo())
        ecco.plot_pstereo(ds.longitude, ds.latitude, tmp_plt, 4326, cmap=cmap, cmin=cmin, cmax=cmax,\
                           show_colorbar=show_colorbar, colorbar_label=cbarLabel,ax=ax2, lat_lim=-65)
        if show_colorbar:
            cax2 = fig.axes[-1] 
            bbox2 = cax2.get_position()  
            ax2pos = ax2.get_position()
            new_bbox2 = [bbox2.x0, ax2pos.y0, bbox2.width, ax2pos.height]
            cax2.set_position(new_bbox2) 

        # plt.suptitle(f'{field.name}: {str(field.time.values[0])[:10]}\n{field.attrs["long_name"]}\n', wrap=True, fontsize='x-large')
        fig.suptitle(str(field.name)+" (Daily Mean)",ha='center',x=.45,y=.91,weight='bold', fontsize=16)
        fig.text(s=field.attrs["long_name"]+"\n", x=0.45, y=.82, fontsize=12, ha='center', va='center')
        plt.figtext(0.45, 0.1,
                    s=dataseteName,
                    wrap=True,horizontalalignment='center', fontsize=11)#'Example fied '+str(field.time.values[0])[:10]+',\n '+
        
    else:

        fig = plt.gcf()  # get the current figure
        fig.set_size_inches(12, 6)
        ax = plt.subplot(1,1,1,
                            projection=ccrs.Robinson(central_longitude=200))
        # the plot (p), the gridlines (gl), and the colorbar (cbar).
        p, gl, cbar = ecco.plot_global(ds.longitude, ds.latitude, tmp_plt, data_epsg_code=4326, cmin=cmin, cmax=cmax, ax=ax,cmap=cmap, \
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
            plt.title(field.attrs["long_name"]+"\n", wrap=True, fontsize=12)
            plt.figtext(0.45, 0.1, 
                        s=str(field.time.values[0])[:10]+',\n '+dataseteName, 
                        wrap=True,horizontalalignment='center', fontsize=11)#'Example fied '+
        else:
            # plt.suptitle(f'{field.name}: \n{field.attrs["long_name"]}', wrap=True, fontsize='x-large')
            plt.suptitle(str(field.name),ha='center',x=.45,y=.91,weight='bold', fontsize=16)
            plt.title(field.attrs["long_name"]+"\n", wrap=True, fontsize=12)
            plt.figtext(0.45, 0.1, 
                        s=dataseteName, 
                        wrap=True,horizontalalignment='center', fontsize=11)#'Example fied: \n '+

def plot_oneD(ds:xr.Dataset, field:xr.DataArray, dataseteName:str)->None:
    """
    Create a plot in 1D projection.

    Parameters
    ----------
    ds : xr.Dataset
        The dataset that contains the field to plot.
    field : xr.DataArray
        The field (data variable) to plot.
    dataseteName : str
        The Name of dataset that is selected with '.nc' at the end.
    """
    #-Files name setting-#
    dataseteName = ds.product_name
    #--------------------#
    ds[field.name].plot() # type: ignore
    fig = plt.gcf()
    fig.set_size_inches(12, 6)
    # plt.suptitle(f'{field.name}: {str(field.time.values[0])[:10]}\n{field.attrs["long_name"]}', wrap=True, fontsize='x-large')
    # plt.suptitle(str(field.name),ha='center',x=.45,y=.91,weight='bold', fontsize=16)
    plt.title(field.attrs["long_name"]+"\n", wrap=True,weight='bold', fontsize=16)# fontsize=12)
    plt.figtext(0.45, -0.05, 
                s=dataseteName, 
                wrap=True,horizontalalignment='center', fontsize=11) #'Example fied: '+


def plot_datasetPicEg(ds:xr.Dataset,save_to:str):
    Dims_box = list(ds.dims)
    Var_box  = list(ds.data_vars)
    # Var selection
    var_sel = 0
    tmp_plt = ds[Var_box[var_sel]]
    if 'time' in Dims_box:
        tmp_plt = ds[Var_box[var_sel]].isel(time=0)
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
        p, gl, cbar = ecco.plot_global(ds.longitude, ds.latitude, tmp_plt, data_epsg_code=4326,
                                       cmin=cmin, cmax=cmax, ax=ax,cmap=cmap,
                                       show_colorbar=False, colorbar_label=False)
#         ax.add_feature(cfeature.LAND)
    #------ getting Dataset name and building the saving path for the figure -----#
    FILEname = ds.metadata_link.split('ShortName=')[1]+'.png'
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
        fac : float, optional | The factor by which to multiply the color axis bounds. Default is 1.0.
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

def save_plt(figure, dir_path: str, plot_name: str) -> str:
    """
    Save a figure to a specified directory and return the path to the saved figure.

    Parameters
    ----------
    figure : matplotlib.figure.Figure
        The figure to save.
    dir_path : str
        The path to the directory where the figure should be saved.
    plot_name : str
        The name of the plot file.

    Returns
    -------
    str
        The relative path to the saved figure.
    """
    # Create the directory if it does not exist
    os.makedirs(dir_path, exist_ok=True)

    # Construct full path to the figure file
    fig_path = os.path.join(dir_path, plot_name)

    # Save the figure
    
    #figure.tight_layout()
    dpi = 300
    plt.savefig(fig_path, dpi=dpi, facecolor='w', bbox_inches='tight', pad_inches = 0.05)
    #plt.savefig(fig_path)
    plt.close('all')
    # Return the path to the figure
    cutoff_index = fig_path.find('images')
    path = '../' + fig_path[cutoff_index:]

    #print(path)
    return path



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

    print(f'{file} {field} {directory} {cbar} {coords}')
    ds = xr.open_dataset(args.file)

    print(list(ds.coords))
    if args.field == 'all':
        if coords:
            fields = list(ds.coords)
        else:
            fields = list(ds.data_vars)
    else:
        fields = [args.field]

    # find file type
    if 'native' in file:
        type = 'native'
    elif 'latlon' in file:
        type = 'latlon'
    else:
        type = 'oneD'
        
    print(fields)
    for fi, f in enumerate(fields):
        print(f'{fi} {f}')
        field = ds[f]

        if directory is None:
            directory = 'images/plots/' + type + '_plots/'
 

        data_var_plot(ds, field, directory, cbar, coords)























            #data_var_plot(ds, field, directory)
        #skip = False

        # if type == 'native':

        #     if 'tile' in field.dims and len(field.dims) > 2 and 'bnds' not in field.name:
        #         plot_native(ds, field, directory, cbar)
        #     else:
        #         print('skipping ', field.name)
        #         skip = True

        # elif type == 'latlon':
        #     if len(field.dims) > 2:
        #         plot_latlon(ds, field, directory, cbar)
        
        # elif type == 'oneD':
        #     plot_oneD(ds, field, directory)
        
        # if not skip:
        #     address = save_plt(fig, directory + utils.get_ds_title(ds).replace(',', ''), str(field.name).replace(' ', '_') + '.png')
        #     print(address) 
        
