# NOTE: This module depends on ecco_v4_py for native-grid tile plots and
# polar stereographic projections. Ensure the library is installed and its
# path is appended to sys.path before importing this module.

import sys
import matplotlib.colors
import matplotlib.pyplot as plt
from mpl_toolkits.axisartist.axislines import AxesZero  # for x/y axis arrow overlays
import xarray as xr
import numpy as np
import cartopy.crs as ccrs
import os
import copy
import cmocean
import argparse
from pathlib import Path
from PIL import Image

# Ensure the project root is on the path so relative imports resolve correctly
base_dir = str(Path(__file__).parent.parent.parent.parent.resolve())
sys.path.append(base_dir)
import src.document_generator.utils.utils_general as utils_general
import src.document_generator.utils.cdf_extract as cdf_extract

sys.path.append('/Users/brucel/ECCOv4-py')
import ecco_v4_py as ecco


# ---------------------------------------------------------------------------
# ----------------------------- Plotting Functions --------------------------
# ---------------------------------------------------------------------------

def data_var_plot(
    config_dictionary: dict,
    dataset: xr.Dataset,
    data_array: xr.DataArray,
    image_directory: str,
    overwrite_switch: bool
) -> str:
    """
    Generate a plot image for a data variable and return a LaTeX include command.

    Dispatches to the appropriate plotting function (native tile, lat-lon, or
    1-D) based on the ``product_name`` attribute of the dataset. If
    ``overwrite_switch`` is ``True``, the image is always regenerated;
    otherwise the existing file is reused. A thumbnail copy is also created
    at the size specified in the config.

    :param config_dictionary: Configuration mapping. Expected keys include
        ``'thumbnail_path_modifier_string'`` (str, suffix inserted before the
        file extension to form the thumbnail filename) and ``'thumbnail_size'``
        (int, longest edge in pixels for the thumbnail).
    :type config_dictionary: dict
    :param dataset: The dataset containing ``data_array``. Used for
        ``product_name`` and coordinate variables needed by the plot functions.
    :type dataset: xr.Dataset
    :param data_array: The variable to plot.
    :type data_array: xr.DataArray
    :param image_directory: Directory where the output PNG should be saved.
    :type image_directory: str
    :param overwrite_switch: If ``True``, regenerate the image even if it
        already exists on disk.
    :type overwrite_switch: bool
    :returns: A LaTeX ``\\includegraphics`` command referencing the saved figure.
    :rtype: str
    """
    # Construct the output path from the dataset title and variable name
    figure_path = os.path.join(
        image_directory,
        utils_general.get_ds_title(dataset).replace(',', ''),
        str(data_array.name).replace(' ', '_') + '.png'
    )
    figure_path_Path_object = Path(figure_path)

    if overwrite_switch:
        figure_path_Path_object.parent.mkdir(parents=True, exist_ok=True)

        # Dispatch to the correct plot function based on product type
        if 'native' in dataset.attrs['product_name']:
            plot_native(dataset, data_array, figure_path)
        elif 'latlon' in dataset.attrs['product_name']:
            plot_latlon(dataset, data_array, figure_path)
        elif '1D' in dataset.attrs['product_name']:
            plot_oneD(dataset, data_array, figure_path)

        # Build the thumbnail path by inserting the modifier before the extension
        thumbnail_output_path = (
            f"{'.' .join(figure_path.split('.')[:-1])}"
            f"{config_dictionary['thumbnail_path_modifier_string']}"
            f".{figure_path.split('.')[-1]}"
        )
        thumbnail_size_tuple = (config_dictionary["thumbnail_size"], config_dictionary["thumbnail_size"])

        try:
            with Image.open(figure_path) as image:
                # BOX resampling is fast and suitable for downscaling to thumbnail size
                image.thumbnail(thumbnail_size_tuple, resample=Image.Resampling.BOX)
                image.save(thumbnail_output_path, format='PNG')
        except IOError as e:
            print(f"Error generating thumbnail: {e}")

    return r'\includegraphics[scale=0.55]{' + f'{figure_path}' + r'}'


def plot_native(dataset: xr.Dataset, field: xr.DataArray, figure_path: str) -> None:
    """
    Create and save a native LLC-grid tile plot for a data variable.

    For sea-ice variables, produces two side-by-side polar stereographic
    subplots (Arctic and Antarctic). For all other native variables, generates
    a 13-tile plot using ``ecco.plot_tiles`` with an x/y axis arrow overlay
    indicating tile orientation.

    The surface layer (k=0 or k_l=0) is plotted by default, except for
    ``WVEL`` and ``DRHODR`` where layer 1 is used because layer 0 is
    identically zero or NaN at the surface.

    :param dataset: The full dataset, used for coordinate arrays (``XC``,
        ``YC``) and global attributes (``product_name``, ``metadata_link``).
    :type dataset: xr.Dataset
    :param field: The variable to plot. May have a ``'time'`` dimension (first
        time step is selected) and optionally a ``'k'`` or ``'k_l'`` depth
        dimension.
    :type field: xr.DataArray
    :param figure_path: Full path to the output PNG file.
    :type figure_path: str
    :returns: None
    """
    show_colorbar = True
    product_name = dataset.product_name
    tmp_plt = field

    # Select first time step if variable has a time dimension
    if 'time' in field.dims:
        tmp_plt = field.isel(time=0)

    # Select vertical level — default to surface (k=0), but skip it for
    # variables that are 0 or NaN at the surface
    target_k = 0
    if 'WVEL' in field.name or 'DRHO' in field.name:
        target_k = 1

    if 'k_l' in field.dims:
        tmp_plt = tmp_plt.isel(k_l=target_k)
    elif 'k' in field.dims:
        tmp_plt = tmp_plt.isel(k=target_k)
    verti_level = target_k + 1  # 1-indexed for display in the title

    cmin = np.nanmin(tmp_plt)
    cmax = np.nanmax(tmp_plt)

    # Default colormap; NaN cells shown as dark grey to distinguish them from ocean
    cmap = copy.copy(plt.get_cmap('jet'))
    cmap.set_bad(color='dimgray')

    shortname_tmp = dataset.metadata_link.split('ShortName=')[1]
    cmap, cmin, cmax = cal_cmin_cmax(cmap, cmin, cmax, shortname_tmp, 'Native')

    if dataset.attrs["product_name"].startswith('SEA_ICE'):
        # Sea-ice variables: side-by-side Arctic / Antarctic polar stereo plots
        if field.name == 'SIhsnow':
            cmin = 0
            cmax = 0.3

        ax1_subplot_grid = [1, 2, 1]
        fig, ax1, p, cbar1, *_ = ecco.plot_proj_to_latlon_grid(
            dataset.XC, dataset.YC, tmp_plt,
            plot_type='pcolormesh', dx=2, dy=2, lat_lim=45,
            projection_type='stereo', show_colorbar=show_colorbar,
            less_output=True, cmap=cmap, subplot_grid=ax1_subplot_grid,
            cmin=cmin, cmax=cmax
        )
        if show_colorbar:
            # Align colorbar height with its associated axes object
            cax1 = cbar1.ax
            bbox1 = cax1.get_position()
            ax1pos = ax1.get_position()
            cax1.set_position([bbox1.x0, ax1pos.y0, bbox1.width, ax1pos.height])
            cbar1.set_label(tmp_plt.attrs['units'])

        ax2_subplot_grid = [1, 2, 2]
        fig, ax2, p, cbar2, *_ = ecco.plot_proj_to_latlon_grid(
            dataset.XC, dataset.YC, tmp_plt,
            plot_type='pcolormesh', dx=2, dy=2, lat_lim=-65,
            projection_type='stereo', show_colorbar=show_colorbar,
            less_output=True, cmap=cmap, subplot_grid=ax2_subplot_grid,
            cmin=cmin, cmax=cmax
        )
        if show_colorbar:
            cax2 = cbar2.ax
            bbox2 = cax2.get_position()
            ax2pos = ax2.get_position()
            cax2.set_position([bbox2.x0, ax2pos.y0, bbox2.width, ax2pos.height])
            cbar2.set_label(tmp_plt.attrs['units'])

        fig = plt.gcf()
        fig.set_size_inches(12, 6)

        # Title block: variable name, long name, vertical level, product name
        if 'time' in field.dims:
            fig.suptitle(str(field.name) + " (Daily Mean)", ha='center', x=.45, y=.90, weight='bold', fontsize=16)
            fig.text(s=field.attrs["long_name"] + "\n (vertical level = " + str(verti_level) + ")", x=0.45, y=.82, fontsize=12, ha='center', va='center')
            plt.figtext(0.45, 0.13, s=product_name, wrap=True, horizontalalignment='center', fontsize=11)
        else:
            fig.suptitle(str(field.name), ha='center', x=.45, y=.90, weight='bold', fontsize=16)
            fig.text(s=field.attrs["long_name"] + "\n (vertical level = " + str(verti_level) + ")", x=0.45, y=.82, fontsize=12, ha='center', va='center')
            plt.figtext(0.45, 0.13, s=product_name, wrap=True, horizontalalignment='center', fontsize=11)

    else:
        # All other native variables: 13-tile plot
        label = tmp_plt.attrs.get('units', '')
        show_label = 'units' in tmp_plt.attrs

        fig, cur_arr = ecco.plot_tiles(
            tmp_plt, cmin=cmin, cmax=cmax, fig_num=0, cmap=cmap,
            show_colorbar=show_colorbar, show_tile_labels=False,
            fig_size=8, cbar_label=label, show_cbar_label=show_label
        )
        if show_colorbar:
            # Stretch colorbar to span the full tile grid height
            cbar_ax = fig.axes[-1]
            bbox = cbar_ax.get_position()
            axpos = fig.axes[-2].get_position()
            cbar_ax.set_position([bbox.x0, axpos.y0, bbox.width, axpos.height * 5])

        # Add axis arrows to indicate tile x/y orientation
        ax_ojh = fig.add_axes([0.1, 0.08, 0.75, 0.8], axes_class=AxesZero)
        for direction in ["xzero", "yzero"]:
            ax_ojh.axis[direction].set_axisline_style("-|>")
            ax_ojh.axis[direction].set_visible(True)
        for direction in ["left", "right", "bottom", "top"]:
            ax_ojh.axis[direction].set_visible(False)
        ax_ojh.set_xticklabels('', fontsize=24)
        ax_ojh.set_yticklabels('', fontsize=24)
        ax_ojh.set_xlabel('tiles x-axis', fontsize=24)
        ax_ojh.set_ylabel('tiles y-axis', fontsize=24)
        ax_ojh.set_facecolor('none')

        if 'time' in field.dims:
            fig.suptitle(str(field.name) + " (Daily Mean)", ha='center', x=.5, y=.968, weight='bold', fontsize=16)
            if len(field.dims) <= 4:
                fig.text(s=field.attrs["long_name"] + "\n", x=0.5, y=.9, fontsize=12, ha='center', va='center')
            else:
                fig.text(s=field.attrs["long_name"] + "\n (vertical level = " + str(verti_level) + ") \n", x=0.5, y=.9, fontsize=12, ha='center', va='center')
            plt.figtext(0.5, -0.02, s=product_name, wrap=True, horizontalalignment='center', fontsize=11)
        else:
            fig.suptitle(str(field.name), ha='center', x=.5, y=.968, weight='bold', fontsize=16)
            if len(field.dims) <= 4:
                fig.text(s=field.attrs["long_name"] + "\n", x=0.5, y=.9, fontsize=12, ha='center', va='center')
            else:
                fig.text(s=field.attrs["long_name"] + "\n (vertical level = " + str(verti_level) + ") \n", x=0.5, y=.9, fontsize=12, ha='center', va='center')
            plt.figtext(0.5, -0.02, s=product_name, wrap=True, horizontalalignment='center', fontsize=11)

    plt.savefig(figure_path, dpi=300, facecolor='w', bbox_inches='tight', pad_inches=0.05)
    plt.close('all')


def plot_latlon(dataset: xr.Dataset, field: xr.DataArray, figure_path: str) -> None:
    """
    Create and save a global lat-lon projection plot for a data variable.

    Sea-ice variables receive side-by-side polar stereographic plots. The
    special 1-D variable ``drF`` (vertical cell thickness) is plotted as a
    depth profile. All other variables are rendered on a Robinson projection
    centred at 200° longitude.

    :param dataset: The full dataset, providing ``longitude``, ``latitude``,
        and global attributes.
    :type dataset: xr.Dataset
    :param field: The variable to plot. Time and depth slicing follows the
        same rules as :func:`plot_native`.
    :type field: xr.DataArray
    :param figure_path: Full path to the output PNG file.
    :type figure_path: str
    :returns: None
    """
    show_colorbar = True
    product_name  = dataset.product_name
    tmp_plt = field

    if 'time' in field.dims:
        tmp_plt = field.isel(time=0)

    # Skip surface level for variables that are zero/NaN there
    target_k = 0
    if 'WVEL' in field.name or 'DRHO' in field.name:
        target_k = 1
    if 'Z' in field.dims:
        tmp_plt = tmp_plt.isel(Z=target_k)
    verti_level = target_k + 1

    cmin = np.nanmin(tmp_plt)
    cmax = np.nanmax(tmp_plt)
    cmap = copy.copy(plt.get_cmap('jet'))

    shortname_tmp = dataset.metadata_link.split('ShortName=')[1]
    cmap, cmin, cmax = cal_cmin_cmax(cmap, cmin, cmax, shortname_tmp, 'Latlon')

    cbarLabel = field.attrs.get('units', None)

    if dataset.attrs['product_name'].startswith('SEA_ICE'):
        # Polar stereographic pair for sea-ice lat-lon products
        fig = plt.gcf()
        fig.set_size_inches(12, 6)

        ax1 = plt.subplot(1, 2, 1, projection=ccrs.NorthPolarStereo())
        ecco.plot_pstereo(
            dataset.longitude, dataset.latitude, tmp_plt, 4326,
            cmap=cmap, cmin=cmin, cmax=cmax,
            show_colorbar=show_colorbar, colorbar_label=cbarLabel, ax=ax1, lat_lim=45
        )
        if show_colorbar:
            cax1 = fig.axes[-1]
            bbox1 = cax1.get_position()
            ax1pos = ax1.get_position()
            cax1.set_position([bbox1.x0, ax1pos.y0, bbox1.width, ax1pos.height])

        ax2 = plt.subplot(1, 2, 2, projection=ccrs.SouthPolarStereo())
        ecco.plot_pstereo(
            dataset.longitude, dataset.latitude, tmp_plt, 4326,
            cmap=cmap, cmin=cmin, cmax=cmax,
            show_colorbar=show_colorbar, colorbar_label=cbarLabel, ax=ax2, lat_lim=-65
        )
        if show_colorbar:
            cax2 = fig.axes[-1]
            bbox2 = cax2.get_position()
            ax2pos = ax2.get_position()
            cax2.set_position([bbox2.x0, ax2pos.y0, bbox2.width, ax2pos.height])

        fig.suptitle(str(field.name) + " (Daily Mean)", ha='center', x=.45, y=.91, weight='bold', fontsize=16)
        fig.text(s=field.attrs["long_name"] + "\n (vertical level = " + str(verti_level) + ")", x=0.45, y=.82, fontsize=12, ha='center', va='center')
        plt.figtext(0.45, 0.1, s=product_name, wrap=True, horizontalalignment='center', fontsize=11)

    elif field.name == 'drF':
        # Special case: vertical grid cell thickness plotted as a depth profile
        fig = plt.gcf()
        fig.set_size_inches(8, 9)
        plt.plot(field, dataset.Z)
        plt.ylim(-6000.1, 50)
        plt.xlim(0, 490)
        plt.yticks(-np.arange(0, 6000.1, 500))
        plt.xticks(np.arange(0, 490.1, 50))
        plt.xlabel(field.long_name.capitalize() + " [m]", wrap=True)
        plt.ylabel("Depth of the grid cell center [m]")
        fig.suptitle(str(field.name), ha='center', x=.5, y=.95, weight='bold', fontsize=16)
        plt.title(field.attrs["long_name"] + "\n", wrap=True, fontsize=12)
        plt.figtext(0.45, 0.025, s=product_name, wrap=True, horizontalalignment='center', fontsize=11)

    else:
        # Standard global Robinson projection plot
        fig = plt.gcf()
        fig.set_size_inches(12, 6)
        ax = plt.subplot(1, 1, 1, projection=ccrs.Robinson(central_longitude=200))
        p, gl, cbar = ecco.plot_global(
            dataset.longitude, dataset.latitude, tmp_plt,
            data_epsg_code=4326, cmin=cmin, cmax=cmax,
            ax=ax, cmap=cmap, show_colorbar=show_colorbar, colorbar_label=cbarLabel
        )
        if show_colorbar:
            cax = cbar.ax
            bbox = cax.get_position()
            axpos = ax.get_position()
            cax.set_position([bbox.x0, axpos.y0, bbox.width, axpos.height])

        if 'time' in field.dims:
            plt.suptitle(str(field.name) + " (Daily Mean)", ha='center', x=.45, y=.91, weight='bold', fontsize=16)
            if len(field.dims) <= 4:
                plt.title(field.attrs["long_name"] + "\n", wrap=True, fontsize=12)
            else:
                plt.title(field.attrs["long_name"] + "\n (vertical level = " + str(verti_level) + ")", wrap=True, fontsize=12)
            plt.figtext(0.45, 0.1, s=str(field.time.values[0])[:10] + ',\n ' + product_name, wrap=True, horizontalalignment='center', fontsize=11)
        else:
            plt.suptitle(str(field.name), ha='center', x=.45, y=.91, weight='bold', fontsize=16)
            if len(field.dims) <= 4:
                plt.title(field.attrs["long_name"] + "\n", wrap=True, fontsize=12)
            else:
                plt.title(field.attrs["long_name"] + "\n (vertical level = " + str(verti_level) + ")", wrap=True, fontsize=12)
            plt.figtext(0.45, 0.1, s=product_name, wrap=True, horizontalalignment='center', fontsize=11)

    plt.savefig(figure_path, dpi=300, facecolor='w', bbox_inches='tight', pad_inches=0.05)
    plt.close('all')


def plot_oneD(dataset: xr.Dataset, field: xr.DataArray, figure_path: str) -> None:
    """
    Create and save a simple xarray default plot for a 1-D data variable.

    Uses xarray's built-in ``.plot()`` method, which selects an appropriate
    plot type (line, step, etc.) based on the data's dimensions.

    :param dataset: The full dataset, used only for the ``product_name``
        attribute.
    :type dataset: xr.Dataset
    :param field: The 1-D variable to plot.
    :type field: xr.DataArray
    :param figure_path: Full path to the output PNG file.
    :type figure_path: str
    :returns: None
    """
    product_name = dataset.product_name
    dataset[field.name].plot()

    fig = plt.gcf()
    fig.set_size_inches(12, 6)
    plt.title(field.attrs["long_name"] + "\n", wrap=True, weight='bold', fontsize=16)
    plt.figtext(0.45, -0.05, s=product_name, wrap=True, horizontalalignment='center', fontsize=11)

    plt.savefig(figure_path, dpi=300, facecolor='w', bbox_inches='tight', pad_inches=0.05)
    plt.close('all')


def plot_datasetPicEg(dataset: xr.Dataset, save_to: str) -> None:
    """
    Generate a quick-look thumbnail plot for a dataset using its first variable.

    Selects the first variable in the dataset and the first time step (if
    present), then dispatches to either a tile plot (native grid) or a global
    Robinson plot (lat-lon). The output filename is derived from the dataset's
    ``metadata_link`` ShortName attribute.

    :param dataset: The dataset to visualise. Must have a ``metadata_link``
        global attribute containing a ``ShortName=`` query parameter.
    :type dataset: xr.Dataset
    :param save_to: Directory where the output PNG should be saved.
    :type save_to: str
    :returns: None
    """
    Dims_box = list(dataset.dims)
    Var_box  = list(dataset.data_vars)

    # Always use the first variable for the quick-look thumbnail
    var_sel = 0
    tmp_plt = dataset[Var_box[var_sel]]

    if 'time' in Dims_box:
        tmp_plt = dataset[Var_box[var_sel]].isel(time=0)

    # Skip surface layer for variables that are 0/NaN there
    target_k = 0
    if 'WVEL' in tmp_plt.name or 'DRHO' in tmp_plt.name:
        target_k = 1

    if 'k_l' in tmp_plt.dims:
        tmp_plt = tmp_plt.isel(k_l=target_k)
    elif 'k' in tmp_plt.dims:
        tmp_plt = tmp_plt.isel(k=target_k)
    elif 'Z' in tmp_plt.dims:
        tmp_plt = tmp_plt.isel(Z=target_k)

    cmin = np.nanmin(tmp_plt)
    cmax = np.nanmax(tmp_plt)
    cmap = copy.copy(plt.get_cmap('jet'))

    fig = plt.gcf()
    fig.set_size_inches(12, 6)

    if 'tile' in Dims_box:
        # Native grid: colour NaN cells dark grey to distinguish them from ocean
        cmap.set_bad(color='dimgray')
        ecco.plot_tiles(
            tmp_plt, cmin=cmin, cmax=cmax, fig_num=0, cmap=cmap,
            show_colorbar=False, show_tile_labels=False,
            fig_size=8, cbar_label=False, show_cbar_label=False
        )
    else:
        ax = plt.subplot(1, 1, 1, projection=ccrs.Robinson(central_longitude=200))
        p, gl, cbar = ecco.plot_global(
            dataset.longitude, dataset.latitude, tmp_plt,
            data_epsg_code=4326, cmin=cmin, cmax=cmax,
            ax=ax, cmap=cmap, show_colorbar=False, colorbar_label=False
        )

    # Derive the output filename from the ShortName in the metadata_link URL
    FILEname = dataset.metadata_link.split('ShortName=')[1] + '.png'
    fig_path = os.path.join(save_to, FILEname)

    plt.savefig(fig_path, dpi=300, facecolor='w', bbox_inches='tight', pad_inches=0.05)
    plt.close('all')


# ---------------------------------------------------------------------------
# ----------------------------- Helper Functions ----------------------------
# ---------------------------------------------------------------------------

def even_cax(cmin: float, cmax: float, fac: float = 1.0) -> tuple:
    """
    Make a colour axis symmetric about zero.

    Sets ``cmin = -tmp * fac`` and ``cmax = tmp * fac`` where
    ``tmp = max(|cmin|, |cmax|)``, ensuring the colour scale is centred on zero.

    :param cmin: Current minimum of the colour axis.
    :type cmin: float
    :param cmax: Current maximum of the colour axis.
    :type cmax: float
    :param fac: Scaling factor applied to the symmetric bound. Default is
        ``1.0``.
    :type fac: float
    :returns: A 2-tuple ``(cmin, cmax)`` with a symmetric range about zero.
    :rtype: tuple[float, float]
    """
    tmp = np.max([np.abs(cmin), np.abs(cmax)])
    cmin = -tmp * fac
    cmax =  tmp * fac
    return cmin, cmax


def cal_cmin_cmax(
    cmap: matplotlib.colors.LinearSegmentedColormap,
    cmin: float,
    cmax: float,
    shortname_tmp: str,
    product_type: str
) -> tuple:
    """
    Adjust the colormap and colour limits based on the variable's short name.

    Applies domain-specific rules to select a perceptually appropriate
    colormap and colour range:

    - Flux, stress, velocity, tendency, bolus → diverging ``cmocean.balance``,
      symmetric colour axis (factor 0.8; 0.25 for fresh-flux / momentum-tend).
    - Temperature / salinity → ``cmocean.thermal``.
    - Mixed layer depth → ``cmocean.deep``, clamped to [0, 250].
    - Sea ice concentration → ``cmocean.ice``, clamped to [0, 1].
    - Density stratification → ``cmocean.dense``.
    - All others → the default ``'jet'`` colormap passed in.

    :param cmap: Starting colormap (typically ``'jet'``).
    :type cmap: matplotlib.colors.LinearSegmentedColormap
    :param cmin: Data minimum used as fallback if no rule matches.
    :type cmin: float
    :param cmax: Data maximum used as fallback if no rule matches.
    :type cmax: float
    :param shortname_tmp: Dataset short name extracted from ``metadata_link``,
        used to identify the variable category via substring matching.
    :type shortname_tmp: str
    :param product_type: ``'Native'`` or ``'Latlon'``. Native products have
        NaN cells coloured dark grey.
    :type product_type: str
    :returns: A 3-tuple ``(cmap, cmin, cmax)`` with adjusted values.
    :rtype: tuple[matplotlib.colors.LinearSegmentedColormap, float, float]
    """
    if (
        ('STRESS'  in shortname_tmp) or
        ('FLUX'    in shortname_tmp) or
        ('VEL'     in shortname_tmp) or
        ('TEND'    in shortname_tmp) or
        ('BOLUS'   in shortname_tmp)
    ):
        fac = 0.8
        # Narrow the range further for products with large outliers
        if ('FRESH_FLUX' in shortname_tmp) or ('MOMENTUM_TEND' in shortname_tmp):
            fac = 0.25
        cmin, cmax = even_cax(cmin, cmax, fac)
        cmap = copy.copy(cmocean.cm.balance)

    elif 'TEMP_SALINITY' in shortname_tmp:
        cmap = copy.copy(cmocean.cm.thermal)

    elif 'MIXED_LAYER' in shortname_tmp:
        cmap = copy.copy(cmocean.cm.deep)
        cmin = 0
        cmax = 250

    elif 'SEA_ICE_CONC' in shortname_tmp:
        cmap = copy.copy(cmocean.cm.ice)
        cmin = 0
        cmax = 1

    elif 'DENS_STRAT' in shortname_tmp:
        cmap = copy.copy(cmocean.cm.dense)

    # Native-grid plots need NaN cells coloured to distinguish them from ocean
    if product_type == 'Native':
        cmap.set_bad(color='dimgray')

    return cmap, cmin, cmax


def compute_cmin_cmax(data, factor: float = 1.5) -> tuple:
    """
    Compute robust colour axis limits using the IQR method.

    Calculates lower and upper bounds as ``Q1 - factor * IQR`` and
    ``Q3 + factor * IQR``, then clamps to the actual data range to avoid
    extrapolation. This is more robust to outliers than using the global
    min/max directly.

    :param data: The data for which to compute colour limits. NaN values are
        ignored. If an :class:`xr.DataArray` is passed, it is converted to a
        numpy array internally.
    :type data: array-like or xr.DataArray
    :param factor: Multiplier for the IQR. Default is ``1.5`` (the standard
        Tukey fence).
    :type factor: float
    :returns: A 2-tuple ``(cmin, cmax)`` of robust colour axis limits.
    :rtype: tuple[float, float]
    """
    if isinstance(data, xr.DataArray):
        data = data.values

    Q1  = np.nanpercentile(data, 25)
    Q3  = np.nanpercentile(data, 75)
    IQR = Q3 - Q1

    lower_bound = Q1 - factor * IQR
    upper_bound = Q3 + factor * IQR

    # Clamp to the actual data range so we never extrapolate beyond the data
    cmin = max(np.nanmin(data), lower_bound)
    cmax = min(np.nanmax(data), upper_bound)

    return cmin, cmax


if __name__ == '__main__':
    """
    Command-line interface for plotting a single variable from a NetCDF file.

    Usage::

        python cdf_plotter.py --file PATH --field VARNAME [--directory DIR]
                              [--cbar BOOL] [--coords BOOL]

    :param --file: Path to the NetCDF file.
    :param --field: Name of the variable to plot, or ``'all'`` to plot every
        variable.
    :param --directory: Output directory for plot images. Defaults to
        ``'images/plots/{type}_plots/'``.
    :param --cbar: ``'True'`` or ``'False'``. Whether to show the colorbar.
        Default is ``True``.
    :param --coords: ``'True'`` or ``'False'``. Whether to plot coordinate
        variables. Default is ``False``.
    """
    parser = argparse.ArgumentParser(description='Plot a data variable.')
    parser.add_argument('--file',      required=True,  type=str)
    parser.add_argument('--field',     required=True,  type=str)
    parser.add_argument('--directory', required=False, type=str)
    parser.add_argument('--cbar',      required=False, type=str, default=None)
    parser.add_argument('--coords',    required=False, type=str, default=None)

    args = parser.parse_args()
    file      = args.file
    field     = args.field
    directory = args.directory

    cbar   = False if args.cbar   == 'False' else True
    coords = True  if args.coords == 'True'  else False

    ds = xr.open_dataset(args.file)

    # Determine the grid type from the file path for the output directory default
    if 'native' in file:
        type = 'native'
    elif 'lat-lon' in file:
        type = 'lat-lon'
    else:
        type = '1D'

    if args.field == 'all':
        fields = list(ds.coords if coords else ds.data_vars)
    else:
        fields = [args.field]

    for fi, f in enumerate(fields):
        field = ds[f]
        if directory is None:
            directory = 'images/plots/' + type + '_plots/'
        data_var_plot(ds, field, directory, cbar, coords)
