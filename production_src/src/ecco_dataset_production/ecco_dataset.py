"""
"""

import logging
import numpy as np
import os
import pandas as pd
import shutil
import tempfile
import xarray as xr

import ecco_v4_py

from . import aws
from . import ecco_file
from . import ecco_grid
from . import ecco_mapping_factors
from . import ecco_task


log = logging.getLogger('edp.'+__name__)


class ECCOMDSDataset(object):
    """Class that supports dataset production-oriented operations on ECCO
    results datasets.

    Args:
        task (str, dict, or taskobj): (Path and) name of json-formatted file
            defining a single task, single task description dictionary, or
            ECCOTask object.
        variable (str) : Selected task list variable.
        grid (ECCOGrid object): Optional grid object. If not provided, grid data
            will be fetched using input task descriptor. Primarily intended to
            optimize i/o performance by allowing collections to share a single
            copy of an ECCOGrid object created by the calling application.
        mapping_factors (ECCOMappingFactors object): Optional mapping_factors
            object. If not provided, mapping_factors data will be fetched using
            input task descriptor.  Primarily intended to optimize i/o
            performance by allowing collections to share a single copy of an
            ECCOMappingFactors object created by the calling application.
        cfg (dict): Parsed ECCO dataset production yaml file. Referenced
            fields include:
            ecco_native_grid_filename: ECCO NetCDF native grid file name (e.g.,
                GRID_GEOMETRY_ECCO_V4r4_native_llc0090.nc).
            model_geometry: ECCO model geometry (e.g., 'llc').
            read_grid: Passed via the read_bin_llc.load_ecco_vars_from_mds input
                parameter of the same name (e.g., 'False').
        tmpdir (str): Optional temporary directory for task list variable input
            data store. If not defined, temporary storage will be created
            locally, and separately, for each variable. The primary intent of
            this shared storage space is to minimize data download for those
            cases in which variables (re)use data such as vector transformed
            fields (UV -> EW/NS).
        **kwargs: If task references AWS S3 endpoint data and if running within
            an institutionally-managed AWS IAM Identity Center (SSO)
            environment, additional arguments that may be necessary include:
            keygen (str): Federated login key generation script (e.g.,
                /usr/local/bin/aws-login-pub.darwin.amd64).
            profile (str): Optional profile to be used in combination with
                keygen (e.g., 'default', 'saml-pub', etc.)

    Attributes:
        cfg (dict): Local store of cfg input.
        ds (xarray.Dataset, or list of same): ECCO MDS dataset(s) for specified
            task and variable (list of xarray.Datasets if variable is a
            vector-transformed quantity, single xarray.Dataset otherwise).
        #data_dir (str): Temporary directory name for local store of input ECCO
        #    MDS files specified by task and variable.
        grid (ECCOGrid object): Local reference to ecco_grid input, if
            provided, or local object if not (and thus fetched using task object
            specifier).
        mapping_factors (ECCOMappingFactors object): Local reference to
            ecco_mapping_factors input, if provided, or local object if not (and
            thus fetched using task object specifier).
        task (ECCOTask): Local object store of input task descriptor.
        #tmp_data_dir (TemporaryDirectory): Temporary local ECCO MDS file storage
        #    for task and variable combination.
        tmpdir (TemporaryDirectory): Local store of TemporaryDirectory instance
            if tmpdir input argument not provided.

    """
    def __init__( self, task=None, variable=None, grid=None,
        mapping_factors=None, cfg=None, tmpdir=None, **kwargs):
        """Create instance of ECCOMDSDataset class.

        """
        self.task = None
        self.grid = None
        self.mapping_factors = None
        self.cfg = cfg

        if grid:
            if isinstance(grid,ecco_grid.ECCOGrid):
                # point to existing (input) object:
                self.grid = grid
            else:
                raise ValueError('Input grid must be an instance of ECCOGrid class.')

        if mapping_factors:
            if isinstance(mapping_factors,ecco_mapping_factors.ECCOMappingFactors):
                # point to existing (input) object:
                self.mapping_factors = mapping_factors
            else:
                raise ValueError(
                    'Input mapping_factors must be an instance of ECCOMappingFactors class.')

        if task:

            if not isinstance(task,ecco_task.ECCOTask):
                # instantiate from file or dict:
                self.task = ecco_task.ECCOTask(task)
            else:
                # just use directly:
                self.task = task

            # create dataset using variable and data references in task
            # description:

            # ensure grid data are locally accessible:
            if not self.grid:
                # fetch grid using task definition:
                self.grid = ecco_grid.ECCOGrid(task=self.task,**kwargs)

            # ensure mapping factors are locally accessible:
            if not self.mapping_factors:
                # fetch mapping factors using task definition:
                self.mapping_factors = ecco_mapping_factors.ECCOMappingFactors(
                    task=self.task,**kwargs)

            # gather variable input locally (not the most efficient approach for
            # data that may already be stored locally, but cleanest way to
            # handle variables with multiple inputs and/or source locations,
            # and/or data stored in aws s3 buckets):

            if not tmpdir:
                self.tmpdir = tempfile.TemporaryDirectory()
                tmpdir = self.tmpdir.name

            #self.tmp_data_dir = tempfile.TemporaryDirectory()
            #self.data_dir = self.tmp_data_dir.name

            if self.task.is_variable_input_local(variable):
                for components in self.task.variable_inputs(variable):
                    for file in components:
                        if os.path.basename(file) not in os.listdir(tmpdir):
                            # TODO: replace with os.symlink
                            shutil.copy(file,tmpdir)
            else:
                for components in self.task.variable_inputs(variable):
                    for file in components:
                        if os.path.basename(file) not in os.listdir(tmpdir):
                            aws.ecco_aws_s3_cp.aws_s3_cp( src=file, dest=tmpdir, **kwargs)

            if self.task.is_variable_single_component(variable):

                # direct ingest:

                # use first file in the list to tell us something about how it's
                # to be read:
                _,ext = os.path.splitext(
                    self.task.variable_inputs(variable)[0][0])

                if ext == '.data' or ext == '.meta':

                    # time-dependent ECCO results from compact data/meta files:

                    mds_file = ecco_file.ECCOMDSFilestr(
                        os.path.basename(
                            self.task.variable_inputs(variable)[0][0]))

                    # back-compatibility with ecco_v4_py.read_bin_llc.load_ecco_vars_from_mds:
                    if self.task['dynamic_metadata']['time_coverage_duration'] == 'P1D':
                        output_freq_code = 'AVG_DAY'
                    elif self.task['dynamic_metadata']['time_coverage_duration'] == 'P1M':
                        output_freq_code = 'AVG_MON'
                    elif self.task['dynamic_metadata']['time_coverage_duration'] == 'PT0S':
                        output_freq_code = 'SNAP'
                    else:
                        e1 = "Unknown task['dynamic_metadata']['time_coverage_duration'] type:"
                        e2 = self.task['dynamic_metadata']['time_coverage_duration']
                        log.error('%s %s',e1,e2)
                        raise RuntimeError(f'{e1} {e2}')

                    self.ds = ecco_v4_py.read_bin_llc.load_ecco_vars_from_mds(
                        mds_var_dir             = tmpdir,
                        mds_grid_dir            = self.grid.grid_dir,
                        mds_files               = mds_file.prefix+'_'+mds_file.averaging_period,
                        vars_to_load            = mds_file.prefix,
                        #vars_to_load            = variable,
                        drop_unused_coords      = True,
                        grid_vars_to_coords     = False,
                        output_freq_code        = output_freq_code,
                        model_time_steps_to_load= [mds_file.time],
                        read_grid               = True,
                        #read_grid               = self.cfg['read_grid'],
                        model_start_datetime    = np.datetime64(self.cfg['model_start_time']))

                    if mds_file.prefix != variable:
                        self.ds = self.ds.rename_vars({mds_file.prefix:variable})

                elif ext == '.nc':

                    # 1D time-invariant results:

                    # TODO:
                    pass

                else:
                    err = f"Unrecognized file type (extension = '{ext}')"
                    log.error(err)
                    raise RuntimeError(err)

            else:

                # field interpolation / vector transformation required:

                ds = []     # accumulate vector components

                for component in self.task.variable_inputs(variable):

                    # determine method by which component is to be read:
                    _,ext = os.path.splitext(component[0])

                    if ext == '.data' or ext == '.meta':

                        # time-dependent ECCO results from compact data/meta files:

                        mds_file = ecco_file.ECCOMDSFilestr(
                            os.path.basename(component[0]))

                        # back-compatibility with ecco_v4_py.read_bin_llc.load_ecco_vars_from_mds:
                        if self.task['dynamic_metadata']['time_coverage_duration'] == 'P1D':
                            output_freq_code = 'AVG_DAY'
                        elif self.task['dynamic_metadata']['time_coverage_duration'] == 'P1M':
                            output_freq_code = 'AVG_MON'
                        elif self.task['dynamic_metadata']['time_coverage_duration'] == 'PT0S':
                            output_freq_code = 'SNAP'
                        else:
                            e1 = "Unknown task['dynamic_metadata']['time_coverage_duration'] type:"
                            e2 = self.task['dynamic_metadata']['time_coverage_duration']
                            log.error('%s %s',e1,e2)
                            raise RuntimeError(f'{e1} {e2}')

                        ds.append(
                            ecco_v4_py.read_bin_llc.load_ecco_vars_from_mds(
                                mds_var_dir             = tmpdir,
                                mds_grid_dir            = self.grid.grid_dir,
                                mds_files               = mds_file.prefix+'_'+mds_file.averaging_period,
                                vars_to_load            = mds_file.prefix,
                                #vars_to_load            = variable,
                                drop_unused_coords      = True,
                                grid_vars_to_coords     = False,
                                output_freq_code        = output_freq_code,
                                model_time_steps_to_load= [mds_file.time],
                                read_grid               = True,
                                #read_grid               = self.cfg['read_grid'],
                                model_start_datetime    = np.datetime64(self.cfg['model_start_time'])) )

                # ds[0], ds[1] will each contain 'x' or 'y' type fields (e.g.,
                # UVEL, VVEL);  for purposes of UEVNfromUXVY call, unambiguously
                # determine which is which, and satisfy numpy array function
                # input requirements:

                _xfld = _yfld = None

                for i in range(len(ds)):

                    if task['dynamic_metadata']['field_components'][variable]['x'] in ds[i].data_vars:
                        _xfld_dataset_varname = task['dynamic_metadata']['field_components'][variable]['x'] # i.e., "UVEL"
                        # make sure data are in np.array form to avoid "The
                        # truth value of a Array is ambiguous. Use a.any() or
                        # a.all()" error in UEVNfromUXVY call:
                        ds[i][_xfld_dataset_varname].data = np.array(ds[i][_xfld_dataset_varname])
                        _xfld = ds[i][_xfld_dataset_varname]

                    elif task['dynamic_metadata']['field_components'][variable]['y'] in ds[i].data_vars:
                        _yfld_dataset_varname = task['dynamic_metadata']['field_components'][variable]['y'] # i.e., "VVEL"
                        # make sure data are in np.array form to avoid "The
                        # truth value of a Array is ambiguous. Use a.any() or
                        # a.all()" error in UEVNfromUXVY call:
                        ds[i][_yfld_dataset_varname].data = np.array(ds[i][_yfld_dataset_varname])
                        _yfld = ds[i][_yfld_dataset_varname]


                # UEVNfromUXVY produces zonal and meridional component fields
                # (in that order); for return value purposes, unambiguously
                # determine output variable ordering (keeping in mind that, in
                # the current context, only one of the output quantities will be
                # retained.

                (_zonal, _meridional) = ecco_v4_py.vector_calc.UEVNfromUXVY(
                    xfld=_xfld,
                    yfld=_yfld,
                    coords=self.grid.native_grid)   # native_grid contains 'CS', 'SN' variables

                # save the applicable output, _zonal or _meridional:

                if task['dynamic_metadata']['field_orientations'][variable] == 'zonal':
                    self.ds = xr.Dataset(data_vars={variable:_zonal})
                elif task['dynamic_metadata']['field_orientations'][variable] == 'meridional':
                    self.ds = xr.Dataset(data_vars={variable:_meridional})
                else:
                    e1 = f"task['dynamic_metadata']['field_orientations'][{variable}]="
                    e2 = f"{task['dynamic_metadata']['field_orientations'][{variable}]}; "
                    e3 = "value must either be 'zonal' or 'meridional'."
                    log.error(e1+e2+e3)
                    raise RuntimeError(e1+e2+e3)


    def as_latlon( self, variable=None):
        """Recast variable in latlon format, return as a named (using variable
        string) xarray DataArray.

        """
        if self.task.is_2d:

            # output allocation:
            variable_as_latlon = np.zeros(
                (self.grid.latlon_grid['latitude'].shape[0],
                self.grid.latlon_grid['longitude'].shape[0]))

            # operate on surface (z=0) only:
            var = self.ds[variable].data.squeeze()                  # dask array, native grid,
                                                                    # no singleton dimensions
            var = var.vindex[self.grid.native_wet_point_indices[0]] # dask array, native grid,
                                                                    # no singleton dimensions,
                                                                    # surface wet points only,
                                                                    # as vector
            var_latlon = self.mapping_factors.native_to_latlon_mapping_factors(level=0).T.dot(var)
                                                                    # numpy (vector) array,
                                                                    # as latlon
            var_latlon_land_masked = np.where(
                np.isnan(self.mapping_factors.latlon_land_mask(level=0)), np.nan, var_latlon)
                                                                    # land values as NaNs
            variable_as_latlon[:] = var_latlon_land_masked.reshape( # numpy 1D array to
                self.grid.latlon_grid['latitude'].shape[0],         # lat x lon array
                self.grid.latlon_grid['longitude'].shape[0])

            # add a "time" axis ((lat,lon) -> (time,lat,lon)):
            variable_as_latlon = np.expand_dims(variable_as_latlon,0)

            variable_as_latlon_da = xr.DataArray(
                name=variable,
                data=variable_as_latlon,
                dims=['time','latitude','longitude'],
                coords=[
                    [pd.Timestamp(self.task['dynamic_metadata']['time_coverage_center'])],
                    self.grid.latlon_grid['latitude'].data,
                    self.grid.latlon_grid['longitude'].data])

        elif self.task.is_3d:

            # num vertical depths:
            nz = self.grid.latlon_grid.sizes['Z']

            # output allocation:
            variable_as_latlon = np.zeros(
                (nz,
                self.grid.latlon_grid['latitude'].shape[0],
                self.grid.latlon_grid['longitude'].shape[0]))

            var = self.ds[variable].data.squeeze()                      # dask array, native grid,
                                                                        # no singleton dimensions
            for z in range(nz):

                var_z = var[z,:].vindex[                                # dask array, native grid,
                    self.grid.native_wet_point_indices[z]]              # no singleton dimensions,
                                                                        # level z wet points only,
                                                                        # as vector
                var_z_latlon = \
                    self.mapping_factors.native_to_latlon_mapping_factors(level=z).T.dot(var_z)
                                                                        # numpy (vector) array,
                                                                        # level z, as latlon
                var_z_latlon_land_masked = np.where(
                    np.isnan(self.mapping_factors.latlon_land_mask(level=z)), np.nan, var_z_latlon)
                                                                        # land values, level z,
                                                                        # as NaNs
                variable_as_latlon[z,:] = var_z_latlon_land_masked.reshape(
                    self.grid.latlon_grid['latitude'].shape[0],         # numpy 1D array to
                    self.grid.latlon_grid['longitude'].shape[0])        # lat x lon array


            # add a "time" axis ((z,lat,lon) -> (time,z,lat,lon)):
            variable_as_latlon = np.expand_dims(variable_as_latlon,0)

            variable_as_latlon_da = xr.DataArray(
                name=variable,
                data=variable_as_latlon,
                dims=['time','Z','latitude','longitude'],
                coords=[
                    [pd.Timestamp(self.task['dynamic_metadata']['time_coverage_center'])],
                    self.grid.latlon_grid['Z'].data,
                    self.grid.latlon_grid['latitude'].data,
                    self.grid.latlon_grid['longitude'].data])

        # note: could "promote" to xr.Dataset and add time_bnds coordinates here
        # as had been done in the original code but, since this is done during
        # dataset production by the calling, and folow-on metadata/attributes,
        # code just skip for now.

        return variable_as_latlon_da


    def apply_land_mask_to_native_variable( self, variable=None):
        """ Apply 1/NaN-based, grid-appropriate land mask to specified variable
        of 'native' results type.

        Args:
            variable (str): ECCO resuls variable name.

        Returns:
            No return; self.ds[variable] masked in-place.

        TODO: Modify routine so lon/lat/native question is answered by
        task['granule'] filename; then function can be generically-named.

        """
        ## get native land mask data from ECCO grid dataset... :
        #ecco_grid_ds = xr.open_dataset(os.path.join(
        #    self.grid.grid_dir,self.cfg['ecco_native_grid_filename']))

        # apply grid-appropriate mask to the variable of interest:
        # ...and apply grid-appropriate mask to the variable of interest:
        if self.is_variable_c_data(variable):
            mask_type = 'maskC'
        elif self.is_variable_w_data(variable):
            mask_type = 'maskW'
        elif self.is_variable_s_data(variable):
            mask_type = 'maskS'
        else:
            raise RuntimeError(f"Could not determine grid type for variable '{variable}'")
        # numpy slice object:
        if self.is_variable_3d(variable):
            so = np.s_[:]
        else:
            so = np.s_[0,:]
        mask = self.grid.native_grid[mask_type]
        if mask.chunks is not None:
            mask.load()
        self.ds[variable] = self.ds[variable] * np.where(mask[so]==True,1,np.nan)
        #self.ds[variable] = self.ds[variable] * np.where(self.grid.native_grid[mask_type][so]==True,1,np.nan)
        #self.ds[variable] = self.ds[variable] * np.where(ecco_grid_ds[mask_type][so]==True,1,np.nan)


    def is_variable_c_data( self, variable=None):
        """Determines whether or not specified variable is 'C' point-based data
        by checking to see if its dimensions are ('i','j') indexed.

        Args:
            variable (str): ECCO resuls variable name.

        Returns:
            True if dimensions include ('i','j'), False otherwise.

        """
        return all(dim in self.ds[variable].dims for dim in ('i','j'))


    def is_variable_w_data( self, variable=None):
        """Determines whether or not specified variable is 'W' ('U') point-based
        data by checking to see if its dimensions are ('i_g','j') indexed.

        Args:
            variable (str): ECCO resuls variable name.

        Returns:
            True if dimensions include ('i_g','j'), False otherwise.

        """
        return all(dim in self.ds[variable].dims for dim in ('i_g','j'))


    def is_variable_s_data( self, variable=None):
        """Determines whether or not specified variable is 'S' ('V') point-based
        data by checking to see if its dimensions are ('i','j_g') indexed.

        Args:
            variable (str): ECCO resuls variable name.

        Returns:
            True if dimensions include ('i','j_g'), False otherwise.

        """
        return all(dim in self.ds[variable].dims for dim in ('i','j_g'))


    def is_variable_3d( self, variable=None):
        """Determines whether or not specified variable is three-dimensional by
        checking to see if any of its dimensions are indexed by (a) string(s)
        that include(s) 'k'.

        Args:
            variable (str): ECCO resuls variable name.

        Returns:
            True if any dimension string includes 'k', False otherwise.

        """
        return any(['k' in dim for dim in self.ds[variable].dims])


    def drop_all_variables_except( self, variable=None):
        """Ensures that no other than specified variable exists in the dataset.

        Args:
            variable (str): ECCO resuls variable name.

        Returns:
            No return value; self.ds modified in place.

        """
        all_vars = set(self.ds.data_vars.keys())
        self.ds.drop_vars(all_vars.difference([variable]))


    def __del__(self):
        """Removes class temporary directories when ECCOMDSDataset goes out of
        scope or is explicitly deleted.

        """
        # clean up TemporaryDirectories:
        try:
            self.tmpdir.cleanup()
        except:
            pass

