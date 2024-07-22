"""
"""

import numpy as np
import os
import shutil
import tempfile
import xarray as xr
from xmitgcm import open_mdsdataset

from . import aws
#from . import ecco_aws_s3_cp
#from . import ecco_aws_s3_sync
from . import ecco_file
from . import ecco_grid
from . import ecco_task


class ECCOMDSDataset(object):
    """Class that organizes production-oriented operations on ECCO MDS results
    datasets.

    Args:
        task (str, dict, or taskobj): (Path and) name of json-formatted file
            defining a single task, single task description dictionary, or
            ECCOTask object.
        variable (str) : Selected task list variable.
        grid (ECCOGrid object): Optional grid object. If not provided, grid will
            be determined from task input. Primarily intended to optimize i/o
            performance by allowing operations on collections to share a single
            copy of an ECCOGrid object created by a top-level application.
        cfg (dict): ECCO Dataset Production configuration data. Referenced
            fields include:
            ecco_native_grid_filename: ECCO NetCDF native grid file name (e.g.,
                GRID_GEOMETRY_ECCO_V4r4_native_llc0090.nc).
            model_geometry: ECCO model geometry (e.g., 'llc').
            read_grid: Passed via the xmitgcm.open_mdsdataset input parameter of
                the same name (e.g., 'False').

    Attributes:
        cfg (dict): Local store of cfg input.
        ds (xarray.Dataset): ECCO MDS dataset for specified task and variable.
        data_dir (str): Temporary directory name for local store of input ECCO
            MDS files specified by task and variable.
        grid (ECCOGrid object): Local reference to ecco_grid input, if
            provided, or local object if not (and thus generated using task
            object specifier).
        task (ECCOTask): Local object store of input task descriptor.
        tmp_grid_dir (TemporaryDirectory): Temporary local ECCO grid directory
            if non-local source as specified in task descriptor.
        tmp_data_dir (TemporaryDirectory): Temporary local ECCO MDS file storage
            for task and variable combination.

    """
    def __init__( self, task=None, grid=None, variable=None, cfg=None,
        **kwargs):
        """Create instance of ECCOMDSDataset class.

        """
        self.task = None
        self.cfg = cfg

        if task:

            if not isinstance(task,ecco_task.ECCOTask):
                # instantiate from file or dict:
                self.task = ecco_task.ECCOTask(task)
            else:
                # just use directly:
                self.task = task

            # create dataset using variable and data references in task
            # description:

            # ensure grid data exists locally:
            if not grid:
                # create grid from task definition:
                self.grid = ecco_grid.ECCOGrid(task=self.task,**kwargs)
            elif isinstance(grid,ecco_grid.ECCOGrid):
                # point to existing object:
                self.grid = grid
            else:
                raise ValueError('If provided, grid must be an instance of ECCOGrid class.')

            # gather variable input locally (not the most efficient approach for
            # data that may already be stored locally, but cleanest way to
            # handle variables with multiple inputs and/or data stored in aws
            # s3 buckets):
            self.tmp_data_dir = tempfile.TemporaryDirectory()
            self.data_dir = self.tmp_data_dir.name
            if self.task.is_variable_input_local(variable):
                for components in self.task.variable_inputs(variable):
                    for file in components:
                        shutil.copy(file,self.data_dir)
            else:
                for components in self.task.variable_inputs(variable):
                    for file in components:
                        aws.ecco_aws_s3_cp.aws_s3_cp( src=file, dest=self.data_dir, **kwargs)

            if self.task.is_variable_single_component(variable):
                # direct ingest:
                # use first file in the list (.data file) to provide some of the
                # input required by open_mdsdataset:
                mds_file = ecco_file.ECCOMDSFilestr(
                    os.path.basename(
                        self.task.variable_inputs(variable)[0][0]))
                self.ds = open_mdsdataset(
                    data_dir=self.data_dir, grid_dir=self.grid.grid_dir,
                    read_grid=self.cfg['read_grid'],
                    geometry=self.cfg['model_geometry'],
                    prefix=mds_file.prefix+'_'+mds_file.averaging_period,
                    iters=[mds_file.time])

            else:
                # vector summation required before ingest:
                pass    # for now...


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
        self.ds[variable] = self.ds[variable] * np.where(self.grid.native_grid[mask_type][so]==True,1,np.nan)
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
        """Determines wheter or not specified variable is three-dimensional by
        checking to see if any of its dimensions are indexed by (a) string(s)
        that include(s) 'k'.

        Args:
            variable (str): ECCO resuls variable name.

        Returns:
            True if any dimension string includes 'k', False otherwise.

        """
        return any(['k' in dim for dim in self.ds[variable].dims])


    def drop_all_variables_except( self, variable=None):
        """Ensures that no other than specified variable exist in the dataset.

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
        #try:
        #    self.tmp_grid_dir.cleanup()
        #except:
        #    pass
        try:
            self.tmp_data_dir.cleanup()
        except:
            pass

