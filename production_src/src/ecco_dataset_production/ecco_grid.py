"""
"""

import fnmatch
import glob
import logging
import numpy as np
import os
import tarfile
import tempfile
import xarray as xr

from . import aws
from . import ecco_task

NETCDF_GLOBSTR = '*.nc'
NETCDF_LATLON_GLOBSTR = '*latlon*.nc'
NETCDF_NATIVE_GLOBSTR = '*native*.nc'
ZIPFILE_GLOBSTR = '*.gz'


log = logging.getLogger('edp.'+__name__)


class ECCOGrid(object):
    """Container class for ECCO grid access. Primarily intended to optimize i/o
    performance by allowing operations, e.g. collections of ECCOMDSDataset
    objects, to share a single copy of ECCO native and target (latlon) grid
    data.

    Args:
        task (str, dict, or taskobj): Optional (path and) name of json-formatted file
            defining a single task, single task description dictionary, or
            ECCOTask object. Either task or grid_loc may be provided, but not both.
        grid_loc (str): Optional pathname of either local ECCO grid directory
            (containing XC.*, YC.*, *latlon*.nc, *native*.nc, etc. files), or
            similar remote location given by AWS S3 bucket/prefix.  Either
            grid_loc or task may be provided but not both.
        **kwargs: If either task or grid_loc reference an AWS S3 endpoint and if
            running within JPL's SSO environment, additional arguments that may
            be necessary include:
            keygen (str): Federated login key generation script (e.g.,
                /usr/local/bin/aws-login-pub.darwin.amd64).
            profile (str): Optional profile to be used in combination with
                keygen (e.g., 'default', 'saml-pub', etc.)

    Attributes:
        task (ECCOTask): If provided, local object store of input task
            descriptor.
        grid_dir (str): Resulting local ECCO grid directory name (see
            tmpdir).
        tmpdir (tempfile.TemporaryDirectory object): If task or grid_loc
            references an AWS S3 endpoint, temporary directory object whose
            'name' attribute is assigned to grid_dir. In the case of a zipped
            archive (see "Notes"), tmpdir's 'name' may be extended accordingly
            prior to assigning to grid_dir.

    Properties:
        latlon_grid (xarray Dataset object): Object resulting from
            xr.open_dataset() on *latlon*.nc (see Notes, item 2).
        native_grid (xarray Dataset object): Object resulting from
            xr.open_dataset() on *native*.nc (see Notes, item 2).
        native_wet_point_indices (dict): Integer-keyed dictionary (by depth;
           0 == surface through nz-1 == max depth) of "numpy.where" indices
           identifying ECCO native grid "wet" points (hFacC>0).

    Notes:
        1.) If the grid location referenced by task or grid_loc is an AWS S3
            endpoint and contains a zipped tarball only, it will be
            unzipped/tarred to a local temporary directory. If, however, the
            grid location is a local directory, unzipping/untarring will not be
            performed so as not to modify any local directory/file structures.
        2.) ECCOGrid assumes the presence of native (*native*.nc) and 'target'
            latlon (*latlon*.nc) grid NetCDF4 files in either the location
            provided by grid_loc or the AWS S3 endpoint referenced in the task
            description.

    """
    def __init__( self, task=None, grid_loc=None, **kwargs):
        """Create instance of ECCOGrid class.

        """
        self.task = None
        self._latlon_grid = None
        self._native_grid = None
        self._native_wet_point_indices = None

        if task and grid_loc:
            raise RuntimeError('Either task or grid_loc may be provided, but not both')

        if task:
            if not isinstance(task,ecco_task.ECCOTask):
            # instantiate from file or dict:
                self.task = ecco_task.ECCOTask(task)
            else:
                # just use directly:
                self.task = task
            grid_loc = self.task['ecco_grid_loc']

        if aws.ecco_aws.is_s3_uri(grid_loc):
            # retrieve ecco grid to temporary local storage:
            self.tmpdir = tempfile.TemporaryDirectory()
            self.grid_dir = self.tmpdir.name
            aws.ecco_aws_s3_sync.aws_s3_sync( src=grid_loc, dest=self.grid_dir, **kwargs)
            if not os.listdir(self.grid_dir):
                raise RuntimeError(
                    f'Remote grid fetch failed. Ensure grid_loc ({grid_loc}) refers to a valid s3 bucket and prefix (only).')
        else:
            # just point to local grid directory:
            self.tmpdir = None
            self.grid_dir = grid_loc

        # unzip/untar if remote archive; if local, check to make sure directory
        # contains unzipped/untarred data (i.e., don't make any changes to local
        # directory):
        if self.tmpdir:
            if  len(os.listdir(self.grid_dir))==1 and \
                fnmatch.fnmatch(os.listdir(self.grid_dir)[0],ZIPFILE_GLOBSTR):
                zf = glob.glob(os.path.join(self.grid_dir,ZIPFILE_GLOBSTR))[0]
                to = tarfile.open(zf)
                to.extractall(self.grid_dir)
                # use archive's hierarchy to (possibly) extend self.grid_dir
                # path. could search on anything, really, but NetCDF grid files
                # are (hopefully) pretty foolproof:
                ncdf_grid_files = \
                    [file for file in to.getnames() if fnmatch.fnmatch(file,NETCDF_GLOBSTR)]
                self.grid_dir = os.path.join(self.grid_dir,os.path.dirname(ncdf_grid_files[0]))
        else:
            if  len(os.listdir(self.grid_dir))==1 and \
                fnmatch.fnmatch(os.listdir(self.grid_dir)[0],ZIPFILE_GLOBSTR):
                raise RuntimeError(
                    'Local grid directory contains zipped tarball only; unzip/tar before proceeding.')

        log.debug('ECCO grid at location %s contains %s', self.grid_dir, os.listdir(self.grid_dir))


    @property
    def latlon_grid(self):
        """Returns latlon grid xarray.Dataset instance, if found. Raises
        RuntimeError exception if not.

        """
        if not self._latlon_grid:
            try:
                self._latlon_grid = xr.open_dataset(
                    glob.glob(os.path.join(self.grid_dir,NETCDF_LATLON_GLOBSTR))[0],
                    chunks='auto')
            except Exception as e:
                log.error("'latlon file with name matching '%s' could either not be opened or found in grid directory '%s'",
                    NETCDF_LATLON_GLOBSTR, self.grid_dir)
                raise RuntimeError(e)
        return self._latlon_grid


    @property
    def native_grid(self):
        """Returns native grid xarray.Dataset instance, if found. Raises
        RuntimeError exception if not.

        """
        if not self._native_grid:
            try:
                self._native_grid = xr.open_dataset(
                    glob.glob(os.path.join(self.grid_dir,NETCDF_NATIVE_GLOBSTR))[0],
                    chunks='auto')
            except Exception as e:
                log.error("'latlon file with name matching '%s' could either not be opened or found in grid directory '%s'",
                    NETCDF_NATIVE_GLOBSTR, self.grid_dir)
                raise RuntimeError(e)
        return self._native_grid


    @property
    def native_wet_point_indices(self):
        """Returns an nz integer-keyed dictionary (by depth; 0 == surface
        through nz-1 == max depth) of "numpy.where" indices identifying ECCO
        native grid "wet" points (hFacC>0).

        """
        if not self._native_wet_point_indices:
            native_wet_point_indices = {}
            for z in range(self.native_grid['hFacC'].shape[0]):
                native_wet_point_indices[z] = \
                    np.where(self.native_grid['hFacC'][z,:]>0)
            self._native_wet_point_indices = native_wet_point_indices
        return self._native_wet_point_indices


    def __del__(self):
        """Remove temporary grid directory when ECCOGrid goes out of scope or is
        explicitly deleted.

        """
        try:
            self.tmpdir.cleanup()
        except:
            pass

