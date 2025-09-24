"""
"""

import lzma
import os
import pickle
from scipy import sparse
import tempfile

from . import aws
from . import ecco_task


class ECCOMappingFactors(object):
    """Container class for ECCO mapping factors access. Primarily intended to
    optimize i/o performance by allowing operations, e.g. collections of
    ECCOMDSDataset objects, to share a single ECCO mapping factors data
    resource.

    Args:
        task (str, dict, or taskobj): Optional (path and) name of json-formatted
            file defining a single task, single task description dictionary, or
            ECCOTask object. Either task or mapping_factors_loc may be provided,
            but not both.
        mapping_factors_loc (str): Optional pathname of either local ECCO
            mapping factors directory (top-level directory containing
            ecco_latlon_grid_mappings_2D.xz, ecco_latlon_grid_mappings_all.xz,
            and the subdirectories 3D, land_mask, latlon_grid, and sparse) or
            similar remote location given by AWS S3 bucket/prefix.  Either
            mapping_factors_loc or task may be provided but not both.
        **kwargs: If either task or mapping_factors_loc reference an AWS S3
            endpoint and if running within an institutionally-managed AWS IAM
            Identity Center (SSO) environment, additional arguments that may be
            necessary include:
            keygen (str): Federated login key generation script (e.g.,
                /usr/local/bin/aws-login-pub.darwin.amd64).
            profile (str): Optional profile to be used in combination with
                keygen (e.g., 'default', 'saml-pub', etc.)

    Attributes:
        task (ECCOTask): If provided, local object store of input task
            descriptor.
        mapping_factors_dir (str): Resulting local ECCO mapping factors
            directory name (see tmpdir).
        tmpdir (tempfile.TemporaryDirectory object): If task or
            mapping_factors_loc references an AWS S3 endpoint, temporary
            directory object whose 'name' attribute is assigned to
            mapping_factors_dir.

    Properties:
        latitude_bounds ((360,2) numpy.ndarray): Latitude grid bounds per
            ./latlon_grid/latlon_grid.xz.
        longitude_bounds ((720,2) numpy.ndarray): Longitude grid bounds per
            ./latlon_grid/latlon_grid.xz.
        depth_bounds ((no. of grid depths, 2) numpy.ndarray): Depth grid bounds
            per ./latlon_grid/latlon_grid.xz.

    """
    def __init__(self, task=None, mapping_factors_loc=None, **kwargs):
        """Create instance of MappingFactors class.
        
        """
        self.task = None
        self.__latlon_grid = None

        if task:
            if not isinstance(task,ecco_task.ECCOTask):
                # instantiate from file or dict:
                self.task = ecco_task.ECCOTask(task)
            else:
                # just use directly:
                self.task = task
            mapping_factors_loc = self.task['ecco_mapping_factors_loc']

        if aws.ecco_aws.is_s3_uri(mapping_factors_loc):
            # retrieve mapping factors to temporary local storage:
            self.tmpdir = tempfile.TemporaryDirectory()
            self.mapping_factors_dir = self.tmpdir.name
            aws.ecco_aws_s3_sync.aws_s3_sync(
                src=mapping_factors_loc, dest=self.mapping_factors_dir, **kwargs)
        else:
            # just point to local mapping factors directory:
            self.tmpdir = None
            self.mapping_factors_dir = mapping_factors_loc


    def latlon_land_mask( self, level):
        """Get numpy land mask vector of length number of latlon grid points
        corresponding to depth "level".

        """
        return pickle.load(lzma.open(os.path.join(
            self.mapping_factors_dir,'land_mask',f'ecco_latlon_land_mask_{level}.xz')))


    def native_to_latlon_mapping_factors( self, level):
        """Get scipy sparse matrix native to latlon grid mapping factors
        corresponding to depth "level".

        """
        return sparse.load_npz(
            os.path.join(self.mapping_factors_dir,'sparse',f'sparse_matrix_{level}.npz'))


    @property
    def latitude_bounds(self):
        if not self.__latlon_grid:
            self.__latlon_grid = pickle.load(lzma.open(os.path.join(
                self.mapping_factors_dir,'latlon_grid','latlon_grid.xz')))
        return self.__latlon_grid[0]['lat']


    @property
    def longitude_bounds(self):
        if not self.__latlon_grid:
            self.__latlon_grid = pickle.load(lzma.open(os.path.join(
                self.mapping_factors_dir,'latlon_grid','latlon_grid.xz')))
        return self.__latlon_grid[0]['lon']


    @property
    def depth_bounds(self):
        if not self.__latlon_grid:
            self.__latlon_grid = pickle.load(lzma.open(os.path.join(
                self.mapping_factors_dir,'latlon_grid','latlon_grid.xz')))
        return self.__latlon_grid[1]


    def __del__(self):
        """Remove temporary mapping factors directory when ECCOMappingFactors
        goes out of scope or is explicitly deleted.

        """
        try:
            self.tmpdir.cleanup()
        except:
            pass

