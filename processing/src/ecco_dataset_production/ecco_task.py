"""
"""

import json

from . import aws
#from . import ecco_aws
from . import ecco_file


class ECCOTask(dict):
    """Wrapper class for storage of, and operations on, ECCO task descriptors.

    Args:
        task (str or dict): (Path and) filename of json-formatted file defining
            a single task, or single task description dictionary.

    """
    def __init__( self, task=None):
        super().__init__()
        if task:
            if isinstance(task,str):
                # assume (path and) filename:
                t = json.load(open(task))
                if len(t) > 1:
                    raise ValueError('Task file input option cannot define multiple tasks')
                self.update(t[0])
            elif isinstance(task,dict):
                # assume parsed or valid task dictionary for single task:
                self.update(task)
            else:
                raise ValueError('If provided, task must be a single task (path and) filename or dict')


    @property
    def averaging_period(self):
        return ecco_file.ECCOGranuleFilestr(self.__getitem__('granule')).averaging_period


    @property
    def grid_type(self):
        return ecco_file.ECCOGranuleFilestr(self.__getitem__('granule')).grid_type
   

    @property
    def is_2d(self):
        return "2d"==self.__getitem__('dynamic_metadata')['dimension'].lower()


    @property
    def is_3d(self):
        return "3d"==self.__getitem__('dynamic_metadata')['dimension'].lower()


    @property
    def is_ecco_grid_dir_local(self):
        """
        """
        if aws.ecco_aws.is_s3_uri(self.__getitem__('ecco_grid_dir')):
            return False
        else:
            return True


    @property
    def is_granule_local(self):
        if aws.ecco_aws.is_s3_uri(self.__getitem__('granule')):
            return False
        else:
            return True


    @property
    def is_latlon(self):
        return ecco_file.ECCOGranuleFilestr(self.__getitem__('granule')).grid_type == 'latlon'


    @property
    def is_native(self):
        return ecco_file.ECCOGranuleFilestr(self.__getitem__('granule')).grid_type == 'native'


    @property
    def variable_names(self):
    #def variables(self):
        try:
            return list(self.__getitem__('variables').keys())
        except:
            return []


    def is_variable_single_component(self,variable=None):
        """
        """
        if len(self.__getitem__('variables')[variable]) == 1:
            return True
        else:
            return False


    def is_variable_input_local(self,variable=None):
        """
        """
        # just look at first file in list (reasonable assumption is that all are
        # local or all are remote (s3://...))

        if aws.ecco_aws.is_s3_uri(self.__getitem__('variables')[variable][0][0]):
            return False
        else:
            return True


    def variable_inputs(self,variable=None):
        try:
            return self.__getitem__('variables')[variable]
        except:
            return []

