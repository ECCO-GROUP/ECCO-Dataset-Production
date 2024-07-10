"""
"""

from . import ecco_aws
from . import ecco_file


class ECCOTask(object):
    """
    """

    def __init__(self,task=None):
        if task:
            self.task = task
        else:
            self.task = {}


    @property
    def ecco_grid_dir(self):
        return self.task['ecco_grid_dir']


    @property
    def is_ecco_grid_dir_local(self):
        """
        """
        if ecco_aws.is_s3_uri(self.task['ecco_grid_dir']):
            return False
        else:
            return True


    @property
    def is_latlon(self):
        return ecco_file.ECCOGranuleFilestr(self.task['granule']).grid_type == 'latlon'


    @property
    def is_native(self):
        return ecco_file.ECCOGranuleFilestr(self.task['granule']).grid_type == 'native'


    @property
    def variables(self):
        try:
            return list(self.task['variables'].keys())
        except:
            return []


    def is_variable_single_component(self,variable=None):
        """
        """
        if len(self.task['variables'][variable]) == 1:
            return True
        else:
            return False


    def is_variable_input_local(self,variable=None):
        """
        """
        # just look at first file in list (reasonable assumption is that all are
        # local or all are remote (s3://...))

        if ecco_aws.is_s3_uri(self.task['variables'][variable][0][0]):
            return False
        else:
            return True


    def variable_inputs(self,variable=None):
        try:
            return self.task['variables'][variable]
        except:
            return []

