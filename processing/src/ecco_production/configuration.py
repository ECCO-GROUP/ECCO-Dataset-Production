
import logging
import os
import yaml
import sys

log = logging.getLogger('ecco_dataset_production')


class ECCOProductionConfig(dict):
    """Wrapper class for storage of, and basic operations on, ECCO Production
    configuration data.

    Args:
        cfgfile (str): (Path and) filename of configuration file (yaml format).

    Attributes:
        cfgfile (str): Local store of cfgfile input string.

    """
    def __init__( self, **kwargs):
        super().__init__()
        self.cfgfile = kwargs.pop('cfgfile',None)
        if self.cfgfile:
            fp = open(self.cfgfile)
            self.update(yaml.safe_load(fp))
            fp.close()


    def parse_cfgfile( self, cfgfile=None):
        """If not defined at object creation (ref. __init__()), define and parse a
        configuration file.

        Args:
            cfgfile (str): (Path and) filename of configuration file (yaml format).

        """
        self.cfgfile = cfgfile
        fp = open(self.cfgfile)
        self.update(yaml.safe_load(fp))
        fp.close()


    def set_default_paths( self, workingdir='.'):
        """If otherwise unspecified via configuation input, set some default
        runtime paths:

            workingdir/
                ecco_grids/
                    ecco_version
                mapping_factors/
                    ecco_version/
                metadata/
                    ecco_version/
                tmp/
                    tmp_model_output/
                        ecco_version/
                    tmp_output/
                        ecco_version

        Args:
            workingdir (str): Path (absolute or relative) to ECCO Production
            top-level working directory.
        """

        try:
            ecco_version = self.__getitem__('ecco_version')

            if not self.__getitem__('ecco_grid_dir'):
                self.__setitem__(
                    'ecco_grid_dir',
                    os.path.join(workingdir,'ecco_grids',ecco_version))

            if not self.__getitem__('ecco_grid_dir_mds'):
                self.__setitem__(
                    'ecco_grid_dir_mds',
                    os.path.join(workingdir,'ecco_grids',ecco_version))

            # if custom factors, default otherwise:
            if self.__getitem__('custom_grid_and_factors'):
                self.__setitem__(
                    'mapping_factors_dir',
                    self.__getitem__('custom_grid_and_factors'))
            elif not self.__getitem__('mapping_factors_dir'):
                self.__setitem__(
                    'mapping_factors_dir',
                    os.path.join(workingdir,'mapping_factors',ecco_version))

            if not self.__getitem__('metadata_dir'):
                self.__setitem__(
                    'metadata_dir',
                    os.path.join(workingdir,'metadata',ecco_version))

            if not self.__getitem__('model_output_dir'):
                self.__setitem__(
                    'model_output_dir',
                    os.path.join(workingdir,'tmp','tmp_model_output',ecco_version))

            if not self.__getitem__('processed_output_dir_base'):
                self.__setitem__(
                    'processed_output_dir_base',
                    os.path.join(workingdir,'tmp','tmp_output',ecco_version))

            # here, if necessary:
            # 'ecco_code_name', 'ecco_code_dir'
            # 'ecco_configurations_name', 'ecco_configurations_subfolder'

        except:
            log.error('Configuration data (cfgfile) have not been provided.')
            sys.exit(1)

