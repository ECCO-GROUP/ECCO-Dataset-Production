"""ECCODatasetProductionConfig class for loading and managing configuration."""

import argparse
from collections import UserDict
import logging
import os
import tempfile
import yaml

from .. import aws
from .schema import Schema

log = logging.getLogger('edp.config')


class ConfigurationValidationError(Exception):
    """Raised when configuration validation fails."""
    pass


class ECCODatasetProductionConfig(UserDict):
    """Wrapper class for storage of, and basic operations on, ECCO Dataset
    Production configuration data.

    Configuration files are automatically validated against the schema and
    default values are applied for any missing optional parameters.

    Args:
        cfgfile (str): (Path and) filename of configuration file (yaml format),
            or similar remote location given by AWS S3 bucket/prefix/filename.
            If config_fields is specified, this is parsed from CLI args instead.
        config_fields (list): Optional list of config field names to expose as
            CLI arguments. When provided, automatically creates an ArgumentParser
            with --cfgfile and arguments for each field (types inferred from schema).
        argv (list): Optional command-line arguments to parse (default: sys.argv).
            Only used when config_fields is specified.
        **kwargs: If cfgfile references an AWS S3 endpoint and if running within
            an institutionally-managed AWS IAM Identity Center (SSO)
            environment, additional arguments that may be necessary include:
            keygen (str): Federated login key generation script (e.g.,
                /usr/local/bin/aws-login.darwin.universal, etc.).
            profile (str): Optional profile to be used in combination with
                keygen (e.g., 'default', 'saml-pub', etc.)

    Attributes:
        cfgfile (str): Local store of cfgfile input string.
        _local_cfgfile (str): Path to local config file (for validation).

    Raises:
        ConfigurationValidationError: If config is invalid.

    Examples:
        >>> # Load from file
        >>> cfg = ECCODatasetProductionConfig('config.yaml')

        >>> # Parse CLI arguments automatically
        >>> cfg = ECCODatasetProductionConfig(
        ...     config_fields=['array_precision', 'num_vertical_levels']
        ... )
        >>> # Creates parser with --cfgfile, --array_precision, --num_vertical_levels
    """

    def __init__(self, cfgfile=None, config_fields=None, argv=None, **kwargs):
        super().__init__()
        self._local_cfgfile = None
        self._schema = Schema()
        overrides = {}

        # If config_fields is specified, parse CLI arguments
        if config_fields is not None:
            parser = argparse.ArgumentParser(
                description='ECCO Dataset Production configuration'
            )
            parser.add_argument(
                '--cfgfile',
                required=True,
                help='Path to configuration YAML file'
            )

            # Add override arguments with type inference from schema
            defaults = self._schema.get_defaults()
            for field in config_fields:
                default_val = defaults.get(field)
                help_text = f'Override {field} from config file'

                arg_type = None
                if default_val is not None:
                    help_text += f' (default: {default_val})'
                    if isinstance(default_val, bool):
                        parser.add_argument(
                            f'--{field}',
                            action='store_true' if not default_val else 'store_false',
                            help=help_text
                        )
                        continue
                    else:
                        arg_type = type(default_val)

                parser.add_argument(
                    f'--{field}',
                    type=arg_type,
                    help=help_text
                )

            # Parse arguments
            args = parser.parse_args(argv)
            args_dict = vars(args)

            # Extract config file and overrides
            cfgfile = args_dict.pop('cfgfile')
            overrides = {
                k: v for k, v in args_dict.items()
                if v is not None
            }

        # Load config file (always required)
        if not cfgfile:
            raise ValueError("cfgfile is required")

        self.cfgfile = cfgfile
        if aws.utils.is_s3_uri(self.cfgfile):
            with tempfile.TemporaryDirectory() as tmpdir:
                tmpdir_and_fname = os.path.join(tmpdir, os.path.basename(self.cfgfile))
                log.debug('Fetching %s to %s', self.cfgfile, tmpdir_and_fname)
                aws.ecco_aws_s3_cp.aws_s3_cp(src=self.cfgfile, dest=tmpdir, **kwargs)
                self.update(yaml.safe_load(open(tmpdir_and_fname)))
                self._local_cfgfile = tmpdir_and_fname
        else:
            self.update(yaml.safe_load(open(self.cfgfile)))
            self._local_cfgfile = self.cfgfile

        # Apply defaults first so required fields with defaults are filled in
        self._apply_defaults()

        # Validate config file after defaults applied
        try:
            self._schema.validate(dict(self), source=self.cfgfile)
            log.info('Configuration file validation successful: %s', self.cfgfile)
        except Exception as e:
            msg = f"Configuration file '{self.cfgfile}' is invalid:\n{str(e)}"
            log.error(msg)
            raise ConfigurationValidationError(msg) from e

        # Apply CLI overrides if any were parsed (these override defaults)
        if overrides:
            log.debug('Applying CLI overrides: %s', overrides)
            self.update(overrides)

            # Validate again with CLI overrides applied
            try:
                self._schema.validate(dict(self), source=f'CLI overrides')
                log.info('Configuration with CLI overrides validation successful')
            except Exception as e:
                msg = f"Configuration with CLI overrides is invalid:\n{str(e)}\nOverrides: {overrides}"
                log.error(msg)
                raise ConfigurationValidationError(msg) from e

        log.debug('Using configuration data per "%s":', cfgfile)
        for k, v in self.items():
            log.debug(' %s: %s', k, v)

    def __getitem__(self, key):
        """In case of an undefined key, log a WARNING and return an empty string
        ('') instead of raising a key error.
        """
        try:
            return self.data[key]
        except:
            log.warning(f'Undefined configuration parameter reference, "%s".', key)
            return ''

    def _apply_defaults(self):
        """Apply default values to optional configuration parameters.

        This method sets default values for any parameters that are not
        already defined in the configuration. It modifies the configuration
        in place.
        """
        defaults = self._schema.get_defaults()

        for key, default_value in defaults.items():
            if key not in self.data or self.data[key] == '':
                self.data[key] = default_value
                log.debug('Applied default for %s: %s', key, default_value)
