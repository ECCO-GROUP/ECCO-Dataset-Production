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

        >>> # For CLI tools with config field overrides
        >>> parser = ECCODatasetProductionConfig.create_parser(
        ...     config_fields=['array_precision', 'num_vertical_levels']
        ... )
        >>> parser.add_argument('--jobfile', required=True, help='Job file')
        >>> parser.add_argument('--outfile', required=True, help='Output file')
        >>> args = parser.parse_args()
        >>> cfg = ECCODatasetProductionConfig.from_parsed_args(
        ...     args, config_fields=['array_precision', 'num_vertical_levels']
        ... )
        >>> # Use cfg for config, args for tool-specific arguments
    """

    def __init__(self, cfgfile, **kwargs):
        super().__init__()
        self._local_cfgfile = None
        self._schema = Schema()

        # Load config file
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

        log.debug('Using configuration data per "%s":', cfgfile)
        for k, v in self.items():
            log.debug(' %s: %s', k, v)

    @classmethod
    def create_parser(cls, config_fields=None, description=None):
        """Create an ArgumentParser with config file and optional config field arguments.

        This factory method creates a parser with --cfgfile and optional config field
        override arguments. Tools can add their own arguments before parsing.

        Args:
            config_fields (list): Optional list of config field names to expose as
                CLI arguments with types inferred from schema defaults.
            description (str): Optional description for the argument parser.

        Returns:
            argparse.ArgumentParser: Parser with --cfgfile and config field arguments.
                The parser has a _config_fields attribute to track which args are
                config overrides.

        Example:
            >>> parser = ECCODatasetProductionConfig.create_parser(
            ...     config_fields=['array_precision'],
            ...     description='My tool'
            ... )
            >>> parser.add_argument('--jobfile', required=True)
            >>> args = parser.parse_args()
            >>> cfg = ECCODatasetProductionConfig.from_parsed_args(args)
        """
        if description is None:
            description = 'ECCO Dataset Production configuration'

        parser = argparse.ArgumentParser(description=description)
        parser.add_argument(
            '--cfgfile',
            required=True,
            help='Path to configuration YAML file'
        )

        # Track which fields are config overrides
        parser._config_fields = config_fields or []

        # Add config field override arguments if specified
        if config_fields:
            schema = Schema()
            defaults = schema.get_defaults()

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

        return parser

    @classmethod
    def from_parsed_args(cls, args, config_fields=None, **kwargs):
        """Create a config instance from parsed argparse arguments.

        This factory method extracts the cfgfile and any config field overrides
        from parsed arguments, then loads and validates the configuration.

        Args:
            args (argparse.Namespace): Parsed arguments from ArgumentParser.
            config_fields (list): Optional list of field names that are config overrides.
                If not provided, will attempt to extract all non-cfgfile args as overrides.
            **kwargs: Additional arguments passed to __init__ (e.g., keygen, profile
                for S3 access).

        Returns:
            ECCODatasetProductionConfig: Loaded and validated configuration instance.

        Raises:
            ConfigurationValidationError: If config is invalid.
            AttributeError: If args doesn't have a 'cfgfile' attribute.

        Example:
            >>> parser = ECCODatasetProductionConfig.create_parser(['array_precision'])
            >>> parser.add_argument('--jobfile', required=True)
            >>> args = parser.parse_args(['--cfgfile', 'config.yaml', '--jobfile', 'jobs.txt'])
            >>> cfg = ECCODatasetProductionConfig.from_parsed_args(
            ...     args, config_fields=['array_precision']
            ... )
            >>> # Access config via cfg, tool args via args.jobfile
        """
        if not hasattr(args, 'cfgfile'):
            raise AttributeError(
                "Parsed args must have 'cfgfile' attribute. "
                "Did you create the parser with create_parser()?"
            )

        args_dict = vars(args)
        cfgfile = args_dict['cfgfile']

        # Extract config field overrides
        overrides = {}
        if config_fields:
            # Only extract fields that were explicitly marked as config fields
            for field in config_fields:
                if field in args_dict and args_dict[field] is not None:
                    overrides[field] = args_dict[field]

        schema = Schema()

        # Create instance by directly loading the file and applying overrides
        instance = cls.__new__(cls)
        UserDict.__init__(instance)
        instance._local_cfgfile = None
        instance._schema = schema
        instance.cfgfile = cfgfile

        # Load config file
        if aws.utils.is_s3_uri(cfgfile):
            with tempfile.TemporaryDirectory() as tmpdir:
                tmpdir_and_fname = os.path.join(tmpdir, os.path.basename(cfgfile))
                log.debug('Fetching %s to %s', cfgfile, tmpdir_and_fname)
                aws.ecco_aws_s3_cp.aws_s3_cp(src=cfgfile, dest=tmpdir, **kwargs)
                instance.update(yaml.safe_load(open(tmpdir_and_fname)))
                instance._local_cfgfile = tmpdir_and_fname
        else:
            instance.update(yaml.safe_load(open(cfgfile)))
            instance._local_cfgfile = cfgfile

        # Apply defaults
        instance._apply_defaults()

        # Validate config file
        try:
            schema.validate(dict(instance), source=cfgfile)
            log.info('Configuration file validation successful: %s', cfgfile)
        except Exception as e:
            msg = f"Configuration file '{cfgfile}' is invalid:\n{str(e)}"
            log.error(msg)
            raise ConfigurationValidationError(msg) from e

        # Apply overrides if any
        if overrides:
            log.debug('Applying CLI overrides: %s', overrides)
            instance.update(overrides)

            # Validate again with overrides
            try:
                schema.validate(dict(instance), source='CLI overrides')
                log.info('Configuration with CLI overrides validation successful')
            except Exception as e:
                msg = f"Configuration with CLI overrides is invalid:\n{str(e)}\nOverrides: {overrides}"
                log.error(msg)
                raise ConfigurationValidationError(msg) from e

        log.debug('Using configuration data per "%s":', cfgfile)
        for k, v in instance.items():
            log.debug(' %s: %s', k, v)

        return instance

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
