"""Consolidated tests for ECCO Dataset Production configuration system."""

import pytest
from pathlib import Path
import yaml

from ecco_dataset_production.config import (
    ECCODatasetProductionConfig,
    ConfigurationValidationError
)
from ecco_dataset_production.config.schema import Schema


# Sample valid configuration for testing
VALID_CONFIG = {
    'ecco_version': 'V4r6',
    'product_version': 'Version 4, Release 6',
    'model_start_time': '1992-01-01T12:00:00',
    'model_end_time': '2025-12-31T12:00:00',
    'model_timestep': 1,
    'model_timestep_units': 'h',
    'geospatial_vertical_min': -6134.5,
    'ecco_native_grid_filename': 'GRID_GEOMETRY_ECCO_V4r6_native_llc0090.nc',
    'ecco_production_filestr_grid_label': {
        'latlon': '0p50deg',
        'native': 'llc0090'
    },
    'array_precision': 'float32',
    'podaac_metadata_filename': 'PODAAC_dataset_table_V4r6.csv',
    'history': 'Test history',
    'references': 'Test references',
    'source': 'Test source',
    'summary': 'Test summary',
    'netcdf4_compression_encodings': {
        'zlib': True,
        'complevel': 5,
        'shuffle': True
    },
    'variable_coordinates_as_encoded_attributes': ['latitude', 'longitude', 'time'],
    'doi_authority': 'org.doi',
    'doi_prefix': '10.5067',
    'dataset_description_tail_1D': '',
    'dataset_description_tail_latlon': 'Test latlon description',
    'dataset_description_tail_native': 'Test native description'
}


@pytest.fixture
def valid_config_file(tmp_path):
    """Create a temporary valid config file."""
    config_file = tmp_path / "config.yaml"
    with open(config_file, 'w') as f:
        yaml.dump(VALID_CONFIG, f)
    return str(config_file)


@pytest.fixture
def invalid_config_file(tmp_path):
    """Create a temporary invalid config file."""
    invalid_config = VALID_CONFIG.copy()
    invalid_config['model_start_time'] = '1992-01-01'  # Missing time component
    invalid_config['model_timestep'] = -1  # Negative value

    config_file = tmp_path / "invalid_config.yaml"
    with open(config_file, 'w') as f:
        yaml.dump(invalid_config, f)
    return str(config_file)


class TestConfigLoading:
    """Tests for basic config loading and validation."""

    def test_load_valid_config(self, valid_config_file):
        """Test loading valid config with defaults applied."""
        cfg = ECCODatasetProductionConfig(valid_config_file)

        # Config values
        assert cfg['ecco_version'] == 'V4r6'
        assert cfg['array_precision'] == 'float32'

        # Default should be applied
        assert cfg['num_vertical_levels'] == 50

    def test_load_invalid_config(self, invalid_config_file):
        """Test loading invalid config raises error."""
        with pytest.raises(ConfigurationValidationError) as exc_info:
            ECCODatasetProductionConfig(invalid_config_file)

        error_msg = str(exc_info.value).lower()
        assert 'invalid' in error_msg
        assert 'configuration file' in error_msg

    def test_undefined_key_backward_compatibility(self, valid_config_file):
        """Test that undefined keys return empty string for backward compatibility."""
        cfg = ECCODatasetProductionConfig(valid_config_file)
        assert cfg['nonexistent_key'] == ''


class TestFactoryMethods:
    """Tests for create_parser() and from_parsed_args() factory methods."""

    def test_create_parser_basic(self):
        """Test creating parser with config file argument."""
        parser = ECCODatasetProductionConfig.create_parser()

        assert any(action.dest == 'ecco_cfg_loc' for action in parser._actions)

        args = parser.parse_args(['--ecco_cfg_loc', 'config.yaml'])
        assert args.ecco_cfg_loc == 'config.yaml'

    def test_create_parser_with_config_fields(self):
        """Test creating parser with config field overrides."""
        parser = ECCODatasetProductionConfig.create_parser(
            config_fields=['array_precision', 'num_vertical_levels']
        )

        action_dests = [action.dest for action in parser._actions]
        assert 'ecco_cfg_loc' in action_dests
        assert 'array_precision' in action_dests
        assert 'num_vertical_levels' in action_dests

    def test_from_parsed_args_with_overrides(self, valid_config_file):
        """Test loading config with CLI overrides."""
        parser = ECCODatasetProductionConfig.create_parser(
            config_fields=['array_precision', 'num_vertical_levels']
        )
        args = parser.parse_args([
            '--ecco_cfg_loc', valid_config_file,
            '--array_precision', 'float64',
            '--num_vertical_levels', '100'
        ])

        cfg = ECCODatasetProductionConfig.from_parsed_args(
            args,
            config_fields=['array_precision', 'num_vertical_levels']
        )

        assert cfg['array_precision'] == 'float64'
        assert cfg['num_vertical_levels'] == 100

    def test_complete_workflow_with_tool_args(self, valid_config_file):
        """Test complete workflow: config fields + tool-specific args."""
        # Create parser with config fields
        parser = ECCODatasetProductionConfig.create_parser(
            config_fields=['array_precision'],
            description='Test Tool'
        )

        # Add tool-specific arguments
        parser.add_argument('--jobfile', required=True)
        parser.add_argument('--outfile', default='output.json')

        # Parse arguments
        args = parser.parse_args([
            '--ecco_cfg_loc', valid_config_file,
            '--array_precision', 'float64',
            '--jobfile', 'jobs.txt'
        ])

        # Load config
        cfg = ECCODatasetProductionConfig.from_parsed_args(
            args,
            config_fields=['array_precision']
        )

        # Config override applied
        assert cfg['array_precision'] == 'float64'
        # Tool args accessible
        assert args.jobfile == 'jobs.txt'
        assert args.outfile == 'output.json'

    def test_invalid_override_fails_validation(self, valid_config_file):
        """Test that invalid overrides fail validation."""
        parser = ECCODatasetProductionConfig.create_parser(
            config_fields=['array_precision']
        )
        args = parser.parse_args([
            '--ecco_cfg_loc', valid_config_file,
            '--array_precision', 'invalid_type'
        ])

        with pytest.raises(ConfigurationValidationError) as exc_info:
            ECCODatasetProductionConfig.from_parsed_args(
                args,
                config_fields=['array_precision']
            )

        error_msg = str(exc_info.value).lower()
        assert 'cli overrides' in error_msg


class TestCustomArgNames:
    """Tests for custom CLI argument names and descriptions."""

    def test_schema_extracts_custom_arg_names(self):
        """Test Schema extracts custom arg names from validators."""
        schema = Schema()

        # Fields with custom arg_name
        assert schema._get_arg_name('latlon_grid_resolution') == 'resolution'
        assert schema._get_arg_name('ecco_grid_dir') == 'ecco_grid_loc'

        # Fields without custom arg_name return field name
        assert schema._get_arg_name('array_precision') == 'array_precision'

    def test_schema_extracts_descriptions(self):
        """Test Schema extracts descriptions from validators."""
        schema = Schema()

        # Fields with descriptions
        assert 'Lat-lon grid resolution' in schema._get_description('latlon_grid_resolution')
        assert 'ECCO grid files' in schema._get_description('ecco_grid_dir')

        # Fields without descriptions return None
        assert schema._get_description('array_precision') is None

    def test_parser_uses_custom_arg_names(self):
        """Test parser uses custom arg names for CLI arguments."""
        parser = ECCODatasetProductionConfig.create_parser(
            config_fields=['latlon_grid_resolution', 'ecco_grid_dir']
        )

        # Parse with custom arg names
        args = parser.parse_args([
            '--ecco_cfg_loc', 'config.yaml',
            '--resolution', '0.5',
            '--ecco_grid_loc', '/path/to/grid'
        ])

        # Accessible via field names (dest parameter)
        assert args.latlon_grid_resolution == 0.5
        assert args.ecco_grid_dir == '/path/to/grid'

    def test_parser_uses_descriptions_in_help(self):
        """Test parser uses descriptions in help text."""
        parser = ECCODatasetProductionConfig.create_parser(
            config_fields=['latlon_grid_resolution', 'ecco_grid_dir']
        )

        for action in parser._actions:
            if action.dest == 'latlon_grid_resolution':
                assert 'Lat-lon grid resolution' in action.help
            elif action.dest == 'ecco_grid_dir':
                assert 'ECCO grid files' in action.help

    def test_end_to_end_with_custom_names(self, valid_config_file):
        """Test complete workflow with custom arg names."""
        parser = ECCODatasetProductionConfig.create_parser(
            config_fields=['latlon_grid_resolution', 'latlon_grid_area_extent']
        )

        # Use custom arg names
        args = parser.parse_args([
            '--ecco_cfg_loc', valid_config_file,
            '--resolution', '0.25',
            '--extent', '-180', '180', '-90', '90'
        ])

        cfg = ECCODatasetProductionConfig.from_parsed_args(
            args,
            config_fields=['latlon_grid_resolution', 'latlon_grid_area_extent']
        )

        # Values accessible via field names in config
        assert cfg['latlon_grid_resolution'] == 0.25
        assert cfg['latlon_grid_area_extent'] == [-180.0, 180.0, -90.0, 90.0]


class TestRealConfigs:
    """Tests against real config files in the repository."""

    def test_validate_real_config_files(self):
        """Test that current config files validate successfully."""
        configs_dir = Path(__file__).parent.parent / 'configs'

        # Explicitly list current config versions to test
        config_filenames = ['config_V4r5.yaml', 'config_V4r6.yaml']

        for filename in config_filenames:
            config_file = configs_dir / filename
            if not config_file.exists():
                continue  # Skip if file doesn't exist
            cfg = ECCODatasetProductionConfig(str(config_file))
            assert cfg['ecco_version'] is not None
            assert cfg['array_precision'] is not None  # Default applied
