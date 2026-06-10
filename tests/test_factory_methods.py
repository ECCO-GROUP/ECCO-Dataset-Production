"""Tests for ECCODatasetProductionConfig factory methods."""

import pytest
import tempfile
from pathlib import Path
import yaml

from ecco_dataset_production.config import (
    ECCODatasetProductionConfig,
    ConfigurationValidationError
)


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


class TestFactoryMethods:
    """Tests for create_parser() and from_parsed_args() factory methods."""

    def test_create_parser_basic(self):
        """Test creating a parser without config fields."""
        parser = ECCODatasetProductionConfig.create_parser()

        # Parser should have cfgfile argument
        assert any(action.dest == 'cfgfile' for action in parser._actions)

        # Should be able to parse
        args = parser.parse_args(['--cfgfile', 'config.yaml'])
        assert args.cfgfile == 'config.yaml'

    def test_create_parser_with_config_fields(self):
        """Test creating a parser with config field overrides."""
        parser = ECCODatasetProductionConfig.create_parser(
            config_fields=['array_precision', 'num_vertical_levels']
        )

        # Should have cfgfile and config field arguments
        action_dests = [action.dest for action in parser._actions]
        assert 'cfgfile' in action_dests
        assert 'array_precision' in action_dests
        assert 'num_vertical_levels' in action_dests

    def test_create_parser_with_description(self):
        """Test creating a parser with custom description."""
        description = 'My Custom Tool'
        parser = ECCODatasetProductionConfig.create_parser(
            description=description
        )
        assert parser.description == description

    def test_from_parsed_args_basic(self, valid_config_file):
        """Test loading config from parsed args without overrides."""
        parser = ECCODatasetProductionConfig.create_parser()
        args = parser.parse_args(['--cfgfile', valid_config_file])

        cfg = ECCODatasetProductionConfig.from_parsed_args(args)

        assert cfg['ecco_version'] == 'V4r6'
        assert cfg['array_precision'] == 'float32'
        # Default should be applied
        assert cfg['num_vertical_levels'] == 50

    def test_from_parsed_args_with_overrides(self, valid_config_file):
        """Test loading config with CLI overrides."""
        parser = ECCODatasetProductionConfig.create_parser(
            config_fields=['array_precision']
        )
        args = parser.parse_args([
            '--cfgfile', valid_config_file,
            '--array_precision', 'float64'
        ])

        cfg = ECCODatasetProductionConfig.from_parsed_args(
            args,
            config_fields=['array_precision']
        )

        # Override should be applied
        assert cfg['array_precision'] == 'float64'

    def test_from_parsed_args_with_tool_args(self, valid_config_file):
        """Test that tool-specific args don't interfere with config."""
        parser = ECCODatasetProductionConfig.create_parser(
            config_fields=['array_precision']
        )
        # Tool adds its own arguments
        parser.add_argument('--jobfile', required=True)
        parser.add_argument('--outfile', required=True)
        parser.add_argument('--log', default='INFO')

        args = parser.parse_args([
            '--cfgfile', valid_config_file,
            '--array_precision', 'float64',
            '--jobfile', 'jobs.txt',
            '--outfile', 'output.json',
            '--log', 'DEBUG'
        ])

        # Load config with only config_fields specified
        cfg = ECCODatasetProductionConfig.from_parsed_args(
            args,
            config_fields=['array_precision']
        )

        # Config should have the override
        assert cfg['array_precision'] == 'float64'

        # Tool args should still be accessible
        assert args.jobfile == 'jobs.txt'
        assert args.outfile == 'output.json'
        assert args.log == 'DEBUG'

    def test_from_parsed_args_without_cfgfile(self):
        """Test that from_parsed_args fails without cfgfile."""
        # Create args namespace without cfgfile
        import argparse
        args = argparse.Namespace(some_arg='value')

        with pytest.raises(AttributeError) as exc_info:
            ECCODatasetProductionConfig.from_parsed_args(args)

        assert 'cfgfile' in str(exc_info.value)

    def test_from_parsed_args_invalid_override(self, valid_config_file):
        """Test that invalid overrides fail validation."""
        parser = ECCODatasetProductionConfig.create_parser(
            config_fields=['array_precision']
        )
        args = parser.parse_args([
            '--cfgfile', valid_config_file,
            '--array_precision', 'invalid_type'
        ])

        with pytest.raises(ConfigurationValidationError):
            ECCODatasetProductionConfig.from_parsed_args(
                args,
                config_fields=['array_precision']
            )

    def test_complete_workflow_example(self, valid_config_file):
        """Test complete workflow as it would be used in a real tool."""
        # Step 1: Create parser with config fields
        parser = ECCODatasetProductionConfig.create_parser(
            config_fields=['array_precision', 'num_vertical_levels'],
            description='ECCO Test Tool'
        )

        # Step 2: Tool adds its own arguments
        parser.add_argument('--jobfile', required=True, help='Job file')
        parser.add_argument('--outfile', default='output.json', help='Output file')
        parser.add_argument('--log', choices=['DEBUG', 'INFO', 'WARNING'],
                          default='INFO', help='Log level')

        # Step 3: Parse arguments
        args = parser.parse_args([
            '--cfgfile', valid_config_file,
            '--array_precision', 'float64',
            '--num_vertical_levels', '100',
            '--jobfile', 'jobs.txt',
            '--log', 'DEBUG'
        ])

        # Step 4: Create config from parsed args
        cfg = ECCODatasetProductionConfig.from_parsed_args(
            args,
            config_fields=['array_precision', 'num_vertical_levels']
        )

        # Step 5: Verify config and tool args are both accessible
        assert cfg['array_precision'] == 'float64'
        assert cfg['num_vertical_levels'] == 100
        assert args.jobfile == 'jobs.txt'
        assert args.outfile == 'output.json'
        assert args.log == 'DEBUG'
