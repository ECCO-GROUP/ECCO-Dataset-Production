"""Tests for ECCODatasetProductionConfig validation functionality."""

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


@pytest.fixture
def invalid_config_file(tmp_path):
    """Create a temporary invalid config file."""
    invalid_config = VALID_CONFIG.copy()
    # Make it invalid - wrong datetime format
    invalid_config['model_start_time'] = '1992-01-01'  # Missing time component
    invalid_config['model_timestep'] = -1  # Negative value

    config_file = tmp_path / "invalid_config.yaml"
    with open(config_file, 'w') as f:
        yaml.dump(invalid_config, f)
    return str(config_file)


class TestConfigurationValidation:
    """Tests for configuration validation functionality."""

    def test_load_valid_config(self, valid_config_file):
        """Test loading valid config."""
        cfg = ECCODatasetProductionConfig(valid_config_file)
        assert cfg['ecco_version'] == 'V4r6'
        assert cfg['model_timestep'] == 1
        # Config specifies float32, so that's what we should get
        assert cfg['array_precision'] == 'float32'
        # Test that a default was applied for a field not in the config
        assert cfg['num_vertical_levels'] == 50  # This is a default

    def test_load_invalid_config(self, invalid_config_file):
        """Test loading invalid config raises error."""
        with pytest.raises(ConfigurationValidationError) as exc_info:
            ECCODatasetProductionConfig(invalid_config_file)

        # Check that error message contains useful information
        error_msg = str(exc_info.value).lower()
        assert 'invalid' in error_msg
        assert 'configuration file' in error_msg

    def test_backward_compatibility(self, valid_config_file):
        """Test that undefined keys return empty string."""
        cfg = ECCODatasetProductionConfig(valid_config_file)
        assert cfg['ecco_version'] == 'V4r6'

        # Undefined keys should still return empty string
        assert cfg['nonexistent_key'] == ''

    def test_validate_real_config_files(self):
        """Test validation against real config files in the repo."""
        configs_dir = Path(__file__).parent.parent / 'configs'

        for config_file in configs_dir.glob('config_V4r*.yaml'):
            # These should all be valid and have defaults applied
            cfg = ECCODatasetProductionConfig(str(config_file))
            assert cfg['ecco_version'] is not None
            assert cfg['array_precision'] is not None  # Should have default


@pytest.mark.skipif(
    not (Path(__file__).parent.parent / 'configs' / 'config_V4r6.yaml').exists(),
    reason="config_V4r6.yaml not found"
)
def test_integration_with_real_config():
    """Integration test with actual V4r6 config."""
    config_path = Path(__file__).parent.parent / 'configs' / 'config_V4r6.yaml'

    cfg = ECCODatasetProductionConfig(str(config_path))
    assert cfg['ecco_version'] == 'V4r6'

    # Defaults should be applied
    assert cfg['array_precision'] is not None


def test_cli_overrides(valid_config_file):
    """Test CLI overrides using factory methods."""
    # Override with valid value
    parser = ECCODatasetProductionConfig.create_parser(
        config_fields=['array_precision']
    )
    args = parser.parse_args(['--cfgfile', valid_config_file, '--array_precision', 'float64'])
    cfg = ECCODatasetProductionConfig.from_parsed_args(
        args, config_fields=['array_precision']
    )
    assert cfg['array_precision'] == 'float64'

    # Override with invalid value should fail validation
    parser = ECCODatasetProductionConfig.create_parser(
        config_fields=['array_precision']
    )
    args = parser.parse_args(['--cfgfile', valid_config_file, '--array_precision', 'invalid_type'])
    with pytest.raises(ConfigurationValidationError) as exc_info:
        ECCODatasetProductionConfig.from_parsed_args(
            args, config_fields=['array_precision']
        )

    # Error should mention CLI overrides
    error_msg = str(exc_info.value).lower()
    assert 'cli overrides' in error_msg
    assert 'invalid' in error_msg


def test_parse_args_via_factory(valid_config_file):
    """Test CLI argument parsing via factory methods."""
    # Test basic parsing with just config file
    parser = ECCODatasetProductionConfig.create_parser()
    args = parser.parse_args(['--cfgfile', valid_config_file])
    cfg = ECCODatasetProductionConfig.from_parsed_args(args)
    assert cfg['ecco_version'] == 'V4r6'

    # Test with override fields
    parser = ECCODatasetProductionConfig.create_parser(
        config_fields=['array_precision']
    )
    args = parser.parse_args(['--cfgfile', valid_config_file, '--array_precision', 'float64'])
    cfg = ECCODatasetProductionConfig.from_parsed_args(
        args, config_fields=['array_precision']
    )
    assert cfg['array_precision'] == 'float64'

    # Test with multiple override fields (types inferred from schema)
    parser = ECCODatasetProductionConfig.create_parser(
        config_fields=['array_precision', 'num_vertical_levels']
    )
    args = parser.parse_args([
        '--cfgfile', valid_config_file,
        '--array_precision', 'float32',
        '--num_vertical_levels', '100'
    ])
    cfg = ECCODatasetProductionConfig.from_parsed_args(
        args, config_fields=['array_precision', 'num_vertical_levels']
    )
    assert cfg['array_precision'] == 'float32'
    assert cfg['num_vertical_levels'] == 100  # Converted to int by argparse
