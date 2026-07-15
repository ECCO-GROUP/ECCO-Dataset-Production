"""Integration tests for ecco_generate_datasets module.

These tests validate that the module produces correct outputs through refactors
by using real (minimal) NetCDF fixtures and validating the actual output.
"""

import json
import tempfile
from pathlib import Path

import numpy as np
import pytest
import xarray as xr

from ecco_dataset_production import ecco_generate_datasets


@pytest.fixture
def minimal_input_netcdf():
    """Create a minimal 2x2 NetCDF file for testing."""
    ds = xr.Dataset(
        {
            'TEMP': (['time', 'lat', 'lon'], np.random.rand(1, 2, 2).astype(np.float64)),
        },
        coords={
            'time': [np.datetime64('1992-01-01')],
            'lat': [0.0, 1.0],
            'lon': [0.0, 1.0],
        },
        attrs={
            'old_attr': 'should be stripped',
            'another_attr': 'also removed'
        }
    )

    with tempfile.NamedTemporaryFile(suffix='.nc', delete=False) as f:
        ds.to_netcdf(f.name)
        yield f.name

    # Cleanup
    Path(f.name).unlink(missing_ok=True)


@pytest.fixture
def minimal_config():
    """Minimal configuration for testing."""
    return {
        'array_precision': 'float32',
        'doi_prefix': 'https://doi.org/10.5067/TEST',
        'doi_authority': 'https://dx.doi.org',
        'history': 'Test history',
        'geospatial_vertical_min': '0.0',
        'model_start_time': '1992-01-01T00:00:00',
        'model_end_time': '2017-12-31T23:59:59',
        'product_version': '4r6',
        'references': 'Test references',
        'source': 'Test source',
        'summary': 'Test summary',
        'netcdf4_compression_encodings': {
            'zlib': True,
            'complevel': 5,
            'shuffle': True
        },
        'variable_coordinates_as_encoded_attributes': ['time', 'lat', 'lon', 'latitude', 'longitude', 'Z']
    }


@pytest.fixture
def mock_task_minimal(minimal_input_netcdf):
    """Mock ECCOTask object with minimal attributes for testing."""
    class MockTask(dict):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)

        @property
        def is_granule_local(self):
            return True

        @property
        def is_time_invariant(self):
            return True

        @property
        def is_latlon(self):
            return True

        @property
        def is_native(self):
            return False

        @property
        def is_3d(self):
            return False

    task = MockTask({
        'input_netcdf': minimal_input_netcdf,
        'granule': None,  # Will be set per test
        'strip_attributes': True,
        'ecco_cfg_loc': 'mock_config.yaml',
        'ecco_grid_loc': 'mock_grid',
        'ecco_mapping_factors_loc': 'mock_factors',
        'ecco_metadata_loc': 'mock_metadata',
        'dynamic_metadata': {
            'name': 'test_granule',
            'dimension': '2D',
            'time_coverage_duration': 'TIME-INVARIANT',
            'time_coverage_resolution': 'TIME-INVARIANT',
            'summary': 'Test granule',
            'comment': 'Integration test',
        }
    })

    return task


class TestProcessTimeInvariantGranule:
    """Integration tests for process_time_invariant_granule."""

    def test_creates_valid_netcdf_output(self, mock_task_minimal, minimal_config, tmp_path):
        """Test that the function creates a valid NetCDF file with expected structure."""
        output_file = tmp_path / "test_output.nc"
        mock_task_minimal['granule'] = str(output_file)

        # Mock the metadata and grid objects (minimal for this test)
        mock_grid = type('MockGrid', (), {
            'native_grid': {}
        })()

        mock_factors = type('MockFactors', (), {
            'latitude_bounds': np.array([[0.0, 0.5], [0.5, 1.0]]),
            'longitude_bounds': np.array([[0.0, 0.5], [0.5, 1.0]]),
        })()

        # Mock metadata directory with minimal JSON files
        metadata_dir = tmp_path / "metadata"
        metadata_dir.mkdir()

        # Create minimal metadata JSON files
        for filename, content in [
            ('variable_metadata.json', []),
            ('coordinate_metadata_for_latlon_datasets.json', []),
            ('global_metadata_for_all_datasets.json', []),
        ]:
            (metadata_dir / filename).write_text(json.dumps(content))

        mock_metadata = type('MockMetadata', (), {
            'metadata_dir': str(metadata_dir)
        })()

        # Execute the function
        ecco_generate_datasets.process_time_invariant_granule(
            task=mock_task_minimal,
            cfg=minimal_config,
            grid=mock_grid,
            mapping_factors=mock_factors,
            metadata=mock_metadata
        )

        # Verify output file was created
        assert output_file.exists(), "Output NetCDF file was not created"

        # Verify output can be opened and read
        output_ds = xr.open_dataset(output_file)
        try:
            # Validate basic structure
            assert 'TEMP' in output_ds.data_vars, "Expected data variable 'TEMP' not found"

            # Verify attributes were stripped (if strip_attributes=True)
            assert 'old_attr' not in output_ds.attrs, "Old attributes should have been stripped"

            # Verify new metadata was added
            assert 'uuid' in output_ds.attrs, "UUID should be added to output"
            assert 'date_created' in output_ds.attrs, "date_created should be added"

            # Verify data precision conversion
            assert output_ds['TEMP'].dtype == np.float32, "Data should be converted to float32"

            # Verify fill values are set
            assert '_FillValue' in output_ds['TEMP'].encoding, "Fill value should be set"

        finally:
            output_ds.close()

    def test_preserves_attributes_when_strip_false(self, mock_task_minimal, minimal_config, tmp_path):
        """Test that attributes are preserved when strip_attributes=False."""
        output_file = tmp_path / "test_output_preserve.nc"
        mock_task_minimal['granule'] = str(output_file)
        mock_task_minimal['strip_attributes'] = False

        # Create input with known attribute
        ds = xr.Dataset(
            {'TEMP': (['lat', 'lon'], np.ones((2, 2)))},
            coords={'lat': [0, 1], 'lon': [0, 1]},
            attrs={'preserve_me': 'important_value'}
        )

        input_file = tmp_path / "input_preserve.nc"
        ds.to_netcdf(input_file)
        mock_task_minimal['input_netcdf'] = str(input_file)

        # Mock objects (minimal)
        mock_grid = type('MockGrid', (), {'native_grid': {}})()
        mock_factors = type('MockFactors', (), {
            'latitude_bounds': np.array([[0.0, 0.5], [0.5, 1.0]]),
            'longitude_bounds': np.array([[0.0, 0.5], [0.5, 1.0]]),
        })()

        metadata_dir = tmp_path / "metadata2"
        metadata_dir.mkdir()
        for filename in ['variable_metadata.json', 'coordinate_metadata_for_latlon_datasets.json',
                         'global_metadata_for_all_datasets.json']:
            (metadata_dir / filename).write_text('[]')

        mock_metadata = type('MockMetadata', (), {'metadata_dir': str(metadata_dir)})()

        # Execute
        ecco_generate_datasets.process_time_invariant_granule(
            task=mock_task_minimal,
            cfg=minimal_config,
            grid=mock_grid,
            mapping_factors=mock_factors,
            metadata=mock_metadata
        )

        # Verify
        output_ds = xr.open_dataset(output_file)
        try:
            # Original attribute should still be present (along with new ones)
            assert 'preserve_me' in output_ds.attrs, "Original attributes should be preserved"
            assert output_ds.attrs['preserve_me'] == 'important_value'
        finally:
            output_ds.close()



class TestSetGranuleAncillaryData:
    """Integration tests for set_granule_ancillary_data function."""

    def test_applies_float32_precision(self, mock_task_minimal, minimal_config):
        """Test that float32 precision is applied correctly."""
        # Create a float64 dataset
        ds = xr.Dataset(
            {'VAR': (['x', 'y'], np.random.rand(3, 3).astype(np.float64))},
            coords={'x': [0, 1, 2], 'y': [0, 1, 2]}
        )

        minimal_config['array_precision'] = 'float32'

        mock_factors = type('MockFactors', (), {
            'latitude_bounds': np.zeros((3, 2)),
            'longitude_bounds': np.zeros((3, 2)),
        })()

        result = ecco_generate_datasets.set_granule_ancillary_data(
            dataset=ds,
            task=mock_task_minimal,
            grid=None,
            mapping_factors=mock_factors,
            cfg=minimal_config
        )

        assert result['VAR'].dtype == np.float32, "Should convert to float32"
        assert 'valid_min' in result['VAR'].attrs, "Should set valid_min"
        assert 'valid_max' in result['VAR'].attrs, "Should set valid_max"

    def test_replaces_nan_with_fill_value(self, mock_task_minimal, minimal_config):
        """Test that NaN values are replaced with fill value."""
        # Create dataset with NaN
        data = np.array([[1.0, 2.0, np.nan], [4.0, np.nan, 6.0]])
        ds = xr.Dataset(
            {'VAR': (['x', 'y'], data)},
            coords={'x': [0, 1], 'y': [0, 1, 2]}
        )

        minimal_config['array_precision'] = 'float32'

        mock_factors = type('MockFactors', (), {
            'latitude_bounds': np.zeros((2, 2)),
            'longitude_bounds': np.zeros((3, 2)),
        })()

        result = ecco_generate_datasets.set_granule_ancillary_data(
            dataset=ds,
            task=mock_task_minimal,
            grid=None,
            mapping_factors=mock_factors,
            cfg=minimal_config
        )

        # Verify no NaNs remain
        assert not np.any(np.isnan(result['VAR'].values)), "NaN values should be replaced"


class TestDatasetOutputContract:
    """Tests that validate the contract/properties of generated datasets."""

    def test_output_has_required_global_attributes(self, mock_task_minimal, minimal_config, tmp_path):
        """Verify that output datasets have required CF/ACDD global attributes."""
        output_file = tmp_path / "contract_test.nc"
        mock_task_minimal['granule'] = str(output_file)

        # Setup minimal mocks
        mock_grid = type('MockGrid', (), {'native_grid': {}})()
        mock_factors = type('MockFactors', (), {
            'latitude_bounds': np.array([[0.0, 0.5], [0.5, 1.0]]),
            'longitude_bounds': np.array([[0.0, 0.5], [0.5, 1.0]]),
        })()

        metadata_dir = tmp_path / "metadata3"
        metadata_dir.mkdir()
        for filename in ['variable_metadata.json', 'coordinate_metadata_for_latlon_datasets.json',
                         'global_metadata_for_all_datasets.json']:
            (metadata_dir / filename).write_text('[]')

        mock_metadata = type('MockMetadata', (), {'metadata_dir': str(metadata_dir)})()

        # Execute
        ecco_generate_datasets.process_time_invariant_granule(
            task=mock_task_minimal,
            cfg=minimal_config,
            grid=mock_grid,
            mapping_factors=mock_factors,
            metadata=mock_metadata
        )

        # Validate required attributes
        output_ds = xr.open_dataset(output_file)
        try:
            required_attrs = [
                'uuid',
                'date_created',
                'date_modified',
                'product_version',
                'product_name',
                'identifier_product_doi',
            ]

            for attr in required_attrs:
                assert attr in output_ds.attrs, f"Required attribute '{attr}' missing from output"

        finally:
            output_ds.close()


class TestGenerateDatasets:
    """Integration tests for generate_datasets top-level function."""

    @pytest.fixture
    def config_file(self, minimal_config, tmp_path):
        """Create a minimal config YAML file."""
        import yaml
        config_path = tmp_path / "config.yaml"
        config_path.write_text(yaml.dump(minimal_config))
        return str(config_path)

    @pytest.fixture
    def metadata_directory(self, tmp_path):
        """Create metadata directory with minimal JSON files."""
        metadata_dir = tmp_path / "metadata"
        metadata_dir.mkdir()

        for filename in [
            'variable_metadata.json',
            'coordinate_metadata_for_latlon_datasets.json',
            'global_metadata_for_all_datasets.json',
        ]:
            (metadata_dir / filename).write_text('[]')

        return str(metadata_dir)

    @pytest.fixture
    def grid_directory(self, tmp_path):
        """Create a minimal grid directory with required files."""
        grid_dir = tmp_path / "grid"
        grid_dir.mkdir()

        # Create minimal grid NetCDF file
        grid_ds = xr.Dataset(
            {
                'XC': (['tile', 'j', 'i'], np.zeros((1, 2, 2))),
                'YC': (['tile', 'j', 'i'], np.zeros((1, 2, 2))),
                'Z': (['k'], np.array([0.0, 1.0])),
            },
            coords={
                'tile': [1],
                'j': [0, 1],
                'i': [0, 1],
                'k': [0, 1],
            }
        )

        grid_file = grid_dir / "GRID.nc"
        grid_ds.to_netcdf(grid_file)

        return str(grid_dir)

    @pytest.fixture
    def mapping_factors_directory(self, tmp_path):
        """Create mapping factors directory."""
        factors_dir = tmp_path / "factors"
        factors_dir.mkdir()

        # Create minimal mapping factors file
        factors_ds = xr.Dataset(
            {
                'wet_mask': (['latitude', 'longitude'], np.ones((2, 2))),
            },
            coords={
                'latitude': [0.0, 1.0],
                'longitude': [0.0, 1.0],
            }
        )

        factors_file = factors_dir / "factors.nc"
        factors_ds.to_netcdf(factors_file)

        return str(factors_dir)

    def test_processes_single_time_invariant_task(
        self, minimal_input_netcdf, config_file, metadata_directory,
        grid_directory, mapping_factors_directory, tmp_path, monkeypatch
    ):
        """Test that generate_datasets processes a single time-invariant task."""
        # Use ECCO naming convention for time-invariant files
        output_file = tmp_path / "TEST_ECCO_V4r6_latlon_0p50deg.nc"

        # Create tasklist JSON
        tasklist = [
            {
                'input_netcdf': minimal_input_netcdf,
                'granule': str(output_file),
                'strip_attributes': True,
                'ecco_cfg_loc': config_file,
                'ecco_grid_loc': grid_directory,
                'ecco_mapping_factors_loc': mapping_factors_directory,
                'ecco_metadata_loc': metadata_directory,
                'dynamic_metadata': {
                    'name': 'test_granule',
                    'dimension': '2D',
                    'time_coverage_duration': 'TIME-INVARIANT',
                    'time_coverage_resolution': 'TIME-INVARIANT',
                    'summary': 'Test single task',
                    'comment': 'Integration test',
                }
            }
        ]

        tasklist_file = tmp_path / "tasklist.json"
        tasklist_file.write_text(json.dumps(tasklist))

        # Mock the complex resource initialization to avoid dependencies
        from unittest import mock

        with mock.patch('ecco_dataset_production.ecco_grid.ECCOGrid') as mock_grid_cls, \
             mock.patch('ecco_dataset_production.ecco_mapping_factors.ECCOMappingFactors') as mock_factors_cls, \
             mock.patch('ecco_dataset_production.ecco_metadata.ECCOMetadata') as mock_metadata_cls:

            # Setup mocks
            mock_grid = mock.MagicMock()
            mock_grid.native_grid = {}
            mock_grid_cls.return_value = mock_grid

            mock_factors = mock.MagicMock()
            mock_factors.latitude_bounds = np.array([[0.0, 0.5], [0.5, 1.0]])
            mock_factors.longitude_bounds = np.array([[0.0, 0.5], [0.5, 1.0]])
            mock_factors_cls.return_value = mock_factors

            mock_metadata = mock.MagicMock()
            mock_metadata.metadata_dir = metadata_directory
            mock_metadata_cls.return_value = mock_metadata

            # Execute
            ecco_generate_datasets.generate_datasets(str(tasklist_file))

        # Verify output was created
        assert output_file.exists(), "Output file should be created"

        # Verify it's a valid NetCDF
        ds = xr.open_dataset(output_file)
        try:
            assert 'TEMP' in ds.data_vars, "Expected variable should be present"
            assert ds['TEMP'].dtype == np.float32, "Should be float32"
        finally:
            ds.close()

    def test_processes_multiple_tasks(
        self, minimal_input_netcdf, config_file, metadata_directory,
        grid_directory, mapping_factors_directory, tmp_path
    ):
        """Test that generate_datasets processes multiple tasks in sequence."""
        # Use ECCO naming convention
        output_file1 = tmp_path / "TEMP_ECCO_V4r6_latlon_0p50deg.nc"
        output_file2 = tmp_path / "SALT_ECCO_V4r6_latlon_0p50deg.nc"

        # Create second input file
        ds2 = xr.Dataset(
            {'SALT': (['time', 'lat', 'lon'], np.random.rand(1, 2, 2))},
            coords={
                'time': [np.datetime64('1992-01-02')],
                'lat': [0.0, 1.0],
                'lon': [0.0, 1.0],
            }
        )
        input_file2 = tmp_path / "input2.nc"
        ds2.to_netcdf(input_file2)

        # Create tasklist with two tasks
        tasklist = [
            {
                'input_netcdf': minimal_input_netcdf,
                'granule': str(output_file1),
                'strip_attributes': True,
                'ecco_cfg_loc': config_file,
                'ecco_grid_loc': grid_directory,
                'ecco_mapping_factors_loc': mapping_factors_directory,
                'ecco_metadata_loc': metadata_directory,
                'dynamic_metadata': {
                    'name': 'task1',
                    'dimension': '2D',
                    'time_coverage_duration': 'TIME-INVARIANT',
                    'time_coverage_resolution': 'TIME-INVARIANT',
                    'summary': 'Task 1',
                    'comment': 'First task',
                }
            },
            {
                'input_netcdf': str(input_file2),
                'granule': str(output_file2),
                'strip_attributes': True,
                'ecco_cfg_loc': config_file,
                'ecco_grid_loc': grid_directory,
                'ecco_mapping_factors_loc': mapping_factors_directory,
                'ecco_metadata_loc': metadata_directory,
                'dynamic_metadata': {
                    'name': 'task2',
                    'dimension': '2D',
                    'time_coverage_duration': 'TIME-INVARIANT',
                    'time_coverage_resolution': 'TIME-INVARIANT',
                    'summary': 'Task 2',
                    'comment': 'Second task',
                }
            }
        ]

        tasklist_file = tmp_path / "tasklist_multi.json"
        tasklist_file.write_text(json.dumps(tasklist))

        # Mock the complex resource initialization
        from unittest import mock

        with mock.patch('ecco_dataset_production.ecco_grid.ECCOGrid') as mock_grid_cls, \
             mock.patch('ecco_dataset_production.ecco_mapping_factors.ECCOMappingFactors') as mock_factors_cls, \
             mock.patch('ecco_dataset_production.ecco_metadata.ECCOMetadata') as mock_metadata_cls:

            mock_grid = mock.MagicMock()
            mock_grid.native_grid = {}
            mock_grid_cls.return_value = mock_grid

            mock_factors = mock.MagicMock()
            mock_factors.latitude_bounds = np.array([[0.0, 0.5], [0.5, 1.0]])
            mock_factors.longitude_bounds = np.array([[0.0, 0.5], [0.5, 1.0]])
            mock_factors_cls.return_value = mock_factors

            mock_metadata = mock.MagicMock()
            mock_metadata.metadata_dir = metadata_directory
            mock_metadata_cls.return_value = mock_metadata

            # Execute
            ecco_generate_datasets.generate_datasets(str(tasklist_file))

        # Verify both outputs were created
        assert output_file1.exists(), "First output file should be created"
        assert output_file2.exists(), "Second output file should be created"

        # Verify they contain different variables
        ds1 = xr.open_dataset(output_file1)
        ds2 = xr.open_dataset(output_file2)
        try:
            assert 'TEMP' in ds1.data_vars, "First file should have TEMP"
            assert 'SALT' in ds2.data_vars, "Second file should have SALT"
        finally:
            ds1.close()
            ds2.close()

    def test_continues_after_task_error(
        self, minimal_input_netcdf, config_file, metadata_directory,
        grid_directory, mapping_factors_directory, tmp_path
    ):
        """Test that generate_datasets continues processing after a task fails."""
        # Use ECCO naming convention
        output_file1 = tmp_path / "FAIL_ECCO_V4r6_latlon_0p50deg.nc"
        output_file2 = tmp_path / "SUCCESS_ECCO_V4r6_latlon_0p50deg.nc"

        # Create tasklist where first task will fail (bad input file)
        tasklist = [
            {
                'input_netcdf': '/nonexistent/file.nc',  # This will fail
                'granule': str(output_file1),
                'strip_attributes': True,
                'ecco_cfg_loc': config_file,
                'ecco_grid_loc': grid_directory,
                'ecco_mapping_factors_loc': mapping_factors_directory,
                'ecco_metadata_loc': metadata_directory,
                'dynamic_metadata': {
                    'name': 'failing_task',
                    'dimension': '2D',
                    'time_coverage_duration': 'TIME-INVARIANT',
                    'time_coverage_resolution': 'TIME-INVARIANT',
                    'summary': 'Failing task',
                    'comment': 'This should fail',
                }
            },
            {
                'input_netcdf': minimal_input_netcdf,  # This should succeed
                'granule': str(output_file2),
                'strip_attributes': True,
                'ecco_cfg_loc': config_file,
                'ecco_grid_loc': grid_directory,
                'ecco_mapping_factors_loc': mapping_factors_directory,
                'ecco_metadata_loc': metadata_directory,
                'dynamic_metadata': {
                    'name': 'working_task',
                    'dimension': '2D',
                    'time_coverage_duration': 'TIME-INVARIANT',
                    'time_coverage_resolution': 'TIME-INVARIANT',
                    'summary': 'Working task',
                    'comment': 'This should work',
                }
            }
        ]

        tasklist_file = tmp_path / "tasklist_error.json"
        tasklist_file.write_text(json.dumps(tasklist))

        # Mock the complex resource initialization
        from unittest import mock

        with mock.patch('ecco_dataset_production.ecco_grid.ECCOGrid') as mock_grid_cls, \
             mock.patch('ecco_dataset_production.ecco_mapping_factors.ECCOMappingFactors') as mock_factors_cls, \
             mock.patch('ecco_dataset_production.ecco_metadata.ECCOMetadata') as mock_metadata_cls:

            mock_grid = mock.MagicMock()
            mock_grid.native_grid = {}
            mock_grid_cls.return_value = mock_grid

            mock_factors = mock.MagicMock()
            mock_factors.latitude_bounds = np.array([[0.0, 0.5], [0.5, 1.0]])
            mock_factors.longitude_bounds = np.array([[0.0, 0.5], [0.5, 1.0]])
            mock_factors_cls.return_value = mock_factors

            mock_metadata = mock.MagicMock()
            mock_metadata.metadata_dir = metadata_directory
            mock_metadata_cls.return_value = mock_metadata

            # Execute - should not raise despite first task failing
            ecco_generate_datasets.generate_datasets(str(tasklist_file))

        # Verify first task failed (no output)
        assert not output_file1.exists(), "First output should not exist due to error"

        # Verify second task succeeded
        assert output_file2.exists(), "Second output should exist despite first task error"

        ds2 = xr.open_dataset(output_file2)
        try:
            assert 'TEMP' in ds2.data_vars, "Second task should have completed successfully"
        finally:
            ds2.close()
