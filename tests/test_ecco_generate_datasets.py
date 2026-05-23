import importlib.util
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest import mock


REPO_ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = REPO_ROOT / 'src' / 'ecco_dataset_production' / 'ecco_generate_datasets.py'


def load_ecco_generate_datasets_module():
    package = types.ModuleType('ecco_dataset_production')
    package.__path__ = [str(MODULE_PATH.parent)]
    sys.modules['ecco_dataset_production'] = package

    aws_module = types.ModuleType('ecco_dataset_production.aws')
    aws_module.ecco_aws_s3_cp = types.SimpleNamespace(aws_s3_cp=mock.Mock())
    sys.modules['ecco_dataset_production.aws'] = aws_module
    package.aws = aws_module

    for module_name in (
        'configuration',
        'ecco_dataset',
        'ecco_file',
        'ecco_grid',
        'ecco_mapping_factors',
        'ecco_metadata',
        'ecco_podaac_metadata',
        'ecco_task',
    ):
        module = types.ModuleType(f'ecco_dataset_production.{module_name}')
        sys.modules[f'ecco_dataset_production.{module_name}'] = module
        setattr(package, module_name, module)

    for external_name in ('netCDF4', 'numpy', 'pandas', 'yaml', 'ecco_v4_py'):
        sys.modules[external_name] = types.ModuleType(external_name)

    xarray_module = types.ModuleType('xarray')
    xarray_module.open_dataset = mock.Mock()
    sys.modules['xarray'] = xarray_module

    spec = importlib.util.spec_from_file_location(
        'ecco_dataset_production.ecco_generate_datasets', MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class LocalTask(dict):
    @property
    def is_granule_local(self):
        return True


class TestProcessTimeInvariantGranule(unittest.TestCase):
    def test_process_time_invariant_granule_closes_input_dataset(self):
        module = load_ecco_generate_datasets_module()

        input_dataset = mock.MagicMock()
        input_dataset.variables = []

        output_dataset = mock.MagicMock()

        module.xr.open_dataset.return_value = input_dataset
        module.set_granule_ancillary_data = mock.Mock(return_value=output_dataset)
        module.set_granule_metadata = mock.Mock(return_value=(output_dataset, {}))

        with tempfile.TemporaryDirectory() as tmpdir:
            granule_path = str(Path(tmpdir) / 'granule.nc')
            task = LocalTask(input_netcdf='input.nc', granule=granule_path)

            module.process_time_invariant_granule(task=task, cfg={})

        input_dataset.close.assert_called_once_with()
        output_dataset.to_netcdf.assert_called_once_with(granule_path, encoding={})


if __name__ == '__main__':
    unittest.main()
