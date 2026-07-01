
import numpy as np

import ecco_dataset_production


# some configuration test data:
cfg = {
    'model_start_time'      : '1992-01-01T12:00:00',
    'model_end_time'        : '2017-12-31T12:00:00',
    'model_timestep'        : 1,
    'model_timestep_units'  : 'h',
}


def test_make_time_bounds_metadata_first_interval():
    """Test time bounds for first interval."""
    tb, center_time = \
        ecco_dataset_production.ecco_time.make_time_bounds_metadata(
            granule_time='0000000018',
            model_start_time=cfg['model_start_time'],
            model_end_time=cfg['model_end_time'],
            model_timestep=cfg['model_timestep'],
            model_timestep_units=cfg['model_timestep_units'],
            averaging_period='AVG_DAY')
    assert list(tb) == [
        np.datetime64('1992-01-01T12:00:00.000000'),
        np.datetime64('1992-01-02T06:00:00.000000')
    ]
    assert center_time == np.datetime64('1992-01-01T21:00:00.000000')


def test_make_time_bounds_metadata_last_interval():
    """Test time bounds for last interval."""
    tb, center_time = \
        ecco_dataset_production.ecco_time.make_time_bounds_metadata(
            granule_time='0000227904',
            model_start_time=cfg['model_start_time'],
            model_end_time=cfg['model_end_time'],
            model_timestep=cfg['model_timestep'],
            model_timestep_units=cfg['model_timestep_units'],
            averaging_period='AVG_DAY')
    assert list(tb) == [
        np.datetime64('2017-12-31T00:00:00.000000'),
        np.datetime64('2017-12-31T12:00:00.000000')
    ]
    assert center_time == np.datetime64('2017-12-31T06:00:00.000000')
