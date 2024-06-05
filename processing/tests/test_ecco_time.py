
import numpy as np
import unittest

import ecco_dataset_production


# some configuration test data:
cfg = {
    'model_start_time': '1992-01-01T12:00:00',
    'model_end_time':   '2017-12-31T12:00:00'
}


class TestECCOTime(unittest.TestCase):

    def test_make_time_bounds_metadata(self):

        # first interval test:
        tb,center_time = \
            ecco_dataset_production.ecco_time.make_time_bounds_metadata(
                granule_time='0000000018',
                model_timestep=1, model_timestep_units='h',
                averaging_period='AVG_DAY', cfg=cfg)
        self.assertListEqual(
            list(tb),
            [np.datetime64('1992-01-01T12:00:00.000000'), np.datetime64('1992-01-02T06:00:00.000000')])
        self.assertEqual(
            center_time, np.datetime64('1992-01-01T21:00:00.000000'))

        # last interval test:
        tb,center_time = \
            ecco_dataset_production.ecco_time.make_time_bounds_metadata(
                granule_time='0000227904',
                model_timestep=1, model_timestep_units='h',
                averaging_period='AVG_DAY', cfg=cfg)
        self.assertListEqual(
            list(tb),
            [np.datetime64('2017-12-31T00:00:00.000000'), np.datetime64('2017-12-31T12:00:00.000000')])
        self.assertEqual(
            center_time, np.datetime64('2017-12-31T06:00:00.000000'))


if __name__=='__main__':
    unittest.main()

