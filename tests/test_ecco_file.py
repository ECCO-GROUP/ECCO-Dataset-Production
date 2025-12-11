
import numpy as np
import unittest

import ecco_dataset_production

# some test filenames:

ECCO_DAY_MEAN_RESULTS_FILE = 'SSH_day_mean.0000000012.data'
ECCO_DAY_MEAN_GRANULE_FILE = 'SEA_SURFACE_HEIGHT_day_mean_1992-01-01_ECCO_V4r4_latlon_0p50deg.nc'

ECCO_DAY_SNAP_RESULTS_FILE = 'SSH_day_snap.0000000012.data'
ECCO_DAY_SNAP_GRANULE_FILE = 'SEA_SURFACE_HEIGHT_day_snap_1992-01-01T000000_ECCO_V4r4_latlon_0p50deg.nc' # (*)
# (*) note special case 'day_snap' -> just 'snap'


class TestECCOFile(unittest.TestCase):

    def test_ecco_day_snap_mdsfilestr(self):
        # "circularity" test:
        ef = ecco_dataset_production.ecco_file.ECCOMDSFilestr(
            filestr = ECCO_DAY_SNAP_RESULTS_FILE)
        self.assertEqual(
            ECCO_DAY_SNAP_RESULTS_FILE,
            ecco_dataset_production.ecco_file.ECCOMDSFilestr(
                prefix=ef.prefix,
                averaging_period=ef.averaging_period,
                time=ef.time,
                ext=ef.ext).filestr)

    def test_ecco_day_mean_mdsfilestr(self):
        # "circularity" test:
        ef = ecco_dataset_production.ecco_file.ECCOMDSFilestr(
            filestr = ECCO_DAY_MEAN_RESULTS_FILE)
        self.assertEqual(
            ECCO_DAY_MEAN_RESULTS_FILE,
            ecco_dataset_production.ecco_file.ECCOMDSFilestr(
                prefix=ef.prefix,
                averaging_period=ef.averaging_period,
                time=ef.time,
                ext=ef.ext).filestr)

    def test_ecco_day_snap_granulefilestr(self):
        # "circularity" test:
        print(f'ECCO_DAY_SNAP_GRANULE_FILE: {ECCO_DAY_SNAP_GRANULE_FILE}')
        epf = ecco_dataset_production.ecco_file.ECCOGranuleFilestr(
            filestr = ECCO_DAY_SNAP_GRANULE_FILE)
        self.assertEqual(
            ECCO_DAY_SNAP_GRANULE_FILE,
            ecco_dataset_production.ecco_file.ECCOGranuleFilestr(
                prefix=epf.prefix,
                averaging_period=epf.averaging_period,
                date=epf.date,
                version=epf.version,
                grid_type=epf.grid_type,
                grid_label=epf.grid_label).filestr)

    def test_ecco_day_mean_granulefilestr(self):
        # "circularity" test:
        epf = ecco_dataset_production.ecco_file.ECCOGranuleFilestr(
            filestr = ECCO_DAY_MEAN_GRANULE_FILE)
        self.assertEqual(
            ECCO_DAY_MEAN_GRANULE_FILE,
            ecco_dataset_production.ecco_file.ECCOGranuleFilestr(
                prefix=epf.prefix,
                averaging_period=epf.averaging_period,
                date=epf.date,
                version=epf.version,
                grid_type=epf.grid_type,
                grid_label=epf.grid_label).filestr)


if __name__=='__main__':
    unittest.main()

