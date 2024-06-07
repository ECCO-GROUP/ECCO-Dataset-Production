
import numpy as np
import unittest

import ecco_dataset_production

# some test filenames:
ECCO_GRANULE_FILE = 'SSH_day_mean.0000000012.data'
ECCO_PRODUCTION_FILE = 'SEA_SURFACE_HEIGHT_day_mean_1992-01-01_ECCO_V4r4_latlon_0p50deg.nc'


class TestECCOFile(unittest.TestCase):

    def test_ecco_filestr(self):

        # "circularity" test:
        ef = ecco_dataset_production.ecco_file.ECCOFilestr(
            filestr = ECCO_GRANULE_FILE)
        self.assertEqual(
            ECCO_GRANULE_FILE,
            ecco_dataset_production.ecco_file.ECCOFilestr(
                varname=ef.varname,
                averaging_period=ef.averaging_period,
                time=ef.time,
                ext=ef.ext).filestr)

    def test_ecco_production_filestr(self):

        # "circularity" test:
        epf = ecco_dataset_production.ecco_file.ECCOProductionFilestr(
            filestr = ECCO_PRODUCTION_FILE)
        self.assertEqual(
            ECCO_PRODUCTION_FILE,
            ecco_dataset_production.ecco_file.ECCOProductionFilestr(
                varname=epf.varname,
                averaging_period=epf.averaging_period,
                date=epf.date,
                version=epf.version,
                grid_type=epf.grid_type,
                grid_label=epf.grid_label).filestr)

        print(
            ecco_dataset_production.ecco_file.ECCOProductionFilestr(
                varname=epf.varname,
                averaging_period=epf.averaging_period,
                date=epf.date,
                version=epf.version,
                grid_type=epf.grid_type,
                grid_label=epf.grid_label).re_filestr)

if __name__=='__main__':
    unittest.main()

