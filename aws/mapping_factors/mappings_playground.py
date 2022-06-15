from pathlib import Path
import sys
import lzma
import pickle
from sys import getsizeof
import numpy as np

def get_size(v):
    if type(v) == type({}):
        print(f'getsizeof {getsizeof(v)}')
        print(f'sum(getsizeof)  {getsizeof(v[list(v.keys())[0]]) * len(list(v.keys())) + getsizeof(list(v.keys())[0]) * len(list(v.keys()))}')
        # print(f'nbytes  {v.nbytes}')
        # print(f'sum(itemsize)  {v.itemsize * len(v)}')
    else:
        print(f'getsizeof {getsizeof(v)}')
        print(f'sum(getsizeof)  {getsizeof(v[0]) * len(v)}')
        print(f'nbytes  {v.nbytes}')
        print(f'sum(itemsize)  {v.itemsize * len(v)}')

sys.path.append('/Users/bark/Documents/ECCO_GROUP/ECCO-ACCESS/generating_netcdf/V1_cloud/lambda_code/')
from mapping_factors import get_mapping_factors
from gen_netcdf_utils import get_land_mask

# a1 = pickle.load(lzma.open('ecco_latlon_grid_mappings_2D.xz', 'rb'))
mapping_factors_dir = Path('/Users/bark/Documents/ECCO_GROUP/ECCO-ACCESS/generating_netcdf/V1_cloud/lambda_code/mapping_factors')

# import pdb; pdb.set_trace()

# (a1, _, _), _ = get_mapping_factors('2D', mapping_factors_dir, 'all')

# import pdb; pdb.set_trace()

# (_, a2, _), _ = get_mapping_factors('2D', mapping_factors_dir, 'all')

# import pdb; pdb.set_trace() 

# (_, _, a3), _ = get_mapping_factors('2D', mapping_factors_dir, 'all')

# import pdb; pdb.set_trace()

# import pdb; pdb.set_trace()

# (a1, a2), _ = get_mapping_factors('2D', mapping_factors_dir, 'all')

# import pdb; pdb.set_trace()

# (a1, _), _ = get_mapping_factors('2D', mapping_factors_dir, 'all')

# import pdb; pdb.set_trace()

# (_, a2), _ = get_mapping_factors('2D', mapping_factors_dir, 'all')

# import pdb; pdb.set_trace()

# a1, _ = get_mapping_factors('2D', mapping_factors_dir, 'all')

# import pdb; pdb.set_trace()

# del(a1)

# import pdb; pdb.set_trace()

import pdb; pdb.set_trace()

ll = get_land_mask(mapping_factors_dir)

import pdb; pdb.set_trace()

del(ll)

import pdb; pdb.set_trace()



# b, _ = get_mapping_factors('2D', mapping_factors_dir, 'all', fn='test.xz')
# a1, a2, a3 = a

# import pdb; pdb.set_trace()
# del(b)
# import pdb; pdb.set_trace()

# import pdb; pdb.set_trace()
# del(b1)
# import pdb; pdb.set_trace()
# del(b2)
# import pdb; pdb.set_trace()
# del(b3)
# import pdb; pdb.set_trace()
# print('done')
# import pdb; pdb.set_trace()




# import pdb; pdb.set_trace()
# del(a)
# import pdb; pdb.set_trace()

# import pdb; pdb.set_trace()
# del(a1)
# import pdb; pdb.set_trace()
# del(a2)
# import pdb; pdb.set_trace()
# del(a3)
# import pdb; pdb.set_trace()
# print('done')
# import pdb; pdb.set_trace()