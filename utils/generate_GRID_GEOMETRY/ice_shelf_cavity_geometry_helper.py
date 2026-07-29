# function to calculate seafloor depth and ice shelf draft

import numpy as np

def calc_depths_beneath_floating_ice_shelf(hFacC_here, ocean_column_thickness_here, drF, debug=True):
    
    #hFacC_here = ecco_grid.hFacC[:,tile, j, i] + 0
    nonzero_hFacC = np.nonzero(hFacC_here.values)[0]

    if hFacC_here[0] > 0:
        print(f'surface hFacC is nonzero, abort')
        return -1
 
    
    if debug:
        print(f'nonzero hFacCs #={len(nonzero_hFacC)}, {nonzero_hFacC}')
        print(hFacC_here.values[nonzero_hFacC])

    # how thick is the ice shelf?  
    # it's this thickness of all the 'dry' points from the surface down to the first 
    # partially or entirely wet point below it
    
    # to caculate ice shelf thickness we
    # ... define a 0/1 mask of length nk with initial values 0,
    # ... starting from the top (k=0), set all entirely dry (hFacC ==0) points to 1  [0:first_wet_k]
    # ... set the first partially (or entirely) wet point (at first_wet_k) to 1-hFacC[first_wet_k]
    #     --> because hFacC is the wet fraction, so 1-hFacC is the dry Fraction
    # ... then muliptly the mask by drF and sum!
    
    # this k is the first k with a nonzero hFacC.  
    first_wet_k = nonzero_hFacC[0]
    last_wet_k = nonzero_hFacC[-1]

    if first_wet_k == last_wet_k:
        print(f'the first wet k is also the last wet k, take a closer look')

    ice_shelf_tmp = np.zeros(50)
    ice_shelf_tmp[0:first_wet_k] = 1
    ice_shelf_tmp[first_wet_k] = 1-hFacC_here[first_wet_k]
    
    if debug:
        print(f'ice_shelf_tmp : {ice_shelf_tmp}')

    ice_shelf_draft_new = np.sum(ice_shelf_tmp*drF)

    # seafloor depth is the sum of the ice shelf draft and the ocean column thickness
    seafloor_depth_new= ice_shelf_draft_new + ocean_column_thickness_here

    if debug:
        print(f'ice_shelf_draft {ice_shelf_draft_new}')
        print(f'seafloor_depth {seafloor_depth_new}')
    
    return ice_shelf_draft_new, seafloor_depth_new