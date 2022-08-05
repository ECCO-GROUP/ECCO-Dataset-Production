# **/mapping_factors/**
Contains all the mapping factors used for processing, organized by ECCO Version.
- **{ecco_version}**
  - **3D/**
    - Contains all the 3D mapping factors, where each level is saved as a separate file. This allows the code to load one level at a time which dramatically reduces memory usage
      - These files are named as follows: *ecco_latlon_grid_mappings_3D_{i}.xz*, where *i* corresponds to the vertical level for the mappings. These files are compressed using *lzma* compression.
      - Each file contains two lists, *source_indices_within_target_radius_i* and *nearest_source_index_to_target_index_i*
        - *source_indices_within_target_radius_i*: list where index is target index, and value is -1 if no source indices in target radius, or a list of source indices within target radius
        - *nearest_source_index_to_target_index_i*: list where index is the target index, and value is the nearest source index to target index
  - **land_mask/**
    - Contains all the land mask files, where each level is saved as a separate file. This allows the code to load one level at a time which dramatically reduces memory usage
      - These files are named as follows: *ecco_latlon_land_mask_{i}.xz*, where *i* corresponds to the vertical level for the land mask. These files are compressed using *lzma* compression.
  - **latlon_grid/**
    - Contains a file called *latlon_grid* which contains latlon bounds and other latlon grid data
        - *latlon_bounds*: Contains the lat and lon bounds for each grid cell
        - *depth_bounds*: Contains the vertical bounds for each vertical level
        - *target_grid_dict*: Contains latlon grid 'shape', and lists of all the 'lats_1D' and 'lons_1D'
        - *wet_pts_k*: Dictionary with key=vertical level index, and value=tuple of numpy.arrays of source grid wet points
  - **sparse/**
    - Contains all the sparse matrix files, where each level's sparse matrix is saved as a separate file. This allows the code to load one sparse matrix at a time which dramatically reduces memory usage.
      - These files are named as follows: *sparse_matrix_{i}.npz*, where *i* corresponds to the vertical level for the sparse matrix. These files are save as compressed sparse matrix files.
      - The sparse matrix is an n x m matrix, where n is the number of source grid points, and m is the number of target grid points. Each value is the weighted mean to apply to the target grid, which is calculated as 1/(number of source grid points within target radius). If there are no source grid points within target radius, that value is just 1 (corresponding to nearest neighbor).
  - **ecco_latlon_grid_mappings_2D.xz**
    - Mapping factors for a 2D grid (equivalent to level 0 of the 3D mapping factors)
      - This file contains two lists, *source_indices_within_target_radius_i* and *nearest_source_index_to_target_index_i*
        - This file is named as follows: *ecco_latlon_grid_mappings_2D.xz*, and corresponds to the first vertical level (k=0). This file is compressed using *lzma* compression, and is the exact same as the 0th 3D grid mappings file (same structure and values).
  - **ecco_latlon_grid_mappings_all.xz**
    - "All" mapping factors, which includes the mapping over wet AND dry points.
      - This file is named as follows: *ecco_latlon_grid_mappings_all.xz*. This file compressed using *lzma* compression, and has the same structure as the 2D and 3D grid mappings.

