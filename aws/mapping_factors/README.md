# **/mapping_factors/**
Contains all the mapping factors used for processing.
- **3D/**
  - Contains all the 3D mapping factors, where each level is saved as a separate file. This allows the code to load one level at a time which dramatically reduces memory usage
- **land_mask/**
  - Contains all the land mask files, where each level is saved as a separate file. This allows the code to load one level at a time which dramatically reduces memory usage
- **latlon_grid/**
  - Contains a file called *latlon_grid* which contains latlon bounds and other latlon grid data
- **sparse/**
  - Contains all the sparse matrix files, where each level's sparse matrix is saved as a separate file. This allows the code to load one sparse matrix at a time which dramatically reduces memory usage
- **ecco_latlon_grid_mappings_2D.xz**
  - Mapping factors for a 2D grid (equivalent to level 0 of 3D mapping factors)
- **ecco_latlon_grid_mappings_all.xz**
  - "All" mapping factors, which includes the mappings of wet AND dry points

