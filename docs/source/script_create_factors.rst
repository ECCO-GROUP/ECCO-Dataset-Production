
edp_create_factors
==================

Creates 2D and/or 3D grid mapping factors, land masks, and longitude/latitude
grid files required for ECCO dataset production.

Overview
--------

``edp_create_factors`` generates the grid mapping factors required to interpolate
ECCO data from its native LLC (latitude-longitude-cap) grid to regular
latitude-longitude grids. This is typically a one-time setup step performed
before dataset generation.

Grid mapping factors are used to:

- Interpolate data from the native LLC grid to regular lat/lon grids
- Create land masks for each vertical level
- Enable efficient sparse matrix multiplication for regridding


Usage
-----

.. code-block:: bash

    edp_create_factors [--cfgfile CFGFILE] [--workingdir WORKINGDIR]
                       [--dims DIMS [DIMS ...]] [-l LOG_LEVEL]


Arguments
---------

``--cfgfile``
    Path and filename of the ECCO Production configuration file (YAML format).
    Default: ``./product_generation_config.yaml``

``--workingdir``
    Working directory used to set default path root values if configuration
    path data are unassigned.
    Default: ``.``

``--dims``
    Dimensions of mapping factors to generate. Specify ``2`` for 2D factors,
    ``3`` for 3D factors, or both (e.g., ``--dims 2 3``).

``-l, --log``
    Set logging level. Choices: ``DEBUG``, ``INFO``, ``WARNING``, ``ERROR``,
    ``CRITICAL``.
    Default: ``WARNING``


Configuration Parameters
------------------------

The following configuration file parameters are referenced:

- ``custom_grid_and_factors`` - Custom target grid mapping specifications
- ``ecco_grid_dir`` - Directory containing ECCO grid files
- ``ecco_grid_filename`` - ECCO grid filename (required)
- ``ecco_version`` - ECCO version string (e.g., "V4r5")
- ``grid_files_dir`` - Output directory for grid files
- ``latlon_effective_grid_radius`` - Effective radius for lat/lon mapping
- ``latlon_grid_area_extent`` - Area extent for lat/lon grid
- ``latlon_grid_dims`` - Dimensions of lat/lon grid
- ``latlon_grid_resolution`` - Resolution of lat/lon grid
- ``latlon_max_lat`` - Maximum latitude for lat/lon grid
- ``mapping_factors_dir`` - Output directory for mapping factors
- ``num_vertical_levels`` - Number of vertical levels
- ``source_grid_min_L`` / ``source_grid_max_L`` - Source grid L bounds


Entry Point
-----------

**Module:** ``ecco_dataset_production.apps.create_factors``

**Function:** ``main()``


Input Files
-----------

+-------------------------------+-----------------------------------------------+
| File                          | Description                                   |
+===============================+===============================================+
| ``product_generation_config.  | YAML configuration file containing grid       |
| yaml``                        | parameters, paths, and processing options     |
+-------------------------------+-----------------------------------------------+
| ``GRID_GEOMETRY_ECCO_*.nc``   | ECCO grid NetCDF file containing:             |
|                               |                                               |
|                               | - ``XC``, ``YC``: Grid cell center coords     |
|                               | - ``hFacC``: Vertical fraction of open cell   |
|                               | - ``drF``: Vertical cell thickness            |
|                               | - ``dxG``, ``dyG``: Grid cell edge lengths    |
+-------------------------------+-----------------------------------------------+

**For Custom Grids (optional):**

+-------------------------------+-----------------------------------------------+
| File                          | Description                                   |
+===============================+===============================================+
| ``source_grids/*.data``       | Binary MDS files for source grid (XC, YC,     |
|                               | hFacC, RAC)                                   |
+-------------------------------+-----------------------------------------------+
| ``source_grids/*.meta``       | Metadata files describing dimensions and      |
|                               | data types for corresponding .data files      |
+-------------------------------+-----------------------------------------------+
| ``target_grids/*.data``       | Binary MDS files for target grid              |
+-------------------------------+-----------------------------------------------+
| ``target_grids/*.meta``       | Metadata files for target grid                |
+-------------------------------+-----------------------------------------------+


Output Files
------------

All output files are written to ``{mapping_factors_dir}/``:

+-------------------------------------------+-----------------------------------+
| File                                      | Description                       |
+===========================================+===================================+
| ``ecco_latlon_grid_mappings_all.xz``      | Mapping factors for ALL grid      |
|                                           | points (including land), used for |
|                                           | land mask creation. LZMA-         |
|                                           | compressed pickle.                |
+-------------------------------------------+-----------------------------------+
| ``ecco_latlon_grid_mappings_2D.xz``       | 2D mapping factors (wet points    |
|                                           | only, surface level)              |
+-------------------------------------------+-----------------------------------+
| ``3D/ecco_latlon_grid_mappings_3D_{k}.xz``| 3D mapping factors for vertical   |
|                                           | level k (0 to num_vertical_levels)|
+-------------------------------------------+-----------------------------------+
| ``land_mask/ecco_latlon_land_mask_{k}.xz``| Land mask for vertical level k,   |
|                                           | transformed to target grid        |
+-------------------------------------------+-----------------------------------+
| ``latlon_grid/latlon_grid.xz``            | Target grid definition containing |
|                                           | lat/lon bounds, depth bounds,     |
|                                           | grid shape, and wet points dict   |
+-------------------------------------------+-----------------------------------+
| ``sparse/sparse_matrix_{k}.npz``          | Scipy sparse CSR matrix for       |
|                                           | efficient interpolation at level k|
+-------------------------------------------+-----------------------------------+

**Example Output Directory Structure:**

.. code-block:: text

    mapping_factors/V4r5/
    ├── ecco_latlon_grid_mappings_all.xz
    ├── ecco_latlon_grid_mappings_2D.xz
    ├── 3D/
    │   ├── ecco_latlon_grid_mappings_3D_0.xz
    │   ├── ecco_latlon_grid_mappings_3D_1.xz
    │   ├── ...
    │   └── ecco_latlon_grid_mappings_3D_49.xz
    ├── land_mask/
    │   ├── ecco_latlon_land_mask_0.xz
    │   ├── ecco_latlon_land_mask_1.xz
    │   ├── ...
    │   └── ecco_latlon_land_mask_49.xz
    ├── latlon_grid/
    │   └── latlon_grid.xz
    └── sparse/
        ├── sparse_matrix_0.npz
        ├── sparse_matrix_1.npz
        ├── ...
        └── sparse_matrix_49.npz


Examples
--------

**Basic usage with 2D and 3D factors:**

.. code-block:: bash

    edp_create_factors --cfgfile ./config/V4r5_config.yaml \
                       --workingdir /data/ecco \
                       --dims 2 3 \
                       -l INFO

**Generate only 2D factors:**

.. code-block:: bash

    edp_create_factors --cfgfile ./config/V4r5_config.yaml \
                       --dims 2

**Using default configuration file:**

.. code-block:: bash

    cd /path/to/working/directory
    edp_create_factors --dims 2 3


Execution Flow Diagram
----------------------

.. code-block:: text

    main()
      |
      +---> create_parser()
      |       |
      |       +---> Define CLI arguments (cfgfile, workingdir, dims, log)
      |
      +---> Parse command-line arguments
      |
      +---> create_factors()
              |
              +---> Initialize ECCODatasetProductionConfig
              |       |
              |       +---> Load YAML configuration file
              |       |
              |       +---> set_default_paths(workingdir)
              |
              +---> Convert dims to format ['2D', '3D']
              |
              +---> create_all_factors()  [mapping_factors_utils]
                      |
                      +---> [If custom_grid_and_factors]
                      |       |
                      |       +---> create_custom_grid_values()
                      |
                      +---> [Else standard ECCO grid]
                      |       |
                      |       +---> create_ecco_grid_values()
                      |               |
                      |               +---> Load ECCO grid (NetCDF)
                      |               |
                      |               +---> Create source swath definitions
                      |               |
                      |               +---> Create target lat/lon grid
                      |               |
                      |               +---> Calculate grid parameters
                      |               |
                      |               +---> Save latlon_grid.xz
                      |
                      +---> For each dimension in dims:
                      |       |
                      |       +---> create_mapping_factors()
                      |       |       |
                      |       |       +---> Create grid_mappings_all.xz
                      |       |       |
                      |       |       +---> Create grid_mappings_{2D,3D}_{k}.xz
                      |       |
                      |       +---> create_land_mask()  [if not custom]
                      |               |
                      |               +---> Load grid_mappings_all
                      |               |
                      |               +---> Transform land mask per level
                      |               |
                      |               +---> Save land_mask_{k}.xz
                      |
                      +---> create_sparse_matrix()
                              |
                              +---> For each vertical level k:
                                      |
                                      +---> Load land_mask and mapping_factors
                                      |
                                      +---> Build sparse weight matrix
                                      |
                                      +---> Save sparse_matrix_{k}.npz


Detailed Flow Description
-------------------------

1. Configuration Initialization
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The script loads configuration from a YAML file containing parameters such as:

- ``ecco_grid_dir`` / ``ecco_grid_filename``: Location of ECCO grid NetCDF file
- ``mapping_factors_dir``: Output directory for generated factors
- ``latlon_grid_resolution``: Target grid resolution (e.g., 0.5 degrees)
- ``latlon_grid_area_extent``: Geographic bounds [lon_min, lat_max, lon_max, lat_min]
- ``num_vertical_levels``: Number of depth levels (e.g., 50)
- ``source_grid_min_L`` / ``source_grid_max_L``: Grid cell size bounds

2. Grid Value Preparation
^^^^^^^^^^^^^^^^^^^^^^^^^

**For Standard ECCO Grids (create_ecco_grid_values):**

a. **Load ECCO Grid:**
   Opens the ECCO grid NetCDF file containing XC, YC, hFacC, drF, etc.

b. **Create Source Swath Definitions:**
   Using pyresample, creates SwathDefinition objects for:

   - ``source_grid``: All grid points (including land) for land mask creation
   - ``source_grid_k[k]``: Wet points only at each vertical level k

c. **Define Target Grid:**
   Creates a regular lat/lon grid using configuration parameters:

   .. code-block:: python

       target_grid = pr.geometry.SwathDefinition(
           lons=target_grid_lons, lats=target_grid_lats)

d. **Calculate Bounds:**
   Computes latitude, longitude, and depth bounds arrays for the target grid.

e. **Save latlon_grid.xz:**
   Pickles the grid information (bounds, shape, wet points) for later use.

**For Custom Grids (create_custom_grid_values):**

Reads source and target grid data from .meta/.data file pairs in specified
directories, allowing non-standard grid configurations.

3. Mapping Factor Creation
^^^^^^^^^^^^^^^^^^^^^^^^^^

The ``create_mapping_factors()`` function computes the spatial relationships
between source and target grids:

**grid_mappings_all:**
   Maps ALL source grid points (including land) to target grid cells.
   Used for land mask creation.

**grid_mappings_{dim}_{k}:**
   Maps only WET source grid points at each vertical level to target cells.
   Used for actual data interpolation.

Each mapping contains:

- ``source_indices_within_target_radius_i``: Source points within each target cell's radius
- ``nearest_source_index_to_target_index_i``: Nearest neighbor fallback mapping

The mapping algorithm uses ``ecco_cloud_utils.mapping.find_mappings_from_source_to_target_for_processing()``.

4. Land Mask Creation
^^^^^^^^^^^^^^^^^^^^^

For each vertical level k:

1. Extract the hFacC (wet/dry) mask from the ECCO grid
2. Transform to target grid using nearest-neighbor interpolation
3. Save as compressed pickle file (``land_mask_{k}.xz``)

5. Sparse Matrix Creation
^^^^^^^^^^^^^^^^^^^^^^^^^

Creates efficient sparse matrix representations for fast interpolation:

For each vertical level k:

1. Load the land mask and mapping factors
2. For each target grid wet point:

   - If source points exist within radius: weight = 1/count
   - If no source points: use nearest neighbor with weight = 1

3. Build scipy sparse CSR matrix: ``B = sparse.csr_matrix(weights, (source_idx, target_idx))``
4. Save as ``sparse_matrix_{k}.npz``


Key Module Dependencies
-----------------------

.. code-block:: text

    ecco_dataset_production.apps.create_factors
        |
        +---> ecco_dataset_production.configuration
        |       |
        |       +---> ECCODatasetProductionConfig (YAML loader)
        |
        +---> ecco_dataset_production.utils.mapping_factors_utils
                |
                +---> create_all_factors()
                +---> create_ecco_grid_values()
                +---> create_custom_grid_values()
                +---> create_mapping_factors()
                +---> create_land_mask()
                +---> create_sparse_matrix()
                |
                +---> ecco_cloud_utils.mapping
                +---> ecco_cloud_utils.generalized_functions
                +---> pyresample


Performance Notes
-----------------

- Mapping factor creation is computationally intensive
- Files are cached and reused if they already exist
- The script checks for existing files before recalculating
- 3D factors require processing each vertical level (50+ iterations)

