ECCO Document Generator — API Reference
========================================

Auto-generated Sphinx reStructuredText documentation.
Organised by source file. Covers all top-level functions and class methods.

.. contents:: Table of Contents
   :depth: 2
   :local:

----

a_step1_download_granules.py
----------------------------

.. py:function:: main()

----

a_step2_generate_document.py
-----------------------------

.. py:function:: main()

----

a_step3_compile_latex.py
-------------------------

.. py:function:: main()

----

general/utility_scripts/cdf_extract.py
---------------------------------------

.. py:function:: fieldTable(config_dictionary, dataset: xr.Dataset, is_coord: bool, grid_type) -> list[str]

.. py:function:: latex_example_netcdf(base_dir, config_dictionary, grid_type)

.. py:function:: get_non_coordinate_vars(filename: str) -> list[xr.DataArray]

.. py:function:: get_coordinate_vars(filename: str) -> list[xr.DataArray]

.. py:function:: extract_field_info(field: xr.DataArray) -> dict[str, str]

.. py:function:: search_and_extract(granule_filename_truncated_stem: str, granule_directory: str, is_coord: bool = False) -> tuple[list[xr.DataArray], xr.Dataset]

.. py:function:: data_var_table(config_dictionary, field_name: str, attrs: dict, dataset_name: str, grid_type) -> list[str]

.. py:function:: get_product_name(dataset: xr.Dataset) -> str

.. py:function:: get_coord_vars_in_dataset(dataset: xr.Dataset, isCoord: bool = False) -> tuple[list[str], list[str], list[str]]

.. py:function:: table_cellSize(field_var: list)

.. py:function:: global_attrs_for_ECCOnetCDF(jsonFileRef: str, GlobalAttrsCollect: str, tableCaption: str, latexFilename: str, saveTo: str)

.. py:function:: get_Global_or_CoordsDimsVarsList(netCDFpath: str, jsonFileName: str, saveTo: str)

.. py:function:: data_products(base_dir, config_dictionary, granule_directory) -> list

----

general/utility_scripts/cdf_plotter.py
----------------------------------------

.. py:function:: data_var_plot(config_dictionary, dataset, data_array, image_directory)

.. py:function:: plot_native(dataset: xr.Dataset, field: xr.DataArray, figure_path: str) -> None

.. py:function:: plot_latlon(dataset: xr.Dataset, field: xr.DataArray, figure_path: str) -> None

.. py:function:: plot_oneD(dataset: xr.Dataset, field: xr.DataArray, figure_path: str) -> None

.. py:function:: plot_datasetPicEg(dataset: xr.Dataset, save_to: str)

.. py:function:: even_cax(cmin: float, cmax: float, fac: float = 1.0) -> tuple[float, float]

.. py:function:: cal_cmin_cmax(cmap: matplotlib.colors.LinearSegmentedColormap, cmin: float, cmax: float, shortname_tmp: str, product_type: str)

.. py:function:: compute_cmin_cmax(data, factor=1.5)

----

general/utility_scripts/cdf_reader.py
---------------------------------------

.. py:function:: get_non_coord_vars(ds_grid: xa.Dataset) -> dict

.. py:function:: readVarAttr(varName: str, var: xa.Variable) -> dict

.. py:function:: readAllVarAttrs(ds_grid: xa.Dataset) -> list

.. py:function:: process_dict_items(data: dict, sanitation_func) -> list

.. py:function:: data_var_table(fieldName, da: dict, ds_name: str) -> list

.. py:function:: compute_ds_dict(varName: str, var: xa.DataArray) -> dict

.. py:function:: read_data_vars(ds: xa.Dataset) -> dict

.. py:function:: generate_CDL(original_nc_path: str, new_nc_path: str) -> str

.. py:function:: cdl_to_latex(cdl_string, name: str = 'example')

----

general/utility_scripts/latex_outline.py
------------------------------------------

.. py:function:: write_data_attributes_tables(base_dir, config_dictionary)

.. py:function:: write_datasets(base_dir, config_dictionary)

----

general/utility_scripts/utils_general.py
------------------------------------------

.. py:function:: write_latex_lines_to_file(latex_lines, output_file)

.. py:function:: append_hanging_indentation_commands_cm_latex(config_dictionary, num_tabs, latex_lines_list)

.. py:function:: download_granules(base_dir, config_dictionary)

.. py:function:: get_a_file_with_min_num_vars(base_dir, nc_dir)

.. py:function:: get_a_file_with_max_num_vars(base_dir, nc_dir)

.. py:function:: sanitize(config_dictionary, string: str) -> str

.. py:function:: sanitize_with_math(config_dictionary, string: str) -> str

.. py:function:: sanitize_with_url(config_dictionary, string: str) -> str

.. py:function:: get_substring(input_string: str) -> str

.. py:function:: add_to_line(line: str, before: str, after: str) -> str

.. py:function:: get_ds_title(ds: xr.Dataset) -> str

.. py:function:: get_granule_and_grid_types(granule_directory)

.. py:function:: generate_thumbnail(input_path, output_path, size)

----

general/utility_scripts/utils_json.py
---------------------------------------

.. py:function:: write_attributes_tables_tex(base_dir, config_dictionary)

.. py:function:: obtain_json_data(base_dir, filename: str) -> list

.. py:function:: obtain_keys(json_data: list) -> set

.. py:function:: verify_columns(available_columns: set, user_columns: list) -> list

.. py:function:: establish_table(dictionary_list_from_json: list, config_dictionary) -> list
