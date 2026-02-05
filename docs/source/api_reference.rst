API Reference
=============

This section provides detailed API documentation for the ``ecco_dataset_production``
Python package, auto-generated from the source code docstrings.

The documentation is organized to mirror the package's module structure.

Key Components
--------------

The following are the major components of the pipeline. Each includes flow
diagrams to illustrate their internal structure and data flow.

**Core Orchestration:**

* :func:`~ecco_dataset_production.ecco_generate_datasets.generate_datasets` -
  Top-level entry point that processes task lists and coordinates granule generation
* :func:`~ecco_dataset_production.ecco_generate_datasets.ecco_make_granule` -
  Creates individual PO.DAAC-ready granules for both latlon and native grid formats
* :func:`~ecco_dataset_production.apps.create_job_task_list.create_job_task_list` -
  Generates task descriptors from job specifications, discovering source files from local or S3

**Data Processing:**

* :class:`~ecco_dataset_production.ecco_dataset.ECCOMDSDataset` -
  Core data container handling MDS binary format parsing, vector transformations, and grid mapping
* :meth:`~ecco_dataset_production.ecco_dataset.ECCOMDSDataset.as_latlon` -
  Transforms native grid variables to lat/lon format using sparse matrix operations

**Metadata & Ancillary Data:**

* :func:`~ecco_dataset_production.ecco_generate_datasets.set_granule_metadata` -
  Aggregates metadata from configuration, task descriptors, and JSON metadata files
* :func:`~ecco_dataset_production.ecco_generate_datasets.set_granule_ancillary_data` -
  Sets array precision, fill values, time bounds, and coordinate bounds

ecco_dataset_production
-----------------------

Top-level modules in the ``ecco_dataset_production`` package.

.. toctree::
   :maxdepth: 1

   api/configuration
   api/ecco_dataset
   api/ecco_file
   api/ecco_generate_datasets
   api/ecco_grid
   api/ecco_mapping_factors
   api/ecco_metadata
   api/ecco_podaac_metadata
   api/ecco_task
   api/ecco_time

ecco_dataset_production.apps
----------------------------

Command-line interface applications and entry points.

.. toctree::
   :maxdepth: 2

   api/apps/index

ecco_dataset_production.aws
---------------------------

AWS integration utilities including S3 operations.

.. toctree::
   :maxdepth: 2

   api/aws/index

ecco_dataset_production.utils
-----------------------------

Data processing and utility functions.

.. toctree::
   :maxdepth: 2

   api/utils/index
