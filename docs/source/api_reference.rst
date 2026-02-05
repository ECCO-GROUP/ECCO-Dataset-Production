API Reference
=============

This section provides detailed API documentation for the ``ecco_dataset_production``
Python package, auto-generated from the source code docstrings.

.. automodule:: ecco_dataset_production
   :noindex:

.. tip:: **Quick Reference - Key Components**

   These are the most important entry points and classes in the pipeline:

   * **Entry Points:**
     :func:`~ecco_dataset_production.ecco_generate_datasets.generate_datasets`,
     :func:`~ecco_dataset_production.apps.create_job_task_list.create_job_task_list`

   * **Core Classes:**
     :class:`~ecco_dataset_production.ecco_dataset.ECCOMDSDataset`,
     :class:`~ecco_dataset_production.ecco_task.ECCOTask`,
     :class:`~ecco_dataset_production.ecco_grid.ECCOGrid`,
     :class:`~ecco_dataset_production.ecco_mapping_factors.ECCOMappingFactors`

   * **Granule Functions:**
     :func:`~ecco_dataset_production.ecco_generate_datasets.ecco_make_granule`,
     :func:`~ecco_dataset_production.ecco_generate_datasets.set_granule_metadata`,
     :func:`~ecco_dataset_production.ecco_generate_datasets.set_granule_ancillary_data`

   All key components include flow diagrams in their documentation.

Module Reference
----------------

Core Processing
^^^^^^^^^^^^^^^

Main entry points and data transformation classes.

.. toctree::
   :maxdepth: 1

   api/ecco_generate_datasets
   api/ecco_dataset

Data Resources
^^^^^^^^^^^^^^

Classes for accessing grid coordinates, transformation matrices, and metadata.

.. toctree::
   :maxdepth: 1

   api/ecco_grid
   api/ecco_mapping_factors
   api/ecco_metadata
   api/ecco_podaac_metadata

Task & Configuration
^^^^^^^^^^^^^^^^^^^^

Task descriptor handling and configuration file parsing.

.. toctree::
   :maxdepth: 1

   api/ecco_task
   api/configuration

Utilities
^^^^^^^^^

File handling and time calculation utilities.

.. toctree::
   :maxdepth: 1

   api/ecco_file
   api/ecco_time

Subpackages
-----------

* :doc:`api/apps/index` - Command-line applications
* :doc:`api/aws/index` - AWS S3 integration
* :doc:`api/utils/index` - Data processing utilities

.. toctree::
   :hidden:
   :maxdepth: 2

   api/apps/index
   api/aws/index
   api/utils/index
