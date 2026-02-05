"""Command-line applications for ECCO Dataset Production.

This subpackage provides command-line entry points for the main pipeline
operations. Each module can be invoked as a standalone CLI tool.

Example command-line usage::

    # Generate task list from job file
    $ edp_create_job_task_list --jobfile jobs.txt --ecco_source_root /data/ecco ...

    # Generate datasets from task list
    $ edp_generate_datasets --tasklist tasks.json

"""
#from . import aws_s3_sync
#from . import create_factors
from . import create_job_files
from . import create_job_task_list
from . import generate_datasets
