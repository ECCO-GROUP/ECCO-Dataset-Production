
Installation
============

ECCO Dataset Production Run Modes
---------------------------------

Generally speaking, ECCO Dataset Production can be run in one of three
"modes":

    * Local
    * Local/Remote
    * Remote

"Local" implies the code is running on a local machine, with all input
data available locally, and all output datasets written to local
storage.

"Local/Remote" imples the code is running on a local machine, with
some or all of the input and output either read from, or written to,
cloud-based storage (for example `AWS S3
<https://aws.amazon.com/s3>`__).

"Remote" implies a fully cloud-based solution: the code is run on a
manually-provisioned instance such as `AWS EC2
<https://aws.amazon.com/pm/ec2/>`__, or within a managed batch service
such as `AWS Batch <https://aws.amazon.com/batch/>`__ / `Fargate
<https://aws.amazon.com/fargate/>`__, and all input and output are
read from, and written to, cloud-based storage.

Package Installation
--------------------

Depending on run mode, ECCO Dataset Production can be pip-installed
just as with any other Python package (Python virtual environments
recommended). Simply clone the repo, ``cd`` to the package directory
and install::

    $ git clone https://github.com/ECCO-GROUP/ECCO-Dataset-Production.git
    $ cd ECCO-Dataset-Production/production_src
    $ pip install .

If you plan to run any of the test cases in ``./tests``, you'll also
need to clone the `ECCO-V4-Configurations
<https://github.com/ECCO-GROUP/ECCO-v4-Configurations>`__ submodule
which could have been done via the ``--recurse-submodules`` option
during the original ``git clone``, or afterwards, from the repository
top-level with::

    $ git submodule init
    $ git submodule update

In addition to the standard Python interface, the package also exposes
several command-line scripts, two of the most useful being
``edp_create_job_task_list`` for creating ``json``-formatted task
lists, and ``edp_generate_dataproducts`` which uses these task lists
to generate NetCDF output files. Successful installation of the
command-line scripts as part of the package install can be verified by
printing their console help messages::

    $ edp_create_job_task_list --help
    $ edp_generate_dataproducts --help

Cloud Provider Prerequisites
----------------------------

If running in Local/Remote or Remote modes, you'll need to have an AWS
account (an `AWS Free Tier <https://aws.amazon.com/free>`__ account
can be used to run any of the included test cases), and you'll also
need to install the `AWS Command Line Interface
<https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html>`__
(AWS CLI).  Just follow the instructions in both links for account
setup and CLI installation.

Given the myriad of options, determining the best approach for a given
use case is not always clear-cut, but as already suggested two common
solution approaches are to install ECCO Dataset Production on a
manually-provisioned AWS EC2 instance (or instances), and using
containerized solutions with AWS Batch/Fargate for parallel
"serverless" (i.e., automatically provisioned) execution.

For manually-provisioned EC2 instances, package installation is the
same as for a local machine; just clone and ``pip`` install.

For "serverless" solutions, AWS Batch/Fargate pulls Docker executable
images at runtime from the `AWS Elastic Container Registry
<https://aws.amazon.com/ecr/>`__ (ECR).  When choosing this mode
you'll need to be able to build Docker images locally and will thus
need to have Docker, or Docker Desktop `installed
<https://www.docker.com/get-started/>`__.  `Amazon Machine Image
<https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/AMIs.html>`__
(AMI)-based Dockerfiles have been provided in ``./containers/docker``
that can be used to build an executable image for
``edp_generate_dataproducts``, the second, and computationally most
intensive, step in ECCO Dataset Production (the first step, task list
generation, has not been containerized, as it's generally just
performed on a local host or single EC2 instance).

Specifics regarding the above will be presented in subsequent sections
and demonstration problem discussions.
