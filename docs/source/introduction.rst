
Introduction
============

Background and Motivation
-------------------------

Development of the ECCO Dataset Production toolset has been driven by
two primary factors: the first has been the need for a scalable
compute infrastructure that can handle current, and future anticipated
workloads at terabyte and petabyte levels, while the second has been
the desire for quick turnaround times, so that updated ECCO ocean
state estimates can be made available to the global earth science
community as soon as they are generated.

As described `elsewhere <https://ecco-group.org/home.htm>`__, ECCO
state estimates are computed with respect to a discretized
`latitude-longitude-cap
<https://ecco-group.org/docs/v4r4_user_guide.pdf>`__ (LLC) grid, for
both 2D and 3D results sets, at regular time intervals, and are output
in internal `MITgcm file formats
<https://mitgcm.readthedocs.io/en/latest/>`__, one response variable
(sea surface height, temperature, pressure, etc.)  and time step per
file.

For general distribution however, it's far more convenient to package
these state estimates using portable, self-describing `NetCDF
<https://www.unidata.ucar.edu/software/netcdf>`__ formats.  For ECCO,
each output file represents a logical grouping of results data (for
example, dynamic sea surface height, and associated barometric
correction) at either the original LLC ("native") grid or an
interpolated longitude/latitude grid, and at either instantaneous,
daily-, or monthly-averaged time values. So while the operations are
relatively straightforward, the processing load isn't, hence the
emphasis on cloud compute infrastructure.

A Note on Cloud Infrastructure
------------------------------

At this time, and primarily for reasons having to do with the
institutional resources available to the package developers, `Amazon
Web Services <https://aws.amazon.com/>`__ has been the chosen cloud
provider, hence the AWS-centric implementation, discussion and
examples. Though it is hoped that subsequent ECCO Dataset Production
releases will implement Infrastructure as Code (IaC) services that can
support other cloud platforms, cloud-oriented aspects of the
discussion will, for now, simply attempt to be as illustrative, and
generally descriptive, as possible.

General Production Workflow
---------------------------

Whether running locally or in a cloud-hosted mode, the ECCO Dataset
Production processing chain consists primarily of two steps:

    #. Task list generation
    #. Dataset production based on generated task list(s) 

In the first step, a list of datasets that are to be produced, along
with their input requirements, is assembled and saved in
``json``-formatted file(s). This list is determined based on a scan of
available MITgcm/ECCO results files, and the resulting "recipes"
simplify dataset generation by providing compact, explicit task
descriptions that can be easily distributed, containerized, and used
for job execution error checking.

The second step, dataset production, walks through the tasks, or
"recipes" in a task list (or task lists) and, for each, fetches the
input MITgcm/ECCO results files from either local disk or cloud
storage, applies dataset-specific rules (regridding, masking, writing
metadata, etc.), and writes the resulting granules to either local, or
cloud storage.

The following sections discuss installation, configuration, and
implementation of this two step workflow in both local and
cloud-hosted modes.

