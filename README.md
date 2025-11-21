
# ECCO Dataset Production

ECCO Dataset Production is a toolset that supports NASA's [Open
Science](https://science.nasa.gov/open-science/) initiative by making
[ECCO's](https://ecco-group.org/) multidecadal, physically- and
statistically-consistent ocean state estimates available in
[NetCDF](https://www.unidata.ucar.edu/software/netcdf) format.

In so doing, it transforms raw
[MITgcm](https://mitgcm.readthedocs.io)-generated results into ordered
collections of date- and time-stamped files, in native and lon/lat
grid formats, for wide use by the broader scientific research
community.

ECCO Dataset Production can run either locally or in the cloud, the
latter mode in regular use by the ECCO group to generate the
multi-terabyte datasets available through the Physical Oceanography
Distributed Active Archive Center
([PO.DAAC](https://podaac.jpl.nasa.gov/)) and NASA's Earthdata [ESDIS
Project](https://www.earthdata.nasa.gov/about/esdis).

See readthedocs.io for more information.


## Project Dependencies

In addition to MITgcm, much of the core computation in ECCO Dataset
Production is provided by [xmitgcm](https://xmitgcm.readthedocs.io),
[ECCOv4-py](https://github.com/ECCO-GROUP/ECCOv4-py), and the cloud
utilities package from
[ECCO-ACCESS](https://github.com/ECCO-GROUP/ECCO-ACCESS).

To this, ECCO Dataset Production adds workflow automation, packaging,
and utilities suitable for both local (i.e. custom dataset) and
cloud-based (i.e., multi-terabyte) production and distribution.


## Installation and Usage

ECCO Dataset Production can be pip-installed as with any other Python
package. Just clone the repo, `cd` to the package directory and
install:

    $ git clone https://github.com/ECCO-GROUP/ECCO-Dataset-Production.git
	$ cd ECCO-Dataset-Production/production_src
    $ pip install .

ECCO Dataset Production also exposes several command-line scripts, two
of the more important ones being `edp_create_job_task_list` for
creating a `json`-formatted explicit list of NetCDF files that are to
be produced, and `edp_generate_dataproducts` that then reads this task
list and generates the resulting files. Command-line help is available
via:

	$ edp_create_job_task_list --help
	$ edp_generate_dataproducts --help

Test/demonstration examples illustrating dataset production in local
and cloud-based modes are in `./tests`, with further discussion in
readthedocs.io.  In order to run the test cases, you'll need to
install the ECCO-v4-Configurations submodule:

    # from ECCO-Dataset-Production/production_src:
    $ git submodule init
    $ git submodule update

Thereafter, `./tests/native_latlon_local` provides a good "getting started" example
illustrating how to generate local NetCDF files from local input files.


## History

Initial dataset production iterations were the work of [Ian
Fenty](https://science.jpl.nasa.gov/people/ifenty/), with subsequent
prototype [AWS Lambda](https://aws.amazon.com/lambda/) cloud
deployment by Ian Fenty and [Duncan
Bark](https://lasp.colorado.edu/people/duncan-bark/).  The current
package is a significant update that includes production tools and
scaling for [AWS Batch](https://aws.amazon.com/batch/)-based cloud
deployment, and has been implemented by Ian Fenty and Greg Moore
(<greg.moore@jpl.nasa.gov>).  Release documentation generation tools
are the work of Jose Gonzales and [Odilon
Houndegnonto](https://science.jpl.nasa.gov/people/houndegn/).


## Contributing

Contributions and use case examples are always welcome. Please feel
free to fork this repo and issue a pull request or
[contact](https://ecco-group.org/contact.htm) the ECCO Group.
