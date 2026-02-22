
# ECCO Dataset Production utilities

This directory contains a collection of routines and utilities that
have been found to be generally useful in a production setting.


## extract\_first\_mid\_last\_tasks.py

Utility for extracting first, middle, and last tasks from individual,
or collections of, full task lists. Particularly useful for
incremental dataset production testing.  Since this utility can be run
either locally or remotely, with or without cloud-hosted data, it's
also a useful template for developing other flexible, standalone,
dataset production tools.

