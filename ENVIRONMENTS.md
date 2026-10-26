# Reproducing GRIB processing

This repository contains historical desktop and Spark/Hadoop experiments.
They are not interchangeable benchmark configurations.

Use an isolated Python 3 environment with pygrib/ecCodes, numpy, pandas and
matplotlib for local analysis; the GUI also requires Tk. On Windows, install
pygrib/ecCodes through an environment manager that provides native binaries.
For the distributed variant additionally install a compatible Java/Spark/Hadoop
stack and PySpark matching the Spark cluster version. Desktop pip requirements
do not configure a Hadoop cluster or supply input GRIB data.

The original README measurements used 83 files / 28 GB. Treat them as historical
observations, not current universal performance claims. To compare variants,
record dataset provenance/checksums, variables, missing-value handling, software
versions, executor count, RAM and elapsed time. Do not compare different inputs
as if they were the same benchmark.

The external standalone GUI archive is a legacy download, not a verified GitHub
release. Prefer running the inspectable source; future binary releases should
include the source revision, dependency manifest and SHA-256 checksums.

Original source code: MIT, Mateusz Sury. Preserve data-provider licensing.
