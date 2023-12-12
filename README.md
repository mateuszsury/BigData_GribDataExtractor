# GRIB Data Processing

## Overview

This repository contains two scripts for processing GRIB files, ecpecially designed for ERA5 data, but works with other data also. ERA5 is a global atmospheric reanalysis dataset produced by the European Centre for Medium-Range Weather Forecasts (ECMWF). The scripts are tailored for extracting and analyzing meteorological variables from GRIB files.

### Script 1: ERA5GribDataExtractor_spark_optimized

#### Purpose

This script is optimized for ERA5 GRIB files that contain data for a single year, including at least one hourly data entry for each day. The GRIB file should also contain 26 specific variables. The script utilizes Apache Spark to efficiently calculate the monthly average, minimum, and maximum values for each variable across all files in a given location.

#### Variables

'100 metre U wind component', '100 metre V wind component', '10 metre U wind component', '10 metre V wind component', '2 metre temperature', 'Evaporation', 'High vegetation cover', 'Ice temperature layer 1', 'Lake bottom temperature', 'Lake total depth', 'Low vegetation cover', 'Maximum temperature at 2 metres since previous post-processing', 'Minimum temperature at 2 metres since previous post-processing', 'Precipitation type', 'Skin temperature', 'Snow depth', 'Snow evaporation', 'Snowmelt', 'Soil temperature level 1', 'Surface pressure', 'Temperature of snow layer', 'Total cloud cover', 'Total precipitation', 'Type of high vegetation', 'Type of low vegetation', 'Volumetric soil water layer 1'

#### Prerequisites

- Apache Spark
- GRIB files with the specified structure
- PyGrib

#### Usage

1. Ensure Apache Spark is installed.
2. Run the script with appropriate parameters, specifying the location of ERA5 GRIB files.
3. The script will preprocess the files, perform Spark-based calculations, and generate charts.
4. Execution time for processing 83 files (28GB) is approximately 2.5 hours.

### Script 2: GribDataExtractor_slow

#### Purpose

This universal script is designed to handle GRIB files from various sources, provided they contain data for a single year. It supports extracting both average values and specific values for a given hour and day. However, its drawback is the slower processing speed.

#### Prerequisites

- Python environment
- PyGrib

#### Usage

1. Run the script with appropriate parameters, specifying the location of GRIB files.
2. The script will process the files and output results in the console or as charts.
3. Execution time for processing 83 files (28GB) is approximately 50 hours.

## Results

- For ERA5GribDataExtractor_spark_optimized, the script efficiently processes ERA5 files and generates insightful charts in a relatively short time.
- GribDataExtractor_slow is a universal tool with slower processing but supports files with varying structures.

## Notes

- Ensure proper dependencies are installed before running each script.
- Adjust parameters and paths according to your file locations.
