# GRIB Data Processing

## Overview

This repository contains three scripts for processing GRIB files, ecpecially designed for ERA5 data, but works with other data also. ERA5 is a global atmospheric reanalysis dataset produced by the European Centre for Medium-Range Weather Forecasts (ECMWF). The scripts are tailored for extracting and analyzing meteorological variables from GRIB files.

### Script 1: GRIBExtractor_GUI_Win

#### Purpose

This newest, most optimazed script with GUI that works on Windows. It is designed for all GRIB files from all datasets that contain data one year data for each day of this year (example: 2 metre temperature for each day of 2022). The script utilizes Numpy and Pandas to efficiently calculate the monthly average, minimum, and maximum values for each variable across all files in a given location and Matplotlib to generate graphs of changes in these values over the years. The program has been developed in two versions, one is a python script and the other is a standalone application that does not require a special environment.

#### Prerequisites (python script version)

- Anaconda
- PyGrib
- Numpy
- Pandas
- Matplotlib
- Windows

#### Prerequisites (stand-alone version)

- Windows


#### Standalone version relase

Because standalone version file is too big for Github it could be downloaded from link below.
Link: https://www.mediafire.com/file/7o5vj4i0llfk1j1/gui.7z/file

#### Usage

1. Run program.
2. Specifying the location of GRIB files.

![Zrzut ekranu 2023-12-28 022430](https://github.com/mateuszsury/BigData_GribDataExtractor/assets/66475105/181a70d6-1da8-4c0e-a609-10fd512d471c)

3. The script will preprocess the files, it will take a while (execution time for processing 83 files (28GB) is approximately 2.5 hours)
  3.1. A CSV file will be generated for each file containing the average value of each variable for each month.

![Zrzut ekranu 2023-12-28 022605](https://github.com/mateuszsury/BigData_GribDataExtractor/assets/66475105/93910d27-ce85-461b-82a1-620371447711)

4. Once processing is complete, a CSV file will be generated containing the average values of all variables in all processed years.

![Zrzut ekranu 2023-12-28 022748](https://github.com/mateuszsury/BigData_GribDataExtractor/assets/66475105/1d6b6183-b0df-405b-aed8-6f190c2604ac)

5. Select which variables you want to generate graphs of.

![Zrzut ekranu 2023-12-28 023401](https://github.com/mateuszsury/BigData_GribDataExtractor/assets/66475105/09b8180e-8427-4b53-b292-22e5b0be610f)

6. Results

![2 metre temperature_trends](https://github.com/mateuszsury/BigData_GribDataExtractor/assets/66475105/7927d869-67a3-45f2-89fc-ae5a4c005891)



### Script 2: Grib_data_extractor_using_spark&hadoop

#### Purpose

This script is optimized for GRIB files uploaded to HDFS that contain data for a single year, including at least one hourly data entry for each day. The script utilizes Apache Spark to efficiently calculate the monthly average, minimum, and maximum values for each variable across all files in a given location. For security results are also beeing saved to HDFS.


#### Prerequisites

- Apache Spark
- Hadoop
- GRIB files on the HDFS
- PyGrib
- Linux

#### Usage

1. Ensure Apache Spark and Hadoop is installed.
2. Run the script with appropriate parameters, specifying the location of Hadoop bin folder, HDFS path and path to save results.
3. The script will preprocess the files, perform Spark-based calculations, and generate charts.
4. Execution time for processing 83 files (28GB) on 3 VM with 4 cores and 8G of RAM is approximately 40 minutes.

### Script 3: GribDataExtractor_slow

#### Purpose

This universal script is designed to handle GRIB files from various sources, provided they contain data for a single year. It supports extracting both average values and specific values for a given hour and day. However, its drawback is the slower processing speed.

#### Prerequisites

- Python environment
- PyGrib
- Linux

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

## Example result
![2 metre temperature_plot](https://github.com/mateuszsury/BigData_GribDataExtractor/assets/66475105/cd14c966-7c5b-48df-aaad-9bb083b9b9e7)

