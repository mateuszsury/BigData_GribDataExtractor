from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os



def extract_year_from_filename(filename):
    try:
        # Extract the filename from the full path
        filename_only = os.path.basename(filename)

        # Assuming the filename is in the format "YYYY.csv"
        return int(filename_only.split(".")[0])
    except ValueError:
        # Handle the case where the filename doesn't match the expected pattern
        print(f"Error extracting year from filename: {filename}")
        return None

def process_data_with_pyspark(csv_path):
    spark = SparkSession.builder.appName("WeatherAnalysis").getOrCreate()

    # Load all CSV files from the specified path into a DataFrame
    df = spark.read.csv(f"{csv_path}/*.csv", header=True, inferSchema=True)

    # Extract year from the filename and create a new 'year' column
    df = df.withColumn("year", F.udf(lambda x: extract_year_from_filename(x))(F.input_file_name()))

    # Filter out rows where the 'year' column is None
    df = df.filter(df.year.isNotNull())

    # Collect unique years in ascending order
    years = sorted([str(row.year) for row in df.select("year").distinct().collect()])

    # Group by year and calculate min, max, and average for each variable
    grouped_df = df.groupBy("year").agg(
        *[F.min(df[variable]).alias(f"min_{variable}") for variable in df.columns[1:-1]],
        *[F.max(df[variable]).alias(f"max_{variable}") for variable in df.columns[1:-1]],
        *[F.avg(df[variable]).alias(f"avg_{variable}") for variable in df.columns[1:-1]]
    )

    # Create a DataFrame with the sorted years
    sorted_years_df = spark.createDataFrame([(int(year),) for year in years], ["year"])

    # Join the sorted years DataFrame with the grouped data
    result_df = sorted_years_df.join(grouped_df, ["year"], "left_outer")

    # Convert the Spark DataFrame to Pandas DataFrame for easier CSV export
    pandas_df = result_df.toPandas()

    # Reorder columns to have year, min, max, and avg for each variable
    min_columns = [f"min_{variable}" for variable in df.columns[1:-1]]
    max_columns = [f"max_{variable}" for variable in df.columns[1:-1]]
    avg_columns = [f"avg_{variable}" for variable in df.columns[1:-1]]
    new_order = ["year"] + [item for pair in zip(min_columns, max_columns, avg_columns) for item in pair]

    # Reorder the Pandas DataFrame columns
    pandas_df = pandas_df[new_order]

    # Save the results to a new CSV file
    result_csv_path = "/home/mateusz/Downloads/weather_analysis_results.csv"
    pandas_df.to_csv(result_csv_path, index=False)

    print(f"Results saved to: {result_csv_path}")

import pandas as pd
import matplotlib.pyplot as plt

def generate_plots(csv_path, variables_to_grab, save_dir):
    df = pd.read_csv(csv_path)

    for variable in variables_to_grab:
        plt.figure(figsize=(10, 6))
        plt.plot(df['year'], df[f'min_{variable}'], label=f'Min {variable}')
        plt.plot(df['year'], df[f'max_{variable}'], label=f'Max {variable}')
        plt.plot(df['year'], df[f'avg_{variable}'], label=f'Avg {variable}')

        plt.title(f'{variable} 1941 - 2022')
        plt.xlabel('Year')
        plt.ylabel(variable)
        plt.legend()
        plt.grid(True)

        # Save the plot to the specified directory
        plot_filename = f"{variable}_plot.png"
        plot_filepath = os.path.join(save_dir, plot_filename)
        plt.savefig(plot_filepath)
        plt.close()




# Assuming the 'weather_analysis_results.csv' file is in the same directory as the script
result_csv_path = "/home/mateusz/Downloads/weather_analysis_results.csv"



variables_to_grab1 = ['100 metre U wind component', '100 metre V wind component', '10 metre U wind component', '10 metre V wind component', '2 metre temperature', 'Evaporation', 'High vegetation cover', 'Ice temperature layer 1', 'Lake bottom temperature', 'Lake total depth', 'Low vegetation cover', 'Maximum temperature at 2 metres since previous post-processing', 'Minimum temperature at 2 metres since previous post-processing', 'Precipitation type', 'Skin temperature', 'Snow depth', 'Snow evaporation', 'Snowmelt', 'Soil temperature level 1', 'Surface pressure', 'Temperature of snow layer', 'Total cloud cover', 'Total precipitation', 'Type of high vegetation', 'Type of low vegetation', 'Volumetric soil water layer 1' ]
path = '/home/mateusz/Downloads/BigData'
save_plots_dir = '/home/mateusz/Downloads/Wykresy'
#load_dictionary(path, variables_to_grab1)

process_data_with_pyspark(path)
generate_plots(result_csv_path, variables_to_grab1, save_plots_dir)
