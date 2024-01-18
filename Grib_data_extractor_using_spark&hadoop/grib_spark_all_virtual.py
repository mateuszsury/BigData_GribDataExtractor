from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import pygrib
import io
import tempfile
import time
import os
import csv
import numpy as np
import calendar
import pandas as pd
import matplotlib.pyplot as plt
import warnings



def warnings_filter():

    warnings.simplefilter(action='ignore', category=FutureWarning)


# Metoda do zaladowania plikow GRIB z HDFS do Spark
def spark_grib_processing(hdfs_path, local_directory):
    # Inicjalizacja sesji Spark
    spark = SparkSession.builder.appName("ReadGRIB").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # Dodaj pliki GRIB do SparkContext
    print("Adding GRIB files to a Spark context")
    grib_files = spark.sparkContext.binaryFiles(hdfs_path + "*.grib")

    # Otwarcie każdego pliku GRIB i zwrócenie zawartości jako tuple
    print("Processing GRIB files started...")
    grib_data = grib_files.flatMap(lambda x: open_grib_file(io.BytesIO(x[1]), local_directory))
    dane = grib_data.collect()
    print("Processing GRIB files finished")
    print()
    spark.stop()
    return dane

# Metoda otwierajaca pliki GRIB, pobierajaca wyniki z klasy Grib_files, zapisujaca do CSV
def open_grib_file(file_path, local_directory):

    # Utworzenie pliku tymczasowego z pliku GRIB
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_file.write(file_path.read())
        filename = temp_file.name

    # Odczyt wartosci z pliku GRIB
    try:
        grib_instance = Grib_file(filename)
        year_data = {'year': grib_instance.rok, 'variables': {}}
        variables_to_grab = grib_instance.variables_to_grab1

        for variable in variables_to_grab:
            variable_data = grib_instance.wyniki[variable]
            year_data['variables'][variable] = variable_data

        del grib_instance
        os.remove(filename)

    except Exception as e:

        print(f'Nie udało się odczytać pliku {filename}: {str(e)}')

    return variables_to_grab, year_data

class Grib_file:
    variables_to_grab1 = []
    rok = None
    path = None
    wyniki = None

    def __init__(self, filepath):
        self.variables_to_grab1 = None
        self.path = filepath

        with pygrib.open(self.path) as gribs:

            # Ilosc wiadomosci w pliku
            num = gribs.messages
            # Odczytanie pierwszych wiadomosci w celu uzyskania roku oraz ilosci i nazw zmiennych
            first_messages = []
            first_messages_names = []
            ilosc_zmiennych = 0

            while (True):
                first_messages.append(gribs.readline())
                element = str(first_messages[ilosc_zmiennych]).split(":")[1]
                if element not in first_messages_names:
                    first_messages_names.append(element)
                    ilosc_zmiennych += 1
                else:
                    break

            rok = int((str(first_messages[0]).split(":")[-1])[-12:-8])
            self.rok = rok
            self.variables_to_grab1 = first_messages_names
            wiadomosci_na_dzien = 0

            # Ilosc dni w miesiacach
            lista_miesiecy = [31, 0, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]

            # Sprawdzenie czy rok jest przestepny
            if (rok % 4 == 0):
                wiadomosci_na_dzien = int(num / 366)
                lista_miesiecy[1] = 29
            elif (rok % 4 != 0):
                wiadomosci_na_dzien = int(num / 365)
                lista_miesiecy[1] = 28

            # Inicjalizacja tablic do przechowywania wartosci
            dane_podzielone_na_mc = [[] for _ in range(len(lista_miesiecy))]
            dane_srednie_podzielone_na_mc = [[] for _ in range(len(lista_miesiecy))]
            lista_wiadomosci = {variable: [] for variable in self.variables_to_grab1}

            # Odczytywanie wartosci z plikow GRIB i wstepne przetworzenie
            for i, miesiac in enumerate(lista_miesiecy):
                wiadomosci_na_miesiac = miesiac * wiadomosci_na_dzien
                for message in range(wiadomosci_na_miesiac):
                    if (i == 0 and message <= ilosc_zmiennych):
                        dane_podzielone_na_mc[i].append(np.mean(first_messages[i].values))
                    else:
                        dane_podzielone_na_mc[i].append(np.mean(gribs.readline().values))

                for j, variable in enumerate(self.variables_to_grab1):
                    dane_srednie_podzielone_na_mc[i].append(np.mean(dane_podzielone_na_mc[i][j::ilosc_zmiennych]))
            for x, variable in enumerate(self.variables_to_grab1):
                for miesiac in range(len(lista_miesiecy)):
                    lista_wiadomosci[variable].append(dane_srednie_podzielone_na_mc[miesiac][x])

            self.wyniki = lista_wiadomosci
            del gribs, wiadomosci_na_miesiac, dane_podzielone_na_mc, dane_srednie_podzielone_na_mc, lista_wiadomosci

def zapis_do_CSV(dane, local_directory):
    print("Saving files to CSV...")
    variables_to_grab = dane[0]
    years_data = dane[1::2]
    virtual_csv_content = {}

    for year, year_data in enumerate(years_data):

        csv_filename = f"{year_data['year']}.csv"
        local_csv_filepath = os.path.join(local_directory, csv_filename)
        with open(local_csv_filepath, 'w', newline='') as csv_file:
            writer = csv.writer(csv_file)
            header = ['Month'] + variables_to_grab
            writer.writerow(header)
            for month in range(1, 13):
                month_data = [calendar.month_abbr[month]] + [year_data['variables'][variable][month - 1] for variable in variables_to_grab]
                writer.writerow(month_data)

        virtual_csv_filename = f"{year_data['year']}.csv"
        virtual_csv_content[virtual_csv_filename] = io.StringIO()
        virtual_csv_writer = csv.writer(virtual_csv_content[virtual_csv_filename])
        header = ['Month'] + variables_to_grab
        virtual_csv_writer.writerow(header)
        for month in range(1, 13):
            month_data = [calendar.month_abbr[month]] + [year_data['variables'][variable][month - 1] for variable in variables_to_grab]
            virtual_csv_writer.writerow(month_data)
        print(f"File created: {csv_filename}")
    print()
    return virtual_csv_content


def process_virtual_data_with_pyspark(virtual_csv_content):
    print("Processing CSV files...")
    spark = SparkSession.builder.appName("VirtualDataAnalysis").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    dfs = []

    for virtual_csv_filename, virtual_csv_content in virtual_csv_content.items():
        virtual_csv_content.seek(0)
        df = pd.read_csv(virtual_csv_content)
        df['year'] = extract_year_from_filename(virtual_csv_filename)
        dfs.append(df)

    combined_df = pd.concat(dfs, ignore_index=True)

    spark_df = spark.createDataFrame(combined_df)

    spark_df = spark_df.withColumn("year", F.col("year").cast("int"))

    years = sorted([str(row.year) for row in spark_df.select("year").distinct().collect()])

    grouped_df = spark_df.groupBy("year").agg(
        *[F.min(spark_df[variable]).alias(f"min_{variable}") for variable in spark_df.columns[1:-1]],
        *[F.max(spark_df[variable]).alias(f"max_{variable}") for variable in spark_df.columns[1:-1]],
        *[F.avg(spark_df[variable]).alias(f"avg_{variable}") for variable in spark_df.columns[1:-1]]
    )

    sorted_years_df = spark.createDataFrame([(int(year),) for year in years], ["year"])
    result_df = sorted_years_df.join(grouped_df, ["year"], "left_outer")

    min_columns = [f"min_{variable}" for variable in spark_df.columns[1:-1]]
    max_columns = [f"max_{variable}" for variable in spark_df.columns[1:-1]]
    avg_columns = [f"avg_{variable}" for variable in spark_df.columns[1:-1]]
    new_order = ["year"] + [item for pair in zip(min_columns, max_columns, avg_columns) for item in pair]

    result_df = result_df.select(new_order)

    # Zapis wynikow do pliku CSV
    csv_filename = "output.csv"

    summary_csv = os.path.join(results_path, "SummaryCSV")
    if not os.path.exists(summary_csv):
        os.makedirs(summary_csv)

    csv_filepath = os.path.join(summary_csv, csv_filename)
    result_df.toPandas().to_csv(csv_filepath, index=False)

    print(f"Results saved to: {csv_filepath}")
    print()
    spark.stop()

def extract_year_from_filename(filename):
    try:
        filename_only = os.path.basename(filename)

        return int(filename_only.split(".")[0])
    except ValueError:
        print(f"Failed to read file: {filename}")
        return None


def save_csv_to_hdfs(local_csv_filepath, hdfs_directory, hadoop_path):
    print("Saving files to HDFS...")
    spark = SparkSession.builder \
        .appName("SaveCSVToHDFS") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
        .getOrCreate()

    for filename in os.listdir(local_csv_filepath):
        if filename.endswith('.csv'):
            local_file_path = os.path.join(local_csv_filepath, filename)
            with open(local_file_path, 'r') as local_file:
                csv_content = local_file.read()
                hdfs_csv_filepath = str(os.path.join(hdfs_directory, filename))
                hdfs_remove = hadoop_path + " fs -rm -r " + hdfs_csv_filepath
                os.system(hdfs_remove)
                spark.sparkContext.parallelize([csv_content], 1).saveAsTextFile(hdfs_csv_filepath)
                print(f'Saved file to HDFS: {filename}')

        else:
            print(f"Ignoring non-CSV file: {filename}")

    summary_csv = os.path.join(local_csv_filepath, "SummaryCSV/output.csv")
    if os.path.exists(summary_csv):
        with open(summary_csv, 'r') as local_file:
            csv_content = local_file.read()
            hdfs_csv_filepath = str(os.path.join(hdfs_directory, "output.csv"))
            hdfs_remove = hadoop_path + " fs -rm -r " + hdfs_csv_filepath
            os.system(hdfs_remove)
            spark.sparkContext.parallelize([csv_content], 1).saveAsTextFile(hdfs_csv_filepath)
            print(f'Saved file to HDFS: output.csv')

    print("Saving to HDFS finished.")
    print()
    spark.stop()


# Funkcja do tworzenia i zapisywania wykresow
def generate_plots(csv_path, variables_to_grab):
    print("Generating plots...")
    df = pd.read_csv(os.path.join(csv_path + "SummaryCSV/", "output.csv"))

    wykresy_folder = os.path.join(csv_path, "Wykresy")
    if not os.path.exists(wykresy_folder):
        os.makedirs(wykresy_folder)

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

        plot_filename = f"{variable}_plot.png"
        plot_filepath = os.path.join(wykresy_folder, plot_filename)
        plt.savefig(plot_filepath)
        plt.close()
    print(f"Plots saved to: {wykresy_folder}")
    print()



hadoop_path = "/usr/local/hadoop/bin/hadoop"
hdfs_filepath = "hdfs://mateusz:9000/user/mateusz/"
results_path = '/home/mateusz/Downloads/Wyniki/'

warnings_filter()
dane = spark_grib_processing(hdfs_filepath, results_path)
process_virtual_data_with_pyspark(zapis_do_CSV(dane, results_path))
save_csv_to_hdfs(results_path, hdfs_filepath, hadoop_path)
generate_plots(results_path, dane[0])


