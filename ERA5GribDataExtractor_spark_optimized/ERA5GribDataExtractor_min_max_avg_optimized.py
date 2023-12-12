import pygrib
import time
import os
import csv
import numpy as np
import calendar

class Grib_file:
    variables_to_grab1 = []
    rok = None
    path = None
    wyniki = None

    def __init__(self, filepath, variables_to_grab):
        self.variables_to_grab1 = variables_to_grab
        self.path = filepath
        start = time.time()
        with pygrib.open(self.path) as gribs:
            end = time.time()
            print("Czas otwierania = ", str(time.time() - start))

            # Ilosc wiadomosci w pliku
            num = gribs.messages
            # Odczytanie pierwszej wiadomosci do uzyskania roku
            grb = gribs.readline()
            grb_str = str(grb)
            rok_str = grb_str.split(":")[-1]
            rok = int(rok_str[-12:-8])
            self.rok = rok
            print(rok)

            wiadomosci_na_dzien = 0

            # Ilosc dni w miesiacach
            lista_miesiecy = [31, 0, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]

            # Sprawdzenie czy rok jest przestepny
            if (rok % 4 == 0):
                wiadomosci_na_dzien = int(num / 366)
                lista_miesiecy[1] = 29
            elif (rok % 4 != 0):
                wiadomosci_na_dzien = int(num / 365)
                luty = 28
                lista_miesiecy[1] = 28

            # Inicjalizacja tablic do przechowywania wartosci
            wiadomosci_na_miesiac = [_ for _ in range(len(lista_miesiecy))]
            dane_podzielone_na_mc = [[] for _ in range(len(lista_miesiecy))]
            dane_srednie_podzielone_na_mc = [[] for _ in range(len(lista_miesiecy))]
            lista_wiadomosci = {variable: [] for variable in self.variables_to_grab1}

            # Odczytywanie wartosci z plikow GRIB i wstepne przetworzenie
            for i, miesiac in enumerate(lista_miesiecy):
                wiadomosci_na_miesiac[i] = miesiac * wiadomosci_na_dzien
                for message in range(wiadomosci_na_miesiac[i]):
                    if (i == 0 and message == 0):
                        dane_podzielone_na_mc[i].append(np.mean(grb.values))
                    else:
                        dane_podzielone_na_mc[i].append(np.mean(gribs.readline().values))
                for j, variable in enumerate(self.variables_to_grab1):
                    dane_srednie_podzielone_na_mc[i].append(np.mean(dane_podzielone_na_mc[i][j::26]))
            for x, variable in enumerate(self.variables_to_grab1):
                for miesiac in range(len(lista_miesiecy)):
                    lista_wiadomosci[variable].append(dane_srednie_podzielone_na_mc[miesiac][x])

            self.wyniki = lista_wiadomosci
            del gribs, wiadomosci_na_miesiac, dane_podzielone_na_mc, dane_srednie_podzielone_na_mc, lista_wiadomosci

# Funkcja do odczytu calego folderu z plikami GRIB
def load_dictionary(path, variables_to_grab):
    data_array = []
    i = 0
    for filename in os.listdir(path):

        if filename.endswith('.grib'):
            print("Odczytano: " + str(i) + " plikow")
            file_path = os.path.join(path, filename)
            try:
                grib_instance = Grib_file(file_path, variables_to_grab)
                year_data = {'year': grib_instance.rok, 'variables': {}}

                for variable in variables_to_grab:
                    variable_data = grib_instance.wyniki[variable]
                    year_data['variables'][variable] = variable_data
                print(f'Odczytano plik: {filename}')
                i = i + 1

                data_array.append(year_data)

                # Zapis wynikow z  pliku GRIB do pliku CSV
                csv_filename = f"{grib_instance.rok}.csv"
                csv_filepath = os.path.join(path, csv_filename)

                with open(csv_filepath, 'w', newline='') as csv_file:
                    writer = csv.writer(csv_file)

                    header = ['Month'] + variables_to_grab
                    writer.writerow(header)

                    for month in range(1, 13):
                        month_data = [calendar.month_abbr[month]] + [year_data['variables'][variable][month - 1] for variable in variables_to_grab]
                        writer.writerow(month_data)

                del grib_instance
                del year_data
            except Exception as e:
                print(f'Nie udało się odczytać pliku {filename}: {str(e)}')

    return data_array

# Import bibliotek PySpark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Funkcja odczytujaca rok z pliku CSV
def extract_year_from_filename(filename):
    try:
        filename_only = os.path.basename(filename)

        return int(filename_only.split(".")[0])
    except ValueError:
        print(f"Blad przy odczytywaniu roku: {filename}")
        return None

# Funkcja przetwarzajaca wszystkie utworzone pliki CSV w PySpark
def process_data_with_pyspark(csv_path):
    spark = SparkSession.builder.appName("WeatherAnalysis").getOrCreate()

    df = spark.read.csv(f"{csv_path}/*.csv", header=True, inferSchema=True)
    df = df.withColumn("year", F.udf(lambda x: extract_year_from_filename(x))(F.input_file_name()))
    df = df.filter(df.year.isNotNull())

    # Sortowanie danych i przetwarzanie wartosci
    years = sorted([str(row.year) for row in df.select("year").distinct().collect()])
    grouped_df = df.groupBy("year").agg(
        *[F.min(df[variable]).alias(f"min_{variable}") for variable in df.columns[1:-1]],
        *[F.max(df[variable]).alias(f"max_{variable}") for variable in df.columns[1:-1]],
        *[F.avg(df[variable]).alias(f"avg_{variable}") for variable in df.columns[1:-1]]
    )

    sorted_years_df = spark.createDataFrame([(int(year),) for year in years], ["year"])
    result_df = sorted_years_df.join(grouped_df, ["year"], "left_outer")
    pandas_df = result_df.toPandas()

    min_columns = [f"min_{variable}" for variable in df.columns[1:-1]]
    max_columns = [f"max_{variable}" for variable in df.columns[1:-1]]
    avg_columns = [f"avg_{variable}" for variable in df.columns[1:-1]]
    new_order = ["year"] + [item for pair in zip(min_columns, max_columns, avg_columns) for item in pair]

    pandas_df = pandas_df[new_order]

    # Zapis wynikow do pliku CSV
    pandas_df.to_csv(csv_path, index=False)

    print(f"Wyniki zapisano w sciezce: {csv_path}")

# Import bibliotek do tworzenia wykresow
import pandas as pd
import matplotlib.pyplot as plt

# Funkcja do tworzenia i zapisywania wykresow
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

        plot_filename = f"{variable}_plot.png"
        plot_filepath = os.path.join(save_dir, plot_filename)
        plt.savefig(plot_filepath)
        plt.close()

# Sciezka do zapisu pliku CSV
path = '/home/mateusz/Downloads/BigData'
save_plots_dir = '/home/mateusz/Downloads/Wykresy'
variables_to_grab = ['100 metre U wind component', '100 metre V wind component', '10 metre U wind component', '10 metre V wind component', '2 metre temperature', 'Evaporation', 'High vegetation cover', 'Ice temperature layer 1', 'Lake bottom temperature', 'Lake total depth', 'Low vegetation cover', 'Maximum temperature at 2 metres since previous post-processing', 'Minimum temperature at 2 metres since previous post-processing', 'Precipitation type', 'Skin temperature', 'Snow depth', 'Snow evaporation', 'Snowmelt', 'Soil temperature level 1', 'Surface pressure', 'Temperature of snow layer', 'Total cloud cover', 'Total precipitation', 'Type of high vegetation', 'Type of low vegetation', 'Volumetric soil water layer 1' ]

# Uruchomienie programu
wyniki = load_dictionary(path, variables_to_grab)
process_data_with_pyspark(path)
generate_plots(path, variables_to_grab, save_plots_dir)
