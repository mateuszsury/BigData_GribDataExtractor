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

    def __init__(self, filepath):
        self.variables_to_grab1 = None
        self.path = filepath
        start = time.time()
        with pygrib.open(self.path) as gribs:
            end = time.time()
            print("Opening time = ", str(time.time() - start))

            # Ilosc wiadomosci w pliku
            num = gribs.messages
            # Odczytanie pierwszych wiadomosci w celu uzyskania roku oraz ilosci i nazw zmiennych
            first_messages = []
            first_messages_names = []
            ilosc_zmiennych = 0
            element = None
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
            print(rok)

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


# Funkcja do odczytu calego folderu z plikami GRIB
def load_dictionary(path):
    data_array = []
    i = 0

    for filename in os.listdir(path):

        if filename.endswith('.grib'):
            if(i == 0):
                print("Processing started")
            else:
                print("Processed: " + str(i) + " files")
            file_path = os.path.join(path, filename)
            try:
                grib_instance = Grib_file(file_path)
                year_data = {'year': grib_instance.rok, 'variables': {}}
                year_data1 = {'year': grib_instance.rok}
                year_data1[grib_instance.rok] = grib_instance.wyniki
                variables_to_grab = grib_instance.variables_to_grab1
                if (i == 0):
                    lista_zmiennych = variables_to_grab
                for variable in variables_to_grab:
                    variable_data = grib_instance.wyniki[variable]
                    year_data['variables'][variable] = variable_data
                print(f'Processed file: {filename}')
                i = i + 1
                data_array.append(year_data)

                # Zapis wynikow z  pliku GRIB do pliku CSV

                csv_for_each_year_path = rf'{path}\YearDataCSV'
                if not os.path.exists(csv_for_each_year_path):
                    os.makedirs(csv_for_each_year_path)

                csv_filename = f"{grib_instance.rok}.csv"
                csv_filepath = os.path.join(csv_for_each_year_path, csv_filename)

                with open(csv_filepath, 'w', newline='') as csv_file:
                    writer = csv.writer(csv_file)

                    header = ['Month'] + variables_to_grab
                    writer.writerow(header)

                    for month in range(1, 13):
                        month_data = [calendar.month_abbr[month]] + [year_data['variables'][variable][month - 1] for
                                                                     variable in variables_to_grab]
                        writer.writerow(month_data)

                del grib_instance
                del year_data
            except Exception as e:
                print(f'Error with reading file: {filename}: {str(e)}')

    return data_array, lista_zmiennych, year_data1

import pandas as pd
def process_data_with_pandas(folder_path):
    all_data = []
    folder_path1 = rf'{folder_path}\YearDataCSV'
    for filename in os.listdir(folder_path1):
        if filename.endswith('.csv') and not filename.startswith('summary_data'):
            file_path = os.path.join(folder_path1, filename)
            df = pd.read_csv(file_path, index_col=0)
            year = int(filename.split('.')[0])

            # Obliczenia dla każdej zmiennej
            result_data = {'Year': year}
            for variable in df.columns[0:]:
                result_data[f'{variable}_min'] = df[variable].min()
                result_data[f'{variable}_max'] = df[variable].max()
                result_data[f'{variable}_avg'] = df[variable].mean()

            all_data.append(result_data)

    # Sortowanie danych po roku
    all_data = sorted(all_data, key=lambda x: x['Year'])

    csv_summary = rf'{folder_path}\SummaryCSV'
    if not os.path.exists(csv_summary):
        os.makedirs(csv_summary)

    # Zapis do nowego pliku CSV
    output_file_path = os.path.join(csv_summary, 'summary_data.csv')
    with open(output_file_path, 'w', newline='') as csvfile:
        fieldnames = ['Year'] + [f'{variable}_{stat}' for variable in df.columns[0:] for stat in ['min', 'max', 'avg']]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for row in all_data:
            writer.writerow(row)


# Import bibliotek do tworzenia wykresow
import matplotlib.pyplot as plt

def generate_plots(path1, variables_to_grab):
    # Wczytanie danych z pliku CSV
    path2 = os.path.join(path1, 'SummaryCSV', 'summary_data.csv')
    df = pd.read_csv(path2)

    for variable in variables_to_grab:
        plt.figure(figsize=(10, 6))

        # Handle dynamic column names
        min_col = f'{variable}_min'
        max_col = f'{variable}_max'
        avg_col = f'{variable}_avg'

        # Check if the columns exist in the DataFrame
        if min_col in df.columns and max_col in df.columns and avg_col in df.columns:
            plt.plot(df['Year'], df[min_col], label=f'{variable}_min')
            plt.plot(df['Year'], df[max_col], label=f'{variable}_max')
            plt.plot(df['Year'], df[avg_col], label=f'{variable}_avg')
            plt.title(f'{variable} Trends Over the Years')
            plt.xlabel('Year')
            plt.ylabel('Value')
            plt.legend()
            plt.grid(True)

            plots_path = os.path.join(path1, 'Plots')
            if not os.path.exists(plots_path):
                os.makedirs(plots_path)

            # Zapisywanie wykresów do folderu
            plot_filename = f'{variable}_trends.png'
            plot_filepath = os.path.join(plots_path, plot_filename)
            plt.savefig(plot_filepath)
            plt.close()
        else:
            print(f"Error with finding data for: {variable}")