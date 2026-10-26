import pygrib
import time
import os
import csv
import numpy as np
import calendar


def aggregate_monthly(messages):
    """Average spatial message means by actual valid month, without order assumptions.

    Each message has equal time weight. Missing grid cells are excluded; months
    without observations are NaN. This is not an area-weighted climate statistic.
    """
    year = None
    totals, counts = {}, {}
    for message in messages:
        timestamp = message.validDate
        if year is None:
            year = timestamp.year
        if timestamp.year != year:
            raise ValueError("Each input GRIB file must contain one calendar year")
        variable = message.name
        if variable not in totals:
            totals[variable] = [0.0] * 12
            counts[variable] = [0] * 12
        cells = np.ma.asarray(message.values).compressed()
        cells = cells[np.isfinite(cells)]
        if cells.size:
            month = timestamp.month - 1
            totals[variable][month] += float(np.mean(cells))
            counts[variable][month] += 1
    if year is None:
        raise ValueError("Input GRIB file contains no messages")
    monthly = {
        variable: [total / count if count else float('nan')
                   for total, count in zip(totals[variable], counts[variable])]
        for variable in totals
    }
    return year, list(totals), monthly


class Grib_file:
    def __init__(self, filepath):
        self.path = filepath
        with pygrib.open(filepath) as messages:
            self.rok, self.variables_to_grab1, self.wyniki = aggregate_monthly(messages)


# Funkcja do odczytu calego folderu z plikami GRIB
def load_dictionary(path):
    data_array = []
    lista_zmiennych = []
    year_data1 = {}
    i = 0

    for filename in sorted(os.listdir(path)):

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

                csv_for_each_year_path = os.path.join(path, 'YearDataCSV')
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
    folder_path1 = os.path.join(folder_path, 'YearDataCSV')
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

    csv_summary = os.path.join(folder_path, 'SummaryCSV')
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
