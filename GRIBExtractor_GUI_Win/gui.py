import tkinter as tk
from tkinter import filedialog
from tkinter import messagebox
import grib_methods
import os
import pyproj as pyproj

class GribGUI:
    folder_path = None

    def __init__(self, master):
        self.master = master
        self.master.title("GRIB Data Processor")

        # Zwiększenie czcionki dla elementów
        default_font = ("Arial", 12)

        self.path_label = tk.Label(master, text="Path to GRIB files:", font=default_font)
        self.path_label.pack()

        self.path_entry = tk.Entry(master, width=50, font=default_font)
        self.path_entry.pack()

        self.browse_button = tk.Button(master, text="Browse", command=self.browse_path, font=default_font)
        self.browse_button.pack()

        self.process_button = tk.Button(master, text="Process Data", command=self.process_data, font=default_font, height=2)
        self.process_button.pack()

        self.variables_label = tk.Label(master, text="", font=default_font)
        self.variables_label.pack()

        self.variable_checkboxes = []  # Lista checkboxów
        self.variable_selection_window = None  # Dodane okno dialogowe

    def browse_path(self):
        folder_path = filedialog.askdirectory()
        self.path_entry.delete(0, tk.END)
        self.path_entry.insert(0, folder_path)

    def process_data(self):
        self.folder_path = folder_path = self.path_entry.get()

        if os.path.isdir(folder_path):
            try:
                wyniki = grib_methods.load_dictionary(folder_path)
                variables_to_grab = wyniki[1]

                # Okno dialogowe z listą checkboxów
                self.variable_selection_window = tk.Toplevel(self.master)
                self.variable_selection_window.title("Select Variables")

                # Zwiększenie czcionki dla elementów w oknie dialogowym
                dialog_font = ("Arial", 12)

                # Utworzenie checkboxów na podstawie zmiennych
                for variable in variables_to_grab:
                    frame = tk.Frame(self.variable_selection_window)
                    frame.pack(side="top", fill="both", expand=True)

                    var_var = tk.IntVar()
                    var_checkbox = tk.Checkbutton(frame, text=variable, variable=var_var, anchor="w", justify="left", font=dialog_font)
                    var_checkbox.pack(side="left", padx=(0, 10))
                    self.variable_checkboxes.append((variable, var_var))

                # Przycisk do potwierdzenia wyboru
                confirm_button = tk.Button(self.variable_selection_window, text="Confirm", command=self.confirm_selection, font=dialog_font, height=2)
                confirm_button.pack(side="bottom", pady=10)

            except Exception as e:
                messagebox.showerror("Error", f"An error occurred: {str(e)}")
        else:
            messagebox.showerror("Error", "Invalid path. Please select a valid folder.")

    def confirm_selection(self):
        # Utworzenie listy zaznaczonych zmiennych
        selected_variables = [variable for variable, var_var in self.variable_checkboxes if var_var.get()]
        print(selected_variables)
        # Zamknięcie okna dialogowego z listą checkboxów
        self.variable_selection_window.destroy()
        path = os.path.normpath(self.folder_path)
        if selected_variables:
            grib_methods.process_data_with_pandas(path)
            grib_methods.generate_plots(path, selected_variables)
            messagebox.showinfo("Success", "Data processed successfully with selected plots!")
        else:
            messagebox.showinfo("Success", "Data processed successfully!")

if __name__ == "__main__":
    root = tk.Tk()
    app = GribGUI(root)
    root.mainloop()
