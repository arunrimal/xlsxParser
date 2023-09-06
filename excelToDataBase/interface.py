import os
import sys
import json
import time
import tkinter as tk
import tkinter.ttk as ttk
from tkinter import filedialog
from fileProcess import DataProcessor
from ttkwidgets import CheckboxTreeview
from databaseService import DatabaseConnector


class InformationCollectionWindow:
    def __init__(self):
        print(" Inside _init__ .............")
        self.global_file_list = []
        self.global_database_config = {}
        self.config_window = None
        self.directory_frame = None

        # Read the config.json file
        # self.config_file_path = os.path.join(
        #     os.path.dirname(__file__), 'config', 'config.json')
        # Check if running as a binary
        if getattr(sys, 'frozen', False):
            binary_path = sys.executable
            binary_dir = os.path.dirname(binary_path)
        else:
            # Running as a Python script
            binary_dir = os.path.dirname(os.path.abspath(__file__))

        self.config_file_path = os.path.join(
            binary_dir, 'config', 'config.json')

        print(self.config_file_path)
        with open(self.config_file_path, 'r') as json_file:
            config_data = json.load(json_file)

        # Extract the values from the config_data dictionary
        self.filepath = config_data['FILEPATH']
        self.mssql_server = config_data['MSSQLCredential']['mssql_server']
        self.mssql_port = config_data['MSSQLCredential']['mssql_port']
        self.mssql_database = config_data['MSSQLCredential']['mssql_database']
        self.mssql_username = config_data['MSSQLCredential']['mssql_username']
        self.mssql_password = config_data['MSSQLCredential']['mssql_password']
        self.mssql_schema = config_data['MSSQLCredential']['mssql_schema']
        self.destination_table = config_data['MSSQLCredential']['destination_table']
        self.destination_total_table = config_data['MSSQLCredential']['destination_total_table']

        self.window = tk.Tk()
        self.window.title("Information Collection Window")

        self.directory_selection_frame = ttk.Frame(self.window)
        self.directory_selection_frame.pack(padx=10, pady=10)

        self.create_directory_frame()
        self.create_treeview_frame()
        self.create_config_frame()
        self.create_button_frame()

        # self.populate_directory()
        # populate directory variable is to get last entered directory in the directory entry field
        populated_directory = self.directory_entry.get()
        # pulate directory function takes the pupulate directory variable to pupulate the xlsx files in the tree view
        self.populate_directory(populated_directory)

        self.window.mainloop()

    def create_directory_frame(self):
        print(" Inside create_directory_frame .............")
        if self.directory_frame is None:
            self.directory_frame = ttk.Frame(self.directory_selection_frame)
            self.directory_frame.pack(pady=10)

            self.directory_label = ttk.Label(
                self.directory_frame, text="Directory Path:")
            self.directory_label.grid(row=0, column=0, padx=10)

            self.directory_entry = ttk.Entry(self.directory_frame, width=50)
            self.directory_entry.grid(row=0, column=1, padx=10)

            # Populate the values into the fields
            self.directory_entry.insert(0, self.filepath)

            directory_button = ttk.Button(
                self.directory_frame, text="Select Directory", command=self.collect_directory_path)
            directory_button.grid(row=0, column=2, padx=10)

    def collect_directory_path(self):
        print(" Inside collect_directory_path .............")
        base_directory = filedialog.askdirectory()
        if base_directory:
            self.directory_entry.delete(0, 'end')
            self.directory_entry.insert(0, base_directory)
            self.global_file_list = self.populate_treeview(base_directory)
            self.getBaseDirectory = self.directory_entry.get()

    def populate_directory(self, populated_directory):
        self.directory_entry.delete(0, 'end')
        self.directory_entry.insert(0, populated_directory)
        self.global_file_list = self.populate_treeview(populated_directory)
        self.getBaseDirectory = self.directory_entry.get()

    def create_treeview_frame(self):
        print(" Inside create_treeview_frame .............")
        self.treeview_frame = ttk.Frame(self.directory_selection_frame)
        self.treeview_frame.pack(padx=10, pady=10)

        self.treeview_01 = ttk.Treeview(
            self.treeview_frame, columns=["FileName"])
        self.treeview_01.column("#0", minwidth=1, width=10)
        self.treeview_01.column("#1", minwidth=1, width=400)
        self.treeview_01.heading('#0', text=" ", anchor='w')
        self.treeview_01.heading('#1', text="File Name", anchor='w')
        self.treeview_01.grid(row=0, column=0, sticky="nsew")

        treeview_vscrollbar = ttk.Scrollbar(
            self.treeview_frame, orient="vertical", command=self.treeview_01.yview)
        treeview_vscrollbar.grid(row=0, column=1, sticky="ns")

        treeview_hscrollbar = ttk.Scrollbar(
            self.treeview_frame, orient="horizontal", command=self.treeview_01.xview)
        treeview_hscrollbar.grid(row=2, column=0, sticky="ew")

        self.treeview_01.configure(
            yscrollcommand=treeview_vscrollbar.set, xscrollcommand=treeview_hscrollbar.set)

        self.treeview_frame.rowconfigure(0, weight=1)
        self.treeview_frame.columnconfigure(0, weight=1)
        self.treeview_frame.columnconfigure(1, weight=1)

    def create_config_frame(self):
        print(" Inside create_config_frame .............")
        self.config_frame = ttk.Frame(self.directory_selection_frame)
        self.config_frame.pack(pady=10)

        config_button = ttk.Button(
            self.config_frame, text="Configure Database", command=self.open_config_window)
        config_button.grid(row=10, column=0, columnspan=2)

    def create_button_frame(self):
        print(" Inside create_button_frame .............")
        button_frame = ttk.Frame(self.directory_selection_frame)
        button_frame.pack(pady=10)

        ok_button = ttk.Button(button_frame, text="Load",
                               command=self.main_window_ok)
        ok_button.grid(row=0, column=0, padx=5)

        cancel_button = ttk.Button(
            button_frame, text="Cancel", command=self.cancel)
        cancel_button.grid(row=0, column=1, padx=5)

    def populate_treeview(self, directory):
        print(" Inside populate_treeview .............")
        self.treeview_01.delete(*self.treeview_01.get_children())
        file_path_list = []
        for root, _, files in os.walk(directory):
            for file in files:
                if file.endswith(".xlsx"):
                    file_path = os.path.join(root, file)
                    file_path_list.append(file_path)
                    relative_path = os.path.relpath(file_path, directory)
                    self.treeview_01.insert('', 'end', values=(relative_path,))
        return file_path_list

    def handle_config_window_close(self):
        self.config_window.destroy()
        self.config_window = None

    def cancel_config_window(self):
        self.config_window.destroy()
        self.config_window = None

    def open_config_window(self):
        print(" Inside open_config_window .............")
        # config_window = self.config_window
        if self.config_window is None:

            self.config_window = tk.Toplevel(self.directory_selection_frame)
            self.config_window.title("Database Configuration")

            # Make the config_window transient for self.window
            self.config_window.transient(self.window)

            config_frame = ttk.Frame(self.config_window)
            config_frame.pack(padx=20, pady=10)

            # Create labels and entry fields for the database configuration
            server_label = ttk.Label(config_frame, text="Server:")
            server_label.grid(row=0, column=0, sticky="e")
            server_entry = ttk.Entry(config_frame, width=30)
            server_entry.grid(row=0, column=1, sticky="w")

            port_label = ttk.Label(config_frame, text="Port:")
            port_label.grid(row=1, column=0, sticky="e")
            port_entry = ttk.Entry(config_frame, width=30)
            port_entry.grid(row=1, column=1, sticky="w")

            database_label = ttk.Label(config_frame, text="Database:")
            database_label.grid(row=2, column=0, sticky="e")
            database_entry = ttk.Entry(config_frame, width=30)
            database_entry.grid(row=2, column=1, sticky="w")

            username_label = ttk.Label(config_frame, text="Username:")
            username_label.grid(row=3, column=0, sticky="e")
            username_entry = ttk.Entry(config_frame, width=30)
            username_entry.grid(row=3, column=1, sticky="w")

            password_label = ttk.Label(config_frame, text="Password:")
            password_label.grid(row=4, column=0, sticky="e")
            password_entry = ttk.Entry(config_frame, width=30, show="*")
            password_entry.grid(row=4, column=1, sticky="w")

            # Create a Checkbutton for showing/hiding password
            show_password_var = tk.BooleanVar()

            def toggle_password_visibility():
                if show_password_var.get():
                    password_entry.config(show="")
                else:
                    password_entry.config(show="*")

            show_password_checkbox = ttk.Checkbutton(
                config_frame,
                text="Show Password",
                variable=show_password_var,
                command=toggle_password_visibility
            )
            show_password_checkbox.grid(row=5, column=1, sticky="w")

            schema_label = ttk.Label(config_frame, text="Schema:")
            schema_label.grid(row=6, column=0, sticky="e")
            schema_entry = ttk.Entry(config_frame, width=30)
            schema_entry.grid(row=6, column=1, sticky="w")

            destination_table_label = ttk.Label(
                config_frame, text="Destination Table:")
            destination_table_label.grid(row=7, column=0, sticky="e")
            destination_table_entry = ttk.Entry(config_frame, width=30)
            destination_table_entry.grid(row=7, column=1, sticky="w")

            destination_total_table_label = ttk.Label(
                config_frame, text="Destination Total Table:")
            destination_total_table_label.grid(row=8, column=0, sticky="e")
            destination_total_table_entry = ttk.Entry(config_frame, width=30)
            destination_total_table_entry.grid(row=8, column=1, sticky="w")

            # Create a combobox for the operation type
            operation_type_label = ttk.Label(
                config_frame, text="Operation Type:")
            operation_type_label.grid(row=9, column=0, sticky="e")
            operation_type_combobox = ttk.Combobox(
                config_frame, values=["Append", "Refresh"])
            operation_type_combobox.grid(row=9, column=1, sticky="w")

            # Add a callback to handle the window closing event
            self.config_window.window = self.directory_selection_frame
            self.config_window.protocol(
                "WM_DELETE_WINDOW", self.handle_config_window_close)

            if not server_entry.get():
                # Populate the values into the fields
                server_entry.insert(0, self.mssql_server)
                port_entry.insert(0, self.mssql_port)
                database_entry.insert(0, self.mssql_database)
                username_entry.insert(0, self.mssql_username)
                password_entry.insert(0, self.mssql_password)
                schema_entry.insert(0, self.mssql_schema)
                destination_table_entry.insert(0, self.destination_table)
                destination_total_table_entry.insert(
                    0, self.destination_total_table)

            def save_config():
                print(" Inside save_config .............")

                # Check if all fields are filled
                if (
                    server_entry.get()
                    and port_entry.get()
                    and database_entry.get()
                    and username_entry.get()
                    and password_entry.get()
                    and schema_entry.get()
                    and destination_table_entry.get()
                    and destination_total_table_entry.get()
                    and operation_type_combobox.get()
                ):
                    # Get the input values from the entry fields
                    getServer = server_entry.get()
                    getPort = port_entry.get()
                    getDatabase = database_entry.get()
                    getUsername = username_entry.get()
                    getPassword = password_entry.get()
                    getSchema = schema_entry.get()
                    getDestination_table = destination_table_entry.get()
                    getDestination_total_table = destination_total_table_entry.get()
                    getOperation_type = operation_type_combobox.get()

                    # Create the configuration dictionary
                    config_data = {
                        "MSSQLCredential": {
                            "mssql_server": getServer,
                            "mssql_port": getPort,
                            "mssql_database": getDatabase,
                            "mssql_username": getUsername,
                            "mssql_password": getPassword,
                            "mssql_schema": getSchema,
                            "destination_table": getDestination_table,
                            "destination_total_table": getDestination_total_table,
                            "operationType": getOperation_type
                        }
                    }

                    self.global_database_config = config_data

                    # Update the values in the config_data dictionary
                    config_data['FILEPATH'] = self.getBaseDirectory
                    config_data['MSSQLCredential']['mssql_server'] = getServer
                    config_data['MSSQLCredential']['mssql_port'] = getPort
                    config_data['MSSQLCredential']['mssql_database'] = getDatabase
                    config_data['MSSQLCredential']['mssql_username'] = getUsername
                    config_data['MSSQLCredential']['mssql_password'] = getPassword
                    config_data['MSSQLCredential']['mssql_schema'] = getSchema
                    config_data['MSSQLCredential']['destination_table'] = getDestination_table
                    config_data['MSSQLCredential']['destination_total_table'] = getDestination_total_table

                    # Write the updated config_data back to the file

                    with open(self.config_file_path, 'w') as json_file:
                        json.dump(config_data, json_file, indent=4)

                    # Print the configuration data
                    # print(config_data)

                    # Close the configuration window
                    self.config_window.destroy()
                    # After clicking Ok button in config window, making config_window none
                    self.config_window = None

                else:
                    # Display an error message if any field is empty
                    error_label.config(
                        text="All fields are mandatory!", fg="red")

            # Create OK and Cancel buttons
            button_frame = ttk.Frame(self.config_window)
            button_frame.pack(pady=10)

            ok_button = ttk.Button(
                button_frame, text="OK", command=save_config)
            ok_button.grid(row=0, column=0, padx=5)

            cancel_button = ttk.Button(
                button_frame, text="Cancel", command=self.cancel_config_window)
            cancel_button.grid(row=0, column=1, padx=5)

            # Create an error label
            error_label = ttk.Label(
                self.config_window, text="", foreground="red")
            error_label.pack()

            # ... (rest of the code for the configuration window)

    def main_window_ok(self):

        print(" self.global_database_config : ", self.global_database_config)
        if self.global_file_list and self.global_database_config:
            # destroying the directory selection frame to eliminate the reselection of directory path.
            self.directory_selection_frame.destroy()

            # resizing the main window for lodding
            self.window.geometry("400x100")

            # creating a loading frame inside main window
            self.loading_frame = ttk.Frame(self.window)
            # making the loding frame at the center of the window
            self.loading_frame.place(relx=0.5, rely=0.5, anchor="center")

            loading_label = ttk.Label(
                self.loading_frame, text="Loading...")   # labeling to show text at center as Loading...
            loading_label.pack()
            # Update the UI to display the loading frame
            self.window.update_idletasks()

            # Introduce a small delay to allow the loading frame to be displayed
            time.sleep(0.1)

            #  calling data transform
            self.data_transformation(
                self.global_file_list, self.global_database_config)
        else:
            # destroying the directory selection frame to make other frame to load.
            self.directory_selection_frame.destroy()
            # resizing the main window for lodding
            self.window.geometry("400x100")

            # creating a error frame inside main window
            self.Error_frame = ttk.Frame(self.window)
            # making the error frame at the center of the window
            self.Error_frame.place(relx=0.5, rely=0.5, anchor="center")

            Error_label = ttk.Label(
                self.Error_frame, text=" Provide Directory Path and Database Config")   # labeling to show text at center as Loading...
            Error_label.pack()
            # Update the UI to display the loading frame
            self.window.update_idletasks()

            # Introduce a small delay to allow the loading frame to be displayed
            time.sleep(0.1)

    def cancel(self):
        print(" Inside cancel .............")
        self.window.destroy()

    def data_transformation(self, file_paths_list, configs):
        ################################ Data Process Part Started #########################

        opetation_type = configs['MSSQLCredential']['operationType']

        data_processor = DataProcessor(file_paths_list)
        merged_df_list_for_files, merged_total_df_list_for_files = data_processor.read_data_files()

        ############################## Database Part Started #############################

        # Create database connector
        print(" Database connection starts...")
        database_connector = DatabaseConnector(configs)
        connection_string = database_connector.load_config()

        if connection_string:
            try:
                # Connect to the database
                # try:
                cursor_status, connection_cursor = database_connector.connect(
                    connection_string)
                print("this is cursor_status : ", cursor_status)
                print("this is connection_cursor : ", connection_cursor)
                # except Exception as e :
                if cursor_status == False:
                    # Handle the exception or print an error message
                    # print("Error connecting to the database:", str(e))
                    # self.cursor = None
                    print("this is inside loading_frame.destroy() : ",
                          connection_cursor)

                    self.loading_frame.destroy()
                    # resizing the main window for lodding
                    self.window.geometry("400x100")

                    # creating a loading frame inside main window
                    self.ConnectionCursorError_frame = ttk.Frame(self.window)
                    # making the loding frame at the center of the window
                    self.ConnectionCursorError_frame.place(
                        relx=0.5, rely=0.5, anchor="center")

                    ConnectionCursorError_frame_label = ttk.Label(
                        self.ConnectionCursorError_frame, text=" Connection Failure")   # labeling to show text at center as Loading...
                    ConnectionCursorError_frame_label.pack()
                    # Update the UI to display the loading frame
                    self.window.update_idletasks()

                    # Introduce a small delay to allow the loading frame to be displayed
                #   # time.sleep(0.1)
                # Create table if it doesn't exist
                if cursor_status == True:
                    createStatus = database_connector.create_table()
                    if createStatus == True and opetation_type == "Refresh":
                        refreshStatus = database_connector.operation_refresh()

                    try:
                        if (createStatus == True and opetation_type == "Append") or (refreshStatus == True and opetation_type == "Refresh"):
                            chunk = 1000
                            # Insert merged dataframes into the database
                            status = database_connector.insert_merged_dataframe_list(
                                merged_df_list_for_files, chunk)
                            if status == True:
                                database_connector.insert_merged_total_dataframe_list(
                                    merged_total_df_list_for_files, chunk)

                                # Close the database connection
                                database_connector.close_connection()

                                self.loading_frame.destroy()
                                # resizing the main window
                                self.window.geometry("400x100")
                                # creating a completed frame inside main window
                                self.completed_frame = ttk.Frame(self.window)
                                # making the completed frame at the center of the window
                                self.completed_frame.place(
                                    relx=0.5, rely=0.5, anchor="center")
                                completed_frame = ttk.Label(
                                    self.completed_frame, text="Data Import Completed")   # labeling to show text at center as Loading...
                                completed_frame.pack()
                                self.window.update_idletasks()
                                time.sleep(0.1)
                    except Exception as e:
                        # Close the database connection
                        database_connector.close_connection()
                        print(
                            f"Data Import Failed : {e}")

                        self.loading_frame.destroy()
                        # resizing the main window
                        self.window.geometry("400x100")
                        # creating a failed frame inside main window
                        self.failed_frame = ttk.Frame(self.window)
                        # making the failed frame at the center of the window
                        self.failed_frame.place(
                            relx=0.5, rely=0.5, anchor="center")
                        failed_frame = ttk.Label(
                            self.failed_frame, text="Data Import Failed")   # labeling to show text at center as Loading...
                        failed_frame.pack()
                        self.window.update_idletasks()
                        time.sleep(0.1)
            except Exception as e:
                # Close the database connection
                database_connector.close_connection()
                print(
                    f"Unable to establish a connection to the database from try catch: {e}")
                self.loading_frame.destroy()
                # resizing the main window for lodding
                self.window.geometry("400x100")
                # creating a loading frame inside main window
                self.failed_frame = ttk.Frame(self.window)
                # making the loding frame at the center of the window
                self.failed_frame.place(relx=0.5, rely=0.5, anchor="center")
                failed_frame = ttk.Label(
                    self.failed_frame, text="Connection Failed")   # labeling to show text at center as Loading...
                failed_frame.pack()
                self.window.update_idletasks()
                time.sleep(0.1)
