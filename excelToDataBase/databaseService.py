import pyodbc


class DatabaseConnector:
    def __init__(self, configs):
        self.configs = configs
        self.conn = None
        self.cursor = None

    def load_config(self):

        if (self.configs):
            # for mssql database
            mssql_server = self.configs['MSSQLCredential']['mssql_server']
            mssql_database = self.configs['MSSQLCredential']['mssql_database']
            mssql_user = self.configs['MSSQLCredential']['mssql_username']
            mssql_password = self.configs['MSSQLCredential']['mssql_password']
            self.schemaName = self.configs['MSSQLCredential']['mssql_schema']
            self.tableName = self.configs['MSSQLCredential']['destination_table']
            self.controlTableName = self.configs['MSSQLCredential']['destination_total_table']
            self.opetationType = self.configs['MSSQLCredential']['operationType']

            driver_mssql = "ODBC Driver 17 for SQL Server"
            charset = "utf8mb4"

            # database is mssql
            conn_string = f"DRIVER={driver_mssql};SERVER={mssql_server};DATABASE={mssql_database};UID={mssql_user};PWD={mssql_password};charset={charset}"
            print(" Connection String Created : ", conn_string)
            return conn_string

    def connect(self, connection_string):

        cursor_status = False
        try:
            self.conn = pyodbc.connect(connection_string)
            self.cursor = self.conn.cursor()
            cursor_status = True
        except pyodbc.Error as error:
            # Handle the exception or print an error message
            print("Error connecting to the database:", str(error))
            self.cursor = None

        return cursor_status, self.cursor

    def create_table(self):

        # for mssql create query
        query_create_for_mssql = [
            f"IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{self.schemaName}') EXEC('CREATE SCHEMA {self.schemaName}');",
            f'''
            IF OBJECT_ID('{self.schemaName}.{self.tableName}', 'U') IS NULL
            BEGIN
                CREATE TABLE {self.schemaName}.{self.tableName} (
                    Entity VARCHAR(250) NOT NULL DEFAULT '',
                    GeneralLedgerAccount VARCHAR(250) NOT NULL DEFAULT '',
                    Date VARCHAR(250) NOT NULL DEFAULT '',
                    TransactionType VARCHAR(250) NULL,
                    Amount Float NULL
                ); 
            END
            ''',
            f'''
            IF OBJECT_ID('{self.schemaName}.{self.controlTableName}', 'U') IS NULL
            BEGIN
                CREATE TABLE {self.schemaName}.{self.controlTableName} (
                    Entity VARCHAR(250) NOT NULL DEFAULT '',
                    GeneralLedgerAccount VARCHAR(250) NOT NULL DEFAULT '',
                    Date VARCHAR(250) NOT NULL DEFAULT '',
                    TransactionType VARCHAR(250) NULL,
                    Amount Float NULL
                ); 
            END
            '''
        ]
        try:
            # for mssql
            for query in query_create_for_mssql:
                self.cursor.execute(query)
                self.cursor.commit()

            return True
        except Exception as e:
            print(f" Table creation Failed : { e }")
            return False

    def operation_refresh(self):
        query_for_refresh_table = [
            f"TRUNCATE TABLE {self.schemaName}.{self.tableName};",
            f"TRUNCATE TABLE {self.schemaName}.{self.controlTableName};"
        ]
        try:
            # for mssql
            for query in query_for_refresh_table:
                self.cursor.execute(query)
                self.cursor.commit()
            return True
        except Exception as e:
            print(f" Truncate Table Failed : { e }")
            return False

    def insert_merged_dataframe_list(self, merged_df_list_for_files, chunk):

        chunk_size = 1000
        # Convert DataFrame to list of tuples
        try:
            for merged_df in merged_df_list_for_files:

                rows = [tuple(row) for row in merged_df.values]

                # Split rows into chunks
                chunks = [rows[i:i+chunk_size]
                          for i in range(0, len(rows), chunk_size)]

                # Insert data in chunks
                print(" Data insert starts.....")
                for chunk in chunks:
                    placeholders = ",".join(["?"] * len(chunk[0]))
                    query = f"INSERT INTO {self.schemaName}.{self.tableName} VALUES ({placeholders})"
                    self.cursor.executemany(query, chunk)
                    self.cursor.commit()
                print(" Data insert ends :)")
            return True
        except Exception as e:
            print(f" Data insert failed in TrialBalance : { e }")
            return False

    def insert_merged_total_dataframe_list(self, merged_total_df_list_for_files, chunk=1000):

        chunk_size = 1000

        # Convert DataFrame to list of tuples
        try:
            for merged_total_df in merged_total_df_list_for_files:
                rows = [tuple(row) for row in merged_total_df.values]

                # Split rows into chunks
                chunks = [rows[i:i+chunk_size]
                          for i in range(0, len(rows), chunk_size)]

                # Insert data in chunks
                print(" Data insert starts.....")
                for chunk in chunks:
                    placeholders = ",".join(["?"] * len(chunk[0]))
                    query = f"INSERT INTO {self.schemaName}.{self.controlTableName} VALUES ({placeholders})"
                    self.cursor.executemany(query, chunk)
                    self.cursor.commit()
                print(" Data insert ends :)")
        except Exception as e:
            print(f" data insert failed in TrialBalanceControlTotal: {e} ")
            return False

    def close_connection(self):
        print(" Connection closed !!")
        self.cursor.close()
        self.conn.close()
