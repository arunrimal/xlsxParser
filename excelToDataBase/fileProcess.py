import pandas as pd


class DataProcessor:
    def __init__(self, data_file_list):
        self.data_file_list = data_file_list
        self.merged_df_list_for_files = []
        self.merged_total_df_list_for_files = []

    def read_data_files(self):
        for data_file in self.data_file_list:

            df = pd.read_excel(data_file)

            merged_df_list_for_files, merged_total_df_list_for_files = self.process_data_file(
                df)

        return merged_df_list_for_files, merged_total_df_list_for_files

    def process_data_file(self, df):
        df = self.preprocess_dataframe(df)
        EntityName = self.get_entity_name(df)
        df_slice, df_for_total = self.slice_data_frames(df)

        static_df = self.static_data_frame(df_slice)

        resulting_dfs = self.process_monthly_data(df_slice, static_df)
        merged_df = self.merge_dataframes(resulting_dfs, EntityName)

        merged_total_df = self.merge_total_dataframes(df_for_total, EntityName)

        self.merged_df_list_for_files.append(merged_df)
        self.merged_total_df_list_for_files.append(merged_total_df)

        return self.merged_df_list_for_files, self.merged_total_df_list_for_files

    def preprocess_dataframe(self, df):
        df = df.drop([0, 1, 2], axis=0)
        df = df.reset_index(drop=True)
        return df

    def get_entity_name(self, df):
        EntityName = df.columns[0]
        return EntityName

    def slice_data_frames(self, df):
        # finding last row index i.e TOTAL row index
        last_row_index = df.index[(df.iloc[:, 0] == "TOTAL")].tolist()
        print(" TOTAL row index : ", last_row_index[0])
        df_slice = df[0:last_row_index[0]]
        df_for_total = df.iloc[[0, 1, last_row_index[0]], 1:]
        return df_slice, df_for_total

    ###### Static Data frame part ######
    def static_data_frame(self, df_slice):
        df_static_from_slice = df_slice.iloc[2:, 0]
        df_static_from_slice_01 = pd.DataFrame(df_static_from_slice)
        df_static_from_slice_01.columns = ["GeneralLedgerAccount"]
        # reset index of data frame after slicing and column name change
        df_static_from_slice_01 = df_static_from_slice_01.reset_index(
            drop=True)
        return df_static_from_slice_01

    def process_monthly_data(self, df_slice, static_df):
        # Process monthly data here
        df_canvas = df_slice.iloc[0:, 1:]
        df_data_canvas = pd.DataFrame(df_canvas).reset_index(drop=True)
        num_columns = 2
        column_names = df_data_canvas.columns.tolist()
        num_dataframes = len(column_names) // num_columns
        df_Tr_DrCr_Melt_list = []
        # ...
        for i in range(num_dataframes):
            # Determine the column range for the current dataframe
            start_idx = i * num_columns
            end_idx = start_idx + num_columns

            # Select the columns for the current dataframe
            selected_columns = column_names[start_idx:end_idx]

            # Create a new dataframe with the selected columns
            new_df = df_data_canvas[selected_columns].copy()

            Date = new_df.iloc[0, 0]
            new_df.columns = new_df.iloc[1].values

            df_DrCr = new_df[2:]
            df_DrCr = df_DrCr.reset_index(drop=True)

            # Add the "TransactionName" column to the df_amounts data frame
            df_Tr_DrCr = pd.concat([static_df, df_DrCr], axis=1)

            # Melt the data frame to combine debit and credit columns into a single column
            df_Tr_DrCr_Melt = pd.melt(df_Tr_DrCr, id_vars=['GeneralLedgerAccount'], value_vars=['Debit', 'Credit'],
                                      var_name='TransactionType', value_name='Amount')

            # Sort the data frame by transaction name
            df_Tr_DrCr_Melt.sort_values('GeneralLedgerAccount', inplace=True)
            df_Tr_DrCr_Melt.reset_index(drop=True, inplace=True)

            # filling nan value with 0 in Amount column
            df_Tr_DrCr_Melt["Amount"].fillna(value=0, inplace=True)

            # inserting Date column in the processed data frame
            df_Tr_DrCr_Melt.insert(loc=1, column="Date", value=Date)

            # Append the new dataframe to the df_Tr_DrCr_Melt_list list
            df_Tr_DrCr_Melt_list.append(df_Tr_DrCr_Melt)
        return df_Tr_DrCr_Melt_list

    def merge_dataframes(self, resulting_dfs, EntityName):
        # Merge dataframes here
        merged_df = pd.DataFrame()
        # Iterate over the list of dataframes
        print("********************  df_monthly merge Starts ****************")
        for i, df in enumerate(resulting_dfs):

            if i == 0:
                # For the first dataframe, append the entire dataframe with headers
                merged_df = pd.concat([merged_df, df], axis=0)
            else:
                # For subsequent dataframes, append all rows except the header row
                merged_df = pd.concat([merged_df, df.iloc[0:]], axis=0)
        print("********************  df_monthly merge Ends ******************")
        # Add Entity Name column in the Data frame
        merged_df.insert(loc=0, column="Entity", value=EntityName)

        # Reset the index of the merged dataframe
        merged_df = merged_df.reset_index(drop=True)

        return merged_df

    def merge_total_dataframes(self, df_for_total, EntityName):
        # Merge total dataframes here
        # For Totals in monthly trial balance
        num_columns_for_total = 2
        column_names = df_for_total.columns.tolist()
        num_dataframes = len(column_names) // num_columns_for_total
        total_df_Melt_list = []
        for i in range(num_dataframes):
            # Determine the column range for the current dataframe
            start_idx = i * num_columns_for_total
            end_idx = start_idx + num_columns_for_total

            # Select the columns for the current dataframe
            selected_columns = column_names[start_idx:end_idx]

            # Create a new dataframe with the selected columns
            new_df = df_for_total[selected_columns].copy()
            Date = new_df.iloc[0, 0]
            new_df = new_df.drop([0])
            new_df = new_df.reset_index(drop=True)
            new_df.columns = new_df.iloc[0]

            new_df = new_df.reset_index(drop=True)
            new_df = new_df[1:]

            # Melt the data frame to combine debit and credit columns into a single column
            df_Total_melt = pd.melt(new_df, value_vars=['Debit', 'Credit'],
                                    var_name='TransactionType', value_name='Amount')

            # Insert series of static values
            df_Total_melt.insert(loc=0, column="Entity", value=EntityName)
            df_Total_melt.insert(
                loc=1, column="GeneralLedgerAccount", value='ControlTotal')
            df_Total_melt.insert(loc=2, column="Date", value=Date)

            # df_Total_melt.insert(loc=0, column="Entity", value=EntityName)
            df_Total_melt.reset_index(drop=True, inplace=True)

            # filling nan value with 0 in Amount column
            df_Total_melt["Amount"].fillna(value=0, inplace=True)

            total_df_Melt_list.append(df_Total_melt)
        print(" total_df_Melt_list: ",
              len(total_df_Melt_list))

        # concating monthly total data frames from the list of total_df_Melt dataframes
        # Create an empty dataframe for the merged result
        merged_total_df = pd.DataFrame()
        # Iterate over the list of dataframes
        print("********************  total_monthly_df merge Starts ****************")
        for i, df in enumerate(total_df_Melt_list):

            # print(df)
            if i == 0:
                merged_total_df = pd.concat([merged_total_df, df], axis=0)
                print()
            else:
                merged_total_df = pd.concat(
                    [merged_total_df, df.iloc[0:]], axis=0)
        print("********************  total_monthly_df merge Ends ******************")
        return merged_total_df
