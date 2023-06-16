import os
import pandas as pd


class Olist:
    def get_data(self):
        """
        This function returns a Python dict.
        Its keys should be 'sellers', 'orders', 'order_items' etc...
        Its values should be pandas.DataFrames loaded from csv files
        """

        root_dir = os.path.dirname(os.path.dirname(__file__))
        csv_path = os.path.join(root_dir, "data", "csv")
        
        # Create a list with all the names of the files in a the csv folder
        file_names = [file for file in os.listdir(csv_path) if file.endswith('csv')]
        
        # Removing the .csv, the _dataset.csv, and the olist_ from the file names list
        key_names = []
        for el in file_names:
            key_names.append(el.replace('olist_', '').replace('_dataset','').replace('.csv',''))
        
        # Create a dictionary with the file names as keys and the dataframes as values
        csv_paths=[]
        for path in file_names:
            csv_paths.append(pd.read_csv(os.path.join(csv_path, path)))
        data = dict(zip(key_names, csv_paths))     

        return data  
