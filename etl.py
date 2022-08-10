"""
This file contains the ETL class, used to extract, transform, and load data.
"""

import os
import sys

import pandas as pd

from tqdm import tqdm

from util import (
    remove_file_extension
)

class ETL:
    def __init__(self):
        """ETL Instance. Extract, transform and load data to a graph-oriented JSON file."""
        pass
    
    def extract(self, folder_path: str, if_exists: str="append") -> dict[str, pd.DataFrame]:
        """
        Extract data from CSV or JSON files and returns a Pandas DataFrame per file into a dictionary.

        Parameters
        ----------
        folder_path : str
            The path to the folder containing the files to be extracted.
        if_exists : str
            The action to be taken if the dataframe already exists in the dictionary.
            Possible values: "append", "replace", "ignore".
        
        Returns
        -------
        dict[str, pd.DataFrame]
            A dictionary containing the extracted data.
            You access the data of each file by using the file name as the key.
        """

        data = {}

        print("Extracting data from files...")

        for file in tqdm(os.listdir(folder_path)):
            if file.endswith(".csv"):
                data = self.__extract_data_from_csv__(data, folder_path + "/" + file, if_exists)
            elif file.endswith(".json"):
                data = self.__extract_data_from_json__(data, folder_path + "/" + file, if_exists)
            else:
                print(f"File format for {file} is not supported. Ignoring.", file=sys.stderr)

        print("Data extracted successfully.")

        return data


    def __extract_data_from_csv__(self, data: dict[str, pd.DataFrame], file_path: str, if_exists: str) -> pd.DataFrame:
        """
        Extract data from a CSV file.

        Parameters
        ----------
        data : dict[str, pd.DataFrame]
            The dictionary containing the data.
        file_path : str
            The path to the file.
        if_exists : str
            The action to be taken if the dataframe already exists in the dictionary.
            Possible values: "append", "replace", "ignore".
        
        Returns
        -------
        pd.DataFrame
            The extracted data.
        """

        file_name = remove_file_extension(file_path)
        if remove_file_extension(file_name) in data.keys():
            match if_exists:
                case "append":
                    data[file_name] = pd.concat([data[file_name],pd.read_csv(file_path)]).reset_index(drop=True)
                case "replace":
                    data[file_name] = pd.read_csv(file_path)
                case "ignore":
                    pass
        else:
            data[file_name] = pd.read_csv(file_path)
            
        return data

    def __extract_data_from_json__(self, data: dict[str, pd.DataFrame], file_path: str, if_exists: str) -> pd.DataFrame:
        """
        Extract data from a JSON file.

        Parameters
        ----------
        data : dict[str, pd.DataFrame]
            The dictionary containing the data.
        file_path : str
            The path to the file.
        if_exists : str
            The action to be taken if the dataframe already exists in the dictionary.
            Possible values: "append", "replace", "ignore".
        
        Returns
        -------
        pd.DataFrame
            The extracted data.
        """

        file_name = remove_file_extension(file_path)
        if remove_file_extension(file_name) in data.keys():
            match if_exists:
                case "append":
                    data[file_name] = pd.concat([data[file_name],pd.read_json(file_path)]).reset_index(drop=True)
                case "replace":
                    data[file_name] = pd.read_json(file_path)
                case "ignore":
                    pass
        else:
            data[file_name] = pd.read_json(file_path)
            
        return data

        
