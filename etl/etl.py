"""
This file contains the ETL class, used to extract, transform, and load data from CSV and JSON files.
"""

import os
import sys

import pandas as pd

from tqdm import tqdm

from etl.enums import ColumnTypesEnum
from etl.util import (
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

    def transform(self, data: dict[str, pd.DataFrame]) -> pd.DataFrame:
        """
        Apply transformations to the data.
        - Technical constraints:
            - Force column types
            - Drop duplicates
        - Functional constraints:
            - Add a surrerogate key
            - Normalize date formats
            - Remove non utf_8 characters
        - Drop primary key missing rows
        - Data structuration:
            - Create a graph oriented pandas DataFrame
        
        Parameters
        ----------
        data : dict[str, pd.DataFrame]
            The dictionary containing the data.
        
        Returns
        -------
        pd.DataFrame
            The transformed data as a graph-oriented pandas DataFrame.
        """

        print("Transforming data...")

        for file_name in tqdm(data.keys()):
            data[file_name] = self.__apply__technical_constraints__(data[file_name])
            data[file_name] = self.__apply__functional_constraints__(data[file_name])
        
        print("Data transformed successfully.")

        return data

    def __apply__technical_constraints__(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Apply technical constraints to the data.
        - Force column types
        - Drop duplicates
        
        Parameters
        ----------
        data : pd.DataFrame
            The data.
        
        Returns
        -------
        pd.DataFrame
            The data with the applied constraints.
        """

        data = self.__force_column_types__(data)
        data = data.drop_duplicates()
        
        return data

    def __apply__functional_constraints__(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Apply functional constraints to the data.
        - Add a surrerogate key
        - Normalize date formats
        - Remove non utf_8 characters
        
        Parameters
        ----------
        data : pd.DataFrame
            The data.
        
        Returns
        -------
        pd.DataFrame
            The data with the applied constraints.
        """

        data = self.__add_surrogate_key__(data)
        data = self.__normalize_date_formats__(data)
        data = self.__remove_non_utf_8_characters__(data)
        
        return data

    def __add_surrogate_key__(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Add a surrogate key to the data.
        
        Parameters
        ----------
        data : pd.DataFrame
            The data.
        
        Returns
        -------
        pd.DataFrame
            The data with the surrogate key.
        """

        data.insert(0, "surrerogate_id", data.index)
        
        return data

    def __normalize_date_formats__(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Normalize dates to the format DD-MM-YYYY.
        
        Parameters
        ----------
        data : pd.DataFrame
            The data.
        
        Returns
        -------
        pd.DataFrame
            The data with the normalized format dates.
        """
        
        try:
            data["date"] = pd.to_datetime(data["date"]).apply(lambda x: x.strftime("%d-%m-%Y")).astype(str)
        except KeyError:
            #if date column is not present, do nothing
            return data

        return data

    def __remove_non_utf_8_characters__(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Remove non utf_8 characters.
        
        Parameters
        ----------
        data : pd.DataFrame
            The data.
        
        Returns
        -------
        pd.DataFrame
            The data with the removed non utf_8 characters.
        """

        for column in data.columns:
            if data[column].dtype == object:
                data[column] = data[column].apply(lambda x: x.split("\\")[0])

        return data

    def __force_column_types__(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        """
        Force column types.
        
        Parameters
        ----------
        dataframe : pd.DataFrame
            The dataframe.
        
        Returns
        -------
        pd.DataFrame
            The dataframe with the forced column types.
        """

        for column in dataframe:
            dataframe[column] = dataframe[column].astype(ColumnTypesEnum[column].value)

        return dataframe


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

        
