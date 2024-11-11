"""
This file contains the ETL class, used to extract, transform, and load data from CSV and JSON files.
"""

import os
import sys

import pandas as pd

from tqdm import tqdm

from etl.enums import ColumnTypesEnum
from etl.util import (
    get_article_info_from_name,
    get_drug_info_from_name,
    remove_file_extension,
    get_article_journal_from_data,
    get_article_date_from_data,
)


class ETL:
    def __init__(self):
        """ETL Instance. Extract, transform and load data to a graph-oriented JSON file."""
        pass

    def run(self, data_folder: str, if_exists: str) -> bool:
        """Run the ETL."""

        if_exists = "replace" if if_exists == "y" else "append"

        data = self._extract(data_folder, if_exists)
        data = self._transform(data)

        return self._load(data)

    def _extract(
        self, folder_path: str, if_exists: str = "append"
    ) -> dict[str, pd.DataFrame]:
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
                data = self._extract_data_from_csv_(
                    data, folder_path + "/" + file, if_exists
                )
            elif file.endswith(".json"):
                data = self._extract_data_from_json_(
                    data, folder_path + "/" + file, if_exists
                )
            else:
                print(
                    f"File format for {file} is not supported. Ignoring.",
                    file=sys.stderr,
                )

        print("Data extracted successfully.")

        return data

    def _transform(self, data: dict[str, pd.DataFrame]) -> pd.DataFrame:
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
            data[file_name] = self._apply_technical_constraints(data[file_name])
            data[file_name] = self._apply_functional_constraints_(data[file_name])

        final_data = self._create_graph_oriented_dataframe(data)

        print("Data transformed successfully.")

        return final_data

    def _load(self, data: pd.DataFrame) -> bool:
        """
        Load the data to a graph-oriented JSON file.

        Parameters
        ----------
        data : pd.DataFrame
            The data.
        """

        print("Loading data to JSON file...")

        data.to_json("data.json", orient="records", force_ascii=False, indent=4)

        print("Data loaded to data.json successfully.")

        return True

    def _apply_technical_constraints(self, data: pd.DataFrame) -> pd.DataFrame:
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

        data = self._force_column_types(data)
        data = data.drop_duplicates()

        return data

    def _apply_functional_constraints_(self, data: pd.DataFrame) -> pd.DataFrame:
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

        data = self._add_surrogate_key(data)
        data = self._normalize_date_formats(data)
        data = self._remove_non_utf_8_characters(data)

        return data

    def _create_graph_oriented_dataframe(
        self, data: dict[str, pd.DataFrame]
    ) -> pd.DataFrame:
        """
        Create a graph-oriented pandas DataFrame from the data.

        Parameters
        ----------
        data : dict[str, pd.DataFrame]
            The dictionary containing the data.

        Returns
        -------
        pd.DataFrame
            The graph-oriented pandas DataFrame.
        """
        drug_nodes = [drug for drug in data["drugs"]["drug"]]
        pubmed_articles_nodes = [
            pubmed_article for pubmed_article in data["pubmed"]["title"]
        ]
        clinical_trials_nodes = [
            clinical_trial
            for clinical_trial in data["clinical_trials"]["scientific_title"]
        ]

        articles_nodes = pubmed_articles_nodes + clinical_trials_nodes

        graph_dataframe = pd.DataFrame()
        for drug in drug_nodes:
            for article in articles_nodes:
                if drug.lower() in article.lower():
                    journal = get_article_journal_from_data(data, article)
                    date = get_article_date_from_data(data, article)

                    graph_dataframe = pd.concat(
                        [
                            graph_dataframe,
                            pd.DataFrame(
                                data=[
                                    {
                                        "drug": get_drug_info_from_name(drug, data),
                                        "article": get_article_info_from_name(
                                            article, data
                                        ),
                                        "journal": journal,
                                        "relationship": "REFERENCED IN",
                                        "date": date,
                                    }
                                ]
                            ),
                        ],
                        ignore_index=True,
                    )

        return graph_dataframe

    def _add_surrogate_key(self, data: pd.DataFrame) -> pd.DataFrame:
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

    def _normalize_date_formats(self, data: pd.DataFrame) -> pd.DataFrame:
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
            data["date"] = (
                pd.to_datetime(data["date"], dayfirst=True, format="mixed")
                .apply(lambda x: x.strftime("%d-%m-%Y"))
                .astype(str)
            )
        except KeyError:
            # if date column is not present, do nothing
            return data

        return data

    def _remove_non_utf_8_characters(self, data: pd.DataFrame) -> pd.DataFrame:
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

    def _force_column_types(self, dataframe: pd.DataFrame) -> pd.DataFrame:
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

    def _extract_data_from_csv_(
        self, data: dict[str, pd.DataFrame], file_path: str, if_exists: str
    ) -> pd.DataFrame:
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
                    data[file_name] = pd.concat(
                        [data[file_name], pd.read_csv(file_path)]
                    ).reset_index(drop=True)
                case "replace":
                    data[file_name] = pd.read_csv(file_path)
                case "ignore":
                    pass
        else:
            data[file_name] = pd.read_csv(file_path)

        return data

    def _extract_data_from_json_(
        self, data: dict[str, pd.DataFrame], file_path: str, if_exists: str
    ) -> pd.DataFrame:
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
                    data[file_name] = pd.concat(
                        [data[file_name], pd.read_json(file_path)]
                    ).reset_index(drop=True)
                case "replace":
                    data[file_name] = pd.read_json(file_path)
                case "ignore":
                    pass
        else:
            data[file_name] = pd.read_json(file_path)

        return data
