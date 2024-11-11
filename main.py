"""
This file contains the main function.
Launch the program with the following command: `python main.py`
"""

import pandas as pd

from etl.etl import ETL


def main() -> bool:
    """
    Main function.
    """
    return ETL().run()


def get_journal_which_quotes_the_most_amount_of_drugs(
    data: pd.DataFrame,
) -> tuple[str, int]:
    """
    Get the journal which quotes the most amount of drugs.

    Parameters
    ----------
    data : pd.DataFrame
        The data.json file into a pandas DataFrame.

    Returns
    -------
    tuple[str, int]
        The journal name and the amount of drugs.
    """
    ranking = data.groupby("journal").count()["drug"].sort_values(ascending=False)
    return ranking.index[0], ranking.values[0]


if __name__ == "__main__":
    if main():
        print("ETL completed successfully.")

    most_prolific_journal, nb_of_drugs = (
        get_journal_which_quotes_the_most_amount_of_drugs(pd.read_json("data.json"))
    )
    print(
        'The journal which quotes the most amount of drugs is "{}" with {} different drugs.'.format(
            most_prolific_journal, nb_of_drugs
        )
    )
