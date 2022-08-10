import pandas as pd

def remove_file_extension(file_name: str) -> str:
    """
    Remove the file extension from a file name.
    
    Parameters
    ----------
    file_name : str
        The file name.
    """

    return file_name.split("/")[-1].split(".")[0]

def get_article_journal_from_data(data: dict[str, pd.DataFrame], article_name: str) -> str:
    """
    Get the journal name from its name
    
    Parameters
    ----------
    article_name : str
        The article name.

    Returns
    -------
    str
        The journal name.
    """
    try:
        return data["pubmed"][data["pubmed"]["title"] == article_name]["journal"].values[0]
    except IndexError:
        return data["clinical_trials"][data["clinical_trials"]["scientific_title"] == article_name]["journal"].values[0]

def get_article_date_from_data(data: dict[str, pd.DataFrame], article_name: str) -> str:
    """
    Get the date of the article from its name
    
    Parameters
    ----------
    article_name : str
        The article name.

    Returns
    -------
    str
        The date of the article.
    """
    try:
        return data["pubmed"][data["pubmed"]["title"] == article_name]["date"].values[0]
    except IndexError:
        return data["clinical_trials"][data["clinical_trials"]["scientific_title"] == article_name]["date"].values[0]

def get_drug_info_from_name(drug: str, data: pd.DataFrame) -> dict[str, str]:
    """
    Get the drug info from its name
    
    Parameters
    ----------
    drug : str
        The drug name.
    data : pd.DataFrame
        The dataframe containing the drug info.

    Returns
    -------
    dict[str, str]
        The drug info.
    """
    return data["drugs"][data["drugs"]["drug"] == drug].to_dict("records")[0]

def get_article_info_from_name(article: str, data: pd.DataFrame) -> dict[str, str]:
    """
    Get the article info from its name
    
    Parameters
    ----------
    article : str
        The article name.
    data : pd.DataFrame
        The dataframe containing the article info.

    Returns
    -------
    dict[str, str]
        The article info.
    """
    try:
        return data["clinical_trials"][data["clinical_trials"]["scientific_title"] == article].to_dict("records")[0]
    except IndexError:
        return data["pubmed"][data["pubmed"]["title"] == article].to_dict("records")[0]

