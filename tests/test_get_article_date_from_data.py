import pandas as pd

from etl.util import get_article_date_from_data


def test_get_article_date_from_data():
    # Set up test data with sample dates
    test_data = {
        "pubmed": pd.DataFrame(
            {"title": ["Article 1", "Article 2"], "date": ["2023-01-01", "2023-02-15"]}
        ),
        "clinical_trials": pd.DataFrame(
            {
                "scientific_title": ["Article 3", "Article 4"],
                "date": ["2023-03-20", "2023-04-30"],
            }
        ),
    }

    # Test finding dates in pubmed data
    assert get_article_date_from_data(test_data, "Article 1") == "2023-01-01"
    assert get_article_date_from_data(test_data, "Article 2") == "2023-02-15"

    # Test finding dates in clinical trials data
    assert get_article_date_from_data(test_data, "Article 3") == "2023-03-20"
    assert get_article_date_from_data(test_data, "Article 4") == "2023-04-30"
