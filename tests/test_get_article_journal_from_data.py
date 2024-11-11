import pandas as pd

from etl.util import get_article_journal_from_data


def test_get_article_journal_from_data():
    # Set up test data
    test_data = {
        "pubmed": pd.DataFrame(
            {"title": ["Article 1", "Article 2"], "journal": ["Journal 1", "Journal 2"]}
        ),
        "clinical_trials": pd.DataFrame(
            {
                "scientific_title": ["Article 3", "Article 4"],
                "journal": ["Journal 3", "Journal 4"],
            }
        ),
    }

    # Test finding article in pubmed data
    assert get_article_journal_from_data(test_data, "Article 1") == "Journal 1"
    assert get_article_journal_from_data(test_data, "Article 2") == "Journal 2"

    # Test finding article in clinical trials data
    assert get_article_journal_from_data(test_data, "Article 3") == "Journal 3"
    assert get_article_journal_from_data(test_data, "Article 4") == "Journal 4"
