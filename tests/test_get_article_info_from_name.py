import pandas as pd

from etl.util import get_article_info_from_name


def test_get_article_info_from_name_clinical_trials():
    # Set up test data with clinical trials articles
    test_data = {
        "clinical_trials": pd.DataFrame(
            {
                "id": ["NCT01967433", "NCT04189588"],
                "scientific_title": [
                    "Use of Diphenhydramine as an Adjunctive Sedative for Colonoscopy in Patients Chronically on Opioids",
                    "Phase 2 Study IV QUZYTTIR™ (Cetirizine Hydrochloride Injection) vs V Diphenhydramine",
                ],
                "date": ["01-01-2020", "01-01-2020"],
                "journal": [
                    "Journal of emergency nursing",
                    "Journal of emergency nursing",
                ],
            }
        ),
        "pubmed": pd.DataFrame(
            {
                "id": ["1", "2"],
                "title": ["Test Article 1", "Test Article 2"],
                "date": ["01-01-2019", "01-01-2019"],
                "journal": ["Test Journal 1", "Test Journal 2"],
            }
        ),
    }

    # Test getting clinical trial article
    article_title = "Use of Diphenhydramine as an Adjunctive Sedative for Colonoscopy in Patients Chronically on Opioids"
    article_info = get_article_info_from_name(article_title, test_data)

    assert article_info == {
        "id": "NCT01967433",
        "scientific_title": article_title,
        "date": "01-01-2020",
        "journal": "Journal of emergency nursing",
    }


def test_get_article_info_from_name_pubmed():
    # Set up test data with pubmed articles
    test_data = {
        "clinical_trials": pd.DataFrame(
            {"id": [], "scientific_title": [], "date": [], "journal": []}
        ),
        "pubmed": pd.DataFrame(
            {
                "id": ["1", "2"],
                "title": [
                    "Tetracycline Resistance Patterns of Lactobacillus buchneri Group Strains.",
                    "The High Cost of Epinephrine Autoinjectors and Possible Alternatives.",
                ],
                "date": ["01-01-2020", "02-01-2020"],
                "journal": [
                    "Journal of food protection",
                    "The journal of allergy and clinical immunology. In practice",
                ],
            }
        ),
    }

    # Test getting pubmed article
    article_title = (
        "Tetracycline Resistance Patterns of Lactobacillus buchneri Group Strains."
    )
    article_info = get_article_info_from_name(article_title, test_data)

    assert article_info == {
        "id": "1",
        "title": article_title,
        "date": "01-01-2020",
        "journal": "Journal of food protection",
    }
