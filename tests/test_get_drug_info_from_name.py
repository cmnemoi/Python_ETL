import json
import pytest
import pandas as pd
from etl.util import get_drug_info_from_name

@pytest.fixture
def test_data():
    """Fixture providing test data from data.json"""
    with open('data.json', 'r') as f:
        json_data = json.load(f)
    
    # Extract unique drug entries
    unique_drugs = {}
    for entry in json_data:
        drug = entry['drug']
        drug_name = drug['drug']
        if drug_name not in unique_drugs:
            unique_drugs[drug_name] = drug

    # Create DataFrame from unique drugs
    drugs_df = pd.DataFrame(unique_drugs.values())
    return {"drugs": drugs_df}

@pytest.mark.parametrize("drug_name,expected", [
    ("DIPHENHYDRAMINE", {
        "surrerogate_id": 0,
        "atccode": "A04AD",
        "drug": "DIPHENHYDRAMINE"
    }),
    ("TETRACYCLINE", {
        "surrerogate_id": 1,
        "atccode": "S03AA",
        "drug": "TETRACYCLINE"
    }),
    ("ETHANOL", {
        "surrerogate_id": 2,
        "atccode": "V03AB",
        "drug": "ETHANOL"
    }),
    ("ATROPINE", {
        "surrerogate_id": 3,
        "atccode": "A03BA",
        "drug": "ATROPINE"
    }),
    ("EPINEPHRINE", {
        "surrerogate_id": 4,
        "atccode": "A01AD",
        "drug": "EPINEPHRINE"
    })
])
def test_get_drug_info_from_name(test_data, drug_name, expected):
    """Test getting drug info for different drugs using real data"""
    assert get_drug_info_from_name(drug_name, test_data) == expected
