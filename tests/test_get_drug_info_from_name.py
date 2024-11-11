import pytest
import pandas as pd
from etl.util import get_drug_info_from_name

@pytest.fixture
def test_data():
    """Fixture providing test data structure matching data.json format"""
    return {
        "drugs": pd.DataFrame({
            "surrerogate_id": [0, 1, 2],
            "atccode": ["A04AD", "S03AA", "V03AB"],
            "drug": ["DIPHENHYDRAMINE", "TETRACYCLINE", "ETHANOL"]
        })
    }

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
    })
])
def test_get_drug_info_from_name(test_data, drug_name, expected):
    """Test getting drug info for different drugs"""
    assert get_drug_info_from_name(drug_name, test_data) == expected
