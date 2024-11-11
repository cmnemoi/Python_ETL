import pytest
import pandas as pd

from etl.util import get_drug_info_from_name


@pytest.fixture
def drug_data():
    """Fixture to load drug data from CSV file"""
    return pd.read_csv("data/drugs.csv")


@pytest.mark.parametrize(
    "drug_name,expected_atccode",
    [
        ("DIPHENHYDRAMINE", "A04AD"),
        ("TETRACYCLINE", "S03AA"),
        ("ETHANOL", "V03AB"),
        ("ATROPINE", "A03BA"),
        ("EPINEPHRINE", "A01AD"),
        ("ISOPRENALINE", "6302001"),
        ("BETAMETHASONE", "R01AD"),
    ],
)
def test_get_drug_info_from_name(drug_data, drug_name, expected_atccode):
    """Test getting drug info for various drugs from the actual dataset"""
    result = get_drug_info_from_name(drug_name, {"drugs": drug_data})

    assert result["drug"] == drug_name
    assert result["atccode"] == expected_atccode
