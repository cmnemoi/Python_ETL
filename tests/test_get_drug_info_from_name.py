import pandas as pd

from etl.util import get_drug_info_from_name

def test_get_drug_info_from_name():
    # Set up test data with the actual structure from data.json
    test_data = {
        "drugs": pd.DataFrame({
            "surrerogate_id": [0, 1, 2],
            "atccode": ["A04AD", "S03AA", "V03AB"],
            "drug": ["DIPHENHYDRAMINE", "TETRACYCLINE", "ETHANOL"]
        })
    }

    # Test getting info for DIPHENHYDRAMINE
    diphenhydramine_info = get_drug_info_from_name("DIPHENHYDRAMINE", test_data)
    assert diphenhydramine_info == {
        "surrerogate_id": 0,
        "atccode": "A04AD",
        "drug": "DIPHENHYDRAMINE"
    }

    # Test getting info for TETRACYCLINE
    tetracycline_info = get_drug_info_from_name("TETRACYCLINE", test_data)
    assert tetracycline_info == {
        "surrerogate_id": 1,
        "atccode": "S03AA",
        "drug": "TETRACYCLINE"
    }

    # Test getting info for ETHANOL
    ethanol_info = get_drug_info_from_name("ETHANOL", test_data)
    assert ethanol_info == {
        "surrerogate_id": 2,
        "atccode": "V03AB",
        "drug": "ETHANOL"
    }