import pandas as pd
from dags.user_etl_pipeline import transform_data


def test_transform_data():
    df = pd.DataFrame(
        {
            "id": [1, 1, 2],
            "email": ["A@EXAMPLE.COM", "A@EXAMPLE.COM", "B@EXAMPLE.COM"],
            "signup_date": ["2023-01-01", "2023-01-01", "2023-01-02"],
        }
    )
    result = transform_data(df)
    assert len(result) == 2
    assert all(result["email"].str.islower())
