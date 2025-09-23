import pandas as pd
from extract.extract_data import get_base_tables


def build_claiminsurance(claiminsurance_vw, insurance):
    """
    Replicates the SQL logic of #ClaimInsurance_vw:
    - Removes wrap-type insurances
    - Adjusts Secondary/Tertiary insurance assignments
    """

    # Identify insurance IDs that belong to wrap-type plans
    wrap_ids = insurance.loc[
        insurance["insuranceName"].str.contains("wrap", case=False, na=False),
        "insId"
    ].tolist()

    df = claiminsurance_vw.copy()

    # Remove Secondary/Tertiary if they are wrap-type insurances
    df["SecondaryInsId"] = df["SecondaryInsId"].apply(lambda x: None if x in wrap_ids else x)
    df["TertiaryInsId"] = df["TertiaryInsId"].apply(lambda x: None if x in wrap_ids else x)

    # If Secondary is null, replace it with Tertiary
    df["SecondaryInsId"] = df["SecondaryInsId"].fillna(df["TertiaryInsId"])

    # Avoid duplication: if Tertiary equals Secondary, clear Tertiary
    df["TertiaryInsId"] = df["TertiaryInsId"].where(df["TertiaryInsId"] != df["SecondaryInsId"])

    return df


def test_claiminsurance():
    """Runs build_claiminsurance with extracted tables and shows 10 rows"""
    try:
        data = get_base_tables()

        result = build_claiminsurance(
            data["dbo.ClaimInsurance_vw"],
            data["dbo.insurance"]
        )

        print("✅ ClaimInsurance dataset sample (10 rows):")
        print(result.head(10))

    except Exception as e:
        print(f"❌ Error in test_claiminsurance: {e}")


if __name__ == "__main__":
    test_claiminsurance()
