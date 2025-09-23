import pandas as pd
from datetime import datetime
from extract.extract_data import get_base_tables


def build_invoice(edi_invoice, enc, doctors, annualnotes, inv_claimstatus_log):
    """
    Replicates the temporary SQL table #invoice:
    - Joins invoice with encounter and doctors
    - Filters by ServiceDt between 2025-01-01 and today, with DeleteFlag == 0
    - Marks same-day visits
    - Handles flipped claims
    - Adds last status update date
    """

    # Date filters
    start_date = pd.to_datetime("2025-01-01")   # fixed start date
    today = pd.to_datetime(datetime.today().date())

    # === Base join: invoice + encounter + doctors
    df = (
        edi_invoice
        .merge(enc, left_on="EncounterId", right_on="encounterID", suffixes=("", "_enc"))
        .merge(doctors, on="doctorID", how="left")
    )

    # === Filter rows by date + deleteFlag
    df = df[
        (pd.to_datetime(df["ServiceDt"], errors="coerce") >= start_date) &
        (pd.to_datetime(df["ServiceDt"], errors="coerce") <= today) &
        (df["DeleteFlag"] == 0)
    ].copy()

    # === Same-day visits
    same_day = (
        edi_invoice
        .merge(enc, left_on="EncounterId", right_on="encounterID")
        .query("DeleteFlag == 0 and STATUS == 'CHK'")
        .groupby(["PatientId", "ServiceDt"])["EncounterId"]
        .apply(lambda x: " ".join(x.astype(str)))
        .reset_index()
        .rename(columns={"EncounterId": "SameDayVisit"})
    )
    df = df.merge(same_day, on=["PatientId", "ServiceDt"], how="left")

    # === Flipped claims
    flipped = annualnotes[
        annualnotes["type"].eq("billing") &
        annualnotes["notes"].str.contains("flipped claim", case=False, na=False)
    ]
    mask = df["EncounterId"].isin(flipped["encounterId"])
    df.loc[mask, ["Id", "SplitClaimId"]] = df.loc[mask, ["SplitClaimId", "Id"]].values

    # === Last status update
    last_status = (
        inv_claimstatus_log
        .assign(date=pd.to_datetime(inv_claimstatus_log["date"], errors="coerce"))
        .groupby("InvId", as_index=False)["date"].max()
        .rename(columns={"InvId": "Id", "date": "LastStatusUpdate"})
    )
    df = df.merge(last_status, on="Id", how="left")

    return df


def test_invoice():
    """Runs build_invoice with extracted tables and shows 10 rows"""
    try:
        data = get_base_tables()

        invoice = build_invoice(
            data["dbo.edi_invoice"],
            data["dbo.enc"],
            data["dbo.doctors"],
            data["dbo.annualnotes"],
            data["dbo.edi_inv_claimstatus_log"]
        )

        print("✅ Invoice dataset sample (10 rows):")
        print(invoice.head(10))

    except Exception as e:
        print(f"❌ Error in test_invoice: {e}")


if __name__ == "__main__":
    test_invoice()
