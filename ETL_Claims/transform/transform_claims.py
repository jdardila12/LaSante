import pandas as pd
from extract.extract_data import get_base_tables
from transform.transform_payments import build_payments
from transform.transform_wrap import build_wrap


def build_claims(invoice, claiminsurance_vw, edi_inspayments, edi_paymentdetail,
                 edi_inv_insurance, insurance, claimstatuscodes, facilities):
    """
    Build the final claims dataset (#ClaimResults).
    """

    # Payments and Wrap
    payments = build_payments(edi_inspayments, edi_paymentdetail, invoice, claiminsurance_vw)
    wrap = build_wrap(edi_inv_insurance, insurance, edi_paymentdetail)

    # Start with invoice
    df = invoice.merge(claiminsurance_vw, left_on="Id", right_on="ClaimId", how="left")
    df = df.merge(payments, left_on="Id", right_on="invoiceId", how="left")
    df = df.merge(wrap, left_on="Id", right_on="InvoiceId", how="left")

    # Facility info (ensure consistent types)
    df["StmtBillingFacilityId"] = pd.to_numeric(df["StmtBillingFacilityId"], errors="coerce").astype("Int64")
    facilities["Id"] = pd.to_numeric(facilities["Id"], errors="coerce").astype("Int64")

    df = df.merge(
        facilities[["Id", "Name"]],
        left_on="StmtBillingFacilityId",
        right_on="Id",
        how="left"
    ).rename(columns={"Name": "FacilityName"})

    # Status info
    df = df.merge(claimstatuscodes[["code", "ShortDesc"]],
                  left_on="FileStatus", right_on="code", how="left")

    # Days since service date and last submit (if columns exist)
    if "ServiceDt" in df.columns:
        df["DaysFromDOS"] = (pd.Timestamp.today() - pd.to_datetime(df["ServiceDt"], errors="coerce")).dt.days
    if "LastStatusUpdate" in df.columns:
        df["DaysFromLastSubmit"] = (pd.Timestamp.today() - pd.to_datetime(df["LastStatusUpdate"], errors="coerce")).dt.days

    # Totals (ensure columns exist)
    for col in ["PrimaryPaid", "SecondaryPaid", "TertiaryPaid", "UnknownPaid"]:
        if col not in df.columns:
            df[col] = 0
    for col in ["MRW", "MDW", "WrapUndefined"]:
        if col not in df.columns:
            df[col] = 0

    df["PrimaryTotal"] = df[["PrimaryPaid", "SecondaryPaid", "TertiaryPaid", "UnknownPaid"]].sum(axis=1, skipna=True)
    df["WrapTotal"] = df[["MRW", "MDW", "WrapUndefined"]].sum(axis=1, skipna=True)

    return df


def test_claims():
    """Runs build_claims with extracted tables and shows 10 rows"""
    try:
        data = get_base_tables()

        claims = build_claims(
            data["dbo.edi_invoice"],
            data["dbo.ClaimInsurance_vw"],
            data["dbo.edi_inspayments"],
            data["dbo.edi_paymentdetail"],
            data["dbo.edi_inv_insurance"],
            data["dbo.insurance"],
            data["dbo.claimstatuscodes"],
            data["dbo.edi_facilities"]
        )

        print("✅ Claims dataset sample (10 rows):")
        print(claims.head(10))

    except Exception as e:
        print(f"❌ Error in test_claims: {e}")


if __name__ == "__main__":
    test_claims()

