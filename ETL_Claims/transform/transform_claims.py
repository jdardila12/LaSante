import pandas as pd
from extract.extract_data import get_base_tables
from transform.transform_payments import build_payments
from transform.transform_wrap import build_wrap


def build_claims(invoice, claiminsurance_vw, payments, wrap,
                 claimstatuscodes, facilities, claimclassvalidation,
                 claimstatus_log):
    """
    Builds the final claims dataset (#ClaimResults).
    """

    # Base
    df = invoice.merge(claiminsurance_vw, left_on="Id", right_on="ClaimId", how="left")
    df = df.merge(payments, left_on="Id", right_on="invoiceId", how="left")
    df = df.merge(wrap, left_on="Id", right_on="InvoiceId", how="left")

    # Facilities (rename to avoid Id duplication)
    fac = facilities.rename(columns={"Id": "FacilityId", "Name": "FacilityName"})
    df = df.merge(fac[["FacilityId", "FacilityName"]], left_on="facilityId", right_on="FacilityId", how="left")

    # Claim class validation
    df = df.merge(claimclassvalidation, on="Class", how="left")

    # Status description
    df = df.merge(claimstatuscodes[["code", "ShortDesc"]], left_on="FileStatus", right_on="code", how="left")

    # Last status update from claimstatus_log
    last_status = (
        claimstatus_log.groupby("InvId")["date"]
        .max()
        .reset_index()
        .rename(columns={"date": "LastStatusUpdate"})
    )
    df = df.merge(last_status, left_on="Id", right_on="InvId", how="left")

    # Days from dates
    df["DaysFromDOS"] = (pd.Timestamp.today() - pd.to_datetime(df["ServiceDt"], errors="coerce")).dt.days
    df["DaysFromLastSubmit"] = (pd.Timestamp.today() - pd.to_datetime(df["LastStatusUpdate"], errors="coerce")).dt.days

    # Totals
    df["PrimaryTotal"] = df[["PrimaryPaid", "SecondaryPaid", "TertiaryPaid", "UnknownPaid"]].sum(axis=1, skipna=True)
    df["WrapTotal"] = df[["MRW", "MDW", "WrapUndefined"]].sum(axis=1, skipna=True)

    return df


def test_claims():
    """Runs build_claims with extracted tables and shows 10 rows"""
    try:
        data = get_base_tables()

        # Build helper datasets
        payments = build_payments(
            data["dbo.edi_inspayments"],
            data["dbo.edi_paymentdetail"]
        )
        wrap = build_wrap(
            data["dbo.edi_inv_insurance"],
            data["dbo.insurance"],
            data["dbo.edi_paymentdetail"]
        )

        # Build claims
        claims = build_claims(
            data["dbo.edi_invoice"],
            data["dbo.ClaimInsurance_vw"],
            payments,
            wrap,
            data["dbo.claimstatuscodes"],
            data["dbo.edi_facilities"],
            data["dbo.ClaimClassValidation"],
            data["dbo.edi_inv_claimstatus_log"]
        )

        print("✅ Claims dataset sample (10 rows):")
        print(claims.head(10))

    except Exception as e:
        print(f"❌ Error in test_claims: {e}")


if __name__ == "__main__":
    test_claims()
