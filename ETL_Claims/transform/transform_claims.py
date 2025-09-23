
import pandas as pd
from extract.extract_data import get_base_tables
from transform.transform_payments import build_payments
from transform.transform_wrap import build_wrap


def build_claims(invoice, claiminsurance_vw, edi_inspayments, edi_paymentdetail,
                 edi_inv_insurance, insurance, claimstatuscodes, facilities, claimclassvalidation):
    """
    Builds the final claims dataset (#ClaimResults).
    Combines invoice, insurance, payments, wrap, facility, status and class validation.
    """

    # Build payments dataset
    payments = build_payments(
        edi_inspayments,
        edi_paymentdetail,
        invoice,
        claiminsurance_vw
    )

    # Build wrap dataset
    wrap = build_wrap(
        edi_inv_insurance,
        insurance,
        edi_paymentdetail
    )

    # Start with invoice + claiminsurance
    df = invoice.merge(claiminsurance_vw, left_on="Id", right_on="ClaimId", how="left")

    # Join with payments
    df = df.merge(payments, left_on="Id", right_on="invoiceId", how="left")

    # Join with wrap
    df = df.merge(wrap, left_on="Id", right_on="InvoiceId", how="left")

    # Facility info
    df = df.merge(facilities[["Id", "Name"]], left_on="facilityId", right_on="Id", how="left")
    df = df.rename(columns={"Name": "FacilityName"})

    # Class validation
    df = df.merge(claimclassvalidation, on="Class", how="left")

    # Claim status
    df = df.merge(claimstatuscodes[["code", "ShortDesc"]], left_on="FileStatus", right_on="code", how="left")

    # Days since service and last submit
    df["DaysFromDOS"] = (pd.Timestamp.today() - pd.to_datetime(df["ServiceDt"], errors="coerce")).dt.days
    df["DaysFromLastSubmit"] = (pd.Timestamp.today() - pd.to_datetime(df["LastStatusUpdate"], errors="coerce")).dt.days

    # Totals
    df["PrimaryTotal"] = df[["PrimaryPaid", "SecondaryPaid", "TertiaryPaid", "UnknownPaid"]].sum(axis=1, skipna=True)
    df["WrapTotal"] = df[["MRW", "MDW", "WrapUndefined"]].sum(axis=1, skipna=True)

    # Ensure mandatory columns exist
    for col in ["PrimaryTotal", "WrapTotal"]:
        if col not in df.columns:
            df[col] = 0

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
            data["dbo.edi_facilities"],
            data["dbo.ClaimClassValidation"]
        )

        print("✅ Claims dataset sample (10 rows):")
        print(claims.head(10))
        print("\nColumns:", claims.columns.tolist())

    except Exception as e:
        print(f"❌ Error in test_claims: {e}")


if __name__ == "__main__":
    test_claims()
