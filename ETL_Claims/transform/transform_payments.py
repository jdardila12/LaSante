import pandas as pd
from extract.extract_data import get_base_tables

def build_payments(edi_inspayments, edi_paymentdetail, edi_invoice, claiminsurance_vw):
    """
    Builds the payments pivot (#Payments).
    Classifies amounts into PrimaryPaid, SecondaryPaid, TertiaryPaid, UnknownPaid.
    """

    # Join payments header with details
    df = edi_inspayments.merge(edi_paymentdetail, on="paymentId", how="inner")

    # Add insurance mapping (Primary / Secondary / Tertiary)
    df = df.merge(claiminsurance_vw, left_on="invoiceId", right_on="ClaimId", how="left")

    # Classify payment type
    def classify(row):
        if row["ClaimInsId"] == row["PrimaryInsId"]:
            return "PrimaryPaid"
        elif row["ClaimInsId"] == row["SecondaryInsId"]:
            return "SecondaryPaid"
        elif row["ClaimInsId"] == row["TertiaryInsId"]:
            return "TertiaryPaid"
        else:
            return "UnknownPaid"

    df["PaymentClass"] = df.apply(classify, axis=1)

    # Pivot to aggregate by invoice
    payments = df.pivot_table(
        index="invoiceId",
        columns="PaymentClass",
        values="paid",
        aggfunc="sum",
        fill_value=0
    ).reset_index()

    return payments


def test_payments():
    """Runs build_payments with extracted tables and shows 10 rows"""
    try:
        data = get_base_tables()

        payments = build_payments(
            data["dbo.edi_inspayments"],
            data["dbo.edi_paymentdetail"],
            data["dbo.edi_invoice"],
            data["dbo.ClaimInsurance_vw"]
        )

        print("✅ Payments dataset sample (10 rows):")
        print(payments.head(10))

    except Exception as e:
        print(f"❌ Error in test_payments: {e}")


if __name__ == "__main__":
    test_payments()
