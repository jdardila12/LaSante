import pandas as pd
from extract.extract_data import get_base_tables
from transform.transform_invoice import build_invoice
from transform.transform_claiminsurance import build_claiminsurance
from transform.transform_payments import build_payments
from transform.transform_wrap import build_wrap
from transform.transform_claims import build_claims
from load.load_data import save_to_csv


def main():
    print("=== Starting ETL Process ===")

    # 1. Extract
    print("Extracting base tables...")
    data = get_base_tables()

    # 2. Transform: Invoice
    print("Building invoice...")
    invoice = build_invoice(
        data["dbo.edi_invoice"],
        data["dbo.enc"],
        data["dbo.doctors"],
        data["dbo.annualnotes"],
        data["dbo.edi_inv_claimstatus_log"]
    )

    # 3. Transform: ClaimInsurance (cleaning wrap insurance)
    print("Building claim insurance...")
    claiminsurance = build_claiminsurance(
        data["dbo.ClaimInsurance_vw"],
        data["dbo.insurance"]
    )

    # 4. Transform: Payments
    print("Building payments...")
    payments = build_payments(
        data["dbo.edi_inspayments"],
        data["dbo.edi_paymentdetail"],
        invoice,
        claiminsurance
    )

    # 5. Transform: Wrap
    print("Building wrap...")
    wrap = build_wrap(
        data["dbo.edi_inv_insurance"],
        data["dbo.insurance"],
        data["dbo.edi_paymentdetail"]
    )

    # 6. Transform: Final Claims
    print("Building claims...")
    claims = build_claims(
        invoice,
        claiminsurance,
        data["dbo.edi_inspayments"],
        data["dbo.edi_paymentdetail"],
        data["dbo.edi_inv_insurance"],
        data["dbo.insurance"],
        data["dbo.claimstatuscodes"],
        data["dbo.edi_facilities"]
    )

    # 7. Load: Save claims dataset to CSV (chunks of 2000 rows)
    print("Saving final claims dataset...")
    save_to_csv(claims, "claims", max_rows=2000)

    print("✅ ETL Process finished successfully!")


if __name__ == "__main__":
    main()
