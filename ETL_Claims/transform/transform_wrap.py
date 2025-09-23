import pandas as pd
from extract_data import get_base_tables  # importa las tablas extraídas

def build_wrap(edi_inv_insurance, insurance, paymentdetail):
    """
    Classifies wrap insurances into MRW, MDW, or WrapUndefined.
    Returns one row per invoice with aggregated amounts.
    """

    # Normalize insurance ID column for consistency  py -m transform.transform_wrap

    insurance = insurance.rename(columns={"insId": "InsId"})

    # Identify wrap insurances by name
    wrap_map = insurance[insurance["insuranceName"].str.contains("wrap|4028", case=False, na=False)]

    # Join insurance relation with payment details (using ClaimInsId)
    df = edi_inv_insurance.merge(paymentdetail, left_on="Id", right_on="ClaimInsId", how="inner")

    # Classification logic
    def classify(ins_id):
        if ins_id == 169:
            return "MRW"
        elif ins_id in wrap_map["InsId"].tolist():
            return "MDW"
        else:
            return "WrapUndefined"

    df["WrapClass"] = df["InsId"].apply(classify)

    # Pivot to aggregate amounts by invoice
    wrap = df.pivot_table(
        index="InvoiceId",
        columns="WrapClass",
        values="paid",
        aggfunc="sum",
        fill_value=0
    ).reset_index()

    return wrap


def test_wrap():
    """Runs build_wrap with extracted tables and shows 10 rows"""
    try:
        data = get_base_tables()

        wrap = build_wrap(
            data["dbo.edi_inv_insurance"],
            data["dbo.insurance"],
            data["dbo.edi_paymentdetail"]
        )

        print("✅ Wrap dataset sample (10 rows):")
        print(wrap.head(10))

    except Exception as e:
        print(f"❌ Error in test_wrap: {e}")


if __name__ == "__main__":
    test_wrap()
