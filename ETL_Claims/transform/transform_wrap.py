def build_wrap(edi_inv_insurance, insurance, payments):
    """
    Clasifica seguros wrap en MRW, MDW o WrapUndefined
    """
    wrap_map = insurance[insurance["insuranceName"].str.contains("wrap|4028", case=False, na=False)]

    df = edi_inv_insurance.merge(payments, left_on="InvoiceId", right_on="invoiceId", how="inner")

    def classify(ins_id):
        if ins_id == 169:
            return "MRW"
        elif ins_id in wrap_map["insId"].tolist():
            return "MDW"
        else:
            return "WrapUndefined"

    df["WrapClass"] = df["InsId"].apply(classify)

    wrap = df.pivot_table(
        index="InvoiceId",
        columns="WrapClass",
        values="paid",
        aggfunc="sum",
        fill_value=0
    ).reset_index()

    return wrap