def build_payments(edi_inspayments, edi_paymentdetail, edi_invoice, claiminsurance_vw):
    """
    Replica el pivot de #Payments:
    Clasifica pagos en PrimaryPaid, SecondaryPaid, TertiaryPaid, UnknownPaid
    """
    df = edi_inspayments.merge(edi_paymentdetail, left_on="paymentId", right_on="paymentId")
    df = df.merge(edi_invoice[["Id"]], left_on="invoiceId", right_on="Id")

    # Agregar columnas de insurance
    df = df.merge(claiminsurance_vw, left_on="invoiceId", right_on="ClaimId")

    # Clasificar pagos
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

    # Pivot
    payments = df.pivot_table(
        index="invoiceId",
        columns="PaymentClass",
        values="paid",
        aggfunc="sum",
        fill_value=0
    ).reset_index()

    return payments