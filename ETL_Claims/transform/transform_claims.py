import pandas as pd

def build_claims(invoice, claiminsurance_vw, payments, wrap,
                 claimstatuscodes, facilities, claimclassvalidation):
    """
    Genera el dataset final de claims (#ClaimResults)
    """

    df = invoice.merge(claiminsurance_vw, left_on="Id", right_on="ClaimId", how="left")
    df = df.merge(payments, left_on="Id", right_on="invoiceId", how="left")
    df = df.merge(wrap, left_on="Id", right_on="InvoiceId", how="left")
    df = df.merge(facilities[["Id", "Name"]], left_on="facilityId", right_on="Id", how="left")

    # Validación de clase
    df = df.merge(claimclassvalidation, left_on="Class", right_on="Class", how="left")

    # Status
    df = df.merge(claimstatuscodes[["code", "ShortDesc"]], left_on="FileStatus", right_on="code", how="left")

    # Días desde fecha de servicio y último submit
    df["DaysFromDOS"] = (pd.Timestamp.today() - pd.to_datetime(df["ServiceDt"])).dt.days
    df["DaysFromLastSubmit"] = (pd.Timestamp.today() - pd.to_datetime(df["LastStatusUpdate"])).dt.days

    # Totales
    df["PrimaryTotal"] = df[["PrimaryPaid", "SecondaryPaid", "TertiaryPaid", "UnknownPaid"]].sum(axis=1, skipna=True)
    df["WrapTotal"] = df[["MRW", "MDW", "WrapUndefined"]].sum(axis=1, skipna=True)

    return df