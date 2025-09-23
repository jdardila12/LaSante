import pandas as pd
from datetime import datetime

def build_invoice(edi_invoice, enc, doctors, annualnotes, inv_claimstatus_log):
    """
    Replica la tabla temporal #invoice del SQL
    Filtrando facturas entre '2025-01-01' y hoy.
    """

    # Fecha inicial fija (igual al SQL original)
    start_date = pd.to_datetime("2025-06-01")
    # Fecha final dinámica (hoy)
    today = pd.to_datetime(datetime.today().date())

    # Join invoice con encounter y doctors
    df = edi_invoice.merge(enc, left_on="EncounterId", right_on="encounterID", suffixes=("", "_enc"))
    df = df.merge(doctors, on="doctorID", how="left")

    # Filtro de fechas y deleteflag
    df = df[
        (pd.to_datetime(df["ServiceDt"]) >= start_date) &
        (pd.to_datetime(df["ServiceDt"]) <= today) &
        (df["DeleteFlag"] == 0)
    ].copy()

    # ===== SameDayVisit =====
    same_day = (
        edi_invoice.merge(enc, left_on="EncounterId", right_on="encounterID")
        .query("DeleteFlag == 0 and STATUS == 'CHK'")
        .groupby(["PatientId", "ServiceDt"])["EncounterId"]
        .apply(lambda x: " ".join(x.astype(str)))
        .reset_index()
    )
    same_day.rename(columns={"EncounterId": "SameDayVisit"}, inplace=True)
    df = df.merge(same_day, on=["PatientId", "ServiceDt"], how="left")

    # ===== Flipped claims =====
    flipped = annualnotes.query("type == 'billing' and notes.str.contains('flipped claim')", engine="python")
    df.loc[df["EncounterId"].isin(flipped["encounterId"]), ["Id", "SplitClaimId"]] = \
        df.loc[df["EncounterId"].isin(flipped["encounterId"]), ["SplitClaimId", "Id"]].values

    # ===== LastStatusUpdate =====
    last_status = (
        inv_claimstatus_log.groupby("InvId")["date"].max().reset_index()
        .rename(columns={"InvId": "Id", "date": "LastStatusUpdate"})
    )
    df = df.merge(last_status, on="Id", how="left")

    return df