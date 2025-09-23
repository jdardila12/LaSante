def build_claiminsurance(claiminsurance_vw, insurance):
    """
    Replica la lógica de #ClaimInsurance_vw del SQL:
    - Quita seguros tipo wrap
    - Ajusta Secundario/Terciario
    """
    # Detectar seguros tipo wrap
    wrap_ids = insurance.loc[insurance["insuranceName"].str.contains("wrap", case=False, na=False), "insId"].tolist()

    df = claiminsurance_vw.copy()

    # Eliminar secundario/terciario si son wrap
    df["SecondaryInsId"] = df["SecondaryInsId"].apply(lambda x: None if x in wrap_ids else x)
    df["TertiaryInsId"] = df["TertiaryInsId"].apply(lambda x: None if x in wrap_ids else x)

    # Ajustar: si secundario es nulo, usar terciario
    df["SecondaryInsId"] = df["SecondaryInsId"].fillna(df["TertiaryInsId"])
    df["TertiaryInsId"] = df["TertiaryInsId"].where(df["TertiaryInsId"] != df["SecondaryInsId"])

    return df