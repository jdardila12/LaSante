from extract.extract_data import get_base_tables
from transform.transform_invoice import build_invoice
from transform.transform_insurance import build_claiminsurance
from transform.transform_payments import build_payments
from transform.transform_wrap import build_wrap
from transform.transform_claims import build_claims


def main():
    print("🚀 Iniciando ETL de Claims\n")

    # 1. Extract
    print("📥 Extrayendo tablas base desde SQL Server...")
    base = get_base_tables()
    print(f"✅ Se cargaron {len(base)} tablas\n")

    # 2. Transform - Invoice
    print("🧾 Transformando facturas (invoice)...")
    invoice = build_invoice(
        base["edi_invoice"],
        base["enc"],
        base["doctors"],
        base["annualnotes"],
        base["inv_claimstatus_log"]
    )
    print(f"   -> Facturas procesadas: {len(invoice)} registros\n")

    # 3. Transform - Insurance
    print("🛡️ Procesando seguros...")
    insurance = build_claiminsurance(
        base["ClaimInsurance_vw"],
        base["insurance"],
        base["edi_inv_insurance"]
    )
    print(f"   -> Seguros procesados: {len(insurance)} registros\n")

    # 4. Transform - Payments
    print("💵 Procesando pagos...")
    payments = build_payments(base["edi_inspayments"], base["edi_paymentdetail"])
    print(f"   -> Pagos procesados: {len(payments)} registros\n")

    # 5. Transform - Wrap
    print("📦 Consolidando wrap...")
    wrap = build_wrap(invoice, insurance, payments)
    print(f"   -> Wrap consolidado: {len(wrap)} registros\n")

    # 6. Transform - Claims
    print("📑 Generando claims finales...")
    claims = build_claims(
        wrap,
        base["claimstatuscodes"],
        base["edi_facilities"],
        base["ClaimClassValidation"]
    )
    print(f"✅ Claims consolidados: {len(claims)} registros\n")

    # 7. Guardar en CSV
    claims.to_csv("ClaimResults.csv", index=False)
    print("📂 Archivo ClaimResults.csv generado.")


if __name__ == "__main__":
    main()