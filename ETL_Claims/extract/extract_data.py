import pyodbc
import pandas as pd
from config import CONN_STR

# Queries específicas con filtro de fechas
QUERIES = {
    # Tablas grandes -> filtramos entre 2025-09-01 y hoy
    "dbo.edi_invoice": """
        SELECT * 
        FROM dbo.edi_invoice
        WHERE DeleteFlag = 0
          AND InvDate BETWEEN '2025-09-01' AND GETDATE()
    """,
    "dbo.enc": """
        SELECT * 
        FROM dbo.enc
        WHERE STATUS = 'CHK' 
          AND deleteFlag = 0
          AND [date] BETWEEN '2025-09-01' AND GETDATE()
    """,
    "dbo.edi_inspayments": """
        SELECT * 
        FROM dbo.edi_inspayments
        WHERE PaymentDate BETWEEN '2025-09-01' AND GETDATE()
    """,
    "dbo.edi_paymentdetail": """
        SELECT * 
        FROM dbo.edi_paymentdetail
        WHERE PaymentDate BETWEEN '2025-09-01' AND GETDATE()
    """,

    # Tablas pequeñas -> sin filtro
    "dbo.ClaimInsurance_vw": "SELECT * FROM dbo.ClaimInsurance_vw",
    "dbo.doctors": "SELECT * FROM dbo.doctors",
    "dbo.annualnotes": "SELECT * FROM dbo.annualnotes",
    "dbo.edi_inv_claimstatus_log": "SELECT * FROM dbo.edi_inv_claimstatus_log",
    "dbo.claimstatuscodes": "SELECT * FROM dbo.claimstatuscodes",
    "dbo.edi_inv_log": "SELECT * FROM dbo.edi_inv_log",
    "dbo.edi_inv_cpt": "SELECT * FROM dbo.edi_inv_cpt",
    "dbo.insurance": "SELECT * FROM dbo.insurance",
    "dbo.insgroups": "SELECT * FROM dbo.insgroups",
    "dbo.insgroupmembers": "SELECT * FROM dbo.insgroupmembers",
    "dbo.ClaimClassValidation": "SELECT * FROM dbo.ClaimClassValidation",
    "dbo.edi_inv_insurance": "SELECT * FROM dbo.edi_inv_insurance",
    "dbo.edi_facilities": "SELECT * FROM dbo.edi_facilities",
    "dbo.groupdetails": "SELECT * FROM dbo.groupdetails",
}


def get_table(name: str) -> pd.DataFrame:
    """Ejecuta la query de una tabla y devuelve DataFrame"""
    with pyodbc.connect(CONN_STR) as conn:
        return pd.read_sql(QUERIES[name], conn)


def get_base_tables():
    """Extrae todas las tablas definidas en QUERIES"""
    data = {}
    for table, query in QUERIES.items():
        try:
            df = get_table(table)
            data[table] = df
        except Exception as e:
            print(f"❌ Error extrayendo {table}: {e}")
    return data


def test_extract():
    """Valida conexión y muestra solo las primeras 2 filas de cada tabla"""
    try:
        with pyodbc.connect(CONN_STR) as conn:
            print("✅ Conexión exitosa a SQL Server\n")
            for table in QUERIES.keys():
                try:
                    df = pd.read_sql(f"SELECT TOP 2 * FROM {table}", conn)
                    print(f"📌 {table}: {len(df)} filas, {len(df.columns)} columnas")
                    print(df.head(2))
                    print("-" * 40)
                except Exception as e:
                    print(f"❌ Error accediendo a {table}: {e}")
    except Exception as e:
        print("❌ Error al conectar:", e)


if __name__ == "__main__":
    test_extract()
