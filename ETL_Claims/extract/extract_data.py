import pyodbc
import pandas as pd
from config import CONN_STR

# Tablas que realmente usamos en el ETL
TABLES = [
    "dbo.ClaimInsurance_vw",
    "dbo.edi_invoice",
    "dbo.enc",
    "dbo.doctors",
    "dbo.annualnotes",
    "dbo.edi_inv_claimstatus_log",
    "dbo.claimstatuscodes",
    "dbo.edi_inv_log",
    "dbo.edi_inv_cpt",
    "dbo.insurance",
    "dbo.insgroups",
    "dbo.insgroupmembers",
    "dbo.ClaimClassValidation",
    "dbo.edi_inspayments",
    "dbo.edi_paymentdetail",
    "dbo.edi_inv_insurance",
    "dbo.edi_facilities",
    "dbo.groupdetails",
]


def get_table(name: str) -> pd.DataFrame:
    """Extrae una tabla completa desde SQL Server"""
    with pyodbc.connect(CONN_STR) as conn:
        return pd.read_sql(f"SELECT * FROM {name}", conn)


def get_base_tables():
    """Extrae todas las tablas de la lista TABLES"""
    data = {}
    for table in TABLES:
        try:
            df = get_table(table)
            data[table] = df
        except Exception as e:
            print(f"❌ Error extrayendo {table}: {e}")
    return data


def test_extract():
    """Prueba la conexión y extrae las 2 primeras filas de cada tabla"""
    try:
        with pyodbc.connect(CONN_STR) as conn:
            print("✅ Conexión exitosa a SQL Server\n")
            for table in TABLES:
                try:
                    query = f"SELECT TOP 2 * FROM {table}"
                    df = pd.read_sql(query, conn)
                    print(f"📌 {table}: {len(df)} filas, {len(df.columns)} columnas")
                    print(df.head(2))  # muestra las primeras 2 filas
                    print("-" * 40)
                except Exception as e:
                    print(f"❌ Error accediendo a {table}: {e}")
    except Exception as e:
        print("❌ Error al conectar:", e)


if __name__ == "__main__":
    test_extract()


