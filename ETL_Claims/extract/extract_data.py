import pyodbc
import pandas as pd
from datetime import datetime
from config import CONN_STR

# 📅 Fechas para filtros
START_DATE = "2025-09-01"
END_DATE = datetime.today().strftime("%Y-%m-%d")

# 📋 Tablas grandes (requieren filtro de fechas)
TABLE_DATE_FILTERS = {
    "dbo.edi_invoice": "InvoiceDt",
    "dbo.enc": "date",
    "dbo.annualnotes": None,  # depende de enc, por ahora completa
    "dbo.edi_inv_claimstatus_log": "date",
    "dbo.edi_inv_log": "date",
    "dbo.edi_inv_cpt": None,  # depende de edi_invoice
    "dbo.edi_inspayments": "checkDate",
    "dbo.edi_paymentdetail": "timestamp",
    "dbo.edi_inv_insurance": None,  # depende de edi_invoice
}

# 📋 Tablas maestras (se cargan completas)
TABLES_STATIC = [
    "dbo.ClaimInsurance_vw",
    "dbo.doctors",
    "dbo.claimstatuscodes",
    "dbo.insurance",
    "dbo.insgroups",
    "dbo.insgroupmembers",
    "dbo.ClaimClassValidation",
    "dbo.groupdetails",
    "dbo.edi_facilities",
]

# ✅ Todas las tablas que usaremos
TABLES = list(TABLE_DATE_FILTERS.keys()) + TABLES_STATIC


def get_table(table: str) -> pd.DataFrame:
    """Extrae una tabla, con filtro de fechas si aplica."""
    col = TABLE_DATE_FILTERS.get(table)
    with pyodbc.connect(CONN_STR) as conn:
        if col:
            query = f"""
                SELECT * 
                FROM {table}
                WHERE {col} >= '{START_DATE}' AND {col} <= '{END_DATE}'
            """
        else:
            query = f"SELECT * FROM {table}"
        return pd.read_sql(query, conn)


def get_base_tables():
    """Extrae todas las tablas definidas en TABLES."""
    data = {}
    for table in TABLES:
        try:
            df = get_table(table)
            data[table] = df
            print(f"✅ {table}: {len(df)} filas extraídas")
        except Exception as e:
            print(f"❌ Error en {table}: {e}")
    return data


def test_extract():
    """Verifica conexión y lista las tablas configuradas."""
    try:
        with pyodbc.connect(CONN_STR):
            print("✅ Conexión exitosa a SQL Server")
            print("📋 Tablas configuradas para extracción:")
            for table in TABLES:
                if TABLE_DATE_FILTERS.get(table):
                    print(f"   - {table} (filtrado por fechas)")
                else:
                    print(f"   - {table} (completa)")
    except Exception as e:
        print(f"❌ Error al conectar: {e}")


if __name__ == "__main__":
    test_extract()
    # 🔽 Para correr la extracción completa, descomenta:
    # data = get_base_tables()

