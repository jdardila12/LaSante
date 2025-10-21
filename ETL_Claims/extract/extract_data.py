import pyodbc
import pandas as pd
from datetime import datetime
from config import CONN_STR

# Dates for filtering
START_DATE = "2025-01-01"
END_DATE = datetime.today().strftime("%Y-%m-%d")

# Large tables (require date filtering)
TABLE_DATE_FILTERS = {
    "dbo.edi_invoice": "InvoiceDt",
    "dbo.enc": "date",
    "dbo.annualnotes": None,  # related to enc, for now full load
    "dbo.edi_inv_claimstatus_log": "date",
    "dbo.edi_inv_log": "date",
    "dbo.edi_inv_cpt": None,  # related to edi_invoice
    "dbo.edi_inspayments": "checkDate",
    "dbo.edi_paymentdetail": "timestamp",
    "dbo.edi_inv_insurance": None,  # related to edi_invoice
}

# Static tables (can be fully loaded)
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

# Final list of tables to extract
TABLES = list(TABLE_DATE_FILTERS.keys()) + TABLES_STATIC


def get_table(table: str) -> pd.DataFrame:
    """Extracts a table, applying date filters if available."""
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
    """Extracts all tables defined in TABLES."""
    data = {}
    for table in TABLES:
        try:
            df = get_table(table)
            data[table] = df
            print(f"{table}: {len(df)} rows extracted")
        except Exception as e:
            print(f"Error on {table}: {e}")
    return data


def test_extract():
    """Checks connection and prints configured tables."""
    try:
        with pyodbc.connect(CONN_STR):
            print("Successful connection to SQL Server")
            print("Configured tables for extraction:")
            for table in TABLES:
                if TABLE_DATE_FILTERS.get(table):
                    print(f"   - {table} (date filtered)")
                else:
                    print(f"   - {table} (full load)")
    except Exception as e:
        print(f"Connection error: {e}")


if __name__ == "__main__":
    test_extract()
    # To run the full extraction, uncomment:
    # data = get_base_tables()
