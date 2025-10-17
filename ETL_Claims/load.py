import pandas as pd
from sqlalchemy import create_engine
from config import DST_SERVER, DST_DATABASE, DST_USER, DST_PASSWORD

def get_engine():
    """
    Create a SQLAlchemy engine for the Azure SQL Server connection.
    """
    connection_string = (
        f"mssql+pyodbc://{DST_USER}:{DST_PASSWORD}@{DST_SERVER}/{DST_DATABASE}"
        "?driver=ODBC+Driver+17+for+SQL+Server"
    )
    return create_engine(connection_string)

def load_to_sql(df: pd.DataFrame, table_name: str, if_exists: str = "replace"):
    """
    Load a pandas DataFrame into an Azure SQL Server table.

    Parameters:
    - df: pandas DataFrame to load
    - table_name: target table name in Azure SQL Server
    - if_exists: behavior when the table already exists
        'fail'    → raise an error
        'replace' → drop the table and recreate it
        'append'  → insert new rows into the existing table
    """
    try:
        engine = get_engine()
        df.to_sql(table_name, con=engine, if_exists=if_exists, index=False)
        print(f"✅ Tabla '{table_name}' cargada exitosamente en {DST_DATABASE} ({len(df)} filas).")
    except Exception as e:
        print(f"❌ Error'{table_name}': {e}")
