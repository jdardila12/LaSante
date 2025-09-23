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


def describe_table_schema(table: str):
    """Devuelve las columnas y sus tipos de datos para una tabla específica"""
    query = f"""
        SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = '{table.split('.')[-1]}'
    """
    try:
        with pyodbc.connect(CONN_STR) as conn:
            df = pd.read_sql(query, conn)
            return df
    except Exception as e:
        print(f"❌ Error obteniendo esquema de {table}: {e}")
        return None


def get_all_schemas():
    """Devuelve un diccionario con esquema (campos y tipos) de todas las tablas"""
    schemas = {}
    for table in TABLES:
        schema_df = describe_table_schema(table)
        if schema_df is not None:
            schemas[table] = schema_df
    return schemas


def count_rows(table: str) -> int:
    """Devuelve el número de filas de una tabla"""
    query = f"SELECT COUNT(*) AS total FROM {table}"
    try:
        with pyodbc.connect(CONN_STR) as conn:
            df = pd.read_sql(query, conn)
            return int(df["total"].iloc[0])
    except Exception as e:
        print(f"❌ Error contando filas en {table}: {e}")
        return -1


def get_all_counts():
    """Devuelve un DataFrame con el conteo de filas por tabla"""
    counts = []
    for table in TABLES:
        total = count_rows(table)
        counts.append({"Tabla": table, "Filas": total})
    return pd.DataFrame(counts)


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

    # 🔎 Esquemas de tablas
    print("\n📋 Esquemas de tablas:")
    all_schemas = get_all_schemas()
    for table, schema in all_schemas.items():
        print(f"\n📌 {table}")
        print(schema)

    # 📊 Conteo de filas
    print("\n📊 Conteo de filas por tabla:")
    counts_df = get_all_counts()
    print(counts_df)

