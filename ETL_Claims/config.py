import pyodbc

DB_SERVER = "ecw-db.lasantehealth.org"
DB_DATABASE = "mobiledoc"
DB_USER = "Temp-JArdila"
DB_PASS = "]@zzySeal59"

CONN_STR = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    f"SERVER={DB_SERVER};"
    f"DATABASE={DB_DATABASE};"
    f"UID={DB_USER};"
    f"PWD={DB_PASS};"
)

try:
    conn = pyodbc.connect(CONN_STR)
    print("✅")
    conn.close()
except:
    print("❌")