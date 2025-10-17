import pyodbc

# === Source database (LaSante on-prem SQL Server) ===
SRC_SERVER = "ecw-db.lasantehealth.org"
SRC_DATABASE = "mobiledoc"
SRC_USER = "Temp-JArdila"
SRC_PASSWORD = "]@zzySeal59"

SRC_CONN_STR = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    f"SERVER={SRC_SERVER};"
    f"DATABASE={SRC_DATABASE};"
    f"UID={SRC_USER};"
    f"PWD={SRC_PASSWORD};"
)

# === Destination database (Azure SQL Server) ===
DST_SERVER = "lst-svr-sql02.database.windows.net"
DST_DATABASE = "sigma_db"
DST_USER = "dw_juan"
DST_PASSWORD = "dw@J!597"

DST_CONN_STR = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    f"SERVER={DST_SERVER};"
    f"DATABASE={DST_DATABASE};"
    f"UID={DST_USER};"
    f"PWD={DST_PASSWORD};"
)

# === Quick connection test (optional) ===
if __name__ == "__main__":
    try:
        conn_src = pyodbc.connect(SRC_CONN_STR)
        print("✅ Successfully connected to source (mobiledoc)")
        conn_src.close()

        conn_dst = pyodbc.connect(DST_CONN_STR)
        print("✅ Successfully connected to destination (sigma_db)")
        conn_dst.close()
    except Exception as e:
        print("❌ Connection error:", e)
