import pyodbc
import os

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

# === Output folder for CSV exports ===
EXPORT_PATH = r"C:\Users\57310\Desktop\LASANTE\LaSante\Images"
os.makedirs(EXPORT_PATH, exist_ok=True)

# Quick connection test (optional)
if __name__ == "__main__":
    try:
        conn = pyodbc.connect(CONN_STR)
        print("✅ Connection successful")
        conn.close()
    except Exception as e:
        print("❌ Connection failed:", e)
