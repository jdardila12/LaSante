import pyodbc

# === SOURCE (on-prem LaSante) ===
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
    "Connection Timeout=30;"
)

# === DESTINATION (Azure SQL Server) ===
DST_SERVER = "lst-svr-sql02.database.windows.net"
DST_DATABASE = "sigma_db"
DST_USER = "dw_juan"
DST_PASSWORD = "dw@J!597"

DST_CONN_STR = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    f"SERVER={DST_SERVER},1433;"
    f"DATABASE={DST_DATABASE};"
    f"UID={DST_USER};"
    f"PWD={DST_PASSWORD};"
    "Encrypt=yes;"
    "TrustServerCertificate=no;"
    "Connection Timeout=30;"
)

def test_connection(name, conn_str):
    """Try connection and print result."""
    print(f"🔹 Testing {name}...")
    try:
        with pyodbc.connect(conn_str) as conn:
            cur = conn.cursor()
            cur.execute("SELECT GETDATE();")
            row = cur.fetchone()
            print(f"   ✅ {name} SUCCESS — Connected at {row[0]}\n")
    except Exception as e:
        print(f"   ❌ {name} FAIL — {e}\n")

if __name__ == "__main__":
    print("🔍 Simple Connection Test (Source & Azure)\n")

    test_connection("SOURCE (mobiledoc)", SRC_CONN_STR)
    test_connection("DESTINATION (sigma_db)", DST_CONN_STR)

    print("🏁 Test completed.")


