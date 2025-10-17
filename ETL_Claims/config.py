import pyodbc

# === Source database (On-prem LaSante) ===
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
    "Connection Timeout=60;"
)

# === Destination database (Azure SQL Server) ===
DST_SERVER = "lst-svr-sql02.database.windows.net"
DST_DATABASE = "sigma_db"
DST_USER = "dw_juan"
DST_PASSWORD = "dw@J!597"

# Using ODBC Driver 18 for better TLS compatibility with Azure SQL
DST_CONN_STR = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    f"SERVER={DST_SERVER};"
    f"DATABASE={DST_DATABASE};"
    f"UID={DST_USER};"
    f"PWD={DST_PASSWORD};"
    "Encrypt=yes;"
    "TrustServerCertificate=no;"
    "Connection Timeout=60;"
)

def test_connection(name: str, conn_str: str):
    """
    Generic connection test function.
    Prints the result of the connection attempt.
    """
    try:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT GETDATE();")
            result = cursor.fetchone()
            print(f"✅ {name} connected successfully → {result[0]}")
    except Exception as e:
        print(f"❌ {name} connection failed → {e}")

if __name__ == "__main__":
    print("🔍 Testing database connections...\n")

    # Test source (on-prem)
    test_connection("SOURCE (mobiledoc)", SRC_CONN_STR)

    # Test destination (Azure)
    test_connection("DESTINATION (sigma_db)", DST_CONN_STR)

    print("\n🏁 Connection tests completed.")
