import pyodbc
import os

# === SOURCE (On-prem LaSante) ===
SRC_SERVER = os.getenv("SRC_SERVER", "ecw-db.lasantehealth.org")
SRC_DATABASE = os.getenv("SRC_DATABASE", "mobiledoc")
SRC_USER = os.getenv("SRC_USER", "Temp-JArdila")
SRC_PASSWORD = os.getenv("SRC_PASSWORD", "]@zzySeal59")

SRC_CONN_STR = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    f"SERVER={SRC_SERVER};"
    f"DATABASE={SRC_DATABASE};"
    f"UID={SRC_USER};"
    f"PWD={SRC_PASSWORD};"
    "Connection Timeout=30;"
)

# === DESTINATION (Azure SQL Server) ===
DST_SERVER = os.getenv("DST_SERVER", "lst-svr-sql02.database.windows.net")
DST_DATABASE = os.getenv("DST_DATABASE", "sigma_db")
DST_USER = os.getenv("DST_USER", "dw_juan")
DST_PASSWORD = os.getenv("DST_PASSWORD", "dw@J!597")

DST_CONN_STR = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    f"SERVER=tcp:{DST_SERVER},1433;"
    f"DATABASE={DST_DATABASE};"
    f"UID={DST_USER};"
    f"PWD={DST_PASSWORD};"
    "Encrypt=yes;"
    "TrustServerCertificate=no;"
    "Connection Timeout=60;"
)


def test_connection(name: str, conn_str: str) -> bool:
    """Try to connect and return True if successful, False otherwise."""
    try:
        with pyodbc.connect(conn_str) as conn:
            conn.cursor().execute("SELECT 1;")
            print(f"✅ {name} connection successful.")
            return True
    except Exception as e:
        print(f"❌ {name} connection failed: {e}")
        return False


if __name__ == "__main__":
    print("🔍 Testing database connections...\n")

    src_ok = test_connection("SOURCE (LaSante)", SRC_CONN_STR)
    dst_ok = test_connection("DESTINATION (Azure Sigma)", DST_CONN_STR)

    print("\n🏁 Test completed.")


