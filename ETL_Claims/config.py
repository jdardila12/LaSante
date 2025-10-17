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
    "Connection Timeout=30;"
)

# === Destination database (Azure SQL) ===
DST_SERVER = "lst-svr-sql02.database.windows.net"
DST_DATABASE = "sigma_db"
DST_USER = "dw_juan"
DST_PASSWORD = "dw@J!597"

DST_CONN_STR = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    f"SERVER={DST_SERVER};"
    f"DATABASE={DST_DATABASE};"
    f"UID={DST_USER};"
    f"PWD={DST_PASSWORD};"
    "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
)

def test_connection(name: str, conn_str: str) -> bool:
    """Try to connect and return True if successful, False otherwise."""
    try:
        with pyodbc.connect(conn_str) as conn:
            return True
    except Exception:
        return False

if __name__ == "__main__":
    print("🔍 Testing connections...\n")

    src_ok = test_connection("SOURCE", SRC_CONN_STR)
    dst_ok = test_connection("DESTINATION", DST_CONN_STR)

    # Results summary
    print(f"SOURCE CONNECTION: {'✅ SUCCESS' if src_ok else '❌ FAIL'}")
    print(f"DESTINATION CONNECTION: {'✅ SUCCESS' if dst_ok else '❌ FAIL'}")

    print("\n🏁 Test completed.")
