import pyodbc
import socket
from datetime import datetime

print("\n--- Connection Test: Local (LaSante) and Azure SQL ---\n")

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
    f"SERVER=tcp:{DST_SERVER},1433;"
    f"DATABASE={DST_DATABASE};"
    f"UID={DST_USER};"
    f"PWD={DST_PASSWORD};"
    "Encrypt=yes;"
    "TrustServerCertificate=no;"
    "Connection Timeout=30;"
)

# --- 1. Test connection to SOURCE (on-prem LaSante) ---
print("Testing SOURCE connection (mobiledoc)...")
try:
    conn_source = pyodbc.connect(SRC_CONN_STR)
    cursor = conn_source.cursor()
    cursor.execute("SELECT GETDATE()")
    result = cursor.fetchone()
    print(f"Source connected successfully — {result[0]}")
    conn_source.close()
except Exception as e:
    print(f"Source connection failed: {e}")

# --- 2. Ping Azure SQL server (port 1433) ---
print(f"\nPinging {DST_SERVER} ...")
try:
    socket.create_connection((DST_SERVER, 1433), timeout=5)
    print("TCP connection successful (port 1433 open).")
except Exception as e:
    print(f"Ping failed or port blocked: {e}")

# --- 3. Test connection to DESTINATION (Azure SQL) ---
print("\nTesting DESTINATION connection (sigma_db)...")
try:
    conn_dest = pyodbc.connect(DST_CONN_STR)
    cursor = conn_dest.cursor()
    cursor.execute("SELECT DB_NAME(), SUSER_NAME(), GETDATE()")
    result = cursor.fetchone()
    print(f"Destination connected successfully — DB: {result[0]}, User: {result[1]}, Time: {result[2]}")
    conn_dest.close()
except Exception as e:
    print(f"Destination connection failed: {e}")

print("\nTest completed.")

