import pyodbc
import socket
from datetime import datetime

print("\n🔍 Test de conexión (LaSante local y Azure SQL)\n")

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

# --- 1. Probar conexión a la base de datos SOURCE (local / mobiledoc) ---
print("🧩 Probando conexión a SOURCE (mobiledoc)...")
try:
    conn_source = pyodbc.connect(SRC_CONN_STR)
    cursor = conn_source.cursor()
    cursor.execute("SELECT GETDATE()")
    result = cursor.fetchone()
    print(f"✅ SOURCE (mobiledoc) conectado correctamente — {result[0]}")
    conn_source.close()
except Exception as e:
    print(f"❌ SOURCE (mobiledoc) error — {e}")

# --- 2. Hacer ping al servidor Azure ---
print(f"\n📡 Haciendo ping a {DST_SERVER} ...")
try:
    socket.create_connection((DST_SERVER, 1433), timeout=5)
    print("✅ Conexión TCP exitosa (puerto 1433 abierto)")
except Exception as e:
    print(f"⚠️ No hubo respuesta (posible bloqueo de red) — {e}")

# --- 3. Probar conexión a la base de datos DESTINATION (Azure SQL) ---
print("\n🧩 Probando conexión a DESTINATION (sigma_db)...")
try:
    conn_dest = pyodbc.connect(DST_CONN_STR)
    cursor = conn_dest.cursor()
    cursor.execute("SELECT DB_NAME(), SUSER_NAME(), GETDATE()")
    result = cursor.fetchone()
    print(f"✅ DESTINATION conectado correctamente — DB: {result[0]}, Usuario: {result[1]}, Hora: {result[2]}")
    conn_dest.close()
except Exception as e:
    print(f"❌ DESTINATION (sigma_db) error — {e}")

print("\n✨ Prueba finalizada.")
