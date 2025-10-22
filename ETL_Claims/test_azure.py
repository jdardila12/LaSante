import pyodbc
import os
import platform
import subprocess

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
    "SERVER=tcp:lst-svr-sql02.database.windows.net,1433;"
    "DATABASE=sigma_db;"
    "UID=dw_juan;"
    "PWD=dw@J!597;"
    "Encrypt=yes;"
    "TrustServerCertificate=no;"
    "Connection Timeout=30;"
)

# === FUNCIONES ===

def ping_host(host):
    """Hacer ping al servidor para verificar conectividad."""
    print(f"📡 Haciendo ping a {host}...")
    try:
        # Windows usa -n, Linux/Mac usa -c
        param = "-n" if platform.system().lower() == "windows" else "-c"
        result = subprocess.run(
            ["ping", param, "2", host],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        if result.returncode == 0:
            print("   ✅ Respuesta recibida (host accesible)\n")
        else:
            print("   ⚠️  No hubo respuesta (posible bloqueo de red)\n")
    except Exception as e:
        print(f"   ⚠️  Error ejecutando ping: {e}\n")

def test_connection(name, conn_str):
    """Probar conexión con pyodbc."""
    print(f"🔹 Probando conexión a {name}...")
    try:
        with pyodbc.connect(conn_str) as conn:
            cur = conn.cursor()
            cur.execute("SELECT GETDATE();")
            row = cur.fetchone()
            print(f"   ✅ {name} conectado correctamente — {row[0]}\n")
    except Exception as e:
        print(f"   ❌ {name} error — {e}\n")

# === MAIN ===
if __name__ == "__main__":
    print("🔍 Test de conexión (LaSante local y Azure SQL)\n")

    # 1️⃣ Probar origen (on-prem)
    test_connection("SOURCE (mobiledoc)", SRC_CONN_STR)

    # 2️⃣ Verificar si Azure responde al ping
    ping_host("lst-svr-sql02.database.windows.net")

    # 3️⃣ Probar destino (Azure)
    test_connection("DESTINATION (sigma_db)", DST_CONN_STR)

    print("🏁 Prueba finalizada.")

