# test_azure.py
# Comments inside code are in English.
# The script tests connectivity scenarios for SOURCE and DESTINATION SQL servers.
# It assumes you have a config.py in the same folder with SRC_* and DST_* variables.
# Run: py test_azure.py

import socket
import pyodbc
import importlib
import sys
import time

# Try to import user's config.py
try:
    config = importlib.import_module("config")
except Exception as e:
    print("No se pudo importar config.py - asegúrate de que el file exista en la raíz del proyecto.")
    print("Error:", e)
    sys.exit(1)

# ----------------- Helpers -----------------
def print_header(title):
    print("\n" + "=" * 60)
    print(title)
    print("=" * 60)

def list_drivers():
    """List installed ODBC drivers."""
    drivers = pyodbc.drivers()
    return drivers

def tcp_ping(host: str, port: int, timeout: float = 3.0) -> bool:
    """Quick TCP reachability check for host:port."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except Exception:
        return False

def try_connect(conn_str: str, timeout_seconds: int = 10):
    """Attempt a connection using pyodbc and return (ok:boolean, message:str)."""
    try:
        start = time.time()
        cn = pyodbc.connect(conn_str, timeout=timeout_seconds)
        elapsed = time.time() - start
        try:
            cur = cn.cursor()
            cur.execute("SELECT GETDATE();")
            row = cur.fetchone()
            cn.close()
            return True, f"connected (GETDATE()={row[0]}) in {elapsed:.2f}s"
        except Exception:
            cn.close()
            return True, f"connected (no GETDATE()) in {elapsed:.2f}s"
    except Exception as e:
        return False, str(e)

# ----------------- Tests -----------------
def test_source():
    """Test the source connection (uses SRC_CONN_STR from config.py)."""
    print_header("TEST SOURCE (on-prem)")
    # show quick tcp test to source server on port 1433
    src_host = getattr(config, "SRC_SERVER", None)
    if src_host:
        tcp_ok = tcp_ping(src_host, 1433, timeout=2)
        print(f"TCP 1433 to SOURCE ({src_host}): {'OPEN' if tcp_ok else 'CLOSED/UNREACHABLE'}")
    else:
        print("SRC_SERVER not found in config.py")

    conn_str = getattr(config, "SRC_CONN_STR", None)
    if not conn_str:
        # build simple connection string fallback
        try:
            conn_str = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={config.SRC_SERVER};DATABASE={config.SRC_DATABASE};"
                f"UID={config.SRC_USER};PWD={config.SRC_PASSWORD};"
                "Connection Timeout=10;"
            )
        except Exception as e:
            print("No SRC_CONN_STR and unable to build fallback:", e)
            return False, "no-src-connstr"

    ok, msg = try_connect(conn_str, timeout_seconds=10)
    print(f"SOURCE connect: {'✅ SUCCESS' if ok else '❌ FAIL'}  -- {msg}")
    return ok, msg

def test_destination():
    """Test destination with several scenarios (driver 18 recommended)."""
    print_header("TEST DESTINATION (Azure SQL)")

    dst_host = getattr(config, "DST_SERVER", None)
    if dst_host:
        # if user included a port in DST_SERVER (comma), split host part for tcp test
        host_for_tcp = dst_host.split(",")[0]
        tcp_1433 = tcp_ping(host_for_tcp, 1433, timeout=2)
        print(f"TCP 1433 to DEST ({host_for_tcp}): {'OPEN' if tcp_1433 else 'CLOSED/UNREACHABLE'}")
    else:
        print("DST_SERVER not found in config.py")

    drivers = list_drivers()
    print("ODBC drivers installed:", drivers)

    scenarios = []

    # Scenario A: Driver 18 recommended (Encrypt=yes, TrustServerCertificate=no)
    scenarios.append({
        "name": "Driver18_Encrypt_on_validcert",
        "driver": "ODBC Driver 18 for SQL Server",
        "encrypt": "yes",
        "trust": "no",
        "database": getattr(config, "DST_DATABASE", "master")
    })

    # Scenario B: Driver 17 fallback (Encrypt=yes, TrustServerCertificate=yes)
    scenarios.append({
        "name": "Driver17_Encrypt_on_trustcert",
        "driver": "ODBC Driver 17 for SQL Server",
        "encrypt": "yes",
        "trust": "yes",
        "database": getattr(config, "DST_DATABASE", "master")
    })

    # Scenario C: Debug (disable encrypt, trust cert)
    scenarios.append({
        "name": "Driver18_Encrypt_off_trustcert",
        "driver": "ODBC Driver 18 for SQL Server",
        "encrypt": "no",
        "trust": "yes",
        "database": getattr(config, "DST_DATABASE", "master")
    })

    results = []

    for s in scenarios:
        driver = s["driver"]
        # Skip scenario if driver not installed
        installed = driver in drivers
        if not installed:
            results.append((s["name"], False, f"driver '{driver}' not installed"))
            print(f"- {s['name']}: SKIPPED (driver '{driver}' not installed)")
            continue

        server = getattr(config, "DST_SERVER", None)
        user = getattr(config, "DST_USER", None)
        pwd = getattr(config, "DST_PASSWORD", None)
        db = s["database"]

        conn_str = (
            f"DRIVER={{{driver}}};"
            f"SERVER={server};DATABASE={db};UID={user};PWD={pwd};"
            f"Encrypt={s['encrypt']};TrustServerCertificate={s['trust']};"
            "Connection Timeout=10;"
        )

        print(f"\n- Trying scenario: {s['name']}  (driver={driver}, Encrypt={s['encrypt']}, Trust={s['trust']}, DB={db})")
        ok, msg = try_connect(conn_str, timeout_seconds=12)
        print(f"  Result: {'✅ SUCCESS' if ok else '❌ FAIL'}  -- {msg}")
        results.append((s["name"], ok, msg))

    # Also try connecting to master explicitly (to wake serverless DBs)
    print
