import socket

server = "lst-svr-sql02.database.windows.net"

# Common Azure SQL ports (standard + dynamic)
ports = [1433] + list(range(11000, 15010, 1000))  # 1433, 11000, 12000, 13000, 14000, 15000

print(f"🔍 Scanning open ports for {server}...\n")

for port in ports:
    try:
        with socket.create_connection((server, port), timeout=3):
            print(f"✅ Port {port} is OPEN and responding.")
    except Exception:
        print(f"❌ Port {port} is CLOSED or not responding.")

print("\n🏁 Scan completed.")
