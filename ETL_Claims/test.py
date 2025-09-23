import pyodbc
from config import CONN_STR

def test_connection():
    try:
        print("🔑 Probando conexión con la cadena de config.py...\n")

        with pyodbc.connect(CONN_STR) as conn:
            cursor = conn.cursor()
            
            # Traer primeras 10 tablas
            cursor.execute("""
                SELECT TOP 10 TABLE_SCHEMA, TABLE_NAME
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_TYPE = 'BASE TABLE'
                ORDER BY TABLE_SCHEMA, TABLE_NAME
            """)
            
            rows = cursor.fetchall()

            print("✅ Conexión exitosa a SQL Server")
            print("📋 Primeras 10 tablas encontradas:\n")
            for schema, table in rows:
                print(f" - {schema}.{table}")

    except Exception as e:
        print("❌ Error al conectar o listar tablas:", e)


if __name__ == "__main__":
    test_connection()