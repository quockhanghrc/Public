import psycopg2
import os
import sys

# Configuration
DB_CONFIG = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "your_password",  # Replace with your actual password
    "host": "34.50.88.3",
    "port": "5432"
}

def validate():
    print(f"Connecting to {DB_CONFIG['host']}...")
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        # Test 1: Simple Select
        cur.execute("SELECT version();")
        print(f"✅ Database Version: {cur.fetchone()[0]}")
        
        # Test 2: Check WAL Level (REQUIRED for Debezium)
        cur.execute("SHOW wal_level;")
        wal = cur.fetchone()[0]
        print(f"✅ WAL Level: {wal}")
        
        if wal != 'logical':
            print("⚠️ WARNING: wal_level is not 'logical'. Debezium will NOT work.")
            print("   Update Cloud SQL Flags: cloudsql.logical_decoding = on")
            
        cur.close()
        conn.close()
        print("🎉 Connection looks perfect!")
        
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    validate()