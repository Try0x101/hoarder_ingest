import sqlite3
import os

DB_FILE = "hoarder_ingest.db"
DB_PATH = os.path.join(os.path.dirname(__file__), "..", DB_FILE)

def initialize_database():
    try:
        con = sqlite3.connect(DB_PATH)
        cur = con.cursor()
        
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='telemetry'")
        table_exists = cur.fetchone() is not None
        
        if table_exists:
            print("Database and 'telemetry' table already exist.")
            con.close()
            return
        
        print("Creating 'telemetry' table...")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS telemetry (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                device_id TEXT NOT NULL,
                payload TEXT NOT NULL,
                received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                request_headers TEXT,
                calculated_event_timestamp TEXT,
                request_id TEXT
            )
        """)
        
        print("Creating indexes on 'telemetry' table...")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_telemetry_device_id ON telemetry (device_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_telemetry_received_at ON telemetry (received_at)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_telemetry_id_device ON telemetry (id, device_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_telemetry_device_event_time_id ON telemetry (device_id, calculated_event_timestamp DESC, id DESC)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_telemetry_request_id ON telemetry (request_id)")

        con.commit()
        con.close()
        print("Database initialized successfully.")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    initialize_database()