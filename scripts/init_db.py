import sqlite3
import os

DB_FILE = "hoarder_ingest.db"
DB_PATH = os.path.join(os.path.dirname(__file__), "..", DB_FILE)

def initialize_database():
    try:
        con = sqlite3.connect(DB_PATH)
        cur = con.cursor()
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS telemetry (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                device_id TEXT NOT NULL,
                payload TEXT NOT NULL,
                received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                request_headers TEXT,
                calculated_event_timestamp TEXT,
                request_id TEXT,
                request_size_bytes INTEGER NOT NULL DEFAULT 0
            )
        """)
        
        cur.execute("PRAGMA table_info(telemetry)")
        columns = [row[1] for row in cur.fetchall()]
        
        columns_to_add = {
            "request_headers": "TEXT",
            "calculated_event_timestamp": "TEXT",
            "request_id": "TEXT",
            "request_size_bytes": "INTEGER NOT NULL DEFAULT 0"
        }
        
        for col, col_type in columns_to_add.items():
            if col not in columns:
                cur.execute(f"ALTER TABLE telemetry ADD COLUMN {col} {col_type}")
        
        cur.execute("CREATE INDEX IF NOT EXISTS idx_telemetry_device_event_time_id ON telemetry (device_id, calculated_event_timestamp DESC, id DESC)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_telemetry_request_id ON telemetry (request_id)")

        con.commit()
        con.close()
        print("Database schema initialization/verification complete.")
    except Exception as e:
        print(f"An error occurred during DB init: {e}")

if __name__ == "__main__":
    initialize_database()
