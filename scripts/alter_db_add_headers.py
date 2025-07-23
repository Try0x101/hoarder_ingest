import sqlite3
import os

DB_FILE = "hoarder_ingest.db"
DB_PATH = os.path.join(os.path.dirname(__file__), "..", DB_FILE)

def alter_database():
    if not os.path.exists(DB_PATH):
        print(f"Database file not found at {DB_PATH}. Skipping alteration.")
        return

    try:
        con = sqlite3.connect(DB_PATH)
        cur = con.cursor()
        
        cur.execute("PRAGMA table_info(telemetry)")
        columns = [row[1] for row in cur.fetchall()]
        
        if 'request_headers' not in columns:
            print("Adding 'request_headers' column...")
            cur.execute("ALTER TABLE telemetry ADD COLUMN request_headers TEXT")
            print("Column 'request_headers' added.")
        
        if 'calculated_event_timestamp' not in columns:
            print("Adding 'calculated_event_timestamp' column...")
            cur.execute("ALTER TABLE telemetry ADD COLUMN calculated_event_timestamp TEXT")
            print("Column 'calculated_event_timestamp' added.")
        
        if 'request_id' not in columns:
            print("Adding 'request_id' column...")
            cur.execute("ALTER TABLE telemetry ADD COLUMN request_id TEXT")
            print("Column 'request_id' added.")
        
        if 'request_size_bytes' not in columns:
            print("Adding 'request_size_bytes' column...")
            cur.execute("ALTER TABLE telemetry ADD COLUMN request_size_bytes INTEGER NOT NULL DEFAULT 0")
            print("Column 'request_size_bytes' added.")

        cur.execute("PRAGMA index_list(telemetry)")
        indexes = [row[1] for row in cur.fetchall()]
        
        idx_event_time = 'idx_telemetry_device_event_time_id'
        if idx_event_time not in indexes:
            print(f"Creating index '{idx_event_time}'...")
            cur.execute(f"CREATE INDEX {idx_event_time} ON telemetry (device_id, calculated_event_timestamp DESC, id DESC)")
            print("Index created.")
        
        idx_req_id = 'idx_telemetry_request_id'
        if idx_req_id not in indexes:
            print(f"Creating index '{idx_req_id}'...")
            cur.execute(f"CREATE INDEX {idx_req_id} ON telemetry (request_id)")
            print("Index created.")

        con.commit()
        con.close()
        print("Database migration check complete.")
    except Exception as e:
        print(f"An error occurred while altering the database: {e}")

if __name__ == "__main__":
    alter_database()
