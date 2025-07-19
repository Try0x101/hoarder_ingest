import aiosqlite
import os
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime

DB_FILE = "hoarder_ingest.db"
DB_PATH = os.path.join(os.path.dirname(__file__), "..", DB_FILE)
db_lock = None
wal_initialized = False

async def get_db_lock():
    import asyncio
    global db_lock
    if db_lock is None:
        db_lock = asyncio.Lock()
    return db_lock

async def ensure_wal_mode():
    global wal_initialized
    if not wal_initialized:
        try:
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute("PRAGMA journal_mode=WAL;")
                await db.execute("PRAGMA synchronous=NORMAL;")
                await db.execute("PRAGMA cache_size=10000;")
                await db.commit()
            wal_initialized = True
        except Exception as e:
            print(f"WAL mode setup error: {e}")

async def save_telemetry(device_id: str, payload: str, headers: str, event_time: datetime, request_id: str):
    await ensure_wal_mode()
    lock = await get_db_lock()
    event_time_str = event_time.strftime("%Y-%m-%d %H:%M:%S.%f")
    async with lock:
        try:
            async with aiosqlite.connect(DB_PATH, timeout=10) as db:
                await db.execute(
                    "INSERT INTO telemetry (device_id, payload, request_headers, calculated_event_timestamp, request_id) VALUES (?, ?, ?, ?, ?)",
                    (device_id, payload, headers, event_time_str, request_id)
                )
                await db.commit()
        except Exception as e:
            print(f"Database error: {e}")

async def save_telemetry_batch(records: List[Tuple[str, str, str, str, str]]):
    if not records:
        return
    
    await ensure_wal_mode()
    lock = await get_db_lock()
    async with lock:
        try:
            async with aiosqlite.connect(DB_PATH, timeout=30) as db:
                await db.execute("BEGIN IMMEDIATE")
                await db.executemany(
                    "INSERT INTO telemetry (device_id, payload, request_headers, calculated_event_timestamp, request_id) VALUES (?, ?, ?, ?, ?)",
                    records
                )
                await db.commit()
        except Exception as e:
            print(f"Database batch error: {e}")
            try:
                await db.rollback()
            except:
                pass

async def get_latest_telemetry(limit: int = 10) -> List[Dict[str, Any]]:
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            query = """
            SELECT device_id, received_at, payload, request_headers
            FROM telemetry 
            WHERE id IN (
                SELECT MAX(id) FROM telemetry GROUP BY device_id
            )
            ORDER BY received_at DESC
            LIMIT ?;
            """
            cursor = await db.execute(query, (limit,))
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]
    except Exception as e:
        print(f"Database query error: {e}")
        return []

async def get_historical_telemetry(device_id: str, limit: int, cursor_ts: Optional[str] = None, cursor_id: Optional[int] = None) -> List[Dict[str, Any]]:
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            capped_limit = min(limit, 500)
            
            if cursor_ts and cursor_id is not None:
                query = """
                SELECT id, payload, received_at, request_headers, calculated_event_timestamp, request_id 
                FROM telemetry 
                WHERE device_id = ? AND (calculated_event_timestamp, id) < (?, ?)
                ORDER BY calculated_event_timestamp DESC, id DESC
                LIMIT ?
                """
                params = (device_id, cursor_ts, cursor_id, capped_limit)
            else:
                query = """
                SELECT id, payload, received_at, request_headers, calculated_event_timestamp, request_id
                FROM telemetry 
                WHERE device_id = ? 
                ORDER BY calculated_event_timestamp DESC, id DESC
                LIMIT ?
                """
                params = (device_id, capped_limit)
            
            cursor = await db.execute(query, params)
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]
    except Exception as e:
        print(f"Database history query error for device {device_id}: {e}")
        return []
