import aiosqlite
import os
import httpx
import orjson
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime

DB_FILE = "hoarder_ingest.db"
DB_PATH = os.path.join(os.path.dirname(__file__), "..", DB_FILE)
db_lock = None
wal_initialized = False

async def _notify_processor(payload: dict):
    webhook_url = os.environ.get("PROCESSOR_WEBHOOK_URL")
    if not webhook_url:
        return

    try:
        async with httpx.AsyncClient() as client:
            await client.post(
                webhook_url,
                content=orjson.dumps(payload),
                headers={"Content-Type": "application/json"},
                timeout=2.0
            )
    except httpx.RequestError as e:
        print(f"Failed to notify processor: {e.__class__.__name__}")
    except Exception as e:
        print(f"An unexpected error occurred during processor notification: {e}")

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
    inserted_id = None
    
    async with lock:
        try:
            async with aiosqlite.connect(DB_PATH, timeout=10) as db:
                cursor = await db.execute(
                    "INSERT INTO telemetry (device_id, payload, request_headers, calculated_event_timestamp, request_id) VALUES (?, ?, ?, ?, ?)",
                    (device_id, payload, headers, event_time_str, request_id)
                )
                inserted_id = cursor.lastrowid
                await db.commit()
        except Exception as e:
            print(f"Database error: {e}")
            return

    if inserted_id:
        notification_payload = {
            "records": [{
                "id": inserted_id,
                "device_id": device_id,
                "payload": payload,
                "request_headers": headers,
                "calculated_event_timestamp": event_time_str,
                "request_id": request_id
            }]
        }
        await _notify_processor(notification_payload)

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
            return

    request_id = records[0][4] if records and len(records[0]) > 4 else None
    if request_id:
        try:
            async with aiosqlite.connect(DB_PATH) as db:
                db.row_factory = aiosqlite.Row
                cursor = await db.execute("SELECT id, device_id, payload, request_headers, calculated_event_timestamp, request_id FROM telemetry WHERE request_id = ?", (request_id,))
                inserted_records = await cursor.fetchall()
                if inserted_records:
                    notification_payload = {"records": [dict(row) for row in inserted_records]}
                    await _notify_processor(notification_payload)
        except Exception as e:
            print(f"Failed to fetch records for processor notification: {e}")

async def get_latest_telemetry(limit: int = 10) -> List[Dict[str, Any]]:
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            query = """
            WITH DeviceCounts AS (
                SELECT device_id, COUNT(*) as total_records
                FROM telemetry
                GROUP BY device_id
            )
            SELECT
                t.device_id,
                t.received_at,
                t.payload,
                t.request_headers,
                dc.total_records
            FROM telemetry t
            JOIN DeviceCounts dc ON t.device_id = dc.device_id
            WHERE t.id IN (
                SELECT MAX(id) FROM telemetry GROUP BY device_id
            )
            ORDER BY t.received_at DESC
            LIMIT ?;
            """
            cursor = await db.execute(query, (limit,))
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]
    except Exception as e:
        print(f"Database query error: {e}")
        return []

async def get_historical_telemetry(device_id: Optional[str], limit: int, cursor_ts: Optional[str] = None, cursor_id: Optional[int] = None) -> List[Dict[str, Any]]:
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            capped_limit = min(limit, 500)
            
            query_parts = ["SELECT id, device_id, payload, received_at, request_headers, calculated_event_timestamp, request_id FROM telemetry"]
            params = []

            conditions = []
            if device_id:
                conditions.append("device_id = ?")
                params.append(device_id)

            if cursor_ts and cursor_id is not None:
                conditions.append("(calculated_event_timestamp, id) < (?, ?)")
                params.extend([cursor_ts, cursor_id])

            if conditions:
                query_parts.append("WHERE " + " AND ".join(conditions))
            
            query_parts.append("ORDER BY calculated_event_timestamp DESC, id DESC")
            query_parts.append("LIMIT ?")
            params.append(capped_limit)
            
            query = " ".join(query_parts)
            
            cursor = await db.execute(query, tuple(params))
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]
    except Exception as e:
        id_str = f"device {device_id}" if device_id else "global history"
        print(f"Database history query error for {id_str}: {e}")
        return []