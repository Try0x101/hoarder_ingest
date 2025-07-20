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
        print(f"Failed to notify processor: {e.__class__.__name__} - {e}")
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
        try:
            notification_payload = {
                "records": [{
                    "id": inserted_id,
                    "device_id": device_id,
                    "payload": orjson.loads(payload),
                    "request_headers": orjson.loads(headers),
                    "calculated_event_timestamp": event_time_str,
                    "request_id": request_id
                }]
            }
            await _notify_processor(notification_payload)
        except Exception:
            pass

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
            return

    request_id = records[0][4] if records else None
    if not request_id: return

    try:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute("SELECT * FROM telemetry WHERE request_id = ?", (request_id,))
            inserted_records = await cursor.fetchall()
            if inserted_records:
                records_for_notification = []
                for row in inserted_records:
                    record_dict = dict(row)
                    try:
                        record_dict['payload'] = orjson.loads(record_dict['payload'])
                        record_dict['request_headers'] = orjson.loads(record_dict['request_headers'])
                    except (orjson.JSONDecodeError, TypeError):
                        pass
                    records_for_notification.append(record_dict)
                await _notify_processor({"records": records_for_notification})
    except Exception as e:
        print(f"Failed to fetch records for notification: {e}")

async def get_latest_telemetry(limit: int = 10) -> List[Dict[str, Any]]:
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            query = "SELECT t.*, (SELECT COUNT(*) FROM telemetry WHERE device_id = t.device_id) as total_records FROM telemetry t WHERE t.id IN (SELECT MAX(id) FROM telemetry GROUP BY device_id) ORDER BY t.received_at DESC LIMIT ?"
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
            
            query_parts = ["SELECT * FROM telemetry"]
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
            
            query_parts.append("ORDER BY calculated_event_timestamp DESC, id DESC LIMIT ?")
            params.append(capped_limit)
            
            cursor = await db.execute(" ".join(query_parts), tuple(params))
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]
    except Exception as e:
        print(f"Database history query error for {device_id or 'global'}: {e}")
        return []
