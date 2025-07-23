import aiosqlite
import os
import httpx
import orjson
import asyncio
import time
import redis.asyncio as redis
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime

DB_FILE = "hoarder_ingest.db"
DB_PATH = os.path.join(os.path.dirname(__file__), "..", DB_FILE)
db_lock = None
wal_initialized = False
REDIS_METRICS_URL = "redis://localhost:6378/2"

_http_client: Optional[httpx.AsyncClient] = None

async def get_http_client() -> httpx.AsyncClient:
    global _http_client
    if _http_client is None:
        _http_client = httpx.AsyncClient(
            timeout=httpx.Timeout(2.0),
            limits=httpx.Limits(max_keepalive_connections=5, max_connections=10)
        )
    return _http_client

async def _log_webhook_stats(duration: float, success: bool, error_msg: Optional[str] = None):
    r = None
    try:
        r = redis.from_url(REDIS_METRICS_URL)
        status = "success" if success else "failure"
        data = {"ts": time.time(), "duration": duration, "status": status}
        pipe = r.pipeline()
        pipe.lpush("webhook_stats", orjson.dumps(data))
        pipe.ltrim("webhook_stats", 0, 1999)
        if not success:
            error_data = {"ts": time.time(), "error": error_msg or "Unknown error"}
            pipe.set("webhook_last_error", orjson.dumps(error_data))
        await pipe.execute()
    except redis.RedisError as e:
        print(f"Failed to log webhook stats: {e}")
    finally:
        if r:
            await r.close()

async def _notify_processor_async(payload: dict):
    webhook_urls = os.environ.get("PROCESSOR_WEBHOOK_URL")
    if not webhook_urls:
        return

    for url in webhook_urls.split(','):
        url = url.strip()
        if not url: continue
        
        start_time, success, error_msg = time.monotonic(), False, None
        try:
            client = await get_http_client()
            response = await client.post(
                url,
                content=orjson.dumps(payload),
                headers={"Content-Type": "application/json"}
            )
            response.raise_for_status()
            success = True
        except Exception as e:
            error_msg = f"{type(e).__name__}: {e}"
            print(f"Webhook notification to {url} failed: {error_msg}")
        finally:
            duration = time.monotonic() - start_time
            await _log_webhook_stats(duration, success, error_msg)

def _notify_processor(payload: dict):
    asyncio.create_task(_notify_processor_async(payload))

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

async def save_telemetry(device_id: str, payload: str, headers: str, event_time: datetime, request_id: str, request_size_bytes: int):
    await ensure_wal_mode()
    lock = await get_db_lock()
    event_time_str = event_time.strftime("%Y-%m-%d %H:%M:%S.%f")
    inserted_id = None
    
    async with lock:
        try:
            async with aiosqlite.connect(DB_PATH, timeout=10) as db:
                cursor = await db.execute(
                    "INSERT INTO telemetry (device_id, payload, request_headers, calculated_event_timestamp, request_id, request_size_bytes) VALUES (?, ?, ?, ?, ?, ?)",
                    (device_id, payload, headers, event_time_str, request_id, request_size_bytes)
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
                    "id": inserted_id, "device_id": device_id,
                    "payload": orjson.loads(payload), "request_headers": orjson.loads(headers),
                    "calculated_event_timestamp": event_time_str, "request_id": request_id,
                    "request_size_bytes": request_size_bytes
                }]
            }
            _notify_processor(notification_payload)
        except Exception:
            pass

async def save_telemetry_batch(records: List[Tuple[str, str, str, str, str, int]]):
    if not records:
        return
    
    await ensure_wal_mode()
    lock = await get_db_lock()
    inserted_ids = []
    
    async with lock:
        try:
            async with aiosqlite.connect(DB_PATH, timeout=30) as db:
                await db.execute("BEGIN IMMEDIATE")
                cursor = await db.cursor()
                for record in records:
                    await cursor.execute(
                        "INSERT INTO telemetry (device_id, payload, request_headers, calculated_event_timestamp, request_id, request_size_bytes) VALUES (?, ?, ?, ?, ?, ?)",
                        record
                    )
                    inserted_ids.append(cursor.lastrowid)
                await db.commit()
        except Exception as e:
            print(f"Database batch error: {e}")
            return

    if inserted_ids and records:
        try:
            records_for_notification = []
            for i, (device_id, payload_json, headers_json, event_time_str, request_id, size) in enumerate(records):
                try:
                    records_for_notification.append({
                        "id": inserted_ids[i], "device_id": device_id,
                        "payload": orjson.loads(payload_json), "request_headers": orjson.loads(headers_json),
                        "calculated_event_timestamp": event_time_str, "request_id": request_id, "request_size_bytes": size
                    })
                except (orjson.JSONDecodeError, TypeError): pass
            
            if records_for_notification:
                _notify_processor({"records": records_for_notification})
        except Exception as e:
            print(f"Failed to prepare notification: {e}")

async def get_latest_telemetry(limit: int = 10) -> List[Dict[str, Any]]:
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            query = """
                SELECT t.*, dev_stats.total_records, dev_stats.total_bytes, dev_stats.first_seen_ts
                FROM telemetry t
                JOIN (
                    SELECT device_id, MAX(id) as max_id, COUNT(*) as total_records,
                           SUM(request_size_bytes) as total_bytes, MIN(received_at) as first_seen_ts
                    FROM telemetry GROUP BY device_id
                ) dev_stats ON t.id = dev_stats.max_id
                ORDER BY t.received_at DESC LIMIT ?
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
