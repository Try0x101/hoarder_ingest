import orjson
import uuid
import time
import redis as sync_redis
from fastapi import APIRouter, Request, HTTPException
from app.processing import process_payload, validate_data
from app.database import save_telemetry, save_telemetry_batch
from app.services.timestamp_calculator import calculate_ingestion_timestamps, format_for_db
from datetime import datetime, timezone

router = APIRouter(prefix="/api", tags=["Ingestion"])
REDIS_METRICS_URL = "redis://localhost:6378/2"

def _log_sync_latency(duration: float, count: int):
    if count <= 0: return
    try:
        r = sync_redis.from_url(REDIS_METRICS_URL)
        data = orjson.dumps({"ts": time.time(), "duration": duration, "count": count})
        pipe = r.pipeline()
        pipe.lpush("sync_ingestion_stats", data)
        pipe.ltrim("sync_ingestion_stats", 0, 1999)
        pipe.execute()
        r.close()
    except sync_redis.RedisError:
        pass

@router.post("/telemetry")
async def handle_telemetry(request: Request):
    start_time = time.monotonic()
    request_id = str(uuid.uuid4())
    raw_body = await request.body()
    received_at = datetime.now(timezone.utc)
    headers, client_ip = dict(request.headers), request.client.host

    is_valid_payload, payload = await process_payload(raw_body)
    if not is_valid_payload: raise HTTPException(status_code=400, detail=payload)

    is_valid_data, message, device_id = validate_data(payload, headers, client_ip)
    if not is_valid_data: raise HTTPException(status_code=400, detail=message)

    headers_json = orjson.dumps({"client_ip": client_ip, "headers": headers}).decode()
    event_times, _ = calculate_ingestion_timestamps([payload], received_at)
    
    payload_json = orjson.dumps(payload).decode()
    await save_telemetry(device_id, payload_json, headers_json, event_times[0], request_id, len(raw_body))
    
    _log_sync_latency(time.monotonic() - start_time, 1)
    return {"status": "ok", "device_id": device_id, "request_id": request_id}

@router.post("/batch")
async def handle_batch(request: Request):
    start_time = time.monotonic()
    request_id = str(uuid.uuid4())
    raw_body = await request.body()
    received_at = datetime.now(timezone.utc)
    headers, client_ip = dict(request.headers), request.client.host
    
    is_valid_payload, payload_list = await process_payload(raw_body)
    if not is_valid_payload or not isinstance(payload_list, list):
        raise HTTPException(status_code=400, detail="Invalid batch format.")

    headers_json = orjson.dumps({"client_ip": client_ip, "headers": headers}).decode()
    event_times, _ = calculate_ingestion_timestamps(payload_list, received_at)

    record_count = len(payload_list)
    avg_size = len(raw_body) // record_count if record_count > 0 else 0

    batch_records = []
    for i, payload in enumerate(payload_list):
        is_valid, _, device_id = validate_data(payload, headers, client_ip)
        if is_valid:
            payload_json = orjson.dumps(payload).decode()
            batch_records.append((device_id, payload_json, headers_json, format_for_db(event_times[i]), request_id, avg_size))
    
    if batch_records: await save_telemetry_batch(batch_records)
    
    _log_sync_latency(time.monotonic() - start_time, len(payload_list))
    return {"status": "ok", "processed_records": len(batch_records), "total_records": len(payload_list), "request_id": request_id}
