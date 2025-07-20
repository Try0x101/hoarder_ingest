import os
import orjson
import ijson
import asyncio
import aiosqlite
from celery import shared_task
from app.services import job_manager, stream_processor
from app.processing import validate_data, orjson_decimal_default
from app.services.timestamp_calculator import calculate_ingestion_timestamps, format_for_db
from app.database import save_telemetry_batch, DB_PATH
from datetime import datetime
from app.utils.async_helpers import AsyncGeneratorReader
from typing import Dict, List, Tuple, Any, Optional
from collections import defaultdict

TARGET_DB_SIZE_MB = 900
MAX_DB_SIZE_MB = 1000
CHUNK_SIZE = 1000

async def _async_cleanup_db():
    db_size = os.path.getsize(DB_PATH) / (1024 * 1024)
    if db_size < MAX_DB_SIZE_MB:
        return

    try:
        async with aiosqlite.connect(DB_PATH) as db:
            while db_size > TARGET_DB_SIZE_MB:
                cursor = await db.execute("DELETE FROM telemetry WHERE id IN (SELECT id FROM telemetry ORDER BY id ASC LIMIT 1000)")
                if cursor.rowcount == 0:
                    break
                await db.commit()
                await db.execute("VACUUM")
                await db.commit()
                await asyncio.sleep(1)
                db_size = os.path.getsize(DB_PATH) / (1024 * 1024)
    except Exception as e:
        print(f"Error during DB cleanup: {e}")

@shared_task(name='tasks.cleanup_db')
def cleanup_db():
    asyncio.run(_async_cleanup_db())

async def _process_and_save_chunk(records, headers, client_ip, headers_json, received_at, request_id, bts_context, start_index):
    valid_payloads, valid_device_ids = [], []
    for payload in records:
        is_valid, _, device_id = validate_data(payload, headers, client_ip)
        if is_valid:
            valid_payloads.append(payload)
            valid_device_ids.append(device_id)

    if not valid_payloads:
        return 0, len(records), bts_context

    event_times, new_bts_context = calculate_ingestion_timestamps(valid_payloads, received_at, bts_context, start_index)
    
    db_records = [
        (valid_device_ids[i], orjson.dumps(p, default=orjson_decimal_default).decode(), headers_json, format_for_db(event_times[i]), request_id)
        for i, p in enumerate(valid_payloads)
    ]
    
    if db_records:
        await save_telemetry_batch(db_records)
        
    skipped_count = len(records) - len(db_records)
    return len(db_records), skipped_count, new_bts_context

async def _async_bulk_processor(job_context: dict):
    job_id, temp_file_path, metadata = job_context["job_id"], job_context["temp_file_path"], job_context["metadata"]
    total_processed, total_skipped, total_records = 0, 0, 0
    bts_context: Dict[str, List[Tuple[int, datetime]]] = defaultdict(list)

    try:
        await job_manager.set_job_status(job_id, {"status": "PROCESSING", "details": "Starting file stream processing."})
        
        headers, client_ip, request_id = metadata.get("headers", {}), metadata.get("client_ip", "Unknown"), job_id
        headers_json = orjson.dumps({"client_ip": client_ip, "headers": headers}).decode()
        received_at = datetime.fromisoformat(metadata["received_at"])
        
        stream_generator = await stream_processor.get_decompressed_stream(temp_file_path, headers.get("content-encoding"))
        async_file_reader = AsyncGeneratorReader(stream_generator)

        chunk = []
        async for record in ijson.items_async(async_file_reader, 'item'):
            chunk.append(record)
            if len(chunk) >= CHUNK_SIZE:
                processed, skipped, bts_context = await _process_and_save_chunk(chunk, headers, client_ip, headers_json, received_at, request_id, bts_context, total_records)
                total_processed += processed
                total_skipped += skipped
                total_records += len(chunk)
                await job_manager.set_job_status(job_id, {"status": "PROCESSING", "processed": total_processed, "skipped": total_skipped, "total_records": "streaming..."})
                chunk = []
        
        if chunk:
            processed, skipped, bts_context = await _process_and_save_chunk(chunk, headers, client_ip, headers_json, received_at, request_id, bts_context, total_records)
            total_processed += processed
            total_skipped += skipped
            total_records += len(chunk)

        final_status = {"status": "COMPLETE", "processed": total_processed, "skipped": total_skipped, "total_records": total_records}
        await job_manager.set_job_status(job_id, final_status)
    except Exception as e:
        await job_manager.set_job_status(job_id, {"status": "FAILED", "error": f"{type(e).__name__}: {e}", "processed": total_processed})
        raise
    finally:
        if os.path.exists(temp_file_path):
            os.remove(temp_file_path)

@shared_task(name='ingest.process_file')
def process_bulk_file_task(job_context: dict):
    asyncio.run(_async_bulk_processor(job_context))
