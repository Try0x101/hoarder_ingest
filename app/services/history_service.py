import orjson
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
from app.database import get_historical_telemetry
from app.utils.formatters import create_pagination_links, format_utc_timestamp, create_device_links
from .timestamp_calculator import format_display_timestamp

def _check_for_warnings(record: Dict[str, Any]) -> List[str]:
    warnings = []
    calculated_ts_str = record.get("calculated_event_timestamp")
    received_ts_str = record.get("received_at")

    if calculated_ts_str and received_ts_str:
        try:
            if '.' in calculated_ts_str:
                calculated_dt = datetime.strptime(calculated_ts_str, "%Y-%m-%d %H:%M:%S.%f")
            else:
                calculated_dt = datetime.strptime(calculated_ts_str, "%Y-%m-%d %H:%M:%S")

            received_dt = datetime.strptime(received_ts_str, "%Y-%m-%d %H:%M:%S")
            
            if calculated_dt > received_dt + timedelta(seconds=5):
                time_diff = round((calculated_dt - received_dt).total_seconds())
                warnings.append(f"client_clock_skew_detected: event_time is {time_diff}s after receive_time")
        except (ValueError, TypeError):
            pass
            
    return warnings

async def process_device_history(device_id: str, limit: int, cursor: Optional[str], base_url: str) -> Dict[str, Any]:
    cursor_ts: Optional[str] = None
    cursor_id: Optional[int] = None
    if cursor:
        try:
            parts = cursor.split(',')
            if len(parts) == 2:
                cursor_ts = parts[0]
                cursor_id = int(parts[1])
        except (ValueError, IndexError):
            pass

    history_data = await get_historical_telemetry(device_id, limit, cursor_ts, cursor_id)
    
    history_data.sort(key=lambda r: (r.get('calculated_event_timestamp', '0'), r.get('id', 0)), reverse=True)

    if not history_data:
        return {
            "links": create_pagination_links(base_url, device_id, limit),
            "device_id": device_id,
            "records_shown": 0,
            "data": [],
            "next_cursor": None,
            "message": f"No data found for device '{device_id}'"
        }

    processed_data = []
    next_cursor = None
    
    for record in history_data:
        try:
            payload = orjson.loads(record["payload"]) if isinstance(record["payload"], str) else record["payload"]
        except orjson.JSONDecodeError:
            payload = {"error": "invalid json stored in db"}

        request_info = None
        headers_str = record.get("request_headers")
        if headers_str:
            try:
                request_info = orjson.loads(headers_str)
            except orjson.JSONDecodeError:
                request_info = {"error": "invalid headers json in db"}

        processed_record = {
            "id": record["id"],
            "request_id": record.get("request_id"),
            "payload": payload,
            "data_timestamp_calculated": format_display_timestamp(record.get("calculated_event_timestamp")),
            "received_at": format_utc_timestamp(record.get("received_at"))
        }
        
        if request_info:
            processed_record["request_info"] = request_info
        
        warnings = _check_for_warnings(record)
        if warnings:
            processed_record["warnings"] = warnings
            
        processed_data.append(processed_record)
    
    if len(history_data) == limit:
        last_record = history_data[-1]
        next_cursor = f"{last_record['calculated_event_timestamp']},{last_record['id']}"
    
    return {
        "links": create_pagination_links(base_url, device_id, limit, next_cursor),
        "device_id": device_id,
        "records_shown": len(processed_data),
        "pagination": {"limit": limit, "next_cursor": next_cursor},
        "data": processed_data
    }

async def get_latest_device_record(device_id: str, base_url: str) -> Dict[str, Any]:
    history_data = await get_historical_telemetry(device_id, 1)
    if not history_data:
        return None
    
    record = history_data[0]
    
    try:
        payload = orjson.loads(record["payload"]) if isinstance(record["payload"], str) else record["payload"]
    except orjson.JSONDecodeError:
        payload = {"error": "invalid json stored in db"}

    request_info = None
    headers_str = record.get("request_headers")
    if headers_str:
        try:
            request_info = orjson.loads(headers_str)
        except orjson.JSONDecodeError:
            request_info = {"error": "invalid headers json in db"}

    result = {
        "links": create_device_links(base_url, device_id),
        "device_id": device_id,
        "id": record["id"],
        "request_id": record.get("request_id"),
        "payload": payload,
        "data_timestamp_calculated": format_display_timestamp(record.get("calculated_event_timestamp")),
        "received_at": format_utc_timestamp(record.get("received_at"))
    }
    
    if request_info:
        result["request_info"] = request_info
        
    warnings = _check_for_warnings(record)
    if warnings:
        result["warnings"] = warnings
        
    return result

async def backfill_timestamps():
    import aiosqlite
    import asyncio
    from app.database import DB_PATH
    from app.services.timestamp_calculator import calculate_ingestion_timestamps, format_for_db
    from datetime import datetime, timezone

    print("Starting backfill process for calculated_event_timestamp...")
    conn = None
    try:
        conn = await aiosqlite.connect(DB_PATH)
        conn.row_factory = aiosqlite.Row
        cursor = await conn.cursor()
        
        await cursor.execute("""
            SELECT request_id, received_at FROM telemetry 
            WHERE calculated_event_timestamp IS NULL AND request_id IS NOT NULL
            GROUP BY request_id ORDER BY id
        """)
        batch_groups = await cursor.fetchall()
        total_groups = len(batch_groups)
        print(f"Found {total_groups} request groups to process.")
        
        processed_count = 0
        for i, group in enumerate(batch_groups):
            try:
                received_at_dt = datetime.strptime(group['received_at'], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                
                await cursor.execute("""
                    SELECT id, payload FROM telemetry
                    WHERE request_id = ? AND calculated_event_timestamp IS NULL
                    ORDER BY id
                """, (group['request_id'],))
                
                records = await cursor.fetchall()
                if not records: continue

                payload_list = [orjson.loads(rec['payload']) for rec in records]
                record_ids = [rec['id'] for rec in records]
                
                calculated_times = calculate_ingestion_timestamps(payload_list, received_at_dt)
                
                update_data = [(format_for_db(ts), r_id) for ts, r_id in zip(calculated_times, record_ids)]
                
                await conn.executemany("UPDATE telemetry SET calculated_event_timestamp = ? WHERE id = ?", update_data)
                await conn.commit()
                
                processed_count += len(records)
                print(f"  Processed group {i+1}/{total_groups} (request_id: {group['request_id']}). Records updated: {len(records)}")
            except Exception as e:
                print(f"  Skipping group {i+1} (request_id: {group['request_id']}) due to error: {e}")
            await asyncio.sleep(0.01)

        print(f"\nBackfill complete. Updated {processed_count} total records.")
    except Exception as e:
        print(f"An error occurred during backfill: {e}")
    finally:
        if conn: await conn.close()

if __name__ == "__main__":
    import asyncio
    asyncio.run(backfill_timestamps())