import os
import math
import datetime
import time
import aiosqlite
import asyncio
import orjson
import redis.asyncio as redis
from fastapi import APIRouter, Request
from fastapi.routing import APIRoute
from collections import defaultdict
from typing import Optional
from ..utils.formatters import build_base_url
from ..utils.device_extractors import extract_device_info
from ..database import get_latest_telemetry

router = APIRouter(tags=["Root"])
DB_FILE = "hoarder_ingest.db"
DB_PATH = os.path.join(os.path.dirname(__file__), "..", "..", DB_FILE)
MAX_DB_SIZE_BYTES = 1000 * 1024 * 1024
APP_START_TIME = time.time()
REDIS_METRICS_URL = "redis://localhost:6378/2"

async def get_service_status(service_name: str) -> str:
    try:
        proc = await asyncio.create_subprocess_shell(f"systemctl is-active {service_name}", stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        stdout, _ = await proc.communicate()
        return "active" if stdout.decode().strip() == "active" else "inactive"
    except Exception:
        return "unknown"

async def _get_historical_metrics():
    r = None
    try:
        r = redis.from_url(REDIS_METRICS_URL, decode_responses=True, socket_timeout=2)
        sys_raw, sync_raw, bulk_raw = await asyncio.gather(r.lrange("system_stats", 0, -1), r.lrange("sync_ingestion_stats", 0, -1), r.lrange("bulk_ingestion_stats", 0, -1))
        now = time.time()
        
        sys_1h = [orjson.loads(i) for i in sys_raw if now - orjson.loads(i)['ts'] <= 3600]
        sys_1m = [d for d in sys_1h if now - d['ts'] <= 60]
        
        sync_1h = [orjson.loads(i) for i in sync_raw if now - orjson.loads(i)['ts'] <= 3600]
        sync_1m = [d for d in sync_1h if now - d['ts'] <= 60]
        
        bulk_1h = [orjson.loads(i) for i in bulk_raw if now - orjson.loads(i)['ts'] <= 3600]
        bulk_1m = [d for d in bulk_1h if now - d['ts'] <= 60]

        sync_count_1m, sync_count_1h = sum(d['count'] for d in sync_1m), sum(d['count'] for d in sync_1h)
        bulk_count_1m, bulk_count_1h = sum(d['count'] for d in bulk_1m), sum(d['count'] for d in bulk_1h)

        return {
            "cpu_usage_average_last_1_minute_perc": round(sum(d['cpu_percent'] for d in sys_1m) / len(sys_1m), 2) if sys_1m else None,
            "cpu_usage_average_last_1_hour_perc": round(sum(d['cpu_percent'] for d in sys_1h) / len(sys_1h), 2) if sys_1h else None,
            "memory_usage_average_last_1_minute_mb": int(round(sum(d['mem_rss_bytes'] for d in sys_1m) / len(sys_1m) / 1024**2)) if sys_1m else None,
            "memory_usage_average_last_1_hour_mb": int(round(sum(d['mem_rss_bytes'] for d in sys_1h) / len(sys_1h) / 1024**2)) if sys_1h else None,
            "telemetry_batch_latency_average_last_1_minute_ms": int(round(sum(d['duration'] for d in sync_1m) / sync_count_1m * 1000)) if sync_count_1m > 0 else None,
            "telemetry_batch_latency_average_last_1_hour_ms": int(round(sum(d['duration'] for d in sync_1h) / sync_count_1h * 1000)) if sync_count_1h > 0 else None,
            "bulk_worker_throughput_average_last_1_minute_ms_per_record": int(round(sum(d['duration'] for d in bulk_1m) / bulk_count_1m * 1000)) if bulk_count_1m > 0 else None,
            "bulk_worker_throughput_average_last_1_hour_ms_per_record": int(round(sum(d['duration'] for d in bulk_1h) / bulk_count_1h * 1000)) if bulk_count_1h > 0 else None,
        }
    except (redis.RedisError, orjson.JSONDecodeError, ZeroDivisionError, TypeError, ValueError): return {}
    finally:
        if r: await r.close()

def format_last_seen_ago(seconds: Optional[float]) -> Optional[str]:
    if seconds is None or seconds < 0: return None
    if seconds < 2: return "1 second ago"
    if seconds < 60: return f"{round(seconds)} seconds ago"
    if seconds < 3600: return f"{round(seconds / 60)} minute{'s' if round(seconds/60) != 1 else ''} ago"
    if seconds < 86400: return f"{round(seconds / 3600)} hour{'s' if round(seconds/3600) != 1 else ''} ago"
    if seconds < 86400 * 7: return f"{round(seconds / 86400)} day{'s' if round(seconds/86400) != 1 else ''} ago"
    if seconds < 86400 * 30.44: return f"{round(seconds / (86400*7))} week{'s' if round(seconds/(86400*7)) != 1 else ''} ago"
    if seconds < 86400 * 365.25: return f"{round(seconds / (86400*30.44))} month{'s' if round(seconds/(86400*30.44)) != 1 else ''} ago"
    years = round(seconds / (86400 * 365.25), 1)
    return f"{int(years) if years == int(years) else years} year{'s' if int(years) != 1 else ''} ago"

def format_utc_timestamp(ts_string: str) -> str:
    if not ts_string: return None
    try: return datetime.datetime.fromisoformat(ts_string.replace(" ", "T")).strftime("%d.%m.%Y %H:%M:%S UTC")
    except (ValueError, TypeError): return None

@router.get("/")
async def root(request: Request):
    base_url = build_base_url(request)
    endpoint_groups = defaultdict(list)
    for r in request.app.routes:
        if isinstance(r, APIRoute) and r.tags and r.include_in_schema:
            endpoint_groups[r.tags[0]].append({"path": f"{base_url}{r.path}", "name": r.name, "methods": sorted(list(r.methods))})
    
    devices_raw = await get_latest_telemetry(limit=10)
    recently_active = []
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    for d in devices_raw:
        dev_info = extract_device_info(d)
        ts_utc, ago = None, None
        if ts_str := d.get('received_at'):
            try:
                aware_dt = datetime.datetime.fromisoformat(ts_str.replace(" ", "T")).replace(tzinfo=datetime.timezone.utc)
                ts_utc, ago = int(aware_dt.timestamp()), format_last_seen_ago((now_utc - aware_dt).total_seconds())
            except (ValueError, TypeError): pass

        traffic = {}
        if first_ts := d.get('first_seen_ts'):
            try:
                days = max(((now_utc - datetime.datetime.fromisoformat(first_ts.replace(" ", "T")).replace(tzinfo=datetime.timezone.utc)).total_seconds() / 86400.0), 1.0/24.0)
                avg_bytes = d.get('total_bytes', 0) / days
                traffic = {
                    "average_total_traffic_per_day_in_kb": int(round(avg_bytes / 1024)),
                    "average_total_traffic_per_week_in_mb": int(round((avg_bytes * 7) / 1024**2)),
                    "average_total_traffic_per_month_in_mb": int(round((avg_bytes * 30.44) / 1024**2)),
                }
            except (ValueError, TypeError, ZeroDivisionError): pass
        
        recently_active.append({
            "device_id": d.get('device_id'), "device_name": dev_info["device_name"], "client_ip": dev_info["client_ip"], "last_seen_ago": ago,
            "diagnostics": {"last_seen_timestamp_utc": ts_utc, "total_records": d.get('total_records', 0), "traffic": traffic},
            'links': {'latest': f"{base_url}/data/latest/{d.get('device_id')}", 'history': f"{base_url}/data/history?device_id={d.get('device_id')}&limit=50"}
        })

    db_stats, sys_health = {}, {}
    try:
        async with aiosqlite.connect(f"file:{DB_PATH}?mode=ro", uri=True) as db:
            db.row_factory = aiosqlite.Row
            queries = {
                "total_devices": "SELECT COUNT(DISTINCT device_id) as c FROM telemetry",
                "total_records": "SELECT COUNT(*) as c FROM telemetry",
                "time_range": "SELECT MIN(calculated_event_timestamp) as o, MAX(calculated_event_timestamp) as n FROM telemetry",
                "records_last_hour": ("SELECT COUNT(*) as c FROM telemetry WHERE received_at > ?", ((now_utc - datetime.timedelta(hours=1)).strftime('%Y-%m-%d %H:%M:%S'),)),
                "active_devices": ("SELECT COUNT(DISTINCT device_id) as c FROM telemetry WHERE calculated_event_timestamp > ?", ((now_utc - datetime.timedelta(hours=24)).strftime('%Y-%m-%d %H:%M:%S'),))
            }
            res = {name: await (await db.execute(*((q,) if isinstance(q, str) else q))).fetchone() for name, q in queries.items()}
        
        db_files, total_size = [], sum(os.path.getsize(DB_PATH + s) for s in ["", "-wal", "-shm"] if os.path.exists(DB_PATH + s))
        if os.path.exists(DB_PATH): db_files.append({"file": os.path.basename(DB_PATH), "size_mb": int(round(os.path.getsize(DB_PATH) / 1024**2)), "path": DB_PATH})
        
        storage_est, tr = {}, res.get('time_range')
        if tr and tr['o'] and tr['n'] and res['total_records']['c'] > 1000:
            try:
                days = max(((datetime.datetime.fromisoformat(tr['n'].replace(" ","T")) - datetime.datetime.fromisoformat(tr['o'].replace(" ","T"))).total_seconds() / 86400.0), 1.0/24.0)
                rate = total_size / days
                storage_est = {"database_retention_in_days": int(round(days)), "storage_rate_per_day_in_mb": int(round(rate / 1024**2)), "estimated_time_untill_full_in_days": int(round((MAX_DB_SIZE_BYTES - total_size) / rate)) if rate > 0 and (MAX_DB_SIZE_BYTES - total_size) > 0 else 0}
            except (ValueError, TypeError, ZeroDivisionError): pass
        
        db_stats = {"total_ingested_records": res['total_records']['c'], "total_unique_devices": res['total_devices']['c'], "oldest_record": format_utc_timestamp(tr['o']), "newest_record": format_utc_timestamp(tr['n']), "database_files": db_files, "total_database_size_in_mb": int(round(total_size / 1024**2)), "database_size_limit_in_mb": int(MAX_DB_SIZE_BYTES / 1024**2), "storage_estimation": storage_est}

        worker, beat, metrics = await asyncio.gather(get_service_status('hoarder-worker.service'), get_service_status('hoarder-beat.service'), _get_historical_metrics())
        time_since = int((now_utc - datetime.datetime.fromisoformat(tr['n'].replace(" ","T")).replace(tzinfo=datetime.timezone.utc)).total_seconds()) if tr and tr['n'] else -1
        
        checks = [worker == 'active', beat == 'active', time_since >= 0 and time_since < 1800]
        sys_health = {
            "uptime": {"time_since_restart_utc": int(APP_START_TIME), "time_since_restart": format_last_seen_ago(time.time() - APP_START_TIME), "uptime_percentage": round(sum(checks) / len(checks) * 100, 2)},
            "activity": {"time_since_last_record_seconds": time_since if time_since >=0 else None, "records_ingested_last_hour": res['records_last_hour']['c'], "active_devices_last_24h": res['active_devices']['c']},
            "celery_status": {"worker": worker, "beat": beat}, "performance": metrics,
            "webhook_status": "Configured and active" if os.environ.get("PROCESSOR_WEBHOOK_URL") else "Not configured"
        }
    except aiosqlite.Error as e:
        db_stats, sys_health = {"error": f"DB error: {e}"}, {"error": "Could not query system health."}

    return {
        "request": {"self_url": f"{base_url}/"}, "server": "Hoarder Ingest Server", "system_health": sys_health,
        "diagnostics": {"database_stats": db_stats}, "recently_active_devices": recently_active,
        "api_endpoints": dict(sorted(endpoint_groups.items()))
    }
