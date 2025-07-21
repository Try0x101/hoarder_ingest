import redis.asyncio as redis
import orjson
from typing import Optional, Dict, Any
import decimal

JOB_STATUS_KEY_PREFIX = "job:status:"
JOB_STATUS_TTL_SECONDS = 86400

def _orjson_decimal_default(obj: Any) -> Any:
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    raise TypeError

async def set_job_status(job_id: str, status_data: Dict[str, Any]):
    r = redis.Redis.from_url("redis://localhost:6378/1")
    key = f"{JOB_STATUS_KEY_PREFIX}{job_id}"
    value = orjson.dumps(status_data, default=_orjson_decimal_default)
    try:
        await r.set(key, value, ex=JOB_STATUS_TTL_SECONDS)
    finally:
        await r.close()

async def get_job_status(job_id: str) -> Optional[Dict[str, Any]]:
    r = redis.Redis.from_url("redis://localhost:6378/1")
    key = f"{JOB_STATUS_KEY_PREFIX}{job_id}"
    try:
        value = await r.get(key)
        if value:
            return orjson.loads(value)
        return None
    finally:
        await r.close()
