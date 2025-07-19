from fastapi import APIRouter, Request, HTTPException, Query
from typing import Optional
from ..services.data_service import get_device_list, process_device_history, get_latest_device_record
from ..utils.formatters import build_base_url

router = APIRouter(prefix="/data", tags=["Data Access"])

@router.get("/history")
async def get_device_history(
    device_id: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=500),
    cursor: Optional[str] = Query(None),
    request: Request = None
):
    try:
        base_url = build_base_url(request)
        return await process_device_history(device_id, limit, cursor, base_url)
    except Exception as e:
        print(f"History endpoint error: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve device history")

@router.get("/latest/{device_id}")
async def get_latest_device_data(device_id: str, request: Request):
    try:
        base_url = build_base_url(request)
        result = await get_latest_device_record(device_id, base_url)
        if not result:
            raise HTTPException(status_code=404, detail=f"Device '{device_id}' not found")
        return result
    except HTTPException:
        raise
    except Exception as e:
        print(f"Latest endpoint error: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve device data")