from fastapi import APIRouter, Request, HTTPException, Query, Depends
from typing import Optional, Dict
from ..services.data_service import get_device_list, process_device_history, get_latest_device_record
from ..utils.formatters import build_base_url
from ..security import get_current_user

router = APIRouter(prefix="/data", tags=["Data Access"])

@router.get("/history", dependencies=[Depends(get_current_user)])
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

@router.get("/latest/{device_id}", dependencies=[Depends(get_current_user)])
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

@router.get("/devices", dependencies=[Depends(get_current_user)])
async def get_devices_endpoint(request: Request, limit: int = Query(20, ge=1, le=100)):
    base_url = build_base_url(request)
    devices = await get_device_list(base_url, limit=limit)
    return {
        "request": {"self_url": f"{base_url}/data/devices?limit={limit}"},
        "navigation": {"root": f"{base_url}/"},
        "data": devices
    }
