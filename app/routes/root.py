import os
from fastapi import APIRouter, Request
from fastapi.routing import APIRoute
from collections import defaultdict
from ..services.data_service import get_device_list
from ..utils.formatters import build_base_url

router = APIRouter(tags=["Root"])

@router.get("/")
async def root(request: Request):
    base_url = build_base_url(request)
    
    endpoint_groups = defaultdict(list)
    for route in request.app.routes:
        if isinstance(route, APIRoute) and route.tags:
            group = route.tags[0]
            endpoint_groups[group].append({
                "path": route.path,
                "name": route.name,
                "methods": sorted(list(route.methods)),
            })
    
    recently_active = await get_device_list(base_url, limit=10)

    # --- Temporary Diagnostic ---
    webhook_url_status = os.environ.get("PROCESSOR_WEBHOOK_URL", "NOT SET")
    # --- End Diagnostic ---

    return {
        "server": "Hoarder Ingest Server",
        "status": "online",
        "diagnostics": {
            "PROCESSOR_WEBHOOK_URL": webhook_url_status
        },
        "endpoints": dict(sorted(endpoint_groups.items())),
        "recently_active_devices": recently_active,
        "links": {"self": f"{base_url}/", "history": f"{base_url}/data/history"}
    }
