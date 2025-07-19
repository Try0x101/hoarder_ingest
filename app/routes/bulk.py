import os
import uuid
import aiofiles
from datetime import datetime, timezone
from fastapi import APIRouter, Request, HTTPException, status
from celery_app import celery_app
from app.services import job_manager

router = APIRouter(prefix="/api/bulk", tags=["Bulk Ingestion"])
UPLOAD_DIR = "/tmp/hoarder_uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)

@router.post("", status_code=status.HTTP_202_ACCEPTED)
async def bulk_upload(request: Request):
    job_id = str(uuid.uuid4())
    temp_file_path = os.path.join(UPLOAD_DIR, f"{job_id}.upload")
    
    metadata = {
        "received_at": datetime.now(timezone.utc).isoformat(),
        "headers": dict(request.headers),
        "client_ip": request.client.host
    }

    try:
        bytes_written = 0
        async with aiofiles.open(temp_file_path, 'wb') as f:
            async for chunk in request.stream():
                await f.write(chunk)
                bytes_written += len(chunk)
        
        if bytes_written == 0:
            raise HTTPException(status_code=400, detail="Empty request body")

    except Exception as e:
        if os.path.exists(temp_file_path):
            os.remove(temp_file_path)
        raise HTTPException(status_code=500, detail=f"Failed to save upload: {e}")

    job_context = {
        "job_id": job_id,
        "temp_file_path": temp_file_path,
        "metadata": metadata
    }
    
    celery_app.send_task('ingest.process_file', args=[job_context])
    
    initial_status = {"status": "PENDING", "details": "Job accepted and queued for processing."}
    await job_manager.set_job_status(job_id, initial_status)

    return {"job_id": job_id}

@router.get("/status/{job_id}")
async def get_bulk_status(job_id: str):
    status_data = await job_manager.get_job_status(job_id)
    if status_data is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found.")
    return status_data
