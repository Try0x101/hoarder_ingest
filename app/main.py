from fastapi import FastAPI
from fastapi.responses import ORJSONResponse
from app.routes.ingestion import router as ingestion_router
from app.routes.data_access import router as data_access_router
from app.routes.root import router as root_router
from app.routes.bulk import router as bulk_router

app = FastAPI(
    default_response_class=ORJSONResponse,
    title="Hoarder Ingest Server"
)

app.include_router(ingestion_router)
app.include_router(data_access_router)
app.include_router(bulk_router)
app.include_router(root_router) # The root router now handles the "/" path correctly

@app.on_event("startup")
async def startup():
    from app.database import get_db_lock
    await get_db_lock()
    print("Application startup complete. Lock initialized.")
