from fastapi import FastAPI
from fastapi.responses import ORJSONResponse
from starlette.middleware.sessions import SessionMiddleware
import os
from app.routes.ingestion import router as ingestion_router
from app.routes.data_access import router as data_access_router
from app.routes.root import router as root_router
from app.routes.bulk import router as bulk_router
from app.routes.auth import router as auth_router

app = FastAPI(
    default_response_class=ORJSONResponse,
    title="Hoarder Ingest Server"
)

app.add_middleware(
    SessionMiddleware,
    secret_key=os.environ.get("SESSION_SECRET_KEY", "your-default-secret-key"),
    https_only=True,
    max_age=86400 * 14
)

app.include_router(ingestion_router)
app.include_router(data_access_router)
app.include_router(bulk_router)
app.include_router(auth_router)
app.include_router(root_router)

@app.on_event("startup")
async def startup():
    from app.database import get_db_lock
    await get_db_lock()
    print("Application startup complete. Lock initialized.")
