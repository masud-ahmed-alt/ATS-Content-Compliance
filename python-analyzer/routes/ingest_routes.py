from fastapi import APIRouter, Request
from controllers.ingest_controller import handle_ingest

router = APIRouter(tags=["ingest"])

@router.post("/ingest")
async def ingest(request: Request):
    return await handle_ingest(request)
