from fastapi import APIRouter, Request
from controllers.ingest_controller import handle_ingest, handle_webhook_task_done

router = APIRouter(tags=["ingest"])

@router.post("/ingest")
async def ingest(request: Request):
    return await handle_ingest(request)

@router.post("/webhook/task_done")
async def webhook_task_done(request: Request):
    """
    Webhook endpoint called by Go Fetcher to deliver crawled page batches.
    Receives batches of pages and queues them for analysis.
    
    Expected payload:
    {
        "request_id": "uuid",
        "batch_id": "uuid", 
        "pages": [
            {
                "url": "https://example.com",
                "html_content": "<html>...",
                "http_status": 200,
                "is_javascript_heavy": false,
                "fetch_duration_ms": 1500
            }
        ]
    }
    """
    return await handle_webhook_task_done(request)
