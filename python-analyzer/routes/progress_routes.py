"""
progress_routes.py — WebSocket and SSE endpoints for real-time ingest progress
"""

import json
import asyncio
import logging
from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Query, HTTPException
from utils.progress_manager import get_progress_manager

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/progress", tags=["progress"])


@router.websocket("/ws/{session_id}")
async def websocket_progress(websocket: WebSocket, session_id: str):
    """
    WebSocket endpoint for real-time progress updates.
    Client connects and receives JSON progress updates.

    Usage:
        ws = new WebSocket("ws://localhost:8000/progress/ws/my-session-id");
        ws.onmessage = (event) => { console.log(JSON.parse(event.data)); };
    """
    await websocket.accept()
    progress_mgr = get_progress_manager()

    # Check if session exists
    session = await progress_mgr.get_session(session_id)
    if not session:
        await websocket.send_json({"error": "session not found"})
        await websocket.close(code=1000)
        return

    # Create callback for progress updates
    async def on_update(state: dict):
        try:
            await websocket.send_json(state)
        except Exception as e:
            logger.error(f"[ws:send_error] {e}")

    try:
        await progress_mgr.subscribe(session_id, on_update)
        # Send initial state
        await on_update(session.to_dict())
        # Keep connection alive
        while True:
            data = await websocket.receive_text()
            # Optional: client can send commands like "ping" or "status"
            if data == "ping":
                await websocket.send_json({"type": "pong"})
    except WebSocketDisconnect:
        logger.info(f"[ws:disconnect] session={session_id}")
        await progress_mgr.unsubscribe(session_id, on_update)
    except Exception as e:
        logger.error(f"[ws:error] {e}")


@router.get("/status/{session_id}")
async def get_progress(session_id: str):
    """
    HTTP endpoint to get current progress for a session.

    Usage:
        GET /progress/status/my-session-id
    """
    progress_mgr = get_progress_manager()
    session = await progress_mgr.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="session not found")
    return session.to_dict()


@router.post("/start/{session_id}")
async def start_session(session_id: str, total_urls: int = Query(0)):
    """
    Start tracking a new ingest session.

    Usage:
        POST /progress/start/my-session-id?total_urls=100
    """
    progress_mgr = get_progress_manager()
    await progress_mgr.create_session(session_id, total_urls)
    session = await progress_mgr.get_session(session_id)
    return session.to_dict()


@router.post("/end/{session_id}")
async def end_session(session_id: str, success: bool = True, error: str = None):
    """
    Mark a session as complete.

    Usage:
        POST /progress/end/my-session-id?success=true
    """
    progress_mgr = get_progress_manager()
    await progress_mgr.complete_session(session_id, success, error)
    session = await progress_mgr.get_session(session_id)
    return session.to_dict()
