"""
POST /refresh — triggers data extraction from EIA API.
GET  /refresh/log — returns refresh history.
WS   /refresh/ws — streams real-time progress during extraction.
"""
import asyncio
import json
import logging
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, WebSocket, WebSocketDisconnect

from api.dependencies import get_extractor, get_writer, get_db, verify_api_key
from api.schemas import RefreshResponse
from connector.extractor import Extractor
from storage.parquet_writer import ParquetWriter
from storage.duckdb_engine import DuckDBEngine

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/refresh", tags=["refresh"])


def _log_run(writer: ParquetWriter, result: dict, source: str = "api") -> None:
    """Persist a refresh run record to the refresh_log Parquet file."""
    writer.append_refresh_log({
        "triggered_at": datetime.now(timezone.utc).isoformat(),
        "trigger_source": source,
        "status": result.get("status", "unknown"),
        "records_added": result.get("records_added", 0),
        "error_msg": result.get("error"),
    })


@router.post("", response_model=RefreshResponse)
def trigger_refresh(
    incremental: bool = True,
    extractor: Extractor = Depends(get_extractor),
    writer: ParquetWriter = Depends(get_writer),
    _: str = Depends(verify_api_key),
):
    """
    Trigger a data extraction run synchronously.
    Set incremental=false to force a full re-extraction.
    For real-time progress, use the WebSocket endpoint instead.
    """
    result = extractor.run(incremental=incremental)
    _log_run(writer, result, source="api")
    return RefreshResponse(**result)


@router.get("/log", tags=["refresh"])
def get_refresh_log(
    limit: int = 20,
    db: DuckDBEngine = Depends(get_db),
    _: str = Depends(verify_api_key),
):
    """Return the last N refresh run records."""
    return db.get_refresh_log(limit=limit)


@router.websocket("/ws")
async def refresh_ws(websocket: WebSocket):
    """
    WebSocket endpoint that streams extraction progress in real time.
    The frontend connects here when the user clicks Refresh to see live updates.

    Messages are JSON: { "event": "progress" | "done" | "error", "message": "...", "count": N }
    """
    await websocket.accept()
    logger.info("WebSocket refresh connection opened")

    async def send(event: str, message: str, count: int = 0):
        await websocket.send_text(json.dumps({
            "event": event,
            "message": message,
            "count": count,
        }))

    try:
        # Read optional config from client (incremental flag)
        raw = await asyncio.wait_for(websocket.receive_text(), timeout=5.0)
        config = json.loads(raw) if raw else {}
    except (asyncio.TimeoutError, Exception):
        config = {}

    incremental = config.get("incremental", True)

    # Progress callback — sends events to the WebSocket client
    # We use a list to capture values from the sync thread
    progress_events: list[tuple[str, int]] = []

    def progress_cb(message: str, count: int):
        progress_events.append((message, count))

    await send("progress", "Starting extraction...", 0)

    # Run extraction in a thread pool so it doesn't block the event loop
    loop = asyncio.get_event_loop()
    extractor = Extractor(writer=get_writer(), db=get_db())

    async def run_and_flush():
        """Run extractor and flush progress events to WebSocket periodically."""
        fut = loop.run_in_executor(None, extractor.run, incremental, progress_cb)
        while not fut.done():
            # Drain any queued progress events
            while progress_events:
                msg, count = progress_events.pop(0)
                await send("progress", msg, count)
            await asyncio.sleep(0.5)
        return await fut

    try:
        result = await run_and_flush()
        # Flush remaining events
        while progress_events:
            msg, count = progress_events.pop(0)
            await send("progress", msg, count)

        _log_run(get_writer(), result, source="websocket")

        if result.get("status") == "error":
            await send("error", result.get("error", "Unknown error"), 0)
        else:
            await send("done", f"Complete — {result.get('records_added', 0)} records added",
                       result.get("records_added", 0))

    except WebSocketDisconnect:
        logger.info("WebSocket client disconnected during refresh")
    except Exception as exc:
        logger.error("Unexpected error in WebSocket refresh: %s", exc)
        try:
            await send("error", str(exc), 0)
        except Exception:
            pass
    finally:
        try:
            await websocket.close()
        except Exception:
            pass