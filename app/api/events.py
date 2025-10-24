from fastapi import APIRouter, HTTPException, status, Depends
from app.models.events import Event, EventBatch
from app.tasks.workers import process_events
from prometheus_client import Counter, Histogram
import structlog

router = APIRouter()
logger = structlog.get_logger()

events_counter = Counter('events_received_total', 'Total events received')
events_failed_counter = Counter('events_failed_total', 'Total events failed')
events_duration = Histogram('events_processing_seconds', 'Event processing duration')


@router.post("/events", status_code=status.HTTP_202_ACCEPTED)
async def ingest_events(batch: EventBatch):
    try:
        events_data = [event.model_dump(mode='json') for event in batch.events]

        process_events.delay(events_data)

        events_counter.inc(len(batch.events))

        logger.info("events_queued", count=len(batch.events))

        return {"status": "accepted", "count": len(batch.events)}

    except Exception as e:
        events_failed_counter.inc()
        logger.error("events_ingest_failed", error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to process events"
        )