from fastapi import APIRouter, Query, HTTPException
from app.db.clickhouse import query_dau, query_top_events, query_retention, query_with_filter
from typing import Optional
import structlog

router = APIRouter()
logger = structlog.get_logger()


@router.get("/stats/dau")
async def get_dau(
        from_date: str = Query(..., alias="from", pattern=r"^\d{4}-\d{2}-\d{2}$"),
        to_date: str = Query(..., alias="to", pattern=r"^\d{4}-\d{2}-\d{2}$"),
        segment: Optional[str] = None
):
    try:
        if segment:
            result = query_with_filter(from_date, to_date, segment)
        else:
            result = query_dau(from_date, to_date)

        logger.info("dau_query", from_date=from_date, to_date=to_date, count=len(result))
        return result

    except Exception as e:
        logger.error("dau_query_failed", error=str(e))
        raise HTTPException(status_code=500, detail="Query failed")


@router.get("/stats/top-events")
async def get_top_events(
        from_date: str = Query(..., alias="from", pattern=r"^\d{4}-\d{2}-\d{2}$"),
        to_date: str = Query(..., alias="to", pattern=r"^\d{4}-\d{2}-\d{2}$"),
        limit: int = Query(10, ge=1, le=100)
):
    try:
        result = query_top_events(from_date, to_date, limit)
        logger.info("top_events_query", from_date=from_date, to_date=to_date, limit=limit)
        return result

    except Exception as e:
        logger.error("top_events_query_failed", error=str(e))
        raise HTTPException(status_code=500, detail="Query failed")


@router.get("/stats/retention")
async def get_retention(
        start_date: str = Query(..., pattern=r"^\d{4}-\d{2}-\d{2}$"),
        windows: int = Query(3, ge=1, le=12)
):
    try:
        result = query_retention(start_date, windows)
        logger.info("retention_query", start_date=start_date, windows=windows)
        return result

    except Exception as e:
        logger.error("retention_query_failed", error=str(e))
        raise HTTPException(status_code=500, detail="Query failed")