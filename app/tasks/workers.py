from datetime import datetime, timedelta, timezone
import asyncio
import json
import structlog
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy import text, delete
from sqlalchemy.pool import NullPool
from app.tasks.celery_app import celery_app
from app.db.clickhouse import insert_events
from app.config import settings

logger = structlog.get_logger()

DATABASE_URL = f"postgresql+asyncpg://{settings.postgres_user}:{settings.postgres_password}@{settings.postgres_host}:{settings.postgres_port}/{settings.postgres_db}"


def get_async_session():
    engine = create_async_engine(DATABASE_URL, echo=False, pool_pre_ping=True, poolclass=NullPool)
    return async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


@celery_app.task(bind=True, max_retries=3)
def process_events(self, events_data: list):
    asyncio.run(_process_events_async(events_data))


async def _process_events_async(events_data: list):
    async_session_maker = get_async_session()
    async with async_session_maker() as session:
        new_events = []
        now = datetime.now(timezone.utc)

        for event in events_data:
            event_id = str(event['event_id'])

            if isinstance(event['occurred_at'], str):
                occurred_at = datetime.fromisoformat(event['occurred_at'].replace('Z', '+00:00'))
            else:
                occurred_at = event['occurred_at']

            result = await session.execute(
                text("SELECT 1 FROM event_dedup WHERE event_id = :event_id"),
                {"event_id": event_id}
            )

            if result.scalar() is None:
                await session.execute(
                    text(
                        "INSERT INTO event_dedup (event_id, created_at) VALUES (:event_id, :created_at) ON CONFLICT DO NOTHING"),
                    {"event_id": event_id, "created_at": now}
                )

                await session.execute(
                    text("""
                        INSERT INTO hot_events (event_id, occurred_at, user_id, event_type, properties, created_at)
                        VALUES (:event_id, :occurred_at, :user_id, :event_type, :properties, :created_at)
                        ON CONFLICT DO NOTHING
                    """),
                    {
                        "event_id": event_id,
                        "occurred_at": occurred_at,
                        "user_id": event['user_id'],
                        "event_type": event['event_type'],
                        "properties": json.dumps(event['properties']),
                        "created_at": now
                    }
                )

                event_copy = event.copy()
                event_copy['occurred_at'] = occurred_at
                new_events.append(event_copy)

        await session.commit()

        if new_events:
            insert_events(new_events)
            logger.info("events_processed", count=len(new_events))


@celery_app.task
def process_batch_import(batch_key: str, events_data: list):
    asyncio.run(_process_batch_import_async(batch_key, events_data))


async def _process_batch_import_async(batch_key: str, events_data: list):
    async_session_maker = get_async_session()
    async with async_session_maker() as session:
        now = datetime.now(timezone.utc)

        result = await session.execute(
            text("SELECT 1 FROM batch_dedup WHERE batch_key = :batch_key"),
            {"batch_key": batch_key}
        )

        if result.scalar() is not None:
            logger.info("batch_already_processed", batch_key=batch_key)
            return

        await session.execute(
            text("INSERT INTO batch_dedup (batch_key, created_at) VALUES (:batch_key, :created_at)"),
            {"batch_key": batch_key, "created_at": now}
        )
        await session.commit()

    await _process_events_async(events_data)
    logger.info("batch_imported", batch_key=batch_key, count=len(events_data))


@celery_app.task
def cleanup_hot_events():
    asyncio.run(_cleanup_hot_events_async())


async def _cleanup_hot_events_async():
    async_session_maker = get_async_session()
    async with async_session_maker() as session:
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=7)

        result = await session.execute(
            text("DELETE FROM hot_events WHERE occurred_at < :cutoff_date"),
            {"cutoff_date": cutoff_date}
        )

        await session.commit()
        logger.info("hot_events_cleaned", deleted_count=result.rowcount)