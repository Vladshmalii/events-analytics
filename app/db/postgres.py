from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, String, DateTime, UniqueConstraint, text, func
from datetime import datetime
from app.config import settings

DATABASE_URL = f"postgresql+asyncpg://{settings.postgres_user}:{settings.postgres_password}@{settings.postgres_host}:{settings.postgres_port}/{settings.postgres_db}"

engine = create_async_engine(DATABASE_URL, echo=False, pool_pre_ping=True)
async_session_maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

Base = declarative_base()


class EventDedup(Base):
    __tablename__ = "event_dedup"

    event_id = Column(String, primary_key=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class HotEvent(Base):
    __tablename__ = "hot_events"

    event_id = Column(String, primary_key=True)
    occurred_at = Column(DateTime(timezone=True), nullable=False)
    user_id = Column(String, nullable=False)
    event_type = Column(String, nullable=False)
    properties = Column(String, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class BatchDedup(Base):
    __tablename__ = "batch_dedup"

    batch_key = Column(String, primary_key=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)


async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_hot_events_occurred ON hot_events(occurred_at)"))
        await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_hot_events_user ON hot_events(user_id)"))


async def get_session() -> AsyncSession:
    async with async_session_maker() as session:
        yield session


async def check_event_exists(session: AsyncSession, event_id: str) -> bool:
    result = await session.execute(
        text("SELECT 1 FROM event_dedup WHERE event_id = :event_id"),
        {"event_id": event_id}
    )
    return result.scalar() is not None


async def check_batch_exists(session: AsyncSession, batch_key: str) -> bool:
    result = await session.execute(
        text("SELECT 1 FROM batch_dedup WHERE batch_key = :batch_key"),
        {"batch_key": batch_key}
    )
    return result.scalar() is not None