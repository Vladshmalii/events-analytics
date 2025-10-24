# conftest.py
import pytest
import pytest_asyncio
from httpx import AsyncClient, ASGITransport
from app.main import app
from app.db.postgres import Base, engine
from app.db.clickhouse import get_client
from app.db.redis_client import redis_client


@pytest.fixture(scope="session")
def anyio_backend():
    return "asyncio"


@pytest_asyncio.fixture(scope="function")
async def setup_postgres():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
    yield


@pytest_asyncio.fixture(scope="function")
async def setup_clickhouse():
    client = get_client()
    try:
        client.command("DROP TABLE IF EXISTS analytics.events_buffer")
        client.command("DROP TABLE IF EXISTS analytics.events")
    except:
        pass

    client.command("""
        CREATE TABLE analytics.events (
            event_id String,
            occurred_at DateTime,
            user_id String,
            event_type String,
            properties String
        ) ENGINE = MergeTree()
        PARTITION BY toYYYYMM(occurred_at)
        ORDER BY (occurred_at, user_id, event_type)
    """)

    client.command("""
        CREATE TABLE analytics.events_buffer AS analytics.events
        ENGINE = Buffer(analytics, events, 16, 1, 5, 1000, 10000, 1000000, 10000000)
    """)
    yield


@pytest_asyncio.fixture(scope="function")
async def setup_redis():
    await redis_client.connect()
    await redis_client.redis.flushdb()
    yield
    await redis_client.redis.flushdb()
    await redis_client.close()


@pytest_asyncio.fixture
async def async_client(setup_postgres, setup_redis, setup_clickhouse):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


@pytest_asyncio.fixture
async def async_client_no_deps():
    await redis_client.connect()
    await redis_client.redis.flushdb()

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client

    await redis_client.redis.flushdb()
    await redis_client.close()