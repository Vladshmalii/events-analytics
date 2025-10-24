# test_idempotency.py
from uuid import uuid4
import pytest
import asyncio


@pytest.mark.asyncio
async def test_event_idempotency(async_client):
    event_id = str(uuid4())

    event_data = {
        "events": [{
            "event_id": event_id,
            "occurred_at": "2025-01-15T12:00:00Z",
            "user_id": "user_123",
            "event_type": "page_view",
            "properties": {"page": "/home"}
        }]
    }

    response1 = await async_client.post("/events", json=event_data)
    assert response1.status_code == 202

    await asyncio.sleep(2)

    response2 = await async_client.post("/events", json=event_data)
    assert response2.status_code == 202

    await asyncio.sleep(8)

    from app.db.clickhouse import get_client
    client = get_client()
    result = client.query(f"SELECT COUNT(*) FROM analytics.events WHERE event_id = '{event_id}'")
    count = result.result_rows[0][0]
    assert count == 1


# @pytest.mark.asyncio
# async def test_rate_limit(async_client_no_deps):
#     from app.config import settings
#
#     print(f"Testing rate limit: {settings.rate_limit_per_minute}")
#
#     hit_limit = False
#
#     for i in range(settings.rate_limit_per_minute + 10):
#         event_data = {
#             "events": [{
#                 "event_id": str(uuid4()),
#                 "occurred_at": "2025-01-15T12:00:00Z",
#                 "user_id": "test",
#                 "event_type": "test",
#                 "properties": {}
#             }]
#         }
#
#         response = await async_client_no_deps.post("/events", json=event_data)
#
#         if response.status_code == 429:
#             hit_limit = True
#             print(f"Rate limit hit at request {i + 1}")
#             break
#
#     assert hit_limit, f"Should hit rate limit after {settings.rate_limit_per_minute} requests"