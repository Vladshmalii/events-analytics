from uuid import uuid4
import pytest
import time


@pytest.mark.asyncio
async def test_ingest_and_query(async_client, setup_database, setup_redis, setup_clickhouse):
    events = []
    for i in range(10):
        events.append({
            "event_id": str(uuid4()),
            "occurred_at": "2025-01-15T12:00:00Z",
            "user_id": f"user_{i % 3}",
            "event_type": "page_view",
            "properties": {"page": "/home"}
        })

    response = await async_client.post("/events", json=events)
    assert response.status_code == 202

    time.sleep(12)

    response = await async_client.get("/stats/dau?from=2025-01-15&to=2025-01-15")
    assert response.status_code == 200

    data = response.json()
    assert len(data) > 0
    assert data[0]["unique_users"] == 3