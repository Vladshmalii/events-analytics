import clickhouse_connect
from app.config import settings
from typing import List, Dict, Any
import json


def get_client():
    return clickhouse_connect.get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        database=settings.clickhouse_db,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password
    )


async def init_clickhouse():
    client = get_client()

    client.command(f"CREATE DATABASE IF NOT EXISTS {settings.clickhouse_db}")

    client.command(f"""
        CREATE TABLE IF NOT EXISTS {settings.clickhouse_db}.events (
            event_id String,
            occurred_at DateTime,
            user_id String,
            event_type String,
            properties String,
            created_at DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        PARTITION BY toYYYYMM(occurred_at)
        ORDER BY (occurred_at, user_id, event_type)
    """)

    client.command(f"""
        CREATE TABLE IF NOT EXISTS {settings.clickhouse_db}.events_buffer AS {settings.clickhouse_db}.events
        ENGINE = Buffer({settings.clickhouse_db}, events, 16, 10, 100, 10000, 1000000, 10000000, 100000000)
    """)


def insert_events(events: List[Dict[str, Any]]):
    if not events:
        return

    client = get_client()
    data = [
        [
            str(event['event_id']),
            event['occurred_at'],
            event['user_id'],
            event['event_type'],
            json.dumps(event['properties'])
        ]
        for event in events
    ]

    client.insert(
        f"{settings.clickhouse_db}.events_buffer",
        data,
        column_names=['event_id', 'occurred_at', 'user_id', 'event_type', 'properties']
    )


def query_dau(from_date: str, to_date: str) -> List[Dict[str, Any]]:
    client = get_client()
    query = f"""
        SELECT 
            toDate(occurred_at) as date,
            uniq(user_id) as unique_users
        FROM {settings.clickhouse_db}.events
        WHERE toDate(occurred_at) BETWEEN '{from_date}' AND '{to_date}'
        GROUP BY date
        ORDER BY date
    """
    result = client.query(query)
    return [{"date": str(row[0]), "unique_users": row[1]} for row in result.result_rows]


def query_top_events(from_date: str, to_date: str, limit: int = 10) -> List[Dict[str, Any]]:
    client = get_client()
    query = f"""
        SELECT 
            event_type,
            count() as count
        FROM {settings.clickhouse_db}.events
        WHERE toDate(occurred_at) BETWEEN '{from_date}' AND '{to_date}'
        GROUP BY event_type
        ORDER BY count DESC
        LIMIT {limit}
    """
    result = client.query(query)
    return [{"event_type": row[0], "count": row[1]} for row in result.result_rows]


def query_retention(start_date: str, windows: int = 3) -> List[Dict[str, Any]]:
    client = get_client()
    query = f"""
        WITH cohort AS (
            SELECT DISTINCT
                user_id,
                toMonday(toDate(occurred_at)) as cohort_week
            FROM {settings.clickhouse_db}.events
            WHERE toDate(occurred_at) >= '{start_date}'
        ),
        activity AS (
            SELECT DISTINCT
                user_id,
                toMonday(toDate(occurred_at)) as activity_week
            FROM {settings.clickhouse_db}.events
            WHERE toDate(occurred_at) >= '{start_date}'
        )
        SELECT
            cohort.cohort_week,
            COUNT(DISTINCT cohort.user_id) as week_0,
            COUNT(DISTINCT CASE WHEN dateDiff('week', cohort.cohort_week, activity.activity_week) = 1 THEN activity.user_id END) as week_1,
            COUNT(DISTINCT CASE WHEN dateDiff('week', cohort.cohort_week, activity.activity_week) = 2 THEN activity.user_id END) as week_2,
            COUNT(DISTINCT CASE WHEN dateDiff('week', cohort.cohort_week, activity.activity_week) = 3 THEN activity.user_id END) as week_3
        FROM cohort
        LEFT JOIN activity ON cohort.user_id = activity.user_id
        GROUP BY cohort.cohort_week
        ORDER BY cohort.cohort_week
        LIMIT {windows}
    """
    result = client.query(query)

    retention_data = []
    for row in result.result_rows:
        week_0 = row[1]
        retention_data.append({
            "cohort_week": str(row[0]),
            "week_0": week_0,
            "week_1": round((row[2] / week_0 * 100) if week_0 > 0 else 0, 2),
            "week_2": round((row[3] / week_0 * 100) if week_0 > 0 else 0, 2),
            "week_3": round((row[4] / week_0 * 100) if week_0 > 0 else 0, 2)
        })

    return retention_data


def query_with_filter(from_date: str, to_date: str, segment: str = None) -> List[Dict[str, Any]]:
    client = get_client()

    where_clause = f"toDate(occurred_at) BETWEEN '{from_date}' AND '{to_date}'"

    if segment:
        key, value = segment.split(':')
        if key.startswith('properties.'):
            prop_key = key.replace('properties.', '')
            where_clause += f" AND JSONExtractString(properties, '{prop_key}') = '{value}'"
        elif key == 'event_type':
            where_clause += f" AND event_type = '{value}'"

    query = f"""
        SELECT 
            toDate(occurred_at) as date,
            uniq(user_id) as unique_users
        FROM {settings.clickhouse_db}.events
        WHERE {where_clause}
        GROUP BY date
        ORDER BY date
    """

    result = client.query(query)
    return [{"date": str(row[0]), "unique_users": row[1]} for row in result.result_rows]