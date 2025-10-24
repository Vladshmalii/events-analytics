import csv
import sys
import asyncio
import hashlib
from pathlib import Path
from uuid import UUID
from datetime import datetime
import json
from app.tasks.workers import process_batch_import
import structlog

logger = structlog.get_logger()


def calculate_file_hash(filepath: str) -> str:
    path = Path(filepath)
    content = f"{filepath}:{path.stat().st_size}:{path.stat().st_mtime}"
    return hashlib.sha256(content.encode()).hexdigest()


def parse_csv(filepath: str) -> list:
    events = []

    with open(filepath, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)

        for row in reader:
            try:
                event = {
                    'event_id': row['event_id'],
                    'occurred_at': datetime.fromisoformat(row['occurred_at'].replace('Z', '+00:00')),
                    'user_id': row['user_id'],
                    'event_type': row['event_type'],
                    'properties': json.loads(row['properties_json']) if row.get('properties_json') else {}
                }
                events.append(event)
            except Exception as e:
                logger.error("row_parse_failed", row=row, error=str(e))
                continue

    return events


def chunk_events(events: list, chunk_size: int = 1000):
    for i in range(0, len(events), chunk_size):
        yield events[i:i + chunk_size]


async def import_events(filepath: str):
    batch_key = calculate_file_hash(filepath)

    logger.info("import_started", filepath=filepath, batch_key=batch_key)

    events = parse_csv(filepath)

    if not events:
        logger.error("no_events_found", filepath=filepath)
        return

    logger.info("events_parsed", count=len(events))

    for idx, chunk in enumerate(chunk_events(events)):
        chunk_data = []
        for event in chunk:
            event_copy = event.copy()
            event_copy['occurred_at'] = event_copy['occurred_at'].isoformat()
            chunk_data.append(event_copy)

        process_batch_import.delay(f"{batch_key}:chunk:{idx}", chunk_data)
        logger.info("chunk_queued", chunk_index=idx, size=len(chunk))

    logger.info("import_completed", total_events=len(events))


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python import_events.py <path-to-csv>")
        sys.exit(1)

    filepath = sys.argv[1]

    if not Path(filepath).exists():
        print(f"File not found: {filepath}")
        sys.exit(1)

    asyncio.run(import_events(filepath))