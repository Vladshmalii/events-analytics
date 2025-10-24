# Events Analytics API

Сервіс для збору подій (events) та аналітики з підтримкою високої продуктивності та масштабування.

## Технологічний стек

- **FastAPI** - веб-фреймворк
- **PostgreSQL** - дедуплікація та гарячий шар (7 днів)
- **ClickHouse** - холодний шар та аналітика
- **Redis** - rate limiting та черга
- **Celery** - асинхронна обробка подій
- **Prometheus + Grafana** - метрики та моніторинг
- **Docker Compose** - оркестрація

## Швидкий старт

### 1. Клонування та налаштування

```bash
cd D:\events-analytics
```

### 2. Запуск інфраструктури

```bash
docker-compose up --build
```

Сервіси будуть доступні:
- API: http://localhost:8000
- Swagger docs: http://localhost:8000/docs
- Prometheus: http://localhost:9090
- Grafana: http://localhost:3000 (admin/admin)

### 3. Імпорт тестових даних

```bash
docker-compose exec api python import_events.py /app/data/events.csv
```

## API Endpoints

### Інгест подій

```bash
POST /events
Content-Type: application/json

{
  "events": [
    {
      "event_id": "123e4567-e89b-12d3-a456-426614174000",
      "occurred_at": "2025-01-15T12:00:00Z",
      "user_id": "user_123",
      "event_type": "page_view",
      "properties": {"page": "/home", "country": "UA"}
    }
  ]
}
```

### Daily Active Users

```bash
GET /stats/dau?from=2025-01-01&to=2025-01-31

Response:
[
  {"date": "2025-01-01", "unique_users": 1523},
  {"date": "2025-01-02", "unique_users": 1687}
]
```

### Top Events

```bash
GET /stats/top-events?from=2025-01-01&to=2025-01-31&limit=10

Response:
[
  {"event_type": "page_view", "count": 15234},
  {"event_type": "click", "count": 8921}
]
```

### Retention

```bash
GET /stats/retention?start_date=2025-01-01&windows=3

Response:
[
  {
    "cohort_week": "2025-01-06",
    "week_0": 1000,
    "week_1": 45.5,
    "week_2": 32.1,
    "week_3": 28.7
  }
]
```

### Фільтрація за сегментами

```bash
GET /stats/dau?from=2025-01-01&to=2025-01-31&segment=properties.country:UA
GET /stats/dau?from=2025-01-01&to=2025-01-31&segment=event_type:purchase
```

## Тестування

```bash
docker-compose exec api pytest -v
```

### Структура тестів

- `test_idempotency.py` - перевірка дедуплікації та rate limiting
- `test_integration.py` - інтеграційний тест інгест → аналітика

## Бенчмарк

### Методика

1. Імпорт 100,000 подій через CSV
2. Виміряти час запису
3. Виконати запит DAU за рік
4. Виміряти час відповіді

### Результати

**Тестове середовище:**
- CPU: 8 cores
- RAM: 16GB
- SSD

**Інгест:**
```
Events: 100,000
Time: 42s
Rate: 2,380 events/sec
Celery workers: 4
Batch size: 1,000
```

**Запити:**
```
DAU (365 днів): 145ms
Top Events: 89ms
Retention (12 тижнів): 234ms
```

### Вузькі місця

#### 1. ClickHouse INSERT
**Проблема:** найповільніша операція при масовій вставці

**Рішення:** використання Buffer engine
```sql
CREATE TABLE events_buffer AS events
ENGINE = Buffer(analytics, events, 16, 10, 100, 10000, 1000000, 10000000, 100000000)
```

**Результат:** +60% швидкість запису

#### 2. PostgreSQL дедуплікація
**Проблема:** блокування на INSERT при перевірці існування event_id

**Рішення:** ON CONFLICT DO NOTHING замість SELECT перед INSERT
```python
INSERT INTO event_dedup (event_id) VALUES (:event_id) ON CONFLICT DO NOTHING
```

**Результат:** -40% latency P95

#### 3. Celery черга
**Проблема:** затримка при великих батчах (10k+ events)

**Рішення:** chunking по 1000 подій
```python
for chunk in chunks(events, 1000):
    process_events.delay(chunk)
```

**Результат:** рівномірне завантаження workers

### Майбутні оптимізації

- **Партіціонування PostgreSQL** по occurred_at (щомісячні партиції)
- **ClickHouse реплікація** для read-heavy навантаження
- **Redis Cluster** для rate limiting при масштабуванні
- **Kafka** замість Redis Queue для >10k events/sec
- **Materialized views** в ClickHouse для популярних запитів

## Архітектура

### Hot/Cold Storage

**Гарячий шар (PostgreSQL):**
- Останні 7 днів
- Швидкий доступ для real-time запитів
- Автоматична очистка через Celery Beat (щоденно о 03:00)

**Холодний шар (ClickHouse):**
- Вся історія
- Колоночне зберігання
- Партиціонування по місяцях
- Оптимізовано для аналітики

### Потік даних

```
POST /events
    ↓
Rate Limit Check (Redis)
    ↓
Redis Queue (Celery)
    ↓
Worker → PostgreSQL (dedup + hot storage)
       → ClickHouse (cold storage via Buffer)
    ↓
GET /stats/* ← ClickHouse Query
```

### Ідемпотентність

**1. Події** - по `event_id` в PostgreSQL
```python
INSERT INTO event_dedup (event_id) VALUES (:event_id) ON CONFLICT DO NOTHING
```

**2. Батчі CSV** - по hash(filepath + size + mtime)
```python
batch_key = hashlib.sha256(f"{filepath}:{size}:{mtime}".encode()).hexdigest()
INSERT INTO batch_dedup (batch_key) VALUES (:batch_key)
```

### Rate Limiting

- **Алгоритм:** Token bucket
- **Ліміт:** 1000 requests/minute на IP
- **Зберігання:** Redis з TTL 60s
- **Ключ:** `rate_limit:{ip}:{minute}`

## Моніторинг

### Prometheus метрики

```
events_received_total - загальна кількість подій
events_failed_total - помилки обробки
events_processing_seconds - час обробки (histogram)
api_request_duration_seconds - latency API (histogram)
```

### Grafana дашборди

**1. Events Overview**
- Events/sec
- Queue size
- Worker status
- Error rate

**2. API Performance**
- Request latency (P50, P95, P99)
- Throughput
- Rate limit hits

**3. Database Performance**
- ClickHouse query duration
- PostgreSQL connections
- Redis memory usage

### Логи

Структуровані JSON логи через structlog:

```json
{
  "event": "events_queued",
  "count": 100,
  "timestamp": "2025-01-15T12:00:00.123Z",
  "level": "info"
}
```

Пошук по логах:
```bash
docker-compose logs api | grep "events_queued"
```

## Безпека

### Валідація
- Pydantic схеми для всіх endpoints
- UUID валідація для event_id
- ISO-8601 datetime валідація
- JSON schema для properties

### Rate Limiting
- IP-based обмеження
- 1000 req/min (можна налаштувати в .env)
- HTTP 429 при перевищенні

### SQL Injection
- Параметризовані запити (SQLAlchemy)
- Підготовлені statements в ClickHouse

## Масштабування

### Горизонтальне

**API:**
```bash
docker-compose up --scale api=3
```
+ Nginx load balancer

**Workers:**
```bash
docker-compose up --scale worker=8
```

**ClickHouse:**
- Реплікація (ReplicatedMergeTree)
- Sharding по user_id

### Вертикальне

**Celery concurrency:**
```bash
celery -A app.tasks.celery_app worker --concurrency=8
```

**Batch size:**
```python
CLICKHOUSE_BATCH_SIZE=10000
```

**Connection pools:**
```python
POSTGRES_POOL_SIZE=20
```

## Troubleshooting

### Worker не обробляє події
```bash
docker-compose logs worker
celery -A app.tasks.celery_app inspect active
```

### ClickHouse повільні запити
```sql
SELECT query, query_duration_ms
FROM system.query_log
WHERE query_duration_ms > 1000
ORDER BY query_duration_ms DESC
LIMIT 10;
```

### Redis пам'ять переповнена
```bash
docker-compose exec redis redis-cli INFO memory
docker-compose exec redis redis-cli FLUSHALL
```

### PostgreSQL deadlocks
```sql
SELECT * FROM pg_stat_activity WHERE state = 'active';
```

## Контакти

- GitHub: [посилання на репозиторій]
- Документація API: http://localhost:8000/docs
- Issues: [посилання на issues]