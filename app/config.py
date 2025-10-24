from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "events_db"
    postgres_user: str = "postgres"
    postgres_password: str = "postgres"

    clickhouse_host: str = "localhost"
    clickhouse_port: int = 8123
    clickhouse_db: str = "analytics"
    clickhouse_user: str = "default"
    clickhouse_password: str = "clickhouse"

    redis_host: str = "localhost"
    redis_port: int = 6379

    celery_broker_url: str = "redis://localhost:6379/0"
    celery_result_backend: str = "redis://localhost:6379/1"

    rate_limit_per_minute: int = 1000

    class Config:
        env_file = ".env"


settings = Settings()