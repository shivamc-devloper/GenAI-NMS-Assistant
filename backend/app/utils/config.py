# config.py
import os
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # LibreNMS
    LIBRENMS_URL: str = "http://localhost:8000/api-access"
    LIBRENMS_TOKEN: str = "4c64719f8307e462f5d72fdc845a4d48"

    # Database
    DB_URL: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/network_monitoring"
    DB_NAME: str = "network_monitoring"

    # Ingestion
    INGEST_INTERVAL: int = 60  # seconds

    # Anomaly Detection
    ANOMALY_WINDOW: int = 10  # last N samples
    CPU_THRESHOLD: int = 85
    LATENCY_THRESHOLD: int = 100

    # RCA
    ENABLE_LLM: bool = False
    OPENAI_API_KEY: str | None = None

    class Config:
        env_file = ".env"
        case_sensitive = True


settings = Settings()
