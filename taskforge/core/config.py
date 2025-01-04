from functools import lru_cache
from typing import List, Optional

from pydantic import AmqpDsn, Field, PostgresDsn, RedisDsn, validator
from pydantic_settings import BaseSettings


class LoggingConfig(BaseSettings):
    """Logging configuration"""

    level: str = "INFO"
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    json_logs: bool = True
    file_enabled: bool = False
    file_path: Optional[str] = None
    queue_enabled: bool = False
    elasticsearch_enabled: bool = False
    batch_size: int = 100


class DatabaseConfig(BaseSettings):
    """Database configuration"""

    url: PostgresDsn = Field(..., env="TASKFORGE_POSTGRES_URL")
    min_connections: int = 5
    max_connections: int = 20
    connection_retry: int = 3
    retry_interval: int = 5
    pool_recycle: int = 3600


class BrokerConfig(BaseSettings):
    """Message broker configuration"""

    url: AmqpDsn = Field(..., env="TASKFORGE_RABBITMQ_URL")
    connection_pool_size: int = 2
    max_priority: int = 10
    prefetch_count: int = 10
    heartbeat: int = 60
    message_ttl: int = 86400  # 24 hours
    enable_dead_letter: bool = True
    dead_letter_exchange: str = "taskforge.dlx"


class RedisConfig(BaseSettings):
    """Redis configuration"""

    url: RedisDsn = Field(..., env="TASKFORGE_REDIS_URL")
    pool_size: int = 10
    max_connections: int = 20
    connection_timeout: int = 30


class WorkerConfig(BaseSettings):
    """Worker configuration"""

    count: int = 4
    prefetch_count: int = 10
    shutdown_timeout: int = 30
    max_retry_delay: int = 300
    max_memory_mb: int = 512
    cpu_limit: float = 1.0
    monitor_interval: int = 60


class SecurityConfig(BaseSettings):
    """Security configuration"""

    api_key: str = Field(..., env="TASKFORGE_API_KEY")
    enable_ssl: bool = False
    ssl_cert: Optional[str] = None
    ssl_key: Optional[str] = None
    allowed_origins: List[str] = ["*"]


class MetricsConfig(BaseSettings):
    """Metrics configuration"""

    enabled: bool = True
    prometheus_port: int = 9090
    statsd_enabled: bool = False
    statsd_host: str = "localhost"
    statsd_port: int = 8125
    export_interval: int = 60


class TracingConfig(BaseSettings):
    """Distributed tracing configuration"""

    enabled: bool = False
    jaeger_host: str = "localhost"
    jaeger_port: int = 6831
    sampling_rate: float = 1.0


class Settings(BaseSettings):
    """Global application settings"""

    # Application
    app_name: str = "taskforge"
    environment: str = "development"
    debug: bool = False
    version: str = "1.0.0"

    # API Server
    api_host: str = "0.0.0.0"
    api_port: int = 8000

    # Components
    database: DatabaseConfig = DatabaseConfig()
    broker: BrokerConfig = BrokerConfig()
    redis: RedisConfig = RedisConfig()
    worker: WorkerConfig = WorkerConfig()
    logging: LoggingConfig = LoggingConfig()
    security: SecurityConfig = SecurityConfig()
    metrics: MetricsConfig = MetricsConfig()
    tracing: TracingConfig = TracingConfig()

    # Job Processing
    max_job_timeout: int = 3600  # 1 hour
    default_job_timeout: int = 300  # 5 minutes
    job_retention_days: int = 7
    max_retries: int = 3

    class Config:
        env_file = ".env"
        env_prefix = "TASKFORGE_"
        case_sensitive = False

    @validator("environment")
    def validate_environment(cls, v: str) -> str:
        allowed = ["development", "staging", "production"]
        if v not in allowed:
            raise ValueError(f"Environment must be one of: {allowed}")
        return v

    def log_level(self) -> str:
        if self.debug:
            return "DEBUG"
        return self.logging.level


@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance"""
    return Settings()


settings = get_settings()
