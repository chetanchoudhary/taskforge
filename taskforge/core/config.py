from typing import Optional
from pydantic import BaseModel, BaseSettings, Field, RedisDsn, PostgresDsn, AmqpDsn

class LogConfig(BaseModel):
    """Logging configuration"""
    level: str = "INFO"
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    json_logs: bool = True

class WorkerConfig(BaseModel):
    """Worker configuration"""
    prefetch_count: int = 10
    shutdown_timeout: int = 30
    max_retry_delay: int = 300

class BrokerConfig(BaseModel):
    """Message broker configuration"""
    connection_pool_size: int = 2
    max_priority: int = 10
    heartbeat: int = 60

class Settings(BaseSettings):
    """Global application settings"""
    # Application
    app_name: str = "taskforge"
    environment: str = "development"
    debug: bool = False
    
    # API Server
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    
    # Database
    postgres_url: PostgresDsn = Field(
        default="postgresql+asyncpg://postgres:password@localhost:5432/taskforge"
    )
    postgres_min_connections: int = 5
    postgres_max_connections: int = 20
    
    # RabbitMQ
    rabbitmq_url: AmqpDsn = Field(
        default="amqp://guest:guest@localhost:5672/"
    )
    
    # Redis
    redis_url: RedisDsn = Field(
        default="redis://localhost:6379/0"
    )
    redis_pool_size: int = 10
    
    # Job Processing
    max_job_timeout: int = 3600  # 1 hour
    default_job_timeout: int = 300  # 5 minutes
    job_retention_days: int = 7
    max_retries: int = 3
    
    # Logging
    log_config: LogConfig = LogConfig()
    
    # Worker
    worker_config: WorkerConfig = WorkerConfig()
    
    # Broker
    broker_config: BrokerConfig = BrokerConfig()
    
    class Config:
        env_file = ".env"
        env_prefix = "TASKFORGE_"

settings = Settings()
