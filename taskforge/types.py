# flake8: noqa

"""Common type definitions"""

from datetime import datetime
from typing import Any
from typing import Awaitable
from typing import Callable
from typing import Dict
from typing import Optional
from typing import Protocol
from typing import TypeVar
from typing import runtime_checkable

from pydantic import BaseModel

# Generic type for job input/output models
Input = TypeVar("Input", bound=BaseModel)
Output = TypeVar("Output", bound=BaseModel)

# Message types
Headers = Dict[str, str]
Payload = Dict[str, Any]


@runtime_checkable
class MessageHandler(Protocol):
    """Protocol for message handlers"""

    async def handle(self, message_id: str, payload: Payload) -> None: ...


@runtime_checkable
class Retryable(Protocol):
    """Protocol for retryable operations"""

    async def execute(self) -> Any: ...
    async def on_failure(self, exception: Exception) -> bool: ...
    async def cleanup(self) -> None: ...


# Storage types
class StorageRecord(BaseModel):
    """Base model for storage records"""

    id: str
    created_at: datetime
    updated_at: datetime

    class Config:
        orm_mode = True


# Broker types
class BrokerMessage(BaseModel):
    """Message in the broker"""

    message_id: str
    routing_key: str
    payload: Payload
    headers: Headers
    timestamp: datetime
    priority: int = 0


# Worker types
class WorkerStats(BaseModel):
    """Worker statistics"""

    worker_id: str
    jobs_processed: int
    errors: int
    uptime_seconds: float
    current_job_id: Optional[str] = None
    memory_usage: int
    cpu_usage: float


# Job types
class JobDefinition(BaseModel):
    """Job definition"""

    job_type: str
    input_schema: type[BaseModel]
    output_schema: type[BaseModel]
    timeout_seconds: int = 300
    max_retries: int = 3
    queue_name: Optional[str] = None


# Event types
EventHandler = Callable[[str, Dict[str, Any]], Awaitable[None]]


class EventMetadata(BaseModel):
    """Event metadata"""

    source: str
    timestamp: datetime
    trace_id: Optional[str] = None
    correlation_id: Optional[str] = None


# Monitoring types
class HealthStatus(BaseModel):
    """Component health status"""

    status: str
    details: Dict[str, Any]
    last_check: datetime


# Configuration types
class ComponentConfig(BaseModel):
    """Base configuration for components"""

    enabled: bool = True
    debug: bool = False
    metrics_enabled: bool = True
