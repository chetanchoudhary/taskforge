from abc import ABC, abstractmethod
from datetime import datetime
from enum import Enum
from typing import Any, Dict, Generic, Optional, TypeVar

from pydantic import BaseModel, Field

Input = TypeVar("Input", bound=BaseModel)
Output = TypeVar("Output", bound=BaseModel)


class JobStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRYING = "retrying"


class JobPriority(int, Enum):
    LOW = 0
    MEDIUM = 1
    HIGH = 2
    CRITICAL = 3


class JobMetadata(BaseModel):
    """Metadata associated with a job"""

    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    retries: int = 0
    max_retries: int = 3
    timeout_seconds: int = 300
    tags: Dict[str, str] = Field(default_factory=dict)
    priority: JobPriority = JobPriority.MEDIUM


class Job(Generic[Input, Output], ABC):
    """Base class for defining jobs in TaskForge"""

    def __init__(
        self, job_id: str, input_data: Input, metadata: Optional[JobMetadata] = None
    ):
        self.job_id = job_id
        self.input_data = input_data
        self.metadata = metadata or JobMetadata()

    @abstractmethod
    async def execute(self) -> Output:
        """Execute the job logic"""
        pass

    @abstractmethod
    async def on_failure(self, exception: Exception) -> None:
        """Handle job failure"""
        pass

    @abstractmethod
    async def on_success(self, result: Output) -> None:
        """Handle job success"""
        pass

    @abstractmethod
    async def validate(self) -> bool:
        """Validate job input and configuration"""
        pass

    async def pre_execute(self) -> None:
        """Hook called before job execution"""
        pass

    async def post_execute(self) -> None:
        """Hook called after job execution"""
        pass

    async def cleanup(self) -> None:
        """Cleanup resources"""
        pass


class JobConfig(BaseModel):
    """Configuration for a job type"""

    max_retries: int = 3
    timeout_seconds: int = 300
    concurrency_limit: int = 10
    queue_name: str
    priority: JobPriority = JobPriority.MEDIUM
    tags: Dict[str, str] = Field(default_factory=dict)


class JobResult(BaseModel, Generic[Output]):
    """Result of a job execution"""

    job_id: str
    job_type: str
    status: JobStatus
    input_data: Dict[str, Any]
    result: Optional[Output] = None
    error: Optional[str] = None
    execution_time: Optional[float] = None
    metadata: JobMetadata
    created_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
