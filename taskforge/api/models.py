from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from taskforge.jobs.base import JobPriority, JobStatus


# Job Models
class JobInput(BaseModel):
    """Base class for job input models"""

    class Config:
        extra = "forbid"


class JobOutput(BaseModel):
    """Base class for job output models"""

    class Config:
        extra = "forbid"


class JobSubmission(BaseModel):
    """Job submission request"""

    input_data: Dict[str, Any]
    priority: Optional[JobPriority] = Field(default=JobPriority.MEDIUM)
    metadata: Optional[Dict[str, Any]] = None

    class Config:
        schema_extra = {
            "example": {
                "input_data": {
                    "to": "user@example.com",
                    "subject": "Test Email",
                    "body": "Hello World!",
                },
                "priority": "HIGH",
                "metadata": {"source": "api", "environment": "production"},
            }
        }


class JobResponse(BaseModel):
    """Job status response"""

    job_id: str
    status: JobStatus
    job_type: str
    input_data: Dict[str, Any]
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    created_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    execution_time: Optional[float] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)

    class Config:
        schema_extra = {
            "example": {
                "job_id": "123e4567-e89b-12d3-a456-426614174000",
                "status": "completed",
                "job_type": "SendEmailJob",
                "input_data": {
                    "to": "user@example.com",
                    "subject": "Test",
                    "body": "Hello",
                },
                "result": {"message_id": "abc123", "sent_at": "2024-01-01T12:00:00Z"},
                "created_at": "2024-01-01T12:00:00Z",
                "started_at": "2024-01-01T12:00:01Z",
                "completed_at": "2024-01-01T12:00:02Z",
                "execution_time": 1.5,
            }
        }


# Schedule Models
class ScheduleCreate(BaseModel):
    """Schedule creation request"""

    job_type: str
    cron_expression: Optional[str] = None
    interval_seconds: Optional[int] = None
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    input_data: Dict[str, Any]
    metadata: Optional[Dict[str, Any]] = None


class ScheduleResponse(BaseModel):
    """Schedule status response"""

    schedule_id: str
    job_type: str
    cron_expression: Optional[str]
    interval_seconds: Optional[int]
    next_run: Optional[datetime]
    last_run: Optional[datetime]
    enabled: bool
    created_at: datetime


# Workflow Models
class WorkflowNodeBase(BaseModel):
    """Base workflow node"""

    id: str
    name: str
    type: str
    depends_on: List[str] = Field(default_factory=list)


class TaskNodeCreate(WorkflowNodeBase):
    """Task node creation"""

    type: str = "task"
    job_type: str
    input_data: Dict[str, Any]


class DecisionNodeCreate(WorkflowNodeBase):
    """Decision node creation"""

    type: str = "decision"
    condition: str
    true_branch: str
    false_branch: str


class ParallelNodeCreate(WorkflowNodeBase):
    """Parallel node creation"""

    type: str = "parallel"
    branches: List[List[str]]
    join_policy: str = "all"


class WorkflowCreate(BaseModel):
    """Workflow creation request"""

    name: str
    nodes: Dict[str, WorkflowNodeBase]
    context: Optional[Dict[str, Any]] = None


class WorkflowResponse(BaseModel):
    """Workflow status response"""

    workflow_id: str
    name: str
    status: str
    nodes: Dict[str, Dict[str, Any]]
    context: Dict[str, Any]
    created_at: datetime
    started_at: Optional[datetime]
    completed_at: Optional[datetime]
    error: Optional[str]


# Admin Models
class HealthCheck(BaseModel):
    """System health check response"""

    status: str
    components: Dict[str, bool]
    message: Optional[str]
    timestamp: datetime


class MetricsResponse(BaseModel):
    """System metrics response"""

    workers: Dict[str, Any]
    queues: Dict[str, Any]
    jobs: Dict[str, Any]
    system: Dict[str, Any]
