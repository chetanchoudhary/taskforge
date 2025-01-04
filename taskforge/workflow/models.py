from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class WorkflowNodeType(str, Enum):
    """Types of workflow nodes"""

    TASK = "task"
    DECISION = "decision"
    PARALLEL = "parallel"
    WAIT = "wait"
    CALLBACK = "callback"


class WorkflowStatus(str, Enum):
    """Workflow execution status"""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    WAITING = "waiting"


class WorkflowNode(BaseModel):
    """Base workflow node"""

    id: str
    type: WorkflowNodeType
    name: str
    depends_on: List[str] = Field(default_factory=list)
    retry_policy: Optional[Dict[str, Any]] = None
    timeout_seconds: Optional[int] = None
    status: WorkflowStatus = WorkflowStatus.PENDING
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error: Optional[str] = None


class TaskNode(WorkflowNode):
    """Task execution node"""

    type: WorkflowNodeType = WorkflowNodeType.TASK
    job_type: str
    input_data: Dict[str, Any]
    job_id: Optional[str] = None


class DecisionNode(WorkflowNode):
    """Conditional branching node"""

    type: WorkflowNodeType = WorkflowNodeType.DECISION
    condition: str
    true_branch: str
    false_branch: str
    condition_result: Optional[bool] = None


class ParallelNode(WorkflowNode):
    """Parallel execution node"""

    type: WorkflowNodeType = WorkflowNodeType.PARALLEL
    branches: List[List[str]]
    join_policy: str = "all"  # all, any, N
    completed_branches: List[str] = Field(default_factory=list)


class WaitNode(WorkflowNode):
    """Wait/delay node"""

    type: WorkflowNodeType = WorkflowNodeType.WAIT
    wait_time: int  # seconds
    wait_until: Optional[datetime] = None


class CallbackNode(WorkflowNode):
    """External callback node"""

    type: WorkflowNodeType = WorkflowNodeType.CALLBACK
    callback_url: str
    callback_token: str
    callback_timeout: int
    callback_received: bool = False
    callback_data: Optional[Dict[str, Any]] = None


class Workflow(BaseModel):
    """Workflow definition"""

    id: str
    name: str
    nodes: Dict[str, WorkflowNode]
    status: WorkflowStatus = WorkflowStatus.PENDING
    context: Dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error: Optional[str] = None
