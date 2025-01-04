from typing import Optional

from fastapi import Header, HTTPException

from taskforge.core.config import settings
from taskforge.orchestrator import JobOrchestrator
from taskforge.scheduler.scheduler import JobScheduler
from taskforge.workflow.engine import WorkflowEngine


async def verify_api_key(
    api_key: Optional[str] = Header(None, alias="X-API-Key"),
) -> str:
    """Verify API key from request header"""
    if api_key != settings.security.api_key:
        raise HTTPException(status_code=401, detail="Invalid API key")
    return api_key


async def get_orchestrator() -> JobOrchestrator:
    """Get JobOrchestrator instance"""
    from taskforge.api.server import app

    return app.state.orchestrator


async def get_workflow_engine() -> WorkflowEngine:
    """Get WorkflowEngine instance"""
    from taskforge.api.server import app

    return app.state.workflow_engine


async def get_scheduler() -> JobScheduler:
    """Get JobScheduler instance"""
    from taskforge.api.server import app

    return app.state.scheduler
