from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from taskforge.api.dependencies import get_orchestrator, verify_api_key
from taskforge.api.models import HealthCheck, MetricsResponse

router = APIRouter(prefix="/admin", tags=["Administration"])


@router.get("/health", response_model=HealthCheck)
async def health_check(
    orchestrator=Depends(get_orchestrator), api_key: str = Depends(verify_api_key)
):
    """System health check"""
    try:
        status = await orchestrator.health_check()
        if not status["healthy"]:
            raise HTTPException(status_code=503, detail=status)
        return status
    except Exception as e:
        raise HTTPException(
            status_code=503, detail={"status": "unhealthy", "message": str(e)}
        )


@router.get("/metrics", response_model=MetricsResponse)
async def get_metrics(api_key: str = Depends(verify_api_key)):
    """Get system metrics"""
    from prometheus_client import REGISTRY

    metrics = generate_latest(REGISTRY)
    return Response(metrics, media_type=CONTENT_TYPE_LATEST)


@router.post("/cleanup")
async def run_cleanup(
    days: int = 7,
    orchestrator=Depends(get_orchestrator),
    api_key: str = Depends(verify_api_key),
):
    """Clean up old job data"""
    try:
        count = await orchestrator.cleanup_old_jobs(days)
        return {"cleaned": count}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Cleanup failed: {str(e)}")
