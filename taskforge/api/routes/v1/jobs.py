from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Path, Query

from taskforge.api.dependencies import get_orchestrator, verify_api_key
from taskforge.api.models import JobResponse, JobSubmission
from taskforge.exceptions import JobError
from taskforge.jobs.base import JobStatus

router = APIRouter(prefix="/jobs", tags=["Jobs"])


@router.post("/{job_type}", response_model=dict)
async def submit_job(
    job_type: str = Path(..., description="Type of job to submit"),
    submission: JobSubmission = None,
    orchestrator=Depends(get_orchestrator),
    api_key: str = Depends(verify_api_key),
):
    """Submit a new job"""
    try:
        job_id = await orchestrator.submit_job(
            job_type=job_type,
            input_data=submission.input_data,
            priority=submission.priority,
            metadata=submission.metadata,
        )
        return {"job_id": job_id}
    except JobError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/{job_id}", response_model=JobResponse)
async def get_job_status(
    job_id: str = Path(..., description="ID of the job to retrieve"),
    orchestrator=Depends(get_orchestrator),
    api_key: str = Depends(verify_api_key),
):
    """Get job status"""
    job_state = await orchestrator.get_job_state(job_id)
    if not job_state:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
    return job_state


@router.get("/", response_model=List[JobResponse])
async def list_jobs(
    status: Optional[JobStatus] = Query(None, description="Filter by job status"),
    limit: int = Query(100, description="Maximum number of jobs to return"),
    offset: int = Query(0, description="Number of jobs to skip"),
    orchestrator=Depends(get_orchestrator),
    api_key: str = Depends(verify_api_key),
):
    """List jobs"""
    jobs = await orchestrator.list_jobs(status=status, limit=limit, offset=offset)
    return jobs


@router.post("/{job_id}/cancel")
async def cancel_job(
    job_id: str = Path(..., description="ID of the job to cancel"),
    orchestrator=Depends(get_orchestrator),
    api_key: str = Depends(verify_api_key),
):
    """Cancel a running job"""
    success = await orchestrator.cancel_job(job_id)
    if not success:
        raise HTTPException(
            status_code=404, detail=f"Job {job_id} not found or already completed"
        )
    return {"status": "cancelled"}
