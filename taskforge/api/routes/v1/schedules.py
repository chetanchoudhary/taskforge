from fastapi import APIRouter, Depends, HTTPException, Path

from taskforge.api.dependencies import get_scheduler, verify_api_key
from taskforge.api.models import ScheduleCreate, ScheduleResponse
from taskforge.exceptions import ScheduleError

router = APIRouter(prefix="/schedules", tags=["Schedules"])


@router.post("/", response_model=ScheduleResponse)
async def create_schedule(
    schedule: ScheduleCreate,
    scheduler=Depends(get_scheduler),
    api_key: str = Depends(verify_api_key),
):
    """Create a new schedule"""
    try:
        result = await scheduler.create_schedule(
            job_type=schedule.job_type,
            cron_expression=schedule.cron_expression,
            interval_seconds=schedule.interval_seconds,
            start_date=schedule.start_date,
            end_date=schedule.end_date,
            input_data=schedule.input_data,
            metadata=schedule.metadata,
        )
        return result
    except ScheduleError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/{schedule_id}", response_model=ScheduleResponse)
async def get_schedule(
    schedule_id: str = Path(..., description="ID of the schedule to retrieve"),
    scheduler=Depends(get_scheduler),
    api_key: str = Depends(verify_api_key),
):
    """Get schedule details"""
    schedule = await scheduler.get_schedule(schedule_id)
    if not schedule:
        raise HTTPException(status_code=404, detail=f"Schedule {schedule_id} not found")
    return schedule


@router.post("/{schedule_id}/disable")
async def disable_schedule(
    schedule_id: str = Path(..., description="ID of the schedule to disable"),
    scheduler=Depends(get_scheduler),
    api_key: str = Depends(verify_api_key),
):
    """Disable a schedule"""
    try:
        await scheduler.disable_schedule(schedule_id)
        return {"status": "disabled"}
    except ScheduleError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/{schedule_id}/enable")
async def enable_schedule(
    schedule_id: str = Path(..., description="ID of the schedule to enable"),
    scheduler=Depends(get_scheduler),
    api_key: str = Depends(verify_api_key),
):
    """Enable a schedule"""
    try:
        await scheduler.enable_schedule(schedule_id)
        return {"status": "enabled"}
    except ScheduleError as e:
        raise HTTPException(status_code=400, detail=str(e))
