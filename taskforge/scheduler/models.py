from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from croniter import croniter
from pydantic import BaseModel, validator


class Schedule(BaseModel):
    """Job schedule definition"""

    cron_expression: Optional[str] = None
    interval_seconds: Optional[int] = None
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    timezone: str = "UTC"
    max_runs: Optional[int] = None

    @validator("cron_expression")
    def validate_cron(cls, v: Optional[str]) -> Optional[str]:
        if v and not croniter.is_valid(v):
            raise ValueError(f"Invalid cron expression: {v}")
        return v

    @validator("interval_seconds")
    def validate_interval(cls, v: Optional[int]) -> Optional[int]:
        if v is not None and v < 1:
            raise ValueError("Interval must be positive")
        return v

    def next_run_time(self, from_time: Optional[datetime] = None) -> Optional[datetime]:
        """Calculate next run time"""
        if not from_time:
            from_time = datetime.now(timezone.utc)

        if self.end_date and from_time > self.end_date:
            return None

        if self.cron_expression:
            cron = croniter(self.cron_expression, from_time)
            next_time = cron.get_next(datetime)
        elif self.interval_seconds:
            next_time = from_time + timedelta(seconds=self.interval_seconds)
        else:
            return None

        if self.end_date and next_time > self.end_date:
            return None

        return next_time


class ScheduledJob(BaseModel):
    """Scheduled job definition"""

    job_type: str
    schedule: Schedule
    input_data: Dict[str, Any]
    metadata: Optional[Dict[str, Any]] = None
    run_count: int = 0
    last_run: Optional[datetime] = None
    next_run: Optional[datetime] = None
    enabled: bool = True
