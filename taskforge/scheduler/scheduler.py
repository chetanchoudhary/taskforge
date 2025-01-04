import asyncio
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import structlog

from taskforge.exceptions import JobError
from taskforge.orchestrator import JobOrchestrator
from taskforge.scheduler.models import Schedule, ScheduledJob

logger = structlog.get_logger()


class JobScheduler:
    """Advanced job scheduler"""

    def __init__(self, orchestrator: JobOrchestrator):
        self.orchestrator = orchestrator
        self.scheduled_jobs: Dict[str, ScheduledJob] = {}
        self._running = False
        self._schedule_task: Optional[asyncio.Task] = None
        self._lock = asyncio.Lock()

    async def start(self):
        """Start the scheduler"""
        async with self._lock:
            if self._running:
                return

            self._running = True
            self._schedule_task = asyncio.create_task(self._run_scheduler())
            logger.info("Job scheduler started")

    async def stop(self):
        """Stop the scheduler"""
        async with self._lock:
            if not self._running:
                return

            self._running = False
            if self._schedule_task:
                self._schedule_task.cancel()
                try:
                    await self._schedule_task
                except asyncio.CancelledError:
                    pass
                self._schedule_task = None

            logger.info("Job scheduler stopped")

    async def add_job(
        self,
        job_id: str,
        job_type: str,
        schedule: Schedule,
        input_data: Dict[str, Any],
        metadata: Optional[Dict[str, Any]] = None,
    ) -> ScheduledJob:
        """Add a new scheduled job"""
        if job_id in self.scheduled_jobs:
            raise JobError(f"Job with ID {job_id} already exists")

        # Calculate initial next run time
        next_run = schedule.next_run_time()
        if not next_run:
            raise JobError("Schedule would never run")

        job = ScheduledJob(
            job_type=job_type,
            schedule=schedule,
            input_data=input_data,
            metadata=metadata,
            next_run=next_run,
        )

        async with self._lock:
            self.scheduled_jobs[job_id] = job

        logger.info(
            "Scheduled job added", job_id=job_id, job_type=job_type, next_run=next_run
        )

        return job

    async def remove_job(self, job_id: str):
        """Remove a scheduled job"""
        async with self._lock:
            if job_id in self.scheduled_jobs:
                del self.scheduled_jobs[job_id]
                logger.info("Scheduled job removed", job_id=job_id)

    async def update_job(
        self,
        job_id: str,
        schedule: Optional[Schedule] = None,
        input_data: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        enabled: Optional[bool] = None,
    ) -> ScheduledJob:
        """Update a scheduled job"""
        async with self._lock:
            if job_id not in self.scheduled_jobs:
                raise JobError(f"Job {job_id} not found")

            job = self.scheduled_jobs[job_id]

            if schedule:
                job.schedule = schedule
                job.next_run = schedule.next_run_time()

            if input_data is not None:
                job.input_data = input_data

            if metadata is not None:
                job.metadata = metadata

            if enabled is not None:
                job.enabled = enabled

            return job

    async def get_job(self, job_id: str) -> Optional[ScheduledJob]:
        """Get a scheduled job"""
        return self.scheduled_jobs.get(job_id)

    async def list_jobs(
        self, job_type: Optional[str] = None, enabled: Optional[bool] = None
    ) -> List[ScheduledJob]:
        """List scheduled jobs with optional filtering"""
        jobs = list(self.scheduled_jobs.values())

        if job_type:
            jobs = [j for j in jobs if j.job_type == job_type]

        if enabled is not None:
            jobs = [j for j in jobs if j.enabled == enabled]

        return jobs

    async def _run_scheduler(self):
        """Main scheduler loop"""
        while self._running:
            try:
                await self._process_due_jobs()
            except Exception as e:
                logger.error("Error in scheduler loop", error=str(e), exc_info=True)

            # Sleep until next check interval
            await asyncio.sleep(1)

    async def _process_due_jobs(self):
        """Process jobs that are due to run"""
        now = datetime.now(timezone.utc)

        async with self._lock:
            for job_id, job in self.scheduled_jobs.items():
                if not job.enabled:
                    continue

                if job.next_run and job.next_run <= now:
                    try:
                        # Submit job
                        await self.orchestrator.submit_job(
                            job_type=job.job_type,
                            input_data=job.input_data,
                            metadata={
                                **(job.metadata or {}),
                                "scheduled": True,
                                "schedule_id": job_id,
                            },
                        )

                        # Update job state
                        job.last_run = now
                        job.run_count += 1

                        # Calculate next run time
                        job.next_run = job.schedule.next_run_time(from_time=now)

                        # Check if job should be disabled
                        if (
                            job.schedule.max_runs
                            and job.run_count >= job.schedule.max_runs
                        ):
                            job.enabled = False

                        # Check end date
                        if job.schedule.end_date and now >= job.schedule.end_date:
                            job.enabled = False

                        logger.info(
                            "Scheduled job executed",
                            job_id=job_id,
                            run_count=job.run_count,
                            next_run=job.next_run,
                        )

                    except Exception as e:
                        logger.error(
                            "Error executing scheduled job", job_id=job_id, error=str(e)
                        )
