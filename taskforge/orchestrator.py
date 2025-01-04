import asyncio
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Type

import structlog

from taskforge.broker.rabbitmq import RabbitMQBroker
from taskforge.exceptions import JobError, TaskForgeError
from taskforge.integrations.events import event_bus
from taskforge.jobs.base import (
    Job,
    JobConfig,
    JobMetadata,
    JobPriority,
    JobResult,
    JobStatus,
)
from taskforge.storage.postgres import PostgresJobStorage
from taskforge.worker.pool import WorkerPool

logger = structlog.get_logger()


class JobOrchestrator:
    """Core orchestrator that manages job submission and lifecycle"""

    def __init__(
        self,
        broker: RabbitMQBroker,
        storage: PostgresJobStorage,
        worker_pool: Optional[WorkerPool] = None,
    ):
        self.broker = broker
        self.storage = storage
        self.worker_pool = worker_pool
        self._job_configs: Dict[str, JobConfig] = {}

    async def start(self):
        """Initialize the orchestrator"""
        await self.broker.connect()
        await self.storage.initialize()
        if self.worker_pool:
            await self.worker_pool.start()

    async def shutdown(self):
        """Cleanup and shutdown"""
        if self.worker_pool:
            await self.worker_pool.shutdown()
        await self.broker.close()

    async def register_job(
        self,
        job_class: Type[Job],
        max_retries: int = 3,
        timeout_seconds: int = 300,
        concurrency_limit: int = 10,
        queue_name: Optional[str] = None,
    ) -> None:
        """Register a job type with its configuration"""
        job_type = job_class.__name__

        # Store job configuration
        self._job_configs[job_type] = JobConfig(
            max_retries=max_retries,
            timeout_seconds=timeout_seconds,
            concurrency_limit=concurrency_limit,
            queue_name=queue_name or job_type,
        )

        # Register with broker for queue setup
        await self.broker.register_job(job_class=job_class, queue_name=queue_name)

        logger.info(
            "Registered job type",
            job_type=job_type,
            config=self._job_configs[job_type].model_dump(),
        )

    async def submit_job(
        self,
        job_type: str,
        input_data: Dict[str, Any],
        priority: JobPriority = JobPriority.MEDIUM,
        metadata: Optional[Dict] = None,
    ) -> str:
        """Submit a new job for processing"""
        if job_type not in self._job_configs:
            raise JobError("job_type", f"Job type {job_type} not registered")

        job_id = str(uuid.uuid4())
        config = self._job_configs[job_type]

        # Create job metadata
        job_metadata = JobMetadata(
            created_at=datetime.now(timezone.utc),
            max_retries=config.max_retries,
            timeout_seconds=config.timeout_seconds,
            priority=priority,
            tags=metadata.get("tags", {}) if metadata else {},
            max_concurrent=config.concurrency_limit,
        )

        try:
            # Save initial job state
            await self.storage.save_job(
                job_id=job_id,
                job_type=job_type,
                input_data=input_data,
                metadata=job_metadata,
            )

            # Publish to message queue
            await self.broker.publish_job(
                job_type=job_type,
                job_id=job_id,
                input_data=input_data,
                metadata=job_metadata.model_dump(),
                priority=priority,
            )

            # Emit event
            await event_bus.publish(
                f"job.submitted.{job_type}",
                {
                    "job_id": job_id,
                    "priority": priority.value,
                    "metadata": job_metadata.model_dump(),
                },
            )

            logger.info(
                "Submitted job",
                job_id=job_id,
                job_type=job_type,
                priority=priority.value,
            )

            return job_id

        except Exception as e:
            logger.error("Error submitting job", job_type=job_type, error=str(e))
            raise TaskForgeError(f"Failed to submit job: {str(e)}")

    async def cancel_job(self, job_id: str) -> bool:
        """Cancel a job if possible"""
        job_state = await self.get_job_state(job_id)
        if not job_state or job_state.status in (JobStatus.COMPLETED, JobStatus.FAILED):
            return False

        await self.storage.update_job_status(
            job_id=job_id, status=JobStatus.FAILED, error="Job cancelled by user"
        )

        # Emit event
        await event_bus.publish(
            "job.cancelled", {"job_id": job_id, "previous_status": job_state.status}
        )

        return True

    async def get_job_state(self, job_id: str) -> Optional[JobResult]:
        """Get current state of a job"""
        return await self.storage.get_job_state(job_id)

    async def list_jobs(
        self,
        status: Optional[JobStatus] = None,
        job_type: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[JobResult]:
        """List jobs with filters"""
        return await self.storage.list_jobs(
            status=status, job_type=job_type, limit=limit, offset=offset
        )

    async def cleanup_old_jobs(self, days: int = 7) -> int:
        """Clean up old job records"""
        return await self.storage.cleanup_old_jobs(days)

    async def health_check(self) -> Dict[str, Any]:
        """Check system health"""
        status = {
            "healthy": True,
            "components": {
                "broker": await self.broker.ping(),
                "storage": await self.storage.ping(),
            },
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        if self.worker_pool:
            workers_healthy = all(
                worker.is_running for worker in self.worker_pool.workers
            )
            status["components"]["workers"] = workers_healthy

        # Check overall health
        status["healthy"] = all(status["components"].values())

        return status

    async def monitor_system_health(self) -> None:
        """Continuous system health monitoring"""
        while True:
            try:
                health = await self.health_check()
                if not health["healthy"]:
                    logger.warning(
                        "Unhealthy system state", components=health["components"]
                    )
            except Exception as e:
                logger.error("Health check failed", error=str(e))

            await asyncio.sleep(60)  # Check every minute
