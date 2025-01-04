import asyncio
import signal
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

import psutil
import structlog

from taskforge.broker.rabbitmq import RabbitMQBroker
from taskforge.core.config import settings
from taskforge.core.metrics import metrics
from taskforge.core.semaphore import JobSemaphore
from taskforge.exceptions import JobError
from taskforge.integrations.events import event_bus
from taskforge.jobs.base import Job, JobMetadata, JobStatus
from taskforge.storage.postgres import PostgresJobStorage

logger = structlog.get_logger()


class TaskWorker:
    """Worker process that executes jobs"""

    def __init__(
        self,
        worker_id: str,
        broker: RabbitMQBroker,
        storage: PostgresJobStorage,
        prefetch_count: int = 10,
        shutdown_timeout: int = 30,
    ):
        self.worker_id = worker_id
        self.broker = broker
        self.storage = storage
        self.prefetch_count = prefetch_count
        self.shutdown_timeout = shutdown_timeout

        self.running = False
        self.current_job: Optional[Job] = None
        self._shutdown_event = asyncio.Event()

        self.jobs_processed = 0
        self.error_count = 0
        self.started_at = datetime.now(timezone.utc)

        self.logger = logger.bind(worker_id=worker_id, prefetch_count=prefetch_count)

    async def start(self) -> None:
        """Start the worker"""
        self.running = True
        self.logger.info("Starting worker")

        # Setup signal handlers
        for sig in (signal.SIGINT, signal.SIGTERM):
            asyncio.get_event_loop().add_signal_handler(
                sig, lambda s=sig: asyncio.create_task(self.handle_signal(s))
            )

        try:
            # Connect to message broker
            await self.broker.connect()
            channel = await self.broker.create_channel()
            await channel.set_qos(prefetch_count=self.prefetch_count)

            while self.running:
                try:
                    message = await self.broker.get_message()
                    if message:
                        await self._process_message(message)
                    elif self._shutdown_event.is_set():
                        break
                    else:
                        # No message available, brief pause
                        await asyncio.sleep(0.1)

                except Exception as e:
                    self.logger.error(
                        "Error processing message", error=str(e), exc_info=True
                    )
                    self.error_count += 1
                    await asyncio.sleep(1)  # Brief pause before retry

        finally:
            await self.cleanup()

    async def stop(self) -> None:
        """Stop the worker gracefully"""
        self.logger.info("Stopping worker")
        self.running = False
        self._shutdown_event.set()

        if self.current_job:
            try:
                await asyncio.wait_for(
                    self._cleanup_current_job(), timeout=self.shutdown_timeout
                )
            except asyncio.TimeoutError:
                self.logger.error(
                    "Timeout waiting for current job to cleanup",
                    job_id=self.current_job.job_id,
                )

    async def handle_signal(self, sig: signal.Signals) -> None:
        """Handle shutdown signals"""
        self.logger.info(f"Received signal {sig.name}")
        await self.stop()

    async def _process_message(self, message: Dict[str, Any]) -> None:
        """Process a single message from the queue"""
        job_id = message["job_id"]
        job_type = message["job_type"]

        self.logger.info("Processing job", job_id=job_id, job_type=job_type)

        metrics.active_jobs.labels(worker_id=self.worker_id).inc()
        start_time = time.monotonic()

        try:
            # Update job status to running
            await self.storage.update_job_status(
                job_id=job_id, status=JobStatus.RUNNING, execution_time=0
            )

            # Get job class and create instance
            job_class = self.broker.get_job_class(job_type)
            job = job_class(
                job_id=job_id,
                input_data=message["input_data"],
                metadata=JobMetadata(**message.get("metadata", {})),
            )

            self.current_job = job

            # Validate job
            if not await job.validate():
                raise JobError(job_id, "Job validation failed")

            # Check concurrency limits
            async with JobSemaphore(
                redis=self.broker.redis,
                job_type=job_type,
                max_concurrent=job.metadata.max_concurrent or 10,
            ):
                # Execute pre-job hooks
                await job.pre_execute()

                # Execute job with timeout
                timeout = job.metadata.timeout_seconds
                try:
                    result = await asyncio.wait_for(job.execute(), timeout=timeout)
                except asyncio.TimeoutError:
                    raise JobError(
                        job_id, f"Job execution timed out after {timeout} seconds"
                    )

                # Execute post-job hooks
                await job.post_execute()

            execution_time = time.monotonic() - start_time

            # Update job as completed
            await self.storage.update_job_status(
                job_id=job_id,
                status=JobStatus.COMPLETED,
                result=result.dict() if result else None,
                execution_time=execution_time,
            )

            # Call success handler
            await job.on_success(result)

            # Update metrics
            metrics.job_processing_time.labels(job_type=job_type).observe(
                execution_time
            )

            metrics.jobs_processed.labels(job_type=job_type, status="success").inc()

            # Emit event
            await event_bus.publish(
                f"job.completed.{job_type}",
                {
                    "job_id": job_id,
                    "execution_time": execution_time,
                    "result": result.dict() if result else None,
                },
            )

            self.jobs_processed += 1
            self.logger.info(
                "Job completed successfully",
                job_id=job_id,
                execution_time=execution_time,
            )

        except Exception as e:
            execution_time = time.monotonic() - start_time
            self.logger.error(
                "Job execution failed",
                job_id=job_id,
                error=str(e),
                execution_time=execution_time,
                exc_info=True,
            )

            self.error_count += 1

            # Update job as failed
            await self.storage.update_job_status(
                job_id=job_id,
                status=JobStatus.FAILED,
                error=str(e),
                execution_time=execution_time,
            )

            # Call failure handler
            if self.current_job:
                await self.current_job.on_failure(e)

            # Update metrics
            metrics.jobs_processed.labels(job_type=job_type, status="failure").inc()

            # Emit event
            await event_bus.publish(
                f"job.failed.{job_type}",
                {"job_id": job_id, "error": str(e), "execution_time": execution_time},
            )

        finally:
            # Cleanup
            if self.current_job:
                await self._cleanup_current_job()

            # Update metrics
            metrics.active_jobs.labels(worker_id=self.worker_id).dec()

            # Acknowledge message
            await message.ack()

    async def _cleanup_current_job(self) -> None:
        """Cleanup currently running job"""
        if self.current_job:
            try:
                await self.current_job.cleanup()
            except Exception as e:
                self.logger.error(
                    "Error cleaning up job",
                    job_id=self.current_job.job_id,
                    error=str(e),
                    exc_info=True,
                )
            self.current_job = None

    async def cleanup(self) -> None:
        """Cleanup worker resources"""
        if self.current_job:
            await self._cleanup_current_job()

        try:
            await self.broker.close()
        except Exception as e:
            self.logger.error(
                "Error closing broker connection", error=str(e), exc_info=True
            )

    async def pause(self) -> None:
        """Pause worker processing"""
        self.running = False
        self.logger.info("Worker paused")

    async def resume(self) -> None:
        """Resume worker processing"""
        self.running = True
        self.logger.info("Worker resumed")

    @property
    def is_running(self) -> bool:
        """Check if worker is running"""
        return self.running

    async def health_check(self) -> bool:
        """Check worker health"""
        try:
            # Check broker connection
            if not await self.broker.ping():
                self.logger.warning("Broker connection check failed")
                return False

            # Check storage connection
            if not await self.storage.ping():
                self.logger.warning("Storage connection check failed")
                return False

            # Check for stuck jobs
            if self.current_job:
                job_start = self.current_job.metadata.started_at
                if job_start:
                    stuck_threshold = timedelta(
                        seconds=self.current_job.metadata.timeout_seconds * 2
                    )
                    if datetime.now(timezone.utc) - job_start > stuck_threshold:
                        self.logger.warning(
                            "Worker has stuck job",
                            job_id=self.current_job.job_id,
                            start_time=job_start,
                        )
                        return False

            # Check system resources
            process = psutil.Process()

            # CPU check
            cpu_percent = process.cpu_percent()
            if cpu_percent > 90:  # 90% CPU threshold
                self.logger.warning("High CPU usage", cpu_percent=cpu_percent)
                return False

            # Memory check
            memory_info = process.memory_info()
            memory_percent = memory_info.rss / psutil.virtual_memory().total * 100
            if memory_percent > 90:  # 90% memory threshold
                self.logger.warning("High memory usage", memory_percent=memory_percent)
                return False

            # Update resource metrics
            metrics.worker_memory_usage.labels(worker_id=self.worker_id).set(
                memory_info.rss
            )

            metrics.worker_cpu_usage.labels(worker_id=self.worker_id).set(cpu_percent)

            return True

        except Exception as e:
            self.logger.error("Health check failed", error=str(e), exc_info=True)
            return False

    def get_stats(self) -> Dict[str, Any]:
        """Get worker statistics"""
        return {
            "worker_id": self.worker_id,
            "running": self.running,
            "jobs_processed": self.jobs_processed,
            "error_count": self.error_count,
            "uptime_seconds": (
                datetime.now(timezone.utc) - self.started_at
            ).total_seconds(),
            "current_job_id": self.current_job.job_id if self.current_job else None,
        }

    async def retry_failed_jobs(self) -> int:
        """Retry failed jobs that haven't exceeded max retries"""
        retry_count = 0
        failed_jobs = await self.storage.get_failed_jobs(
            max_retries=settings.max_retries
        )

        for job in failed_jobs:
            try:
                # Reset job status and increment retry count
                await self.storage.update_job_status(
                    job_id=job.job_id,
                    status=JobStatus.PENDING,
                    retries=job.metadata.retries + 1,
                )

                # Re-queue the job
                await self.broker.publish_job(
                    job_type=job.job_type,
                    job_id=job.job_id,
                    input_data=job.input_data,
                    metadata=job.metadata.model_dump(),
                )

                retry_count += 1
                self.logger.info(
                    "Retrying failed job",
                    job_id=job.job_id,
                    retry_count=job.metadata.retries + 1,
                )

            except Exception as e:
                self.logger.error("Error retrying job", job_id=job.job_id, error=str(e))

        metrics.job_retries.inc(retry_count)
        return retry_count

    async def handle_timeout(self, job: Job) -> None:
        """Handle job timeout"""
        try:
            # Update job status
            await self.storage.update_job_status(
                job_id=job.job_id,
                status=JobStatus.FAILED,
                error="Job execution timed out",
            )

            # Call timeout handler
            await job.on_timeout()

            # Emit timeout event
            await event_bus.publish(
                f"job.timeout.{job.__class__.__name__}",
                {"job_id": job.job_id, "timeout_seconds": job.metadata.timeout_seconds},
            )

            metrics.job_timeouts.labels(job_type=job.__class__.__name__).inc()

        except Exception as e:
            self.logger.error(
                "Error handling job timeout", job_id=job.job_id, error=str(e)
            )
