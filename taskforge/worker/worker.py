import asyncio
import time
import signal
from typing import Optional, Dict, Any
from datetime import datetime, timedelta
import structlog
from prometheus_client import Counter, Histogram, Gauge

from taskforge.jobs.base import Job, JobStatus, JobResult, JobMetadata
from taskforge.storage.postgres import PostgresJobStorage
from taskforge.broker.rabbitmq import RabbitMQBroker

# Metrics
JOBS_PROCESSED = Counter(
    'taskforge_jobs_processed_total',
    'Number of jobs processed',
    ['job_type', 'status']
)

JOB_PROCESSING_TIME = Histogram(
    'taskforge_job_processing_seconds',
    'Time spent processing jobs',
    ['job_type']
)

ACTIVE_JOBS = Gauge(
    'taskforge_active_jobs',
    'Number of currently active jobs',
    ['worker_id']
)

logger = structlog.get_logger()

class TaskWorker:
    """Worker process that executes jobs"""
    
    def __init__(self,
                 worker_id: str,
                 broker: RabbitMQBroker,
                 storage: PostgresJobStorage,
                 prefetch_count: int = 10,
                 shutdown_timeout: int = 30):
        self.worker_id = worker_id
        self.broker = broker
        self.storage = storage
        self.prefetch_count = prefetch_count
        self.shutdown_timeout = shutdown_timeout
        self.running = False
        self.current_job: Optional[Job] = None
        self._shutdown_event = asyncio.Event()
        
        # Create structured logger with worker context
        self.logger = logger.bind(
            worker_id=worker_id,
            prefetch_count=prefetch_count
        )

    async def start(self) -> None:
        """Start the worker"""
        self.running = True
        self.logger.info("Starting worker")
        
        # Setup signal handlers
        for sig in (signal.SIGINT, signal.SIGTERM):
            asyncio.get_event_loop().add_signal_handler(
                sig,
                lambda s=sig: asyncio.create_task(self.handle_signal(s))
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
                        "Error processing message",
                        error=str(e),
                        exc_info=True
                    )
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
                    self._cleanup_current_job(),
                    timeout=self.shutdown_timeout
                )
            except asyncio.TimeoutError:
                self.logger.error(
                    "Timeout waiting for current job to cleanup",
                    job_id=self.current_job.job_id
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
        ACTIVE_JOBS.labels(worker_id=self.worker_id).inc()
        
        start_time = time.monotonic()
        
        try:
            # Update job status to running
            await self.storage.update_job_status(
                job_id=job_id,
                status=JobStatus.RUNNING,
                execution_time=0
            )
            
            # Create and validate job
            job_class = self.broker.get_job_class(job_type)
            job = job_class(
                job_id=job_id,
                input_data=message["input_data"],
                metadata=JobMetadata(**message.get("metadata", {}))
            )
            
            self.current_job = job
            
            # Validate job
            if not await job.validate():
                raise ValueError("Job validation failed")
            
            # Execute pre-job hooks
            await job.pre_execute()
            
            # Execute job with timeout
            timeout = job.metadata.timeout_seconds
            try:
                result = await asyncio.wait_for(
                    job.execute(),
                    timeout=timeout
                )
            except asyncio.TimeoutError:
                raise TimeoutError(
                    f"Job execution timed out after {timeout} seconds"
                )
            
            # Execute post-job hooks
            await job.post_execute()
            
            execution_time = time.monotonic() - start_time
            
            # Update job as completed
            await self.storage.update_job_status(
                job_id=job_id,
                status=JobStatus.COMPLETED,
                result=result.dict() if result else None,
                execution_time=execution_time
            )
            
            # Call success handler
            await job.on_success(result)
            
            # Update metrics
            JOBS_PROCESSED.labels(
                job_type=job_type,
                status="success"
            ).inc()
            JOB_PROCESSING_TIME.labels(
                job_type=job_type
            ).observe(execution_time)
            
            self.logger.info(
                "Job completed successfully",
                job_id=job_id,
                execution_time=execution_time
            )
            
        except Exception as e:
            execution_time = time.monotonic() - start_time
            self.logger.error(
                "Job execution failed",
                job_id=job_id,
                error=str(e),
                execution_time=execution_time,
                exc_info=True
            )
            
            # Update job as failed
            await self.storage.update_job_status(
                job_id=job_id,
                status=JobStatus.FAILED,
                error=str(e),
                execution_time=execution_time
            )
            
            # Call failure handler
            if self.current_job:
                await self.current_job.on_failure(e)
            
            # Update metrics
            JOBS_PROCESSED.labels(
                job_type=job_type,
                status="failure"
            ).inc()
            
        finally:
            # Cleanup
            if self.current_job:
                await self._cleanup_current_job()
            
            # Decrement active jobs
            ACTIVE_JOBS.labels(worker_id=self.worker_id).dec()
            
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
                    exc_info=True
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
                "Error closing broker connection",
                error=str(e),
                exc_info=True
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
                return False
                
            # Check for stuck jobs
            if self.current_job:
                job_start = self.current_job.metadata.started_at
                if job_start:
                    stuck_threshold = timedelta(
                        seconds=self.current_job.metadata.timeout_seconds * 2
                    )
                    if datetime.utcnow() - job_start > stuck_threshold:
                        self.logger.warning(
                            "Worker has stuck job",
                            job_id=self.current_job.job_id,
                            start_time=job_start
                        )
                        return False
            
            return True
            
        except Exception as e:
            self.logger.error(
                "Health check failed",
                error=str(e),
                exc_info=True
            )
            return False
        