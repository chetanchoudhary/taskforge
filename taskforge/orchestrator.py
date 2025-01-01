import uuid
from typing import Dict, Type, Optional, List
import structlog
from datetime import datetime

from taskforge.jobs.base import Job, JobStatus, JobPriority, JobConfig, JobMetadata, JobResult
from taskforge.storage.postgres import PostgresJobStorage
from taskforge.broker.rabbitmq import RabbitMQBroker

logger = structlog.get_logger()

class JobOrchestrator:
    """
    Core orchestrator that manages job registration, submission, and lifecycle
    """
    
    def __init__(self,
                 broker: RabbitMQBroker,
                 storage: PostgresJobStorage):
        self.broker = broker
        self.storage = storage
        self._job_configs: Dict[str, JobConfig] = {}
        
    async def start(self):
        """Initialize the orchestrator"""
        await self.broker.connect()
        await self.storage.initialize()
        
    async def shutdown(self):
        """Cleanup and shutdown"""
        await self.broker.close()
        
    async def register_job(self,
                          job_class: Type[Job],
                          max_retries: int = 3,
                          timeout_seconds: int = 300,
                          concurrency_limit: int = 10,
                          queue_name: Optional[str] = None) -> None:
        """
        Register a job type with its configuration
        """
        job_type = job_class.__name__
        
        # Store job configuration
        self._job_configs[job_type] = JobConfig(
            max_retries=max_retries,
            timeout_seconds=timeout_seconds,
            concurrency_limit=concurrency_limit,
            queue_name=queue_name or job_type
        )
        
        # Register with broker for queue setup
        await self.broker.register_job(
            job_class=job_class,
            queue_name=queue_name
        )
        
        logger.info(
            "Registered job type",
            job_type=job_type,
            config=self._job_configs[job_type].dict()
        )
        
    async def submit_job(self,
                        job_type: str,
                        input_data: dict,
                        priority: JobPriority = JobPriority.MEDIUM,
                        metadata: Optional[Dict] = None) -> str:
        """
        Submit a new job for processing
        """
        if job_type not in self._job_configs:
            raise ValueError(f"Job type {job_type} not registered")
            
        job_id = str(uuid.uuid4())
        config = self._job_configs[job_type]
        
        # Create job metadata
        job_metadata = JobMetadata(
            created_at=datetime.utcnow(),
            max_retries=config.max_retries,
            timeout_seconds=config.timeout_seconds,
            priority=priority,
            tags=metadata.get('tags', {}) if metadata else {}
        )
        
        # Save initial job state
        await self.storage.save_job(
            job_id=job_id,
            job_type=job_type,
            input_data=input_data,
            metadata=job_metadata
        )
        
        # Publish to message queue
        await self.broker.publish_job(
            job_type=job_type,
            job_id=job_id,
            input_data=input_data,
            metadata=job_metadata.dict(),
            priority=priority
        )
        
        logger.info(
            "Submitted job",
            job_id=job_id,
            job_type=job_type,
            priority=priority.value
        )
        
        return job_id
        
    async def get_job_state(self, job_id: str) -> Optional[JobResult]:
        """
        Get current state of a job
        """
        return await self.storage.get_job_state(job_id)
        
    async def list_jobs(self,
                       status: Optional[JobStatus] = None,
                       limit: int = 100,
                       offset: int = 0) -> List[JobResult]:
        """
        List jobs with optional filters
        """
        if status:
            return await self.storage.get_jobs_by_status(
                status=status,
                limit=limit,
                offset=offset
            )
        else:
            # TODO: Implement unfiltered job listing
            return []
            
    async def cancel_job(self, job_id: str) -> bool:
        """
        Cancel a job if possible
        """
        job_state = await self.get_job_state(job_id)
        if not job_state or job_state.status in (JobStatus.COMPLETED, JobStatus.FAILED):
            return False
            
        await self.storage.update_job_status(
            job_id=job_id,
            status=JobStatus.FAILED,
            error="Job cancelled by user"
        )
        
        return True
        
    def get_job_config(self, job_type: str) -> Optional[JobConfig]:
        """
        Get configuration for a job type
        """
        return self._job_configs.get(job_type)
        
    async def get_queue_depths(self) -> Dict[str, int]:
        """
        Get current depth of all job queues
        """
        depths = {}
        for job_type, config in self._job_configs.items():
            queue_name = config.queue_name
            depth = await self.broker.get_queue_depth(queue_name)
            depths[job_type] = depth
        return depths
        
    async def retry_failed_jobs(self, job_type: Optional[str] = None) -> int:
        """
        Retry failed jobs that haven't exceeded max retries
        """
        retry_count = 0
        # Get failed jobs
        failed_jobs = await self.storage.get_jobs_by_status(JobStatus.FAILED)
        
        for job in failed_jobs:
            config = self._job_configs.get(job.job_type)
            if not config:
                continue
                
            if job_type and job.job_type != job_type:
                continue
                
            if job.metadata.retries < config.max_retries:
                # Reset job status and increment retry count
                await self.storage.update_job_status(
                    job_id=job.job_id,
                    status=JobStatus.PENDING
                )
                
                # Re-publish to queue
                await self.broker.publish_job(
                    job_type=job.job_type,
                    job_id=job.job_id,
                    input_data=job.input_data,
                    metadata=job.metadata.dict(),
                    priority=job.metadata.priority
                )
                
                retry_count += 1
                
        return retry_count