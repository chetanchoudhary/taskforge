from abc import ABC, abstractmethod
from typing import Optional, List, Dict, Any
from datetime import datetime

from taskforge.jobs.base import JobStatus, JobResult, JobMetadata

class BaseStorage(ABC):
    """Base interface for job storage backends"""
    
    @abstractmethod
    async def initialize(self) -> None:
        """Initialize storage (create tables, indexes, etc.)"""
        pass
    
    @abstractmethod
    async def ping(self) -> bool:
        """Check storage connectivity"""
        pass
    
    @abstractmethod
    async def save_job(self,
                      job_id: str,
                      job_type: str,
                      input_data: Dict[str, Any],
                      metadata: JobMetadata) -> None:
        """Save a new job"""
        pass
    
    @abstractmethod
    async def update_job_status(self,
                              job_id: str,
                              status: JobStatus,
                              result: Optional[Dict[str, Any]] = None,
                              error: Optional[str] = None,
                              execution_time: Optional[float] = None) -> None:
        """Update job status and result"""
        pass
    
    @abstractmethod
    async def get_job_state(self, job_id: str) -> Optional[JobResult]:
        """Get current state of a job"""
        pass
    
    @abstractmethod
    async def list_jobs(self,
                       status: Optional[JobStatus] = None,
                       job_type: Optional[str] = None,
                       start_date: Optional[datetime] = None,
                       end_date: Optional[datetime] = None,
                       limit: int = 100,
                       offset: int = 0) -> List[JobResult]:
        """List jobs with optional filters"""
        pass
    
    @abstractmethod
    async def cleanup_old_jobs(self, 
                             days: int,
                             status: Optional[List[JobStatus]] = None) -> int:
        """Clean up old job records"""
        pass
    
    @abstractmethod
    async def get_job_counts(self,
                           job_type: Optional[str] = None) -> Dict[JobStatus, int]:
        """Get counts of jobs by status"""
        pass
    
    @abstractmethod
    async def get_failed_jobs(self,
                            max_retries: int,
                            limit: int = 100) -> List[JobResult]:
        """Get failed jobs eligible for retry"""
        pass
    
    @abstractmethod
    async def get_stalled_jobs(self,
                             timeout_minutes: int,
                             limit: int = 100) -> List[JobResult]:
        """Get jobs that have been running too long"""
        pass

class BaseJobLock(ABC):
    """Base interface for distributed job locking"""
    
    @abstractmethod
    async def acquire_lock(self,
                         job_id: str,
                         timeout_seconds: int = 300) -> bool:
        """Acquire a lock for processing a job"""
        pass
    
    @abstractmethod
    async def release_lock(self, job_id: str) -> None:
        """Release a job lock"""
        pass
    
    @abstractmethod
    async def refresh_lock(self, job_id: str) -> bool:
        """Refresh a job lock timeout"""
        pass
    
    @abstractmethod
    async def get_locked_jobs(self) -> List[str]:
        """Get list of currently locked job IDs"""
        pass
    