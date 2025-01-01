import asyncio
from contextlib import asynccontextmanager
from typing import Optional
from datetime import datetime, timedelta
import structlog
from redis.asyncio import Redis

logger = structlog.get_logger()

class DistributedSemaphore:
    """
    Redis-based distributed semaphore for controlling job concurrency
    """
    
    def __init__(self,
                 redis: Redis,
                 name: str,
                 max_leases: int,
                 timeout_seconds: int = 300,
                 retry_delay: float = 0.5):
        self.redis = redis
        self.name = name
        self.max_leases = max_leases
        self.timeout_seconds = timeout_seconds
        self.retry_delay = retry_delay
        self.logger = logger.bind(semaphore_name=name)

    async def _cleanup_expired(self) -> None:
        """Remove expired leases"""
        now = datetime.utcnow()
        async with self.redis.pipeline() as pipe:
            # Get all leases
            leases = await self.redis.hgetall(self.name)
            
            # Check for expired leases
            for lease_id, timestamp_str in leases.items():
                timestamp = float(timestamp_str)
                if now.timestamp() - timestamp > self.timeout_seconds:
                    await pipe.hdel(self.name, lease_id)
            
            await pipe.execute()

    async def _get_active_count(self) -> int:
        """Get count of active leases"""
        leases = await self.redis.hgetall(self.name)
        now = datetime.utcnow().timestamp()
        
        # Count non-expired leases
        active_count = sum(
            1 for timestamp_str in leases.values()
            if now - float(timestamp_str) <= self.timeout_seconds
        )
        
        return active_count

    async def acquire(self,
                     lease_id: str,
                     timeout: Optional[float] = None,
                     blocking: bool = True) -> bool:
        """
        Acquire a semaphore lease
        
        Args:
            lease_id: Unique identifier for this lease
            timeout: Maximum time to wait for lease (None for no timeout)
            blocking: Whether to wait for lease to become available
        
        Returns:
            bool: Whether lease was acquired
        """
        start_time = datetime.utcnow()
        
        while True:
            # Clean up expired leases
            await self._cleanup_expired()
            
            # Check current lease count
            active_count = await self._get_active_count()
            
            if active_count < self.max_leases:
                # Try to acquire lease
                timestamp = datetime.utcnow().timestamp()
                success = await self.redis.hsetnx(self.name, lease_id, str(timestamp))
                
                if success:
                    self.logger.debug(
                        "Acquired semaphore lease",
                        lease_id=lease_id,
                        active_count=active_count + 1
                    )
                    return True
            
            if not blocking:
                return False
                
            # Check timeout
            if timeout is not None:
                elapsed = (datetime.utcnow() - start_time).total_seconds()
                if elapsed >= timeout:
                    self.logger.debug(
                        "Semaphore acquisition timed out",
                        lease_id=lease_id,
                        timeout=timeout
                    )
                    return False
            
            # Wait before retry
            await asyncio.sleep(self.retry_delay)

    async def release(self, lease_id: str) -> None:
        """Release a semaphore lease"""
        await self.redis.hdel(self.name, lease_id)
        self.logger.debug("Released semaphore lease", lease_id=lease_id)

    @asynccontextmanager
    async def lease(self,
                   lease_id: str,
                   timeout: Optional[float] = None,
                   blocking: bool = True):
        """Context manager for semaphore lease"""
        try:
            acquired = await self.acquire(lease_id, timeout, blocking)
            if not acquired:
                raise TimeoutError("Failed to acquire semaphore lease")
            yield acquired
        finally:
            await self.release(lease_id)

class JobSemaphore(DistributedSemaphore):
    """Specialized semaphore for job concurrency control"""
    
    def __init__(self,
                 redis: Redis,
                 job_type: str,
                 max_concurrent: int,
                 timeout_seconds: int = 300):
        super().__init__(
            redis=redis,
            name=f"taskforge:semaphore:{job_type}",
            max_leases=max_concurrent,
            timeout_seconds=timeout_seconds
        )
        self.job_type = job_type
        
    async def check_job_allowed(self, job_id: str) -> bool:
        """Check if a new job can be started"""
        return await self.acquire(job_id, blocking=False)
        
    @asynccontextmanager
    async def run_job(self, job_id: str):
        """Context manager for running a job"""
        async with self.lease(job_id) as acquired:
            if not acquired:
                raise RuntimeError(f"Maximum concurrent jobs reached for {self.job_type}")
            yield
            