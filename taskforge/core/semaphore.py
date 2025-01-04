import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Optional

import structlog
from redis.asyncio import Redis

from taskforge.exceptions import ResourceError

logger = structlog.get_logger()


class DistributedSemaphore:
    """Redis-based distributed semaphore for concurrency control"""

    def __init__(
        self,
        redis: Redis,
        name: str,
        max_leases: int,
        timeout_seconds: int = 300,
        retry_delay: float = 0.5,
        max_retry_attempts: int = 3,
    ):
        self.redis = redis
        self.name = name
        self.max_leases = max_leases
        self.timeout_seconds = timeout_seconds
        self.retry_delay = retry_delay
        self.max_retry_attempts = max_retry_attempts
        self.logger = logger.bind(semaphore_name=name)

    async def _cleanup_expired(self) -> None:
        """Remove expired leases"""
        now = datetime.now(timezone.utc)

        async with self.redis.pipeline() as pipe:
            # Get all leases
            leases = await self.redis.hgetall(self.name)

            # Check for expired leases
            for lease_id, timestamp_str in leases.items():
                timestamp = float(timestamp_str)
                if now.timestamp() - timestamp > self.timeout_seconds:
                    await pipe.hdel(self.name, lease_id)
                    self.logger.debug(
                        "Removed expired lease",
                        lease_id=lease_id.decode()
                        if isinstance(lease_id, bytes)
                        else lease_id,
                    )

            await pipe.execute()

    async def _get_active_count(self) -> int:
        """Get count of active (non-expired) leases"""
        leases = await self.redis.hgetall(self.name)
        now = datetime.now(timezone.utc).timestamp()

        active_count = sum(
            1
            for timestamp_str in leases.values()
            if now
            - float(
                timestamp_str.decode()
                if isinstance(timestamp_str, bytes)
                else timestamp_str
            )
            <= self.timeout_seconds
        )

        return active_count

    async def acquire(
        self, lease_id: str, timeout: Optional[float] = None, blocking: bool = True
    ) -> bool:
        """
        Acquire a semaphore lease

        Args:
            lease_id: Unique identifier for this lease
            timeout: Maximum time to wait for lease (None for no timeout)
            blocking: Whether to wait for lease to become available

        Returns:
            bool: Whether lease was acquired

        Raises:
            ResourceError: If acquisition fails
        """
        start_time = datetime.now(timezone.utc)
        attempt = 0

        while True:
            try:
                # Clean up expired leases
                await self._cleanup_expired()

                # Check current lease count
                active_count = await self._get_active_count()

                if active_count < self.max_leases:
                    # Try to acquire lease
                    timestamp = datetime.now(timezone.utc).timestamp()
                    success = await self.redis.hsetnx(
                        self.name, lease_id, str(timestamp)
                    )

                    if success:
                        self.logger.debug(
                            "Acquired semaphore lease",
                            lease_id=lease_id,
                            active_count=active_count + 1,
                        )
                        return True

                if not blocking:
                    return False

                # Check timeout
                if timeout is not None:
                    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
                    if elapsed >= timeout:
                        self.logger.debug(
                            "Semaphore acquisition timed out",
                            lease_id=lease_id,
                            timeout=timeout,
                        )
                        return False

                # Check retry attempts
                attempt += 1
                if attempt >= self.max_retry_attempts:
                    raise ResourceError(
                        f"Failed to acquire lease after {attempt} attempts"
                    )

                # Wait before retry
                await asyncio.sleep(self.retry_delay)

            except Exception as e:
                if not isinstance(e, ResourceError):
                    raise ResourceError(f"Error acquiring lease: {str(e)}")
                raise

    async def release(self, lease_id: str) -> None:
        """Release a semaphore lease"""
        try:
            await self.redis.hdel(self.name, lease_id)
            self.logger.debug("Released semaphore lease", lease_id=lease_id)
        except Exception as e:
            raise ResourceError(f"Error releasing lease: {str(e)}")

    async def refresh(self, lease_id: str) -> bool:
        """Refresh a lease's timeout"""
        try:
            timestamp = datetime.now(timezone.utc).timestamp()
            success = await self.redis.hset(self.name, lease_id, str(timestamp))
            return bool(success)
        except Exception as e:
            raise ResourceError(f"Error refreshing lease: {str(e)}")

    @asynccontextmanager
    async def lease(
        self, lease_id: str, timeout: Optional[float] = None, blocking: bool = True
    ):
        """Context manager for semaphore lease"""
        try:
            acquired = await self.acquire(lease_id, timeout, blocking)
            if not acquired:
                raise ResourceError("Failed to acquire semaphore lease")
            yield acquired
        finally:
            await self.release(lease_id)


class JobSemaphore(DistributedSemaphore):
    """Specialized semaphore for job concurrency control"""

    def __init__(
        self,
        redis: Redis,
        job_type: str,
        max_concurrent: int,
        timeout_seconds: int = 300,
    ):
        super().__init__(
            redis=redis,
            name=f"taskforge:semaphore:{job_type}",
            max_leases=max_concurrent,
            timeout_seconds=timeout_seconds,
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
                raise ResourceError(
                    f"Maximum concurrent jobs reached for {self.job_type}"
                )
            yield

    async def get_running_jobs(self) -> list[str]:
        """Get list of currently running jobs"""
        leases = await self.redis.hgetall(self.name)
        now = datetime.now(timezone.utc).timestamp()

        return [
            lease_id.decode() if isinstance(lease_id, bytes) else lease_id
            for lease_id, timestamp_str in leases.items()
            if now
            - float(
                timestamp_str.decode()
                if isinstance(timestamp_str, bytes)
                else timestamp_str
            )
            <= self.timeout_seconds
        ]


class WorkerSemaphore(DistributedSemaphore):
    """Specialized semaphore for worker concurrency control"""

    def __init__(
        self,
        redis: Redis,
        worker_id: str,
        max_concurrent: int,
        timeout_seconds: int = 300,
    ):
        super().__init__(
            redis=redis,
            name=f"taskforge:worker:{worker_id}",
            max_leases=max_concurrent,
            timeout_seconds=timeout_seconds,
        )
        self.worker_id = worker_id

    @asynccontextmanager
    async def process_job(self, job_id: str):
        """Context manager for worker job processing"""
        async with self.lease(job_id) as acquired:
            if not acquired:
                raise ResourceError(
                    f"Maximum concurrent jobs reached for worker {self.worker_id}"
                )
            yield
