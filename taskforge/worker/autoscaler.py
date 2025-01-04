# taskforge/worker/autoscaler.py
import asyncio
from datetime import datetime, timezone
from typing import Optional

import structlog
from prometheus_client import Gauge

from taskforge.core.config import settings
from taskforge.worker.pool import WorkerPool

logger = structlog.get_logger()

# Autoscaler metrics
worker_desired = Gauge(
    "taskforge_worker_desired",
    "Desired number of workers",
)

worker_pending_scale = Gauge(
    "taskforge_worker_pending_scale",
    "Number of workers pending scale up/down",
)


class WorkerAutoscaler:
    """Automatically scales worker pool based on metrics"""

    def __init__(
        self,
        worker_pool: WorkerPool,
        min_workers: int = 1,
        max_workers: int = 10,
        scale_up_threshold: float = 0.8,
        scale_down_threshold: float = 0.3,
        cooldown_seconds: int = 300,
        check_interval: int = 60,
    ):
        self.worker_pool = worker_pool
        self.min_workers = min_workers
        self.max_workers = max_workers
        self.scale_up_threshold = scale_up_threshold
        self.scale_down_threshold = scale_down_threshold
        self.cooldown_seconds = cooldown_seconds
        self.check_interval = check_interval

        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._last_scale: Optional[datetime] = None

        self.logger = logger.bind(
            min_workers=min_workers,
            max_workers=max_workers,
            scale_up_threshold=scale_up_threshold,
            scale_down_threshold=scale_down_threshold,
        )

    async def start(self):
        """Start autoscaler"""
        if self._running:
            return

        self._running = True
        self._task = asyncio.create_task(self._run())
        self.logger.info("Worker autoscaler started")

    async def stop(self):
        """Stop autoscaler"""
        if not self._running:
            return

        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

        self.logger.info("Worker autoscaler stopped")

    async def _run(self):
        """Main autoscaling loop"""
        while self._running:
            try:
                await self._check_scaling()
            except Exception as e:
                self.logger.error("Error in autoscaler", error=str(e), exc_info=True)

            await asyncio.sleep(self.check_interval)

    async def _check_scaling(self):
        """Check if scaling is needed"""
        if self._in_cooldown():
            return

        # Get current metrics
        queue_depths = await self.worker_pool.get_queue_depths()
        worker_stats = await self.worker_pool.get_stats()

        # Calculate utilization
        total_messages = sum(queue_depths.values())
        active_workers = worker_stats["active_workers"]
        current_capacity = active_workers * settings.worker.prefetch_count

        if current_capacity == 0:
            utilization = 1.0  # Force scale up if no capacity
        else:
            utilization = total_messages / current_capacity

        # Update metrics
        worker_desired.set(active_workers)

        # Scale up if needed
        if utilization > self.scale_up_threshold and active_workers < self.max_workers:
            new_count = min(
                active_workers + self._calculate_scale_up(active_workers, utilization),
                self.max_workers,
            )
            await self._scale(new_count, "up", utilization)

        # Scale down if needed
        elif (
            utilization < self.scale_down_threshold
            and active_workers > self.min_workers
        ):
            new_count = max(
                active_workers
                - self._calculate_scale_down(active_workers, utilization),
                self.min_workers,
            )
            await self._scale(new_count, "down", utilization)

    def _calculate_scale_up(self, current_workers: int, utilization: float) -> int:
        """Calculate number of workers to add"""
        # Add workers proportional to utilization overage
        overage = utilization - self.scale_up_threshold
        addition = max(1, int(current_workers * overage))
        return min(addition, self.max_workers - current_workers)

    def _calculate_scale_down(self, current_workers: int, utilization: float) -> int:
        """Calculate number of workers to remove"""
        # Remove workers proportional to utilization underage
        underage = self.scale_down_threshold - utilization
        reduction = max(1, int(current_workers * underage))
        return min(reduction, current_workers - self.min_workers)

    async def _scale(self, new_count: int, direction: str, utilization: float):
        """Scale worker pool"""
        current_workers = self.worker_pool.active_workers
        worker_pending_scale.set(abs(new_count - current_workers))

        self.logger.info(
            f"Scaling {direction} workers",
            current=current_workers,
            new=new_count,
            utilization=f"{utilization:.2f}",
        )

        try:
            await self.worker_pool.scale(new_count)
            self._last_scale = datetime.now(timezone.utc)
        except Exception as e:
            self.logger.error(f"Error scaling {direction}", error=str(e), exc_info=True)
        finally:
            worker_pending_scale.set(0)

    def _in_cooldown(self) -> bool:
        """Check if in cooldown period after scaling"""
        if not self._last_scale:
            return False

        elapsed = datetime.now(timezone.utc) - self._last_scale
        return elapsed.total_seconds() < self.cooldown_seconds
