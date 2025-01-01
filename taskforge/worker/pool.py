import asyncio
import uuid
import signal
from typing import List, Dict, Optional
import structlog
from datetime import datetime
from contextlib import asynccontextmanager

from taskforge.worker.worker import TaskWorker
from taskforge.broker.rabbitmq import RabbitMQBroker
from taskforge.storage.postgres import PostgresJobStorage
from taskforge.core.metrics import metrics
from taskforge.core.config import settings
from taskforge.core.semaphore import JobSemaphore

logger = structlog.get_logger()

class WorkerPool:
    """
    Manages a pool of task workers for distributed job processing
    """
    
    def __init__(
        self,
        broker: RabbitMQBroker,
        storage: PostgresJobStorage,
        num_workers: int = 4,
        prefetch_count: int = 10,
        monitor_interval: int = 10
    ):
        self.broker = broker
        self.storage = storage
        self.num_workers = num_workers
        self.prefetch_count = prefetch_count
        self.monitor_interval = monitor_interval
        self.workers: List[TaskWorker] = []
        self.running = False
        self._shutdown_event = asyncio.Event()
        self._monitor_task: Optional[asyncio.Task] = None
        self.logger = logger.bind(
            component="WorkerPool",
            num_workers=num_workers,
            prefetch_count=prefetch_count
        )
        
        # Track worker stats
        self.stats: Dict[str, Dict] = {}

    async def start(self) -> None:
        """Start the worker pool"""
        if self.running:
            return
            
        self.running = True
        self.logger.info("Starting worker pool")
        
        # Set up signal handlers
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(
                sig,
                lambda s=sig: asyncio.create_task(self.handle_signal(s))
            )
            
        # Initialize workers
        for _ in range(self.num_workers):
            worker = await self._create_worker()
            self.workers.append(worker)
            
        # Start monitoring task
        self._monitor_task = asyncio.create_task(self._monitor_workers())
        
        self.logger.info("Worker pool started", active_workers=len(self.workers))

    async def _create_worker(self) -> TaskWorker:
        """Create and initialize a new worker"""
        worker_id = str(uuid.uuid4())
        worker = TaskWorker(
            worker_id=worker_id,
            broker=self.broker,
            storage=self.storage,
            prefetch_count=self.prefetch_count
        )
        
        # Initialize worker stats
        self.stats[worker_id] = {
            "started_at": datetime.utcnow(),
            "jobs_processed": 0,
            "errors": 0,
            "last_health_check": None
        }
        
        # Start worker processing
        asyncio.create_task(worker.start())
        
        return worker

    async def _monitor_workers(self) -> None:
        """Monitor worker health and restart failed workers"""
        while self.running:
            try:
                current_time = datetime.utcnow()
                
                # Check each worker
                for i, worker in enumerate(self.workers):
                    worker_stats = self.stats[worker.worker_id]
                    
                    try:
                        # Perform health check
                        is_healthy = await worker.health_check()
                        worker_stats["last_health_check"] = current_time
                        
                        if not is_healthy:
                            self.logger.warning(
                                "Worker unhealthy, restarting",
                                worker_id=worker.worker_id
                            )
                            # Stop unhealthy worker
                            await worker.stop()
                            
                            # Create replacement worker
                            new_worker = await self._create_worker()
                            self.workers[i] = new_worker
                            
                            # Update metrics
                            metrics.worker_errors.labels(
                                worker_id=worker.worker_id,
                                error_type="health_check_failed"
                            ).inc()
                            worker_stats["errors"] += 1
                            
                    except Exception as e:
                        self.logger.error(
                            "Worker health check failed",
                            worker_id=worker.worker_id,
                            error=str(e)
                        )
                        worker_stats["errors"] += 1
                
                # Update metrics
                self._update_metrics()
                
            except Exception as e:
                self.logger.error(
                    "Error in worker monitoring",
                    error=str(e),
                    exc_info=True
                )
                
            await asyncio.sleep(self.monitor_interval)

    def _update_metrics(self) -> None:
        """Update Prometheus metrics"""
        for worker_id, stats in self.stats.items():
            # Update worker metrics
            metrics.worker_errors.labels(
                worker_id=worker_id,
                error_type="total"
            ).inc(stats["errors"])
            
            # Update job metrics
            metrics.jobs_processed.labels(
                worker_id=worker_id
            ).inc(stats["jobs_processed"])

    async def shutdown(self) -> None:
        """Shutdown the worker pool gracefully"""
        if not self.running:
            return
            
        self.logger.info("Shutting down worker pool")
        self.running = False
        
        # Cancel monitor task
        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass
        
        # Stop all workers
        stop_tasks = []
        for worker in self.workers:
            stop_tasks.append(asyncio.create_task(worker.stop()))
        
        if stop_tasks:
            await asyncio.wait(stop_tasks)
        
        self.workers.clear()
        self.stats.clear()
        
        self.logger.info("Worker pool shutdown complete")

    async def scale(self, num_workers: int) -> None:
        """Scale the worker pool up or down"""
        if num_workers == self.num_workers:
            return
            
        self.logger.info(
            "Scaling worker pool",
            current=self.num_workers,
            target=num_workers
        )
        
        if num_workers > self.num_workers:
            # Scale up
            for _ in range(num_workers - self.num_workers):
                worker = await self._create_worker()
                self.workers.append(worker)
        else:
            # Scale down
            workers_to_remove = self.workers[num_workers:]
            self.workers = self.workers[:num_workers]
            
            # Stop excess workers
            stop_tasks = []
            for worker in workers_to_remove:
                stop_tasks.append(asyncio.create_task(worker.stop()))
                # Clean up stats
                self.stats.pop(worker.worker_id, None)
            
            if stop_tasks:
                await asyncio.wait(stop_tasks)
        
        self.num_workers = num_workers
        self.logger.info(
            "Worker pool scaled",
            new_size=num_workers,
            active_workers=len(self.workers)
        )

    async def handle_signal(self, sig: signal.Signals) -> None:
        """Handle shutdown signals"""
        self.logger.info(f"Received signal {sig.name}")
        await self.shutdown()
        self._shutdown_event.set()

    @property
    def active_workers(self) -> int:
        """Get number of currently active workers"""
        return len([w for w in self.workers if w.is_running])

    @asynccontextmanager
    async def worker_context(self):
        """Context manager for worker pool lifecycle"""
        try:
            await self.start()
            yield self
        finally:
            await self.shutdown()

    async def get_stats(self) -> Dict:
        """Get current worker pool statistics"""
        return {
            "active_workers": self.active_workers,
            "total_workers": self.num_workers,
            "worker_stats": self.stats,
            "running": self.running,
            "uptime_seconds": (
                datetime.utcnow() - self.stats[self.workers[0].worker_id]["started_at"]
            ).total_seconds() if self.workers else 0
        }
    