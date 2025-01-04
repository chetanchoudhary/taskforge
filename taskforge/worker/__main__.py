import asyncio
import signal
import sys
from typing import Optional

import psutil
import structlog

from taskforge.core import metrics
from taskforge.core.config import settings
from taskforge.core.logging import setup_logging
from taskforge.di import Container
from taskforge.worker.autoscaler import WorkerAutoscaler
from taskforge.worker.pool import WorkerPool

logger = structlog.get_logger()


async def run_worker_pool(
    num_workers: Optional[int] = None,
    prefetch_count: Optional[int] = None,
    enable_autoscaling: bool = True,
) -> None:
    """Run the worker pool"""
    try:
        # Set up logging
        setup_logging()

        # Initialize container and components
        container = await Container.init()
        broker = container.broker()
        storage = container.storage()

        num_workers = num_workers or settings.worker.count
        prefetch_count = prefetch_count or settings.worker.prefetch_count

        logger.info(
            "Starting TaskForge worker pool",
            num_workers=num_workers,
            prefetch_count=prefetch_count,
            autoscaling=enable_autoscaling,
        )

        # Create worker pool
        pool = WorkerPool(
            broker=broker,
            storage=storage,
            num_workers=num_workers,
            prefetch_count=prefetch_count,
            monitor_interval=settings.worker.monitor_interval,
        )

        # Create autoscaler if enabled
        autoscaler = None
        if enable_autoscaling:
            autoscaler = WorkerAutoscaler(
                worker_pool=pool,
                min_workers=max(1, num_workers // 2),
                max_workers=num_workers * 2,
                scale_up_threshold=0.8,
                scale_down_threshold=0.3,
            )

        # Set up signal handlers
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(
                sig,
                lambda s=sig: asyncio.create_task(handle_shutdown(s, pool, autoscaler)),
            )

        try:
            # Start worker pool
            async with pool.worker_context():
                if autoscaler:
                    await autoscaler.start()

                # Monitor system resources
                await monitor_resources(pool)

                # Wait for shutdown
                await pool._shutdown_event.wait()

        except Exception as e:
            logger.error("Error in worker pool", error=str(e), exc_info=True)
            raise

    finally:
        if autoscaler:
            await autoscaler.stop()
        await container.cleanup()
        logger.info("Worker pool shutdown complete")


async def handle_shutdown(
    sig: signal.Signals, pool: WorkerPool, autoscaler: Optional[WorkerAutoscaler] = None
):
    """Handle shutdown signals"""
    logger.info(f"Received signal {sig.name}")

    if autoscaler:
        await autoscaler.stop()

    await pool.shutdown()
    pool._shutdown_event.set()


async def monitor_resources(pool: WorkerPool):
    """Monitor system resource usage"""
    process = psutil.Process()

    while pool.running:
        try:
            # CPU usage
            cpu_percent = process.cpu_percent(interval=1)
            if cpu_percent > settings.worker.cpu_limit * 100:
                logger.warning(
                    "High CPU usage detected",
                    cpu_percent=cpu_percent,
                    limit=settings.worker.cpu_limit * 100,
                )

            # Memory usage
            memory_info = process.memory_info()
            memory_mb = memory_info.rss / (1024 * 1024)
            if memory_mb > settings.worker.max_memory_mb:
                logger.warning(
                    "High memory usage detected",
                    memory_mb=memory_mb,
                    limit=settings.worker.max_memory_mb,
                )

            # Update metrics
            metrics.worker_cpu_usage.labels(worker_id="pool").set(cpu_percent)
            metrics.worker_memory_usage.labels(worker_id="pool").set(memory_info.rss)

        except Exception as e:
            logger.error("Error monitoring resources", error=str(e))

        await asyncio.sleep(60)


def main():
    """Main entry point"""
    try:
        asyncio.run(run_worker_pool())
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
    except Exception as e:
        logger.error("Fatal error", error=str(e), exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
