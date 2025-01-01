import asyncio
import structlog
from typing import Optional

from taskforge.core.config import settings
from taskforge.core.logging import setup_logging
from taskforge.broker.rabbitmq import RabbitMQBroker
from taskforge.storage.postgres import PostgresJobStorage
from taskforge.worker.pool import WorkerPool
from taskforge.jobs.registry import registry

logger = structlog.get_logger()

async def run_worker_pool(
    num_workers: Optional[int] = None,
    prefetch_count: Optional[int] = None
) -> None:
    """Run the worker pool"""
    
    # Set up logging
    setup_logging()
    
    num_workers = num_workers or settings.worker_config.num_workers
    prefetch_count = prefetch_count or settings.worker_config.prefetch_count
    
    logger.info(
        "Starting TaskForge worker pool",
        num_workers=num_workers,
        prefetch_count=prefetch_count
    )
    
    # Set up components
    broker = RabbitMQBroker(
        connection_url=str(settings.rabbitmq_url),
        connection_pool_size=settings.broker_config.connection_pool_size
    )
    
    storage = PostgresJobStorage(
        connection_url=str(settings.postgres_url)
    )
    
    # Initialize storage
    await storage.initialize()
    
    # Create worker pool
    pool = WorkerPool(
        broker=broker,
        storage=storage,
        num_workers=num_workers,
        prefetch_count=prefetch_count
    )
    
    try:
        # Start worker pool with context manager
        async with pool.worker_context() as active_pool:
            # Wait for shutdown
            await active_pool._shutdown_event.wait()
            
    except Exception as e:
        logger.error("Error in worker pool", error=str(e), exc_info=True)
        raise
    finally:
        logger.info("Worker pool shutdown complete")

def main() -> None:
    """Main entry point"""
    try:
        asyncio.run(run_worker_pool())
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
    except Exception as e:
        logger.error("Fatal error", error=str(e), exc_info=True)
        raise

if __name__ == "__main__":
    main()
    