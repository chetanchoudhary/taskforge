import aioredis
import structlog
from dependency_injector import containers, providers
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from taskforge.broker.rabbitmq import RabbitMQBroker
from taskforge.core.config import settings
from taskforge.core.metrics import metrics
from taskforge.orchestrator import JobOrchestrator
from taskforge.scheduler.scheduler import JobScheduler
from taskforge.storage.postgres import PostgresJobStorage
from taskforge.workflow.engine import WorkflowEngine

logger = structlog.get_logger()


class Container(containers.DeclarativeContainer):
    """Dependency Injection Container"""

    # Configuration
    config = providers.Singleton(settings)

    # Logging
    logger = providers.Singleton(
        structlog.get_logger,
    )

    # Metrics
    metrics = providers.Singleton(metrics)

    # Redis
    redis = providers.Singleton(
        aioredis.from_url,
        str(config.provided.redis.url),
        max_connections=config.provided.redis.pool_size,
    )

    # Database
    database_engine = providers.Singleton(
        create_async_engine,
        str(config.provided.database.url),
        pool_size=config.provided.database.min_connections,
        max_overflow=config.provided.database.max_connections
        - config.provided.database.min_connections,
        pool_pre_ping=True,
        echo=config.provided.debug,
    )

    session_factory = providers.Factory(
        sessionmaker, database_engine, class_=AsyncSession, expire_on_commit=False
    )

    # Storage
    storage = providers.Singleton(
        PostgresJobStorage, engine=database_engine, session_factory=session_factory
    )

    # Message Broker
    broker = providers.Singleton(
        RabbitMQBroker,
        connection_url=config.provided.broker.url,
        connection_pool_size=config.provided.broker.connection_pool_size,
    )

    # Job Orchestrator
    orchestrator = providers.Singleton(JobOrchestrator, broker=broker, storage=storage)

    # Workflow Engine
    workflow_engine = providers.Singleton(
        WorkflowEngine, orchestrator=orchestrator, storage=storage
    )

    # Job Scheduler
    scheduler = providers.Singleton(JobScheduler, orchestrator=orchestrator)

    # Wire up dependencies for FastAPI
    wiring_config = containers.WiringConfiguration(
        packages=["taskforge.api", "taskforge.worker", "taskforge.jobs"]
    )

    @classmethod
    async def init(cls) -> "Container":
        """Initialize the container and its components"""
        container = cls()

        try:
            # Initialize storage
            await container.storage().initialize()

            # Initialize broker
            await container.broker().connect()

            # Test redis connection
            redis = container.redis()
            await redis.ping()

            return container

        except Exception as e:
            logger.error("Failed to initialize container", error=str(e))
            raise ContainerInitError(f"Container initialization failed: {str(e)}")

    async def cleanup(self):
        """Cleanup container resources"""
        try:
            # Close broker connections
            await self.broker().close()

            # Close redis connection
            redis = self.redis()
            await redis.close()

            # Close database engine
            engine = self.database_engine()
            await engine.dispose()

        except Exception as e:
            logger.error("Error cleaning up container", error=str(e))
            raise ContainerCleanupError(f"Container cleanup failed: {str(e)}")


class ContainerInitError(Exception):
    """Raised when container initialization fails"""

    pass


class ContainerCleanupError(Exception):
    """Raised when container cleanup fails"""

    pass


# Global container instance
container = Container()
