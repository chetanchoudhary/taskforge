import asyncio
from datetime import datetime, timezone
from typing import Awaitable, Callable, Optional, TypeVar

import structlog
from fastapi.concurrency import asynccontextmanager

from taskforge.exceptions import ConnectionError

logger = structlog.get_logger()
T = TypeVar("T")


class ConnectionManager:
    """Manages connections with retry logic"""

    def __init__(
        self,
        connect_func: Callable[[], Awaitable[T]],
        disconnect_func: Callable[[T], Awaitable[None]],
        ping_func: Optional[Callable[[T], Awaitable[bool]]] = None,
        max_retries: int = 3,
        retry_delay: float = 1.0,
        ping_interval: float = 30.0,
        name: str = "connection",
    ):
        self.connect_func = connect_func
        self.disconnect_func = disconnect_func
        self.ping_func = ping_func
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.ping_interval = ping_interval
        self.name = name

        self.connection: Optional[T] = None
        self.last_connected: Optional[datetime] = None
        self._monitoring_task: Optional[asyncio.Task] = None
        self._lock = asyncio.Lock()
        self.logger = logger.bind(connection=name)

    async def connect(self) -> T:
        """Establish connection with retry logic"""
        async with self._lock:
            if self.connection:
                return self.connection

            for attempt in range(self.max_retries):
                try:
                    self.connection = await self.connect_func()
                    self.last_connected = datetime.now(timezone.utc)

                    # Start monitoring if ping function provided
                    if self.ping_func:
                        self._monitoring_task = asyncio.create_task(
                            self._monitor_connection()
                        )

                    self.logger.info("Connection established")
                    return self.connection

                except Exception as e:
                    if attempt == self.max_retries - 1:
                        raise ConnectionError(
                            self.name,
                            f"Failed to connect after {self.max_retries} attempts: {str(e)}",
                        )

                    self.logger.warning(
                        "Connection attempt failed",
                        attempt=attempt + 1,
                        max_retries=self.max_retries,
                        error=str(e),
                    )
                    await asyncio.sleep(self.retry_delay * (2**attempt))

    async def disconnect(self):
        """Close connection"""
        async with self._lock:
            if self.connection:
                if self._monitoring_task:
                    self._monitoring_task.cancel()
                    try:
                        await self._monitoring_task
                    except asyncio.CancelledError:
                        pass
                    self._monitoring_task = None

                try:
                    await self.disconnect_func(self.connection)
                except Exception as e:
                    self.logger.error("Error during disconnect", error=str(e))

                self.connection = None
                self.last_connected = None
                self.logger.info("Connection closed")

    async def _monitor_connection(self):
        """Monitor connection health"""
        while True:
            try:
                if self.connection and self.ping_func:
                    if not await self.ping_func(self.connection):
                        self.logger.warning("Connection health check failed")
                        await self.reconnect()
            except Exception as e:
                self.logger.error("Error monitoring connection", error=str(e))

            await asyncio.sleep(self.ping_interval)

    async def reconnect(self):
        """Reconnect to service"""
        try:
            await self.disconnect()
        except Exception as e:
            self.logger.warning("Error during disconnect for reconnect", error=str(e))

        await self.connect()

    @property
    def is_connected(self) -> bool:
        """Check if currently connected"""
        return self.connection is not None

    async def __aenter__(self) -> T:
        """Async context manager entry"""
        return await self.connect()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.disconnect()


class ConnectionPool:
    """Connection pool manager"""

    def __init__(
        self,
        create_func: Callable[[], Awaitable[T]],
        close_func: Callable[[T], Awaitable[None]],
        min_size: int = 1,
        max_size: int = 10,
        name: str = "pool",
    ):
        self.create_func = create_func
        self.close_func = close_func
        self.min_size = min_size
        self.max_size = max_size
        self.name = name

        self._pool: asyncio.Queue[T] = asyncio.Queue()
        self._size = 0
        self._lock = asyncio.Lock()
        self.logger = logger.bind(pool=name)

    async def initialize(self):
        """Initialize minimum connections"""
        async with self._lock:
            for _ in range(self.min_size):
                conn = await self.create_func()
                await self._pool.put(conn)
                self._size += 1

    async def acquire(self) -> T:
        """Acquire a connection from the pool"""
        async with self._lock:
            if self._pool.empty() and self._size < self.max_size:
                # Create new connection if pool empty and under max size
                conn = await self.create_func()
                self._size += 1
                return conn

        # Wait for available connection
        return await self._pool.get()

    async def release(self, conn: T):
        """Release a connection back to the pool"""
        await self._pool.put(conn)

    async def close(self):
        """Close all connections"""
        while not self._pool.empty():
            conn = await self._pool.get()
            await self.close_func(conn)
            self._size -= 1

    @asynccontextmanager
    async def connection(self) -> T:
        """Context manager for connection acquisition"""
        conn = await self.acquire()
        try:
            yield conn
        finally:
            await self.release(conn)
