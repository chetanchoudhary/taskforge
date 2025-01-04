import asyncio
import time
from enum import Enum
from typing import Any, Callable, TypeVar

import structlog

from taskforge.exceptions import CircuitBreakerError

logger = structlog.get_logger()
T = TypeVar("T")


class CircuitState(str, Enum):
    """Circuit breaker states"""

    CLOSED = "closed"  # Normal operation
    OPEN = "open"  # Failing, reject requests
    HALF_OPEN = "half_open"  # Testing if service is back


class CircuitBreaker:
    """Circuit breaker pattern implementation"""

    def __init__(
        self,
        failure_threshold: int = 5,
        reset_timeout: float = 60.0,
        half_open_timeout: float = 5.0,
    ):
        self.failure_threshold = failure_threshold
        self.reset_timeout = reset_timeout
        self.half_open_timeout = half_open_timeout

        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.last_failure_time = 0.0
        self.lock = asyncio.Lock()

    async def call(self, func: Callable[..., T], *args: Any, **kwargs: Any) -> T:
        """Execute function with circuit breaker protection"""
        async with self.lock:
            if self.state == CircuitState.OPEN:
                if time.monotonic() - self.last_failure_time > self.reset_timeout:
                    # Try half-open state
                    self.state = CircuitState.HALF_OPEN
                else:
                    raise CircuitBreakerError("Circuit breaker is open")

        try:
            result = await func(*args, **kwargs)

            # Success, close circuit
            if self.state == CircuitState.HALF_OPEN:
                async with self.lock:
                    self.state = CircuitState.CLOSED
                    self.failure_count = 0

            return result

        except Exception as e:
            async with self.lock:
                self.failure_count += 1
                self.last_failure_time = time.monotonic()

                if (
                    self.state == CircuitState.CLOSED
                    and self.failure_count >= self.failure_threshold
                ):
                    # Too many failures, open circuit
                    self.state = CircuitState.OPEN
                    logger.warning(
                        "Circuit breaker opened",
                        failures=self.failure_count,
                        reset_timeout=self.reset_timeout,
                    )

                elif self.state == CircuitState.HALF_OPEN:
                    # Failed during testing, back to open
                    self.state = CircuitState.OPEN
                    logger.warning("Circuit breaker reopened after half-open failure")

            raise CircuitBreakerError(str(e))
