import asyncio
import functools
import time
from typing import Any, Callable, Optional, TypeVar

import structlog
from opentelemetry import trace

from taskforge.utils.circuit_breaker import CircuitBreaker

logger = structlog.get_logger()
T = TypeVar("T")


def retry(
    max_attempts: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
    exceptions: tuple = (Exception,),
):
    """Retry decorator with exponential backoff"""

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> T:
            last_exception = None

            for attempt in range(max_attempts):
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt < max_attempts - 1:
                        sleep_time = delay * (backoff**attempt)
                        logger.warning(
                            f"Retrying {func.__name__}",
                            attempt=attempt + 1,
                            delay=sleep_time,
                            error=str(e),
                        )
                        await asyncio.sleep(sleep_time)

            raise last_exception

        return wrapper

    return decorator


def traced(name: Optional[str] = None):
    """OpenTelemetry tracing decorator"""

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> T:
            span_name = name or func.__name__
            tracer = trace.get_tracer(__name__)

            with tracer.start_as_current_span(span_name) as span:
                # Add function arguments to span
                span.set_attribute("function", func.__name__)

                # Add relevant kwargs as span attributes
                safe_kwargs = {
                    k: str(v)
                    for k, v in kwargs.items()
                    if not isinstance(v, (bytes, bytearray))
                }
                span.set_attributes(safe_kwargs)

                try:
                    result = await func(*args, **kwargs)
                    return result
                except Exception as e:
                    span.set_attribute("error", str(e))
                    span.set_attribute("error.type", e.__class__.__name__)
                    raise

        return wrapper

    return decorator


def metered(name: Optional[str] = None):
    """Prometheus metrics decorator"""

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> T:
            metric_name = name or func.__name__

            # Create metrics if they don't exist
            if not hasattr(wrapper, "calls"):
                from prometheus_client import Counter, Histogram

                wrapper.calls = Counter(
                    f"taskforge_{metric_name}_total",
                    f"Total calls to {metric_name}",
                    ["status"],
                )
                wrapper.latency = Histogram(
                    f"taskforge_{metric_name}_seconds",
                    f"Latency of {metric_name}",
                    buckets=(0.1, 0.5, 1.0, 2.0, 5.0, 10.0),
                )

            start_time = time.monotonic()
            try:
                result = await func(*args, **kwargs)
                wrapper.calls.labels(status="success").inc()
                return result
            except Exception as e:
                print(e)
                wrapper.calls.labels(status="error").inc()
                raise
            finally:
                duration = time.monotonic() - start_time
                wrapper.latency.observe(duration)

        return wrapper

    return decorator


def async_timed():
    """Timing decorator for async functions"""

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> T:
            start_time = time.monotonic()
            try:
                return await func(*args, **kwargs)
            finally:
                duration = time.monotonic() - start_time
                logger.debug(
                    f"{func.__name__} execution time", duration=duration, unit="seconds"
                )

        return wrapper

    return decorator


def with_circuit_breaker(circuit: CircuitBreaker):
    """Circuit breaker decorator"""

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> T:
            return await circuit.call(func, *args, **kwargs)

        return wrapper

    return decorator


def validate_input(model):
    """Input validation decorator using Pydantic"""

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> T:
            # Validate input data
            input_data = kwargs.get("input_data", {})
            validated_data = model(**input_data)
            kwargs["input_data"] = validated_data.model_dump()
            return await func(*args, **kwargs)

        return wrapper

    return decorator
