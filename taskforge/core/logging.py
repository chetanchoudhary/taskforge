import asyncio
import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any, Dict

import structlog
from opentelemetry import trace

from taskforge.core.config import settings


def setup_logging() -> None:
    """Configure structured logging"""

    # Base processors
    processors = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        add_trace_context,
    ]

    # Environment-specific processors
    if settings.environment == "development":
        processors.extend(
            [
                structlog.dev.ConsoleRenderer(
                    colors=True, exception_formatter=structlog.dev.exception_formatter
                )
            ]
        )
    else:
        processors.extend(
            [
                structlog.processors.format_exc_info,
                structlog.processors.JSONRenderer(
                    serializer=lambda obj: json.dumps(obj, default=str)
                ),
            ]
        )

    structlog.configure(
        processors=processors,
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        wrapper_class=structlog.BoundLogger,
        cache_logger_on_first_use=True,
    )

    # Configure standard logging
    logging.basicConfig(
        format=settings.logging.format, level=settings.logging.level, stream=sys.stdout
    )


def add_trace_context(logger, name, event_dict: Dict[str, Any]) -> Dict[str, Any]:
    """Add trace context to log events"""
    current_span = trace.get_current_span()
    if current_span:
        context = current_span.get_span_context()
        if context.trace_id:
            event_dict["trace_id"] = format(context.trace_id, "032x")
        if context.span_id:
            event_dict["span_id"] = format(context.span_id, "016x")
    return event_dict


class LogContext:
    """Context manager for temporary log context"""

    def __init__(self, **kwargs):
        self.bind = kwargs
        self.previous = {}
        self.logger = structlog.get_logger()

    def __enter__(self):
        self.previous = self.logger._context.copy()
        self.logger._context.update(self.bind)
        return self.logger

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.logger._context = self.previous


class AsyncLogHandler:
    """Asynchronous log handler with buffering"""

    def __init__(self, batch_size: int = 100, flush_interval: float = 1.0):
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.buffer = []
        self.last_flush = datetime.now(timezone.utc)
        self.lock = asyncio.Lock()
        self.logger = structlog.get_logger()

    async def handle(self, log_event: Dict[str, Any]):
        """Handle a log event"""
        async with self.lock:
            self.buffer.append(log_event)

            should_flush = (
                len(self.buffer) >= self.batch_size
                or (datetime.now(timezone.utc) - self.last_flush).total_seconds()
                >= self.flush_interval
            )

            if should_flush:
                await self.flush()

    async def flush(self):
        """Flush buffered log events"""
        if not self.buffer:
            return

        async with self.lock:
            try:
                await self._process_batch(self.buffer)
                self.buffer = []
                self.last_flush = datetime.now(timezone.utc)
            except Exception as e:
                self.logger.error(
                    "Error flushing log buffer",
                    error=str(e),
                    buffer_size=len(self.buffer),
                )
