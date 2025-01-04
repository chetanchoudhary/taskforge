# taskforge/utils/logging.py
import asyncio
import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import structlog
from opentelemetry import trace
from prometheus_client import Counter, Histogram

from taskforge.broker.rabbitmq import RabbitMQBroker
from taskforge.core.config import settings

# Metrics
log_events = Counter(
    "taskforge_log_events_total", "Total number of log events", ["level"]
)

log_processing_time = Histogram(
    "taskforge_log_processing_seconds", "Time spent processing log events", ["handler"]
)


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
    """Base class for asynchronous log handlers"""

    def __init__(self, batch_size: int = 100, flush_interval: float = 1.0):
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.buffer: List[Dict[str, Any]] = []
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

    async def _process_batch(self, batch: List[Dict[str, Any]]):
        """Process a batch of log events - override in subclasses"""
        raise NotImplementedError


class FileLogHandler(AsyncLogHandler):
    """Log handler that writes to file"""

    def __init__(self, filename: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.filename = filename

    async def _process_batch(self, batch: List[Dict[str, Any]]):
        with open(self.filename, "a") as f:
            for event in batch:
                f.write(json.dumps(event) + "\n")


class QueueLogHandler(AsyncLogHandler):
    """Log handler that sends to message queue"""

    def __init__(self, broker: RabbitMQBroker, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.broker = broker

    async def _process_batch(self, batch: List[Dict[str, Any]]):
        # Group by log level
        grouped = {}
        for event in batch:
            level = event.get("level", "INFO")
            if level not in grouped:
                grouped[level] = []
            grouped[level].append(event)

        # Send to appropriate queues
        for level, events in grouped.items():
            await self.broker.publish_batch(f"logs.{level.lower()}", events)


# class ElasticsearchLogHandler(AsyncLogHandler):
#     """Log handler that sends to Elasticsearch"""

#     def __init__(self,
#                  es_client: "AsyncElasticsearch",
#                  index_prefix: str = "taskforge-logs",
#                  *args, **kwargs):
#         super().__init__(*args, **kwargs)
#         self.es_client = es_client
#         self.index_prefix = index_prefix

#     async def _process_batch(self, batch: List[Dict[str, Any]]):
#         # Prepare bulk request
#         bulk_data = []
#         for event in batch:
#             # Create daily index
#             index = f"{self.index_prefix}-{datetime.now(timezone.utc):%Y.%m.%d}"
#             bulk_data.extend([
#                 {"index": {"_index": index}},
#                 event
#             ])

#         if bulk_data:
#             await self.es_client.bulk(body=bulk_data)


def setup_logging():
    """Configure structured logging"""

    # Base processors
    processors = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.processors.TimeStamper(fmt="iso"),
        _add_trace_context,
    ]

    # Environment-specific processors
    if settings.environment == "development":
        processors.extend([structlog.dev.ConsoleRenderer(colors=True)])
    else:
        processors.extend(
            [structlog.processors.format_exc_info, structlog.processors.JSONRenderer()]
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
        format="%(message)s", stream=sys.stdout, level=settings.log_level.upper()
    )


def _add_trace_context(logger, name, event_dict):
    """Add trace context to log events"""
    current_span = trace.get_current_span()
    if current_span:
        context = current_span.get_span_context()
        if context.trace_id:
            event_dict["trace_id"] = format(context.trace_id, "032x")
        if context.span_id:
            event_dict["span_id"] = format(context.span_id, "016x")
    return event_dict


# Create handlers based on configuration
def create_handlers(config) -> List[AsyncLogHandler]:
    """Create log handlers based on configuration"""
    handlers = []

    if config.file.enabled:
        handlers.append(
            FileLogHandler(filename=config.file.path, batch_size=config.batch_size)
        )

    if config.queue.enabled:
        handlers.append(
            QueueLogHandler(broker=config.broker, batch_size=config.batch_size)
        )

    # if config.elasticsearch.enabled:
    #     handlers.append(ElasticsearchLogHandler(
    #         es_client=config.elasticsearch,
    #         batch_size=config.batch_size
    #     ))

    return handlers


# Global logger instance
logger = structlog.get_logger()


# Utility functions
async def log_event(level: str, message: str, context: Optional[Dict[str, Any]] = None):
    """Log an event with metrics"""
    start_time = datetime.now(timezone.utc)

    event = {
        "level": level,
        "message": message,
        "timestamp": start_time.isoformat(),
        **(context or {}),
    }

    # Update metrics
    log_events.labels(level=level).inc()

    # Process through handlers
    handlers = create_handlers(settings.logging)
    tasks = [handler.handle(event) for handler in handlers]

    if tasks:
        await asyncio.gather(*tasks)

    # Record processing time
    duration = (datetime.now(timezone.utc) - start_time).total_seconds()
    log_processing_time.observe(duration)
