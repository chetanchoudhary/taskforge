import asyncio
import re
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict, List, Optional, Pattern
from uuid import uuid4

import structlog
from pydantic import BaseModel

from taskforge.core.metrics import metrics

logger = structlog.get_logger()

EventHandler = Callable[["Event"], Awaitable[None]]


class EventMetadata(BaseModel):
    """Event metadata"""

    event_id: str
    timestamp: datetime
    source: str
    correlation_id: Optional[str] = None
    causation_id: Optional[str] = None
    trace_id: Optional[str] = None


class Event(BaseModel):
    """Base event model"""

    type: str
    data: Dict[str, Any]
    metadata: EventMetadata

    def add_correlation(self, correlation_id: str):
        """Add correlation ID to event"""
        self.metadata.correlation_id = correlation_id
        return self

    def add_causation(self, causation_id: str):
        """Add causation ID to event"""
        self.metadata.causation_id = causation_id
        return self


class EventSubscription:
    """Event subscription definition"""

    def __init__(
        self,
        pattern: str,
        handler: EventHandler,
        filter_: Optional[Dict[str, Any]] = None,
    ):
        self.pattern = self._compile_pattern(pattern)
        self.handler = handler
        self.filter = filter_

    def _compile_pattern(self, pattern: str) -> Pattern:
        """Compile event pattern to regex"""
        # Convert glob-style pattern to regex
        regex = pattern.replace(".", "\\.").replace("*", ".*")
        return re.compile(f"^{regex}$")

    def matches(self, event_type: str) -> bool:
        """Check if event type matches pattern"""
        return bool(self.pattern.match(event_type))

    def matches_filter(self, event_data: Dict[str, Any]) -> bool:
        """Check if event data matches filter"""
        if not self.filter:
            return True

        return all(
            key in event_data and event_data[key] == value
            for key, value in self.filter.items()
        )


class EventBus:
    """Event pub/sub system"""

    def __init__(self):
        self.subscriptions: List[EventSubscription] = []
        self.queue: asyncio.Queue[Event] = asyncio.Queue()
        self._running = False
        self._process_task: Optional[asyncio.Task] = None
        self._handlers: Dict[str, List[EventHandler]] = {}
        self.logger = logger.bind(component="EventBus")

    async def start(self):
        """Start event processing"""
        if self._running:
            return

        self._running = True
        self._process_task = asyncio.create_task(self._process_events())
        self.logger.info("Event bus started")

    async def stop(self):
        """Stop event processing"""
        if not self._running:
            return

        self._running = False
        if self._process_task:
            self._process_task.cancel()
            try:
                await self._process_task
            except asyncio.CancelledError:
                pass

        self.logger.info("Event bus stopped")

    def subscribe(
        self,
        pattern: str,
        handler: EventHandler,
        filter_: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Subscribe to events"""
        subscription = EventSubscription(pattern, handler, filter_)
        self.subscriptions.append(subscription)
        self.logger.info("Event subscription added", pattern=pattern, filter=filter_)

    async def publish(
        self,
        event_type: str,
        data: Dict[str, Any],
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Publish an event"""
        event = Event(
            type=event_type,
            data=data,
            metadata=EventMetadata(
                event_id=str(uuid4()),
                timestamp=datetime.now(timezone.utc),
                source="taskforge",
                **(metadata or {}),
            ),
        )

        await self.queue.put(event)

        # Update metrics
        metrics.events_processed.labels(type=event_type).inc()

        self.logger.debug(
            "Event published", event_type=event_type, event_id=event.metadata.event_id
        )

    async def _process_events(self):
        """Process events from queue"""
        while self._running:
            try:
                event = await self.queue.get()
                start_time = datetime.now(timezone.utc)

                await self._dispatch_event(event)

                # Update metrics
                duration = (datetime.now(timezone.utc) - start_time).total_seconds()
                metrics.event_processing_duration.labels(type=event.type).observe(
                    duration
                )

            except Exception as e:
                self.logger.error("Error processing event", error=str(e), exc_info=True)
            finally:
                self.queue.task_done()

    async def _dispatch_event(self, event: Event):
        """Dispatch event to matching handlers"""
        tasks = []

        for subscription in self.subscriptions:
            if subscription.matches(event.type) and subscription.matches_filter(
                event.data
            ):
                task = asyncio.create_task(subscription.handler(event))
                tasks.append(task)

        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Handle any errors
            for result in results:
                if isinstance(result, Exception):
                    self.logger.error(
                        "Event handler error",
                        error=str(result),
                        event_type=event.type,
                        event_id=event.metadata.event_id,
                    )
                    metrics.event_handler_errors.labels(type=event.type).inc()


# Global event bus instance
event_bus = EventBus()
