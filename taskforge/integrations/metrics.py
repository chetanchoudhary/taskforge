import time
from typing import Any, Dict

import structlog
from prometheus_client import Counter, Histogram

logger = structlog.get_logger()

# Integration metrics
webhook_requests = Counter(
    "taskforge_webhook_requests_total",
    "Total webhook requests sent",
    ["name", "event_type", "status"],
)

webhook_request_duration = Histogram(
    "taskforge_webhook_request_duration_seconds", "Webhook request duration", ["name"]
)

external_service_requests = Counter(
    "taskforge_external_service_requests_total",
    "Total external service requests",
    ["service", "method", "status"],
)

external_service_duration = Histogram(
    "taskforge_external_service_duration_seconds",
    "External service request duration",
    ["service", "method"],
)

event_processing = Counter(
    "taskforge_events_processed_total", "Total events processed", ["type", "status"]
)

event_handler_errors = Counter(
    "taskforge_event_handler_errors_total", "Total event handler errors", ["type"]
)

event_processing_duration = Histogram(
    "taskforge_event_processing_duration_seconds", "Event processing duration", ["type"]
)


class MetricsCollector:
    """Collects and exposes integration metrics"""

    @staticmethod
    def record_webhook_request(
        name: str, event_type: str, success: bool, duration: float
    ):
        """Record webhook request metrics"""
        status = "success" if success else "failure"
        webhook_requests.labels(name=name, event_type=event_type, status=status).inc()
        webhook_request_duration.labels(name=name).observe(duration)

    @staticmethod
    def record_service_request(
        service: str, method: str, success: bool, duration: float
    ):
        """Record external service request metrics"""
        status = "success" if success else "failure"
        external_service_requests.labels(
            service=service, method=method, status=status
        ).inc()
        external_service_duration.labels(service=service, method=method).observe(
            duration
        )

    @staticmethod
    def record_event_processing(event_type: str, success: bool, duration: float):
        """Record event processing metrics"""
        status = "success" if success else "failure"
        event_processing.labels(type=event_type, status=status).inc()
        event_processing_duration.labels(type=event_type).observe(duration)

    def export_metrics(self) -> Dict[str, Any]:
        """Export current metrics"""
        return {
            "webhooks": {
                "total_requests": webhook_requests._value.sum(),
                "success_rate": self._calculate_success_rate(webhook_requests),
                "average_duration": self._calculate_average(webhook_request_duration),
            },
            "external_services": {
                "total_requests": external_service_requests._value.sum(),
                "success_rate": self._calculate_success_rate(external_service_requests),
                "average_duration": self._calculate_average(external_service_duration),
            },
            "events": {
                "total_processed": event_processing._value.sum(),
                "error_count": event_handler_errors._value.sum(),
                "average_duration": self._calculate_average(event_processing_duration),
            },
        }

    def _calculate_success_rate(self, counter: Counter) -> float:
        """Calculate success rate from counter with status label"""
        total = 0
        successes = 0
        for labels, value in counter._metrics.items():
            total += value
            if labels[2] == "success":  # Assuming status is the third label
                successes += value
        return (successes / total) * 100 if total > 0 else 0

    def _calculate_average(self, histogram: Histogram) -> float:
        """Calculate average from histogram"""
        if histogram._count.get() == 0:
            return 0
        return histogram._sum.get() / histogram._count.get()


# Global metrics collector instance
metrics_collector = MetricsCollector()


# Integration metrics recorder middleware
async def record_integration_metrics(request, call_next):
    """Middleware to record integration metrics"""
    start_time = time.monotonic()

    try:
        response = await call_next(request)
        duration = time.monotonic() - start_time

        if "webhook" in request.url.path:
            metrics_collector.record_webhook_request(
                name=request.path_params.get("name", "unknown"),
                event_type=request.headers.get("X-TaskForge-Event", "unknown"),
                success=response.status_code < 400,
                duration=duration,
            )
        elif "external" in request.url.path:
            metrics_collector.record_service_request(
                service=request.path_params.get("service", "unknown"),
                method=request.method,
                success=response.status_code < 400,
                duration=duration,
            )

        return response

    except Exception as e:
        # Log the exception for debugging purposes
        logger.warning("Error processing request", error=str(e), exc_info=True)
        duration = time.monotonic() - start_time

        if "webhook" in request.url.path:
            metrics_collector.record_webhook_request(
                name=request.path_params.get("name", "unknown"),
                event_type=request.headers.get("X-TaskForge-Event", "unknown"),
                success=False,
                duration=duration,
            )
        elif "external" in request.url.path:
            metrics_collector.record_service_request(
                service=request.path_params.get("service", "unknown"),
                method=request.method,
                success=False,
                duration=duration,
            )

        raise
