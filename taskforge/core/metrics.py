import structlog
from prometheus_client import Counter, Gauge, Histogram

logger = structlog.get_logger()


class TaskForgeMetrics:
    """Central metrics registry"""

    def __init__(self):
        # Job Metrics
        self.jobs_processed = Counter(
            "taskforge_jobs_processed_total",
            "Total number of processed jobs",
            ["job_type", "status"],
        )

        self.job_processing_time = Histogram(
            "taskforge_job_processing_seconds",
            "Time spent processing jobs",
            ["job_type"],
            buckets=(0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0),
        )

        self.active_jobs = Gauge(
            "taskforge_active_jobs",
            "Number of currently active jobs",
            ["job_type", "worker_id"],
        )

        self.job_retries = Counter(
            "taskforge_job_retries_total", "Number of job retries", ["job_type"]
        )

        # Queue Metrics
        self.queue_depth = Gauge(
            "taskforge_queue_depth", "Number of jobs in queue", ["queue_name"]
        )

        self.queue_latency = Histogram(
            "taskforge_queue_latency_seconds",
            "Time jobs spend in queue before processing",
            ["queue_name"],
        )

        # Worker Metrics
        self.worker_uptime = Counter(
            "taskforge_worker_uptime_seconds_total",
            "Total worker uptime in seconds",
            ["worker_id"],
        )

        self.worker_errors = Counter(
            "taskforge_worker_errors_total",
            "Number of worker errors",
            ["worker_id", "error_type"],
        )

        self.worker_cpu_usage = Gauge(
            "taskforge_worker_cpu_usage_percent",
            "Worker CPU usage percentage",
            ["worker_id"],
        )

        self.worker_memory_usage = Gauge(
            "taskforge_worker_memory_usage_bytes",
            "Worker memory usage in bytes",
            ["worker_id"],
        )

        # Resource Metrics
        self.memory_usage = Gauge(
            "taskforge_memory_usage_bytes",
            "Current memory usage in bytes",
            ["worker_id"],
        )

        self.cpu_usage = Gauge(
            "taskforge_cpu_usage_percent", "Current CPU usage percentage", ["worker_id"]
        )

        # API Metrics
        self.api_requests = Counter(
            "taskforge_api_requests_total",
            "Total number of API requests",
            ["method", "endpoint", "status"],
        )

        self.api_request_duration = Histogram(
            "taskforge_api_request_duration_seconds",
            "API request duration in seconds",
            ["method", "endpoint"],
        )
        # Integration metrics
        self.webhook_requests = Counter(
            "taskforge_webhook_requests_total",
            "Total webhook requests sent",
            ["name", "event_type", "status"],
        )

        self.webhook_request_duration = Histogram(
            "taskforge_webhook_request_duration_seconds",
            "Webhook request duration",
            ["name"],
        )

        self.external_service_requests = Counter(
            "taskforge_external_service_requests_total",
            "Total external service requests",
            ["service", "method", "status"],
        )

        self.external_service_duration = Histogram(
            "taskforge_external_service_duration_seconds",
            "External service request duration",
            ["service", "method"],
        )

        self.event_processing = Counter(
            "taskforge_events_processed_total",
            "Total events processed",
            ["type", "status"],
        )

        self.event_handler_errors = Counter(
            "taskforge_event_handler_errors_total",
            "Total event handler errors",
            ["type"],
        )

        self.event_processing_duration = Histogram(
            "taskforge_event_processing_duration_seconds",
            "Event processing duration",
            ["type"],
        )

    def record_job_processed(self, job_type: str, status: str) -> None:
        """Record a processed job"""
        self.jobs_processed.labels(job_type=job_type, status=status).inc()

    def record_job_duration(self, job_type: str, duration: float) -> None:
        """Record job processing duration"""
        self.job_processing_time.labels(job_type=job_type).observe(duration)

    def set_queue_depth(self, queue_name: str, depth: int) -> None:
        """Update queue depth"""
        self.queue_depth.labels(queue_name=queue_name).set(depth)

    def update_worker_stats(
        self, worker_id: str, memory_bytes: int, cpu_percent: float
    ) -> None:
        """Update worker resource usage stats"""
        self.memory_usage.labels(worker_id=worker_id).set(memory_bytes)
        self.cpu_usage.labels(worker_id=worker_id).set(cpu_percent)

    def record_api_request(
        self, method: str, endpoint: str, status: int, duration: float
    ) -> None:
        """Record API request metrics"""
        self.api_requests.labels(method=method, endpoint=endpoint, status=status).inc()

        self.api_request_duration.labels(method=method, endpoint=endpoint).observe(
            duration
        )


# Global metrics instance
metrics = TaskForgeMetrics()
