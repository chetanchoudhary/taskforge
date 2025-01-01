from prometheus_client import Counter, Histogram, Gauge, Summary
from typing import Dict, Optional

class Metrics:
    """Central metrics registry for TaskForge"""
    
    def __init__(self):
        # Job processing metrics
        self.jobs_processed = Counter(
            'taskforge_jobs_processed_total',
            'Total number of processed jobs',
            ['job_type', 'status']
        )
        
        self.job_processing_time = Histogram(
            'taskforge_job_processing_seconds',
            'Time spent processing jobs',
            ['job_type'],
            buckets=(0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0)
        )
        
        self.active_jobs = Gauge(
            'taskforge_active_jobs',
            'Number of currently active jobs',
            ['job_type', 'worker_id']
        )
        
        # Queue metrics
        self.queue_depth = Gauge(
            'taskforge_queue_depth',
            'Number of jobs in queue',
            ['queue_name']
        )
        
        self.queue_latency = Histogram(
            'taskforge_queue_latency_seconds',
            'Time jobs spend in queue before processing',
            ['queue_name']
        )
        
        # Worker metrics
        self.worker_uptime = Counter(
            'taskforge_worker_uptime_seconds_total',
            'Total worker uptime in seconds',
            ['worker_id']
        )
        
        self.worker_errors = Counter(
            'taskforge_worker_errors_total',
            'Number of worker errors',
            ['worker_id', 'error_type']
        )
        
        # Memory and resource metrics
        self.memory_usage = Gauge(
            'taskforge_memory_usage_bytes',
            'Current memory usage in bytes',
            ['worker_id']
        )
        
        # Job-specific metrics
        self.job_retries = Counter(
            'taskforge_job_retries_total',
            'Number of job retries',
            ['job_type']
        )
        
        self.job_timeout = Counter(
            'taskforge_job_timeouts_total',
            'Number of job timeouts',
            ['job_type']
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
    
    def record_queue_latency(self, queue_name: str, latency: float) -> None:
        """Record queue latency"""
        self.queue_latency.labels(queue_name=queue_name).observe(latency)
    
    def record_worker_error(self, worker_id: str, error_type: str) -> None:
        """Record worker error"""
        self.worker_errors.labels(worker_id=worker_id, error_type=error_type).inc()
    
    def update_active_jobs(self, job_type: str, worker_id: str, count: int) -> None:
        """Update number of active jobs"""
        self.active_jobs.labels(job_type=job_type, worker_id=worker_id).set(count)
    
    def update_memory_usage(self, worker_id: str, bytes_used: int) -> None:
        """Update memory usage"""
        self.memory_usage.labels(worker_id=worker_id).set(bytes_used)

# Global metrics instance
metrics = Metrics()
