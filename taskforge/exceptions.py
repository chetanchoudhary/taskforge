# taskforge/exceptions.py

from datetime import datetime, timezone
from typing import Any, Dict, Optional


class TaskForgeError(Exception):
    """Base exception for all TaskForge errors"""

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        self.message = message
        self.details = details or {}
        self.timestamp = datetime.now(timezone.utc)
        super().__init__(message)

    def to_dict(self) -> Dict[str, Any]:
        """Convert error to dictionary format"""
        return {
            "error": self.__class__.__name__,
            "message": self.message,
            "details": self.details,
            "timestamp": self.timestamp.isoformat(),
        }


class ConfigurationError(TaskForgeError):
    """Raised when there's a configuration-related error"""

    pass


# Connection Errors
class ConnectionError(TaskForgeError):
    """Base class for connection-related errors"""

    def __init__(
        self, service: str, message: str, details: Optional[Dict[str, Any]] = None
    ):
        self.service = service
        super().__init__(f"{service} connection error: {message}", details)


class BrokerConnectionError(ConnectionError):
    """Raised when unable to connect to message broker"""

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__("broker", message, details)


class StorageConnectionError(ConnectionError):
    """Raised when unable to connect to storage"""

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__("storage", message, details)


class RedisConnectionError(ConnectionError):
    """Raised when unable to connect to Redis"""

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__("redis", message, details)


# Job-related Errors
class JobError(TaskForgeError):
    """Base class for job-related errors"""

    def __init__(
        self, job_id: str, message: str, details: Optional[Dict[str, Any]] = None
    ):
        self.job_id = job_id
        super().__init__(message, {"job_id": job_id, **(details or {})})


class JobNotFoundError(JobError):
    """Raised when a job cannot be found"""

    pass


class JobValidationError(JobError):
    """Raised when job validation fails"""

    pass


class JobExecutionError(JobError):
    """Raised when job execution fails"""

    pass


class JobTimeoutError(JobError):
    """Raised when job execution times out"""

    def __init__(
        self, job_id: str, timeout: int, details: Optional[Dict[str, Any]] = None
    ):
        super().__init__(
            job_id, f"Job execution timed out after {timeout} seconds", details
        )


class JobCancellationError(JobError):
    """Raised when job cancellation fails"""

    pass


# Workflow Errors
class WorkflowError(TaskForgeError):
    """Base class for workflow-related errors"""

    def __init__(
        self, workflow_id: str, message: str, details: Optional[Dict[str, Any]] = None
    ):
        self.workflow_id = workflow_id
        super().__init__(message, {"workflow_id": workflow_id, **(details or {})})


class WorkflowNotFoundError(WorkflowError):
    """Raised when a workflow cannot be found"""

    pass


class WorkflowValidationError(WorkflowError):
    """Raised when workflow validation fails"""

    pass


class WorkflowExecutionError(WorkflowError):
    """Raised when workflow execution fails"""

    pass


class CircularDependencyError(WorkflowError):
    """Raised when circular dependencies are detected in workflow"""

    pass


# Schedule Errors
class ScheduleError(TaskForgeError):
    """Base class for schedule-related errors"""

    def __init__(
        self, schedule_id: str, message: str, details: Optional[Dict[str, Any]] = None
    ):
        self.schedule_id = schedule_id
        super().__init__(message, {"schedule_id": schedule_id, **(details or {})})


class ScheduleNotFoundError(ScheduleError):
    """Raised when a schedule cannot be found"""

    pass


class ScheduleValidationError(ScheduleError):
    """Raised when schedule validation fails"""

    pass


# Resource Errors
class ResourceError(TaskForgeError):
    """Base class for resource-related errors"""

    pass


class ResourceExhaustedError(ResourceError):
    """Raised when a resource limit is hit"""

    def __init__(
        self, resource: str, limit: int, details: Optional[Dict[str, Any]] = None
    ):
        super().__init__(
            f"Resource limit exceeded for {resource}: {limit}",
            {"resource": resource, "limit": limit, **(details or {})},
        )


class ConcurrencyLimitError(ResourceError):
    """Raised when concurrency limit is exceeded"""

    def __init__(
        self, job_type: str, limit: int, details: Optional[Dict[str, Any]] = None
    ):
        super().__init__(
            f"Concurrency limit exceeded for {job_type}: {limit}",
            {"job_type": job_type, "limit": limit, **(details or {})},
        )


# Integration Errors
class IntegrationError(TaskForgeError):
    """Base class for integration-related errors"""

    pass


class WebhookError(IntegrationError):
    """Raised for webhook-related errors"""

    def __init__(
        self, webhook_id: str, message: str, details: Optional[Dict[str, Any]] = None
    ):
        super().__init__(
            f"Webhook error ({webhook_id}): {message}",
            {"webhook_id": webhook_id, **(details or {})},
        )


class EventError(IntegrationError):
    """Raised for event-related errors"""

    def __init__(
        self, event_type: str, message: str, details: Optional[Dict[str, Any]] = None
    ):
        super().__init__(
            f"Event error ({event_type}): {message}",
            {"event_type": event_type, **(details or {})},
        )


# Retryable Errors
class RetryableError(TaskForgeError):
    """Base class for errors that can be retried"""

    def __init__(
        self,
        message: str,
        retry_after: int = 5,
        details: Optional[Dict[str, Any]] = None,
    ):
        self.retry_after = retry_after
        super().__init__(message, {"retry_after": retry_after, **(details or {})})


class TemporaryError(RetryableError):
    """Raised for temporary failures that should be retried"""

    pass


class CircuitBreakerError(RetryableError):
    """Raised when circuit breaker is open"""

    def __init__(
        self,
        service: str,
        message: str = "Circuit breaker is open",
        retry_after: int = 60,
    ):
        super().__init__(
            f"{service}: {message}",
            retry_after=retry_after,
            details={"service": service},
        )


# Authentication Errors
class AuthenticationError(TaskForgeError):
    """Base class for authentication-related errors"""

    pass


class InvalidAPIKeyError(AuthenticationError):
    """Raised when API key is invalid"""

    def __init__(self):
        super().__init__("Invalid API key")


class AuthorizationError(AuthenticationError):
    """Raised when operation is not authorized"""

    def __init__(self, operation: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(f"Not authorized to perform operation: {operation}", details)


# Storage Errors
class StorageError(TaskForgeError):
    """Base class for storage-related errors"""

    pass


class StorageOperationError(StorageError):
    """Raised when a storage operation fails"""

    def __init__(
        self, operation: str, message: str, details: Optional[Dict[str, Any]] = None
    ):
        super().__init__(f"Storage operation '{operation}' failed: {message}", details)


# Broker Errors
class BrokerError(TaskForgeError):
    """Base class for broker-related errors"""

    pass


class MessageError(BrokerError):
    """Raised for message-related errors"""

    def __init__(
        self, message_id: str, error: str, details: Optional[Dict[str, Any]] = None
    ):
        super().__init__(
            f"Message error ({message_id}): {error}",
            {"message_id": message_id, **(details or {})},
        )


# Utility function for error handling
def handle_exceptions(func):
    """Decorator for consistent exception handling"""

    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except TaskForgeError:
            # Re-raise TaskForge exceptions as is
            raise
        except Exception as e:
            # Wrap unknown exceptions
            raise TaskForgeError(
                f"Unexpected error: {str(e)}",
                {"function": func.__name__, "args": str(args), "kwargs": str(kwargs)},
            ) from e

    return wrapper
