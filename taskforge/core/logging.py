import logging
import sys
from typing import Any, Dict
import structlog
from structlog.stdlib import ProcessorFormatter

from taskforge.core.config import settings

def setup_logging() -> None:
    """Configure structured logging"""
    
    # Configure structlog processors
    processors = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]
    
    if settings.log_config.json_logs:
        processors.append(structlog.processors.JSONRenderer())
    else:
        processors.append(structlog.dev.ConsoleRenderer())
    
    structlog.configure(
        processors=processors,
        wrapper_class=structlog.stdlib.BoundLogger,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )
    
    # Configure standard logging
    logging.basicConfig(
        format=settings.log_config.format,
        level=settings.log_config.level,
        stream=sys.stdout,
    )

def get_logger(name: str) -> structlog.BoundLogger:
    """Get a structured logger"""
    return structlog.get_logger(name)

class JobLoggerAdapter:
    """Adapter to add job context to logs"""
    
    def __init__(self, logger: structlog.BoundLogger, job_id: str):
        self.logger = logger.bind(job_id=job_id)
    
    def bind(self, **kwargs: Any) -> "JobLoggerAdapter":
        """Bind additional context"""
        return JobLoggerAdapter(self.logger.bind(**kwargs), job_id=self.job_id)
    
    def debug(self, msg: str, **kwargs: Any) -> None:
        self.logger.debug(msg, **kwargs)
    
    def info(self, msg: str, **kwargs: Any) -> None:
        self.logger.info(msg, **kwargs)
    
    def warning(self, msg: str, **kwargs: Any) -> None:
        self.logger.warning(msg, **kwargs)
    
    def error(self, msg: str, **kwargs: Any) -> None:
        self.logger.error(msg, **kwargs)
    
    def exception(self, msg: str, **kwargs: Any) -> None:
        self.logger.exception(msg, **kwargs)