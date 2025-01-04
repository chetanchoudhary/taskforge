from datetime import datetime, timezone
from typing import Any, Dict, Optional

import aiohttp
import structlog
from pydantic import BaseModel, Field, HttpUrl

from taskforge.core.metrics import metrics
from taskforge.exceptions import IntegrationError
from taskforge.utils.circuit_breaker import CircuitBreaker

logger = structlog.get_logger()


class ExternalServiceConfig(BaseModel):
    """External service configuration"""

    name: str
    base_url: HttpUrl
    api_key: Optional[str] = None
    timeout: int = 30
    retry_count: int = 3
    retry_delay: int = 5
    headers: Dict[str, str] = Field(default_factory=dict)
    circuit_breaker: bool = True
    circuit_breaker_threshold: int = 5
    circuit_breaker_timeout: int = 60


class ExternalServiceManager:
    """Manages external service integrations"""

    def __init__(self):
        self.configs: Dict[str, ExternalServiceConfig] = {}
        self.session: Optional[aiohttp.ClientSession] = None
        self.circuit_breakers: Dict[str, CircuitBreaker] = {}
        self.logger = logger.bind(component="ExternalServiceManager")

    async def start(self):
        """Initialize service manager"""
        self.session = aiohttp.ClientSession()

    async def stop(self):
        """Cleanup service manager"""
        if self.session:
            await self.session.close()
            self.session = None

    def register_service(self, config: ExternalServiceConfig):
        """Register an external service"""
        self.configs[config.name] = config

        if config.circuit_breaker:
            self.circuit_breakers[config.name] = CircuitBreaker(
                name=config.name,
                failure_threshold=config.circuit_breaker_threshold,
                reset_timeout=config.circuit_breaker_timeout,
            )

        self.logger.info(
            "External service registered",
            name=config.name,
            base_url=str(config.base_url),
        )

    async def call_service(
        self,
        service_name: str,
        method: str,
        path: str,
        data: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, str]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> Any:
        """Make external service call with retries and circuit breaker"""
        if not self.session:
            await self.start()

        config = self.configs.get(service_name)
        if not config:
            raise IntegrationError(f"Service {service_name} not found")

        url = f"{config.base_url.rstrip('/')}/{path.lstrip('/')}"
        request_headers = {
            "User-Agent": "TaskForge/1.0",
            **config.headers,
            **(headers or {}),
        }

        if config.api_key:
            request_headers["Authorization"] = f"Bearer {config.api_key}"

        # Get circuit breaker if enabled
        circuit_breaker = self.circuit_breakers.get(service_name)

        start_time = datetime.now(timezone.utc)
        try:

            async def make_request():
                async with self.session.request(
                    method,
                    url,
                    json=data,
                    params=params,
                    headers=request_headers,
                    timeout=config.timeout,
                ) as response:
                    response.raise_for_status()
                    return await response.json()

            # Use circuit breaker if enabled
            if circuit_breaker:
                result = await circuit_breaker.call(make_request)
            else:
                result = await make_request()

            # Update metrics
            duration = (datetime.now(timezone.utc) - start_time).total_seconds()
            metrics.external_service_duration.labels(
                service=service_name, method=method
            ).observe(duration)

            metrics.external_service_requests.labels(
                service=service_name, method=method, status="success"
            ).inc()

            return result

        except Exception as e:
            # Update error metrics
            metrics.external_service_requests.labels(
                service=service_name, method=method, status="error"
            ).inc()

            self.logger.error("Service call failed", service=service_name, error=str(e))
            raise IntegrationError(f"Service call failed: {str(e)}")
