import asyncio
import hashlib
import hmac
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from uuid import uuid4

import aiohttp
import structlog
from pydantic import BaseModel, HttpUrl

from taskforge.core.config import settings
from taskforge.core.metrics import metrics
from taskforge.exceptions import IntegrationError

logger = structlog.get_logger()


class WebhookConfig(BaseModel):
    """Webhook configuration"""

    url: HttpUrl
    secret: Optional[str] = None
    retry_count: int = 3
    retry_delay: int = 5
    timeout: int = 30
    headers: Dict[str, str] = {}


class WebhookEvent(BaseModel):
    """Webhook event payload"""

    id: str
    type: str
    data: Dict[str, Any]
    timestamp: datetime
    signature: Optional[str] = None


class WebhookManager:
    """Manages webhook delivery and verification"""

    def __init__(self):
        self.configs: Dict[str, WebhookConfig] = {}
        self.session: Optional[aiohttp.ClientSession] = None
        self.logger = logger.bind(component="WebhookManager")

    async def start(self):
        """Initialize webhook manager"""
        self.session = aiohttp.ClientSession()

    async def stop(self):
        """Cleanup webhook manager"""
        if self.session:
            await self.session.close()
            self.session = None

    def register_webhook(self, name: str, config: WebhookConfig):
        """Register a webhook configuration"""
        self.configs[name] = config
        self.logger.info("Webhook registered", name=name, url=str(config.url))

    async def send_webhook(
        self, name: str, event_type: str, data: Dict[str, Any]
    ) -> bool:
        """Send webhook with retries"""
        if not self.session:
            await self.start()

        config = self.configs.get(name)
        if not config:
            raise IntegrationError(f"Webhook {name} not found")

        event = WebhookEvent(
            id=str(uuid4()),
            type=event_type,
            data=data,
            timestamp=datetime.now(timezone.utc),
        )

        headers = {
            "Content-Type": "application/json",
            "User-Agent": f"TaskForge/{settings.version}",
            "X-TaskForge-Event": event_type,
            "X-TaskForge-Delivery": event.id,
            **config.headers,
        }

        # Add signature if secret configured
        if config.secret:
            payload = event.json()
            signature = self._sign_payload(payload, config.secret)
            headers["X-TaskForge-Signature"] = signature
            event.signature = signature

        start_time = datetime.now(timezone.utc)
        # success = False

        for attempt in range(config.retry_count):
            try:
                async with self.session.post(
                    str(config.url),
                    json=event.model_dump(),
                    headers=headers,
                    timeout=config.timeout,
                ) as response:
                    response.raise_for_status()
                    # success = True

                    # Update metrics
                    duration = (datetime.now(timezone.utc) - start_time).total_seconds()
                    metrics.webhook_request_duration.labels(name=name).observe(duration)
                    metrics.webhook_requests.labels(
                        name=name, event_type=event_type, status="success"
                    ).inc()

                    self.logger.info(
                        "Webhook delivered",
                        name=name,
                        event_id=event.id,
                        status=response.status,
                    )
                    return True

            except Exception as e:
                self.logger.warning(
                    "Webhook delivery failed",
                    name=name,
                    attempt=attempt + 1,
                    error=str(e),
                )

                if attempt < config.retry_count - 1:
                    await asyncio.sleep(
                        config.retry_delay * (2**attempt)
                    )  # Exponential backoff
                else:
                    metrics.webhook_requests.labels(
                        name=name, event_type=event_type, status="failure"
                    ).inc()

        return False

    def _sign_payload(self, payload: str, secret: str) -> str:
        """Create HMAC signature of payload"""
        hmac_obj = hmac.new(secret.encode(), payload.encode(), hashlib.sha256)
        return f"sha256={hmac_obj.hexdigest()}"

    def verify_signature(self, payload: str, signature: str, secret: str) -> bool:
        """Verify webhook signature"""
        expected_sig = self._sign_payload(payload, secret)
        return hmac.compare_digest(signature, expected_sig)
