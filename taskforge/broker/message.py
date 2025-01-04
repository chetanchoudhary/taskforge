import zlib
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import msgpack

from taskforge.exceptions import MessageError


@dataclass
class Message:
    """Message envelope with metadata and compression"""

    message_id: str
    routing_key: str
    payload: Dict[str, Any]
    headers: Dict[str, str]
    timestamp: datetime
    content_type: str = "application/msgpack"
    content_encoding: Optional[str] = None
    priority: int = 0

    @classmethod
    def create(
        cls,
        routing_key: str,
        payload: Dict[str, Any],
        headers: Optional[Dict[str, str]] = None,
        compress: bool = True,
        priority: int = 0,
    ):
        """Create a new message"""
        from uuid import uuid4

        # Serialize payload
        try:
            serialized = msgpack.packb(payload)
        except Exception as e:
            raise MessageError(str(uuid4()), f"Failed to serialize payload: {e}")

        # Compress if needed
        if compress and len(serialized) > 1024:
            serialized = zlib.compress(serialized)
            content_encoding = "deflate"
        else:
            content_encoding = None

        return cls(
            message_id=str(uuid4()),
            routing_key=routing_key,
            payload=payload,
            headers=headers or {},
            timestamp=datetime.now(timezone.utc),
            content_type="application/msgpack",
            content_encoding=content_encoding,
            priority=priority,
        )

    def decode(self) -> Dict[str, Any]:
        """Decode message payload"""
        try:
            # Decompress if needed
            data = (
                zlib.decompress(self.payload)
                if self.content_encoding == "deflate"
                else self.payload
            )

            # Deserialize
            if self.content_type == "application/msgpack":
                return msgpack.unpackb(data)
            else:
                raise MessageError(
                    self.message_id, f"Unsupported content type: {self.content_type}"
                )
        except Exception as e:
            raise MessageError(self.message_id, f"Failed to decode message: {str(e)}")


# taskforge/worker/autoscaler.py

# taskforge/workflow/executor.py
