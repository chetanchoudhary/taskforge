from abc import ABC, abstractmethod
from typing import Optional, Dict, Any, Type
from taskforge.jobs.base import Job

class BaseBroker(ABC):
    """Base interface for message brokers"""
    
    @abstractmethod
    async def connect(self) -> None:
        """Establish connection to the broker"""
        pass
        
    @abstractmethod
    async def close(self) -> None:
        """Close broker connection"""
        pass
        
    @abstractmethod
    async def create_channel(self):
        """Create a new channel"""
        pass
        
    @abstractmethod
    async def register_job(self, job_class: Type[Job], queue_name: Optional[str] = None) -> None:
        """Register a job type and create its queue"""
        pass
        
    @abstractmethod
    async def publish_job(self,
                         job_type: str,
                         job_id: str,
                         input_data: Dict[str, Any],
                         metadata: Optional[Dict[str, Any]] = None,
                         priority: Optional[int] = None) -> None:
        """Publish a job to its queue"""
        pass
        
    @abstractmethod
    async def get_message(self, queue_name: str):
        """Get a message from a queue"""
        pass
        
    @abstractmethod
    async def get_queue_depth(self, queue_name: str) -> int:
        """Get number of messages in a queue"""
        pass
        
    @abstractmethod
    async def ping(self) -> bool:
        """Check broker connection"""
        pass
    