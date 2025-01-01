import json
from typing import Optional, Dict, Type, Any
from aio_pika import connect_robust, Message, Channel, Queue
from aio_pika.pool import Pool

from taskforge.jobs.base import Job, JobPriority

class RabbitMQBroker:
    """RabbitMQ implementation for job message broker"""
    
    def __init__(self, 
                 connection_url: str,
                 connection_pool_size: int = 2):
        self.connection_url = connection_url
        self.connection_pool_size = connection_pool_size
        self.job_registry: Dict[str, Type[Job]] = {}
        self._channel_pool: Optional[Pool[Channel]] = None
        
    async def connect(self) -> None:
        """Establish connection to RabbitMQ"""
        # Create connection pool
        self._connection_pool = Pool(
            self._get_connection,
            max_size=self.connection_pool_size,
        )
        
        # Create channel pool
        self._channel_pool = Pool(
            self._get_channel,
            max_size=self.connection_pool_size * 2,
        )
        
    async def _get_connection(self):
        """Get a RabbitMQ connection"""
        return await connect_robust(self.connection_url)
        
    async def _get_channel(self) -> Channel:
        """Get a RabbitMQ channel"""
        async with self._connection_pool.acquire() as connection:
            return await connection.channel()
            
    async def register_job(self, 
                          job_class: Type[Job],
                          queue_name: Optional[str] = None) -> None:
        """Register a job type and create its queue"""
        job_type = job_class.__name__
        queue_name = queue_name or job_type
        
        self.job_registry[job_type] = job_class
        
        # Create queue if it doesn't exist
        async with self._channel_pool.acquire() as channel:
            await channel.declare_queue(
                queue_name,
                durable=True,
                arguments={
                    'x-max-priority': max(jp.value for jp in JobPriority),
                }
            )
            
    def get_job_class(self, job_type: str) -> Type[Job]:
        """Get job class by type name"""
        if job_type not in self.job_registry:
            raise ValueError(f"Unknown job type: {job_type}")
        return self.job_registry[job_type]
        
    async def publish_job(self,
                         job_type: str,
                         job_id: str,
                         input_data: Dict[str, Any],
                         metadata: Optional[Dict[str, Any]] = None,
                         priority: JobPriority = JobPriority.MEDIUM) -> None:
        """Publish a job to its queue"""
        if job_type not in self.job_registry:
            raise ValueError(f"Job type {job_type} not registered")
            
        message_body = {
            "job_id": job_id,
            "job_type": job_type,
            "input_data": input_data,
            "metadata": metadata
        }
        
        message = Message(
            body=json.dumps(message_body).encode(),
            content_type="application/json",
            priority=priority.value,
            delivery_mode=2  # persistent
        )
        
        async with self._channel_pool.acquire() as channel:
            await channel.default_exchange.publish(
                message,
                routing_key=job_type
            )
            
    async def get_message(self, queue_name: str) -> Optional[Message]:
        """Get a message from a queue"""
        async with self._channel_pool.acquire() as channel:
            queue: Queue = await channel.declare_queue(
                queue_name,
                durable=True
            )
            return await queue.get(fail=False)
            
    async def close(self) -> None:
        """Close broker connections"""
        if self._channel_pool:
            await self._channel_pool.close()
        if self._connection_pool:
            await self._connection_pool.close()
            
    async def get_queue_depth(self, queue_name: str) -> int:
        """Get number of messages in a queue"""
        async with self._channel_pool.acquire() as channel:
            queue = await channel.declare_queue(
                queue_name,
                durable=True,
                passive=True  # Don't create, just check
            )
            return queue.declaration_result.message_count
            
    async def ping(self) -> bool:
        """Check broker connection"""
        try:
            async with self._channel_pool.acquire() as channel:
                return channel.is_open
        except Exception:
            return False
        