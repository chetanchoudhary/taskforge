from datetime import datetime
from typing import Optional
from pydantic import BaseModel, EmailStr
from taskforge.broker.rabbitmq import RabbitMQBroker
from taskforge.jobs.base import Job, JobMetadata, JobPriority
import asyncio
import structlog
from taskforge.orchestrator import JobOrchestrator
from taskforge.storage.postgres import PostgresJobStorage

logger = structlog.get_logger()

class EmailJobInput(BaseModel):
    to: EmailStr
    subject: str
    body: str
    template_id: Optional[str] = None
    attachments: Optional[list[str]] = None

class EmailJobOutput(BaseModel):
    message_id: str
    sent_at: datetime
    recipient: EmailStr

class SendEmailJob(Job[EmailJobInput, EmailJobOutput]):
    """Example email sending job implementation"""
    
    async def validate(self) -> bool:
        """Validate job inputs"""
        # Add custom validation logic
        if self.input_data.template_id and not self.input_data.template_id.startswith('tmpl_'):
            raise ValueError("Invalid template ID format")
        return True
    
    async def execute(self) -> EmailJobOutput:
        """Execute the email sending logic"""
        # Simulate email sending
        await asyncio.sleep(1)  # Simulate API call
        
        return EmailJobOutput(
            message_id=f"msg_{self.job_id}",
            sent_at=datetime.utcnow(),
            recipient=self.input_data.to
        )
    
    async def on_failure(self, exception: Exception) -> None:
        """Handle job failure"""
        logger.error(
            "Email sending failed",
            job_id=self.job_id,
            error=str(exception),
            recipient=self.input_data.to
        )
        # Could implement retry logic or notification here
    
    async def on_success(self, result: EmailJobOutput) -> None:
        """Handle successful job execution"""
        logger.info(
            "Email sent successfully",
            job_id=self.job_id,
            message_id=result.message_id,
            recipient=result.recipient
        )

# Example usage
async def main():
    # Initialize TaskForge components
    broker = RabbitMQBroker("amqp://localhost:5672")
    storage = PostgresJobStorage("postgresql+asyncpg://user:pass@localhost/taskforge")
    
    # Create orchestrator
    orchestrator = JobOrchestrator(broker, storage)
    
    # Register job type
    await orchestrator.register_job(
        SendEmailJob,
        max_retries=3,
        timeout_seconds=30,
        concurrency_limit=50
    )
    
    # Submit a job
    job_id = await orchestrator.submit_job(
        "SendEmailJob",
        input_data=EmailJobInput(
            to="user@example.com",
            subject="Welcome",
            body="Hello from TaskForge!"
        ),
        priority=JobPriority.HIGH
    )
    
    # Check job status
    job_state = await orchestrator.get_job_state(job_id)
    print(f"Job status: {job_state.status}")

if __name__ == "__main__":
    asyncio.run(main())