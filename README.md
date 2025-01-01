# TaskForge

A powerful, type-safe distributed job processing framework built with modern Python.

## Features

- 🚀 Type-safe job definitions using Pydantic
- 💪 Reliable message processing with RabbitMQ
- 📊 Job state persistence in PostgreSQL
- 🔄 Automatic retries and error handling
- 🎯 Priority-based job processing
- 🔍 Job progress tracking and monitoring
- 📈 Prometheus metrics integration
- 🔒 Concurrency control and rate limiting
- 🐳 Docker and Kubernetes support

## Quick Start

1. Install TaskForge:

```bash
pip install taskforge
```

2. Create a job:

```python
from taskforge.jobs import Job, JobInput, JobOutput

class EmailJobInput(JobInput):
    to: str
    subject: str
    body: str

class EmailJobOutput(JobOutput):
    message_id: str
    sent_at: datetime

class SendEmailJob(Job[EmailJobInput, EmailJobOutput]):
    async def execute(self) -> EmailJobOutput:
        # Implement email sending logic
        return EmailJobOutput(
            message_id="123",
            sent_at=datetime.utcnow()
        )
```

3. Start TaskForge services:

```bash
# Start with Docker Compose
docker-compose up -d

# Or start components individually
taskforge api
taskforge worker
```

4. Submit a job:

```python
from taskforge import JobOrchestrator

orchestrator = JobOrchestrator()
job_id = await orchestrator.submit_job(
    "SendEmailJob",
    input_data={
        "to": "user@example.com",
        "subject": "Hello",
        "body": "World"
    }
)
```

## Configuration

TaskForge can be configured using environment variables or a configuration file:

```env
RABBITMQ_URL=amqp://localhost:5672/
POSTGRES_URL=postgresql+asyncpg://localhost:5432/taskforge
REDIS_URL=redis://localhost:6379/0
WORKER_COUNT=4
PREFETCH_COUNT=10
```

## Deployment

### Docker Compose

```bash
docker-compose up -d
```

### Kubernetes

```bash
# Create secrets
kubectl create secret generic taskforge-secrets \
  --from-literal=rabbitmq-url=amqp://user:pass@rabbitmq:5672/ \
  --from-literal=postgres-url=postgresql://user:pass@postgres:5432/taskforge

# Deploy services
kubectl apply -f k8s/
```

## Monitoring

TaskForge exposes Prometheus metrics at `/metrics` and provides a health check endpoint at `/health`.

## Contributing

We welcome contributions! Please see our [Contributing Guide](CONTRIBUTING.md) for details.

## License