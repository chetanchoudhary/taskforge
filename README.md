# TaskForge

<div align="center">


**A Modern, Type-Safe Distributed Job Processing Framework**


</div>

TaskForge is a robust distributed job processing framework built for modern Python applications. Think of it as a sophisticated factory where each worker specializes in processing specific types of jobs efficiently while maintaining high reliability and scalability.

## Why TaskForge?

TaskForge stands out by providing enterprise-grade job processing capabilities with developer-friendly ergonomics:

### 🎯 Type Safety at Every Level
Unlike traditional job queues, TaskForge enforces type safety throughout the job lifecycle:
- Job inputs and outputs are defined using Pydantic models
- Runtime type validation prevents data inconsistencies
- IDE autocompletion support for job definitions
- Type checking catches errors before deployment

```python
class EmailJobInput(JobInput):
    to: str
    subject: str
    body: str
    template_id: Optional[str] = None

class EmailJobOutput(JobOutput):
    message_id: str
    sent_at: datetime

class SendEmailJob(Job[EmailJobInput, EmailJobOutput]):
    async def execute(self) -> EmailJobOutput:
        # Your implementation with full type safety
        pass
```

### 🚀 Modern Async Architecture
Built from the ground up for asynchronous processing:
- FastAPI-powered REST API for job management
- Asynchronous job execution with proper resource management
- Efficient worker pool with automatic scaling
- Built-in rate limiting and concurrency control

### 💪 Enterprise-Ready Features
Production-grade capabilities out of the box:
- Automatic retries with configurable backoff
- Distributed locking and semaphores
- Priority-based job processing
- Job progress tracking and monitoring
- Prometheus metrics integration
- OpenTelemetry distributed tracing
- Comprehensive logging and debugging tools

### 🔄 Workflow Orchestration
Complex job orchestration made simple:
```python
workflow = await engine.create_workflow(
    name="image_processing",
    nodes={
        "upload": TaskNode(
            id="upload",
            job_type="UploadImageJob",
            input_data={"source": "user_input"}
        ),
        "process": TaskNode(
            id="process",
            job_type="ProcessImageJob",
            depends_on=["upload"]
        ),
        "notify": TaskNode(
            id="notify",
            job_type="NotificationJob",
            depends_on=["process"]
        )
    }
)
```

### 📊 Real-Time Monitoring
Built-in observability at every level:
- Real-time job progress tracking
- Resource usage monitoring
- Queue depth metrics
- Worker health checks
- Performance analytics
- Custom metric support

### 🔌 Easy Integration
Designed for seamless integration with modern cloud infrastructure:
- Docker and Kubernetes support
- Cloud-native design patterns
- Extensible storage backends
- Pluggable message brokers
- Webhook integration
- Event-driven architecture

## Quick Start

### Installation

```bash
pip install taskforge
```

### Define a Job

```python
from taskforge.jobs import Job, JobInput, JobOutput

class ProcessImageInput(JobInput):
    image_url: str
    width: int
    height: int

class ProcessImageOutput(JobOutput):
    processed_url: str
    size_bytes: int

class ProcessImageJob(Job[ProcessImageInput, ProcessImageOutput]):
    async def execute(self) -> ProcessImageOutput:
        # Your implementation here
        return ProcessImageOutput(
            processed_url="https://example.com/processed.jpg",
            size_bytes=12345
        )
```

### Start the Services

```bash
# Using Docker Compose
docker-compose up -d

# Or start components individually
taskforge api
taskforge worker
```

### Submit a Job

```python
from taskforge import JobOrchestrator

orchestrator = JobOrchestrator()
job_id = await orchestrator.submit_job(
    "ProcessImageJob",
    input_data={
        "image_url": "https://example.com/image.jpg",
        "width": 800,
        "height": 600
    }
)
```

## Core Components

TaskForge is built on several robust components:

- **Job Orchestrator**: Central coordinator managing job lifecycle
- **Worker Pool**: Efficient worker management with automatic scaling
- **Task Workers**: Individual processing units with resource management
- **Message Broker**: Reliable message delivery using RabbitMQ
- **Storage Backend**: State persistence using PostgreSQL
- **Workflow Engine**: Complex job orchestration and dependencies
- **Scheduler**: Cron-style job scheduling
- **Monitoring**: Comprehensive metrics and health checks

## Deployment & Configuration

TaskForge provides flexible deployment options with comprehensive configuration capabilities:

### Docker Deployment

1. Development Environment:
```bash
# Start with development configuration
docker-compose -f docker-compose.yml -f docker-compose.dev.yml up -d

# Access development tools
# API: http://localhost:8000
# Adminer: http://localhost:8080
# Redis Commander: http://localhost:8081
# RabbitMQ Management: http://localhost:15672
```

2. Production Environment:
```bash
# Start with production configuration
docker-compose -f docker-compose.yml -f docker-compose.prod.yml up -d

# Scale workers as needed
docker-compose -f docker-compose.yml -f docker-compose.prod.yml up -d --scale worker=4
```

### Environment Configuration

TaskForge can be configured through environment variables:

```bash
# Core Settings
TASKFORGE_ENVIRONMENT=production
TASKFORGE_LOG_LEVEL=INFO
TASKFORGE_API_KEY=your-secure-key

# Worker Settings
TASKFORGE_WORKER_COUNT=4
TASKFORGE_MAX_RETRIES=3
TASKFORGE_JOB_TIMEOUT=300

# Service URLs
TASKFORGE_RABBITMQ_URL=amqp://rabbitmq:5672/
TASKFORGE_POSTGRES_URL=postgresql+asyncpg://postgres:password@db:5432/taskforge
TASKFORGE_REDIS_URL=redis://redis:6379/0
```

### Service-Specific Configuration

Each service can be customized through configuration files:

1. RabbitMQ (`docker/rabbitmq/rabbitmq.conf`):
```ini
# Memory and disk limits
vm_memory_high_watermark.relative = 0.8
disk_free_limit.relative = 2.0

# Queue configuration
queue_master_locator = min-masters
```

2. Redis (`docker/redis/redis.conf`):
```ini
maxmemory 512mb
maxmemory-policy allkeys-lru
appendonly yes
```

3. PostgreSQL (`docker/postgres/init.sql`):
```sql
-- Performance settings
ALTER SYSTEM SET shared_buffers TO '256MB';
ALTER SYSTEM SET work_mem TO '16MB';
```

## Development Tools

When running in development mode, TaskForge provides several tools to help with debugging and monitoring:

### Included Development Tools

1. **Adminer**: Database management tool
   - Access at `http://localhost:8080`
   - Easily view and edit database contents
   - Monitor database performance

2. **Redis Commander**: Redis management interface
   - Access at `http://localhost:8081`
   - View and manage Redis keys
   - Monitor Redis memory usage

3. **RabbitMQ Management**: Queue monitoring
   - Access at `http://localhost:15672`
   - Monitor queue depths and message rates
   - View consumer status

### Debugging Capabilities

1. Remote Debugging:
```python
# Connect to worker debug port
import debugpy
debugpy.connect(("localhost", 5678))
```

2. Hot Reloading:
- API and workers automatically reload on code changes
- Real-time configuration updates
- Development-specific logging

## Monitoring & Observability

TaskForge provides comprehensive monitoring capabilities:

### 1. Metrics

Access metrics through the `/metrics` endpoint:
```bash
curl http://localhost:8000/metrics
```

Available metrics include:
- Job processing rates and latencies
- Queue depths and processing times
- Worker health and resource usage
- Service-specific performance metrics

### 2. Health Checks

Regular health checks for all services:
```bash
# Check API health
curl http://localhost:8000/health

# Service-specific health checks
docker-compose ps  # View health status
```

### 3. Logging

Structured logging with configurable outputs:
```python
logger.info("Processing job", 
    job_id="123",
    job_type="ProcessImageJob",
    duration=1.5
)
```

### 4. Tracing

OpenTelemetry integration for distributed tracing:
```yaml
TASKFORGE_TRACING_ENABLED: true
TASKFORGE_TRACING_EXPORTER: jaeger
```



## Contributing

We welcome contributions! See our [Contributing Guide](CONTRIBUTING.md) for details.

## License

TaskForge is MIT licensed. See [LICENSE](LICENSE) for details.
