# TaskForge: Distributed Job Processing Framework

## Introduction

TaskForge is a robust distributed job processing framework designed to handle asynchronous tasks with reliability and scalability. Think of it as a sophisticated assembly line in a factory, where each worker specializes in processing specific types of jobs efficiently.

## Core Components

### 1. Job Definition

At the heart of TaskForge is the concept of a Job. Each job is a self-contained unit of work with clear inputs and outputs:

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
        # Job implementation
        return EmailJobOutput(...)
```

### 2. Component Architecture

TaskForge uses a modular architecture where each component has a specific responsibility:

1. **JobOrchestrator**: The conductor of the orchestra
   - Manages job registration
   - Handles job submission
   - Coordinates between broker and storage
   - Monitors system health

2. **WorkerPool**: The factory floor supervisor
   - Manages multiple workers
   - Handles worker lifecycle
   - Monitors worker health
   - Scales workers up/down

3. **TaskWorker**: The individual workers
   - Processes jobs
   - Handles job execution
   - Manages job lifecycle
   - Reports health status

4. **RabbitMQ Broker**: The message delivery system
   - Handles message routing
   - Manages message queues
   - Ensures reliable delivery
   - Controls message flow (prefetch)

5. **PostgreSQL Storage**: The system of record
   - Stores job states
   - Maintains job history
   - Enables job recovery
   - Provides audit trail

## Job Lifecycle

Let's follow a job through its complete lifecycle:

1. **Job Submission**
```python
job_id = await orchestrator.submit_job(
    "SendEmailJob",
    input_data={"to": "user@example.com", "subject": "Hello"}
)
```

2. **Message Queue**
   - Job is serialized and sent to RabbitMQ
   - Assigned to appropriate queue based on job type
   - Queue manages priority and ordering

3. **Worker Processing**
   - Worker picks up job with prefetch control
   - Job state updated to RUNNING
   - Job executed with timeout monitoring
   - Results saved to storage

4. **Completion/Error Handling**
   - Success: Results stored, metrics updated
   - Error: Retry mechanism engaged if configured
   - Final state recorded in storage

## Parallel and Concurrent Processing

TaskForge achieves high throughput through several mechanisms:

1. **Worker Pool Parallelism**
```python
# Multiple workers process jobs simultaneously
pool = WorkerPool(
    broker=broker,
    storage=storage,
    num_workers=4,  # 4 parallel workers
    prefetch_count=10  # Each worker handles 10 messages
)
```

2. **Message Prefetching**
   - Workers prefetch multiple messages
   - Reduces network round trips
   - Optimizes throughput

3. **Distributed Semaphores**
```python
async with JobSemaphore(redis, "email_jobs", max_concurrent=100):
    # Ensures no more than 100 email jobs run simultaneously
    await process_job()
```

## Integration with Existing Projects

### Integration with FastAPI Project

1. **Installation**
```bash
pip install taskforge
```

2. **Basic Integration**
```python
from fastapi import FastAPI
from taskforge import JobOrchestrator, WorkerPool

app = FastAPI()
orchestrator = JobOrchestrator(broker, storage)
worker_pool = WorkerPool(broker, storage)

@app.on_event("startup")
async def startup():
    # Start worker pool
    await worker_pool.start()

@app.on_event("shutdown")
async def shutdown():
    # Graceful shutdown
    await worker_pool.shutdown()
```

3. **AI Agent Integration Example**
```python
from taskforge.jobs import Job

class AIAgentJob(Job[AgentInput, AgentOutput]):
    async def execute(self) -> AgentOutput:
        # Your AI agent logic here
        result = await self.agent.run(self.input_data.prompt)
        return AgentOutput(result=result)

# In your FastAPI route
@app.post("/run-agent")
async def run_agent(input: AgentInput):
    job_id = await orchestrator.submit_job(
        "AIAgentJob",
        input_data=input.dict()
    )
    return {"job_id": job_id}
```

### Advanced Integration Features

1. **Job Progress Tracking**
```python
class LongRunningJob(Job):
    async def execute(self):
        total_steps = 10
        for i in range(total_steps):
            # Update progress
            await self.update_progress(i / total_steps)
            await self.process_step(i)
```

2. **Custom Queue Configuration**
```python
# Configure specific queues for different agent types
orchestrator.register_job(
    GPTAgentJob,
    queue="gpt_queue",
    max_concurrent=5
)

orchestrator.register_job(
    StableDiffusionJob,
    queue="sd_queue",
    max_concurrent=2
)
```

3. **Error Handling and Retries**
```python
class ReliableJob(Job):
    async def on_failure(self, exception: Exception):
        if isinstance(exception, TemporaryError):
            # Retry after delay
            await self.retry(delay_seconds=30)
        else:
            # Permanent failure
            await self.fail(str(exception))
```

## Best Practices

1. **Job Design**
   - Keep jobs atomic and focused
   - Include validation logic
   - Handle cleanup properly
   - Log meaningful information

2. **Resource Management**
   - Configure appropriate timeouts
   - Set reasonable prefetch counts
   - Monitor memory usage
   - Use distributed semaphores

3. **Error Handling**
   - Implement proper retry logic
   - Log errors with context
   - Clean up resources
   - Monitor failed jobs

4. **Monitoring**
   - Use provided metrics
   - Set up alerts
   - Monitor queue depths
   - Track worker health

## Performance Tuning

1. **Worker Configuration**
```python
# Adjust based on workload
pool = WorkerPool(
    num_workers=cpu_count() * 2,  # CPU-bound jobs
    prefetch_count=50,  # I/O-bound jobs
    monitor_interval=5  # Frequent health checks
)
```

2. **Queue Management**
```python
# Configure queue properties
await broker.declare_queue(
    "high_priority",
    max_length=1000,
    message_ttl=3600000  # 1 hour
)
```

3. **Resource Limits**
```python
# Set appropriate limits
semaphore = JobSemaphore(
    redis,
    "resource_intensive_jobs",
    max_concurrent=cpu_count()
)
```

TaskForge provides a solid foundation for handling distributed job processing in your applications. Its modular design makes it easy to integrate and extend, while its robust feature set ensures reliable job processing at scale.
