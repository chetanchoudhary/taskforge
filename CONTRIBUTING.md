# Contributing to TaskForge

## Table of Contents
- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
  - [Development Setup](#development-setup)
  - [Project Structure](#project-structure)
- [Development Guidelines](#development-guidelines)
  - [Code Style](#code-style)
  - [Type Hints](#type-hints)
  - [Testing](#testing)
  - [Documentation](#documentation)
- [Development Scenarios](#development-scenarios)
  - [Adding New Jobs](#adding-new-jobs)
  - [Custom Storage Backends](#custom-storage-backends)
  - [Custom Workers](#custom-workers)
  - [Custom Metrics](#custom-metrics)
  - [Integration Tests](#integration-tests)
- [Pull Request Process](#pull-request-process)
- [Community](#community)

## Code of Conduct

Our community strives to:
- Be welcoming and inclusive
- Show respect and empathy to all contributors
- Accept constructive criticism gracefully
- Focus on what's best for the community
- Show empathy towards others

Unacceptable behavior includes:
- Harassment of any kind
- Discriminatory jokes and language
- Personal or political attacks
- Public or private harassment
- Publishing others' private information

## Getting Started

### Development Setup

1. Fork and clone the repository:
```bash
git clone git@github.com:your-username/taskforge.git
cd taskforge
```

2. Install dependencies:
```bash
# Install poetry
curl -sSL https://install.python-poetry.org | python3 -

# Install project dependencies
poetry install

# Install pre-commit hooks
pre-commit install
```

3. Set up development environment:
```bash
# Copy example environment file
cp .env.example .env

# Start development services
docker-compose -f docker-compose.yml -f docker-compose.dev.yml up -d
```

4. Run tests:
```bash
# Run all tests
poetry run pytest

# Run with coverage
poetry run pytest --cov=taskforge
```

### Project Structure

```
taskforge/
├── api/               # FastAPI application
│   ├── routes/       # API endpoints
│   ├── models.py     # API models
│   └── server.py     # Server configuration
├── broker/           # Message broker interface
│   ├── base.py       # Base broker interface
│   └── rabbitmq.py   # RabbitMQ implementation
├── core/             # Core functionality
│   ├── config.py     # Configuration
│   ├── metrics.py    # Metrics system
│   └── logging.py    # Logging setup
├── jobs/             # Job definitions
│   ├── base.py       # Base job classes
│   └── registry.py   # Job registration
├── storage/          # Storage backends
│   ├── base.py       # Base storage interface
│   └── postgres.py   # PostgreSQL implementation
├── worker/           # Worker implementation
│   ├── pool.py       # Worker pool
│   └── worker.py     # Worker process
└── tests/            # Test suite
```

## Development Guidelines

### Code Style

We follow strict code style guidelines:

1. Use black for code formatting:
```bash
poetry run black taskforge tests
```

2. Use isort for import sorting:
```bash
poetry run isort taskforge tests
```

3. Follow consistent naming:
```python
# Classes use PascalCase
class WorkerPool:
    pass

# Functions and variables use snake_case
async def process_job(job_id: str) -> None:
    pass

# Constants use UPPER_CASE
MAX_RETRIES = 3
```

### Type Hints

All code must use type hints:

```python
from typing import Optional, List, Dict, Any

async def process_items(
    items: List[str],
    timeout: Optional[int] = None,
    metadata: Dict[str, Any] = None
) -> bool:
    """Process a list of items."""
    pass
```

### Testing

1. Unit Tests:
```python
import pytest
from taskforge.worker import TaskWorker

class TestTaskWorker:
    @pytest.fixture
    async def worker(self):
        worker = TaskWorker(worker_id="test")
        yield worker
        await worker.cleanup()

    async def test_process_job(self, worker):
        result = await worker.process_job("test_job")
        assert result.status == "completed"
```

2. Integration Tests:
```python
@pytest.mark.integration
async def test_job_flow():
    """Test complete job processing flow."""
    # Initialize components
    broker = await create_test_broker()
    storage = await create_test_storage()
    
    # Submit job
    job_id = await submit_test_job(broker)
    
    # Verify results
    result = await storage.get_job_state(job_id)
    assert result.status == "completed"
```

### Documentation

1. Docstring Format:
```python
def process_job(job_id: str, timeout: int = 30) -> bool:
    """
    Process a job with the given ID.

    Args:
        job_id: Unique identifier for the job
        timeout: Maximum processing time in seconds

    Returns:
        bool: True if processing was successful

    Raises:
        JobError: If job processing fails

    Example:
        ```python
        success = await process_job("job-123", timeout=60)
        ```
    """
    pass
```

2. README Updates:
- Keep the README.md up to date
- Document new features
- Update configuration examples
- Add usage examples for new functionality

## Development Scenarios

### Adding New Jobs

```python
from taskforge.jobs import Job, JobInput, JobOutput
from datetime import datetime

class ImageProcessingInput(JobInput):
    image_urls: List[str]
    target_size: tuple[int, int]
    format: str = "jpeg"

class ImageProcessingOutput(JobOutput):
    processed_urls: List[str]
    total_size: int
    processing_time: float

class BatchImageProcessingJob(Job[ImageProcessingInput, ImageProcessingOutput]):
    """Process multiple images in batch."""
    
    async def validate(self) -> bool:
        """Validate job inputs."""
        for url in self.input_data.image_urls:
            if not url.startswith(('http://', 'https://')):
                raise ValueError(f"Invalid image URL: {url}")
        return True

    async def execute(self) -> ImageProcessingOutput:
        """Execute image processing."""
        start_time = time.monotonic()
        processed = []
        total_size = 0
        
        # Process each image
        for i, url in enumerate(self.input_data.image_urls):
            await self.update_progress(i / len(self.input_data.image_urls))
            result = await self._process_single_image(url)
            processed.append(result.url)
            total_size += result.size
        
        return ImageProcessingOutput(
            processed_urls=processed,
            total_size=total_size,
            processing_time=time.monotonic() - start_time
        )
```

### Custom Storage Backends

```python
from taskforge.storage.base import BaseStorage

class CustomStorage(BaseStorage):
    """Custom storage implementation."""

    async def initialize(self) -> None:
        """Initialize storage."""
        await self.setup_schema()
        await self.create_indexes()

    async def save_job(self, job_id: str, job_type: str, 
                      input_data: Dict[str, Any]) -> None:
        """Save a job."""
        await self.collection.insert_one({
            "job_id": job_id,
            "job_type": job_type,
            "input_data": input_data,
            "status": "pending",
            "created_at": datetime.utcnow()
        })
```

### Custom Workers

```python
from taskforge.worker import TaskWorker

class GPUWorker(TaskWorker):
    """Worker for GPU-intensive tasks."""

    async def initialize(self) -> None:
        """Initialize GPU resources."""
        await self.setup_gpu()
        await super().initialize()

    async def process_job(self, job: Job) -> None:
        """Process a job using GPU."""
        try:
            await self.acquire_gpu_memory()
            await super().process_job(job)
        finally:
            await self.release_gpu_memory()
```

### Custom Metrics

```python
from taskforge.core.metrics import metrics
from prometheus_client import Counter, Histogram

class CustomMetrics:
    """Custom metrics collection."""

    def __init__(self):
        self.gpu_utilization = Gauge(
            "gpu_utilization_percent",
            "GPU utilization percentage",
            ["gpu_id"]
        )

        self.processed_items = Counter(
            "items_processed_total",
            "Number of items processed",
            ["item_type"]
        )
```

### Integration Tests

```python
class TestJobExecution:
    """Test job execution flow."""

    async def test_complete_flow(self, test_client):
        """Test end-to-end job execution."""
        # Submit job
        response = await test_client.post("/api/jobs", 
            json={"job_type": "TestJob", "input": {"data": "test"}})
        job_id = response.json()["job_id"]

        # Wait for completion
        while True:
            status = await get_job_status(job_id)
            if status in ("completed", "failed"):
                break
            await asyncio.sleep(0.1)

        # Verify results
        job = await get_job_details(job_id)
        assert job.status == "completed"
        assert job.result["success"] == True
```

## Pull Request Process

1. Create a feature branch:
```bash
git checkout -b feature/your-feature-name
```

2. Make your changes:
- Write tests for new functionality
- Update documentation
- Follow code style guidelines

3. Run checks:
```bash
# Format code
poetry run black taskforge tests
poetry run isort taskforge tests

# Run linting
poetry run flake8 taskforge tests
poetry run mypy taskforge

# Run tests
poetry run pytest
```

4. Push changes:
```bash
git push origin feature/your-feature-name
```

5. Create a Pull Request:
- Use a clear title
- Describe your changes
- Reference any related issues
- Fill out the PR template

## Community

- Join our [Discord]() for discussions
- Check the [GitHub Discussions]() for questions
- Follow our [Blog]() for updates
- Report bugs through [GitHub Issues]()

### Communication Tips

1. When asking questions:
- Provide context
- Share relevant code
- Include error messages
- List steps to reproduce

2. When reporting bugs:
- Use the issue template
- Include system information
- Provide minimal reproduction
- List expected vs actual behavior

### Getting Help

If you need assistance:
1. Check the documentation
2. Search existing issues
3. Ask in Discord
4. Open a GitHub issue

## License

By contributing to TaskForge, you agree that your contributions will be licensed under the MIT License.
