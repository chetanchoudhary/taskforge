import asyncio
import json

import click
import structlog
import uvicorn

from taskforge.api.server import TaskForgeAPI
from taskforge.core.config import settings
from taskforge.di import Container
from taskforge.worker.pool import WorkerPool

logger = structlog.get_logger()


@click.group()
def cli():
    """TaskForge CLI - Distributed Job Processing System"""
    pass


@cli.group()
def worker():
    """Worker commands"""
    pass


@worker.command(name="start")
@click.option("--workers", "-w", default=4, help="Number of worker processes")
@click.option("--prefetch-count", "-p", default=10, help="Message prefetch count")
@click.option(
    "--monitor-interval", "-m", default=10, help="Health check interval (seconds)"
)
def start_workers(workers: int, prefetch_count: int, monitor_interval: int):
    """Start worker pool"""

    async def run():
        container = await Container.init()
        pool = WorkerPool(
            broker=container.broker(),
            storage=container.storage(),
            num_workers=workers,
            prefetch_count=prefetch_count,
            monitor_interval=monitor_interval,
        )

        async with pool.worker_context() as active_pool:
            logger.info(
                "Worker pool started", workers=workers, prefetch_count=prefetch_count
            )
            await active_pool._shutdown_event.wait()

    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
    except Exception as e:
        logger.error("Fatal error", error=str(e), exc_info=True)
        raise


@worker.command(name="status")
@click.option("--api-url", default="http://localhost:8000", help="TaskForge API URL")
def worker_status(api_url: str):
    """Get worker pool status"""
    import httpx

    try:
        with httpx.Client() as client:
            response = client.get(f"{api_url}/api/metrics/workers")
            response.raise_for_status()
            stats = response.json()

            click.echo("Worker Pool Status:")
            click.echo(f"Active Workers: {stats['active_workers']}")
            click.echo(f"Total Workers: {stats['total_workers']}")
            click.echo("\nQueue Depths:")
            for queue, depth in stats["queue_depths"].items():
                click.echo(f"  {queue}: {depth}")
            click.echo("\nWorker Stats:")
            for worker_id, worker_stats in stats["processing_stats"].items():
                click.echo(f"\nWorker {worker_id}:")
                click.echo(f"  Jobs Processed: {worker_stats['jobs_processed']}")
                click.echo(f"  Errors: {worker_stats['errors']}")
                click.echo(f"  Uptime: {worker_stats['uptime_seconds']}s")
    except Exception as e:
        click.echo(f"Error getting worker status: {str(e)}", err=True)


@cli.command()
@click.option("--host", default="0.0.0.0", help="API server host")
@click.option("--port", default=8000, help="API server port")
def api(host: str, port: int):
    """Start the API server"""
    api_server = TaskForgeAPI()
    uvicorn.run(
        api_server.app, host=host, port=port, log_level=settings.log_level.lower()
    )


@cli.command()
@click.argument("job_type")
@click.argument("input_file", type=click.File("r"))
@click.option("--api-url", default="http://localhost:8000", help="TaskForge API URL")
@click.option(
    "--priority",
    type=click.Choice(["low", "medium", "high", "critical"]),
    default="medium",
    help="Job priority",
)
def submit(job_type: str, input_file, api_url: str, priority: str):
    """Submit a job from JSON file"""
    import httpx

    try:
        input_data = json.load(input_file)

        with httpx.Client() as client:
            response = client.post(
                f"{api_url}/api/jobs/{job_type}",
                json={"input_data": input_data, "priority": priority},
            )
            response.raise_for_status()
            result = response.json()
            click.echo(f"Job submitted successfully. Job ID: {result['job_id']}")
    except Exception as e:
        click.echo(f"Error submitting job: {str(e)}", err=True)


@cli.command()
@click.argument("job_id")
@click.option("--api-url", default="http://localhost:8000", help="TaskForge API URL")
def status(job_id: str, api_url: str):
    """Get job status"""
    import httpx

    try:
        with httpx.Client() as client:
            response = client.get(f"{api_url}/api/jobs/{job_id}")
            response.raise_for_status()
            job = response.json()

            click.echo(f"Job {job_id}:")
            click.echo(f"Status: {job['status']}")
            click.echo(f"Type: {job['job_type']}")
            if job.get("error"):
                click.echo(f"Error: {job['error']}")
            if job.get("execution_time"):
                click.echo(f"Execution Time: {job['execution_time']}s")
    except Exception as e:
        click.echo(f"Error getting job status: {str(e)}", err=True)


def main():
    """Main CLI entry point"""
    cli()


if __name__ == "__main__":
    main()
