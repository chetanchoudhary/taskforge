from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from prometheus_fastapi_instrumentator import Instrumentator
from contextlib import asynccontextmanager
import structlog

from taskforge.jobs.base import JobStatus, JobPriority
from taskforge.orchestrator import JobOrchestrator
from taskforge.storage.postgres import PostgresJobStorage
from taskforge.broker.rabbitmq import RabbitMQBroker

logger = structlog.get_logger()

class TaskForgeAPI:
    """API server for TaskForge"""
    
    def __init__(self,
                 orchestrator: JobOrchestrator):
        self.orchestrator = orchestrator
        self.app = FastAPI(
            title="TaskForge API",
            description="Distributed Job Processing API",
            version="1.0.0",
            lifespan=self.lifespan
        )
        self.setup_routes()
        self.setup_middleware()
        self.setup_monitoring()
        
    @asynccontextmanager
    async def lifespan(self, app: FastAPI):
        """Manage API server lifecycle"""
        logger.info("Starting TaskForge API")
        await self.orchestrator.start()
        yield
        logger.info("Shutting down TaskForge API")
        await self.orchestrator.shutdown()
        
    def setup_middleware(self):
        """Set up API middleware"""
        self.app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )
        
    def setup_monitoring(self):
        """Set up Prometheus metrics"""
        Instrumentator().instrument(self.app).expose(self.app)
        
    def setup_routes(self):
        """Set up API routes"""
        
        @self.app.post("/api/jobs/{job_type}")
        async def submit_job(
            job_type: str,
            input_data: dict,
            priority: JobPriority = JobPriority.MEDIUM
        ):
            """Submit a new job"""
            try:
                job_id = await self.orchestrator.submit_job(
                    job_type=job_type,
                    input_data=input_data,
                    priority=priority
                )
                return {"job_id": job_id}
            except ValueError as e:
                raise HTTPException(status_code=400, detail=str(e))
                
        @self.app.get("/api/jobs/{job_id}")
        async def get_job_status(job_id: str):
            """Get job status"""
            job_state = await self.orchestrator.get_job_state(job_id)
            if not job_state:
                raise HTTPException(
                    status_code=404,
                    detail=f"Job {job_id} not found"
                )
            return job_state
            
        @self.app.get("/api/jobs")
        async def list_jobs(
            status: Optional[JobStatus] = None,
            limit: int = 100,
            offset: int = 0
        ):
            """List jobs with optional filters"""
            jobs = await self.orchestrator.list_jobs(
                status=status,
                limit=limit,
                offset=offset
            )
            return {
                "jobs": jobs,
                "total": len(jobs),
                "limit": limit,
                "offset": offset
            }
            
        @self.app.post("/api/jobs/{job_id}/cancel")
        async def cancel_job(job_id: str):
            """Cancel a job"""
            success = await self.orchestrator.cancel_job(job_id)
            if not success:
                raise HTTPException(
                    status_code=404,
                    detail=f"Job {job_id} not found or already completed"
                )
            return {"status": "cancelled"}
            
        @self.app.get("/api/health")
        async def health_check():
            """Health check endpoint"""
            health_status = await self.orchestrator.monitor_system_health()
            if health_status["status"] != "healthy":
                raise HTTPException(
                    status_code=503,
                    detail=health_status
                )
            return health_status
            
        @self.app.get("/api/metrics/workers")
        async def worker_metrics():
            """Get worker pool metrics"""
            return {
                "active_workers": len([w for w in self.worker_pool.workers if w.is_running]),
                "total_workers": self.worker_pool.num_workers,
                "queue_depths": await self.orchestrator.get_queue_depths(),
                "processing_stats": {
                    worker.worker_id: {
                        "jobs_processed": worker.jobs_processed,
                        "errors": worker.error_count,
                        "uptime_seconds": worker.uptime_seconds
                    }
                    for worker in self.worker_pool.workers
                }
            }
            
    def start(self, host: str = "0.0.0.0", port: int = 8000):
        """Start the API server"""
        import uvicorn
        uvicorn.run(self.app, host=host, port=port)