from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
from sqlalchemy import Column, String, JSON, DateTime, Integer, Enum as SQLEnum, and_, delete
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.future import select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text
from urllib.parse import urlparse

from taskforge.jobs.base import JobStatus, JobMetadata, JobResult

Base = declarative_base()

class JobRecord(Base):
    """Database model for storing job information"""
    __tablename__ = 'jobs'
    
    id = Column(String, primary_key=True)
    job_type = Column(String, nullable=False)
    status = Column(SQLEnum(JobStatus), nullable=False)
    input_data = Column(JSON, nullable=False)
    output_data = Column(JSON, nullable=True)
    error = Column(String, nullable=True)
    retries = Column(Integer, default=0)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    job_metadata = Column(JSON, nullable=True)
    execution_time = Column(Integer, nullable=True)  # in milliseconds

class PostgresJobStorage:
    """PostgreSQL implementation of job storage"""
    
    def __init__(self, connection_url: str):
        self.connection_url = connection_url
        parsed_url = urlparse(connection_url)
        self.database_name = parsed_url.path[1:]  # Remove leading '/'
        
        # Create connection URL to postgres database
        self.postgres_url = f"{parsed_url.scheme}://{parsed_url.username}:{parsed_url.password}@{parsed_url.hostname}:{parsed_url.port}/postgres"
        
        # Initialize engine as None - will be set up after database creation
        self.engine = None
        self.async_session = None

    async def create_database_if_not_exists(self):
        """Create the database if it doesn't exist"""
        # Create temporary engine to connect to 'postgres' database
        temp_engine = create_async_engine(
            self.postgres_url,
            isolation_level="AUTOCOMMIT"
        )

        try:
            async with temp_engine.connect() as conn:
                # Check if database exists
                result = await conn.execute(
                    text(f"SELECT 1 FROM pg_database WHERE datname = '{self.database_name}'")
                )
                database_exists = result.scalar() is not None

                if not database_exists:
                    # Create database if it doesn't exist
                    await conn.execute(text(f"CREATE DATABASE {self.database_name}"))
                    print(f"Created database {self.database_name}")
        finally:
            await temp_engine.dispose()

    async def setup_engine(self):
        """Set up the main engine and session maker"""
        self.engine = create_async_engine(
            self.connection_url,
            echo=False,
            pool_size=20,
            max_overflow=10,
            pool_pre_ping=True
        )
        self.async_session = sessionmaker(
            self.engine,
            class_=AsyncSession,
            expire_on_commit=False
        )

    async def initialize(self):
        """Initialize the database and tables"""
        # First, ensure database exists
        await self.create_database_if_not_exists()
        
        # Set up engine and session maker
        await self.setup_engine()
        
        # Create tables
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    async def save_job(self, 
                      job_id: str,
                      job_type: str, 
                      input_data: Dict[str, Any],
                      job_metadata: JobMetadata) -> None:
        """Save a new job"""
        async with self.async_session() as session:
            async with session.begin():
                job_record = JobRecord(
                    id=job_id,
                    job_type=job_type,
                    status=JobStatus.PENDING,
                    input_data=input_data,
                    job_metadata=job_metadata.dict(),
                    created_at=datetime.utcnow()
                )
                session.add(job_record)

    async def update_job_status(self,
                              job_id: str,
                              status: JobStatus,
                              result: Optional[Dict[str, Any]] = None,
                              error: Optional[str] = None,
                              execution_time: Optional[float] = None) -> None:
        """Update job status and result"""
        async with self.async_session() as session:
            async with session.begin():
                job_record = await session.get(JobRecord, job_id)
                if job_record:
                    job_record.status = status
                    job_record.updated_at = datetime.utcnow()
                    
                    if status == JobStatus.RUNNING:
                        job_record.started_at = datetime.utcnow()
                    elif status in (JobStatus.COMPLETED, JobStatus.FAILED):
                        job_record.completed_at = datetime.utcnow()
                    
                    if result is not None:
                        job_record.output_data = result
                    if error is not None:
                        job_record.error = error
                    if execution_time is not None:
                        job_record.execution_time = int(execution_time * 1000)

    async def get_job_state(self, job_id: str) -> Optional[JobResult]:
        """Get current state of a job"""
        async with self.async_session() as session:
            job_record = await session.get(JobRecord, job_id)
            if job_record:
                return JobResult(
                    job_id=job_record.id,
                    status=job_record.status,
                    result=job_record.output_data,
                    error=job_record.error,
                    execution_time=job_record.execution_time or 0,
                    metadata=JobMetadata(**job_record.job_metadata)
                )
            return None

    async def get_jobs_by_status(self, 
                                status: JobStatus,
                                limit: int = 100,
                                offset: int = 0) -> List[JobResult]:
        """Get jobs with specific status"""
        async with self.async_session() as session:
            result = await session.execute(
                select(JobRecord)
                .where(JobRecord.status == status)
                .order_by(JobRecord.created_at.desc())
                .limit(limit)
                .offset(offset)
            )
            records = result.scalars().all()
            return [
                JobResult(
                    job_id=record.id,
                    status=record.status,
                    result=record.output_data,
                    error=record.error,
                    execution_time=record.execution_time or 0,
                    metadata=JobMetadata(**record.job_metadata)
                )
                for record in records
            ]

    async def cleanup_old_jobs(self, days: int) -> int:
        """Clean up jobs older than specified days"""
        cutoff_date = datetime.utcnow() - timedelta(days=days)
        async with self.async_session() as session:
            async with session.begin():
                result = await session.execute(
                    delete(JobRecord).where(
                        and_(
                            JobRecord.created_at < cutoff_date,
                            JobRecord.status.in_([
                                JobStatus.COMPLETED,
                                JobStatus.FAILED
                            ])
                        )
                    )
                )
                return result.rowcount