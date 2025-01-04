# taskforge/storage/postgres.py
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from sqlalchemy import (
    JSON,
    Column,
    DateTime,
    Integer,
    String,
    and_,
    delete,
    func,
    select,
)
from sqlalchemy import Enum as SQLEnum
from sqlalchemy.ext.declarative import declarative_base

# from sqlalchemy.future import select
from sqlalchemy.sql import text

from taskforge.exceptions import StorageError
from taskforge.jobs.base import JobMetadata, JobResult, JobStatus
from taskforge.storage.base import BaseStorage

Base = declarative_base()


class JobRecord(Base):
    """Database model for storing job information"""

    __tablename__ = "jobs"

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


class PostgresJobStorage(BaseStorage):
    """PostgreSQL implementation of job storage"""

    def __init__(self, engine, session_factory):
        self.engine = engine
        self.session_factory = session_factory

    async def initialize(self) -> None:
        """Initialize storage schema"""
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    async def ping(self) -> bool:
        """Check storage connectivity"""
        try:
            async with self.session_factory() as session:
                await session.execute(text("SELECT 1"))
            return True
        except Exception:
            return False

    async def save_job(
        self,
        job_id: str,
        job_type: str,
        input_data: Dict[str, Any],
        metadata: JobMetadata,
    ) -> None:
        """Save a new job"""
        async with self.session_factory() as session:
            async with session.begin():
                try:
                    job_record = JobRecord(
                        id=job_id,
                        job_type=job_type,
                        status=JobStatus.PENDING,
                        input_data=input_data,
                        job_metadata=metadata.dict(),
                        created_at=datetime.utcnow(),
                        updated_at=datetime.utcnow(),
                    )
                    session.add(job_record)
                except Exception as e:
                    raise StorageError(f"Failed to save job: {str(e)}")

    async def update_job_status(
        self,
        job_id: str,
        status: JobStatus,
        result: Optional[Dict[str, Any]] = None,
        error: Optional[str] = None,
        execution_time: Optional[float] = None,
    ) -> None:
        """Update job status and result"""
        async with self.session_factory() as session:
            async with session.begin():
                try:
                    job_record = await session.get(JobRecord, job_id)
                    if not job_record:
                        raise StorageError(f"Job {job_id} not found")

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
                except Exception as e:
                    raise StorageError(f"Failed to update job status: {str(e)}")

    async def get_job_state(self, job_id: str) -> Optional[JobResult]:
        """Get current state of a job"""
        async with self.session_factory() as session:
            try:
                job_record = await session.get(JobRecord, job_id)
                if job_record:
                    return JobResult(
                        job_id=job_record.id,
                        status=job_record.status,
                        result=job_record.output_data,
                        error=job_record.error,
                        execution_time=job_record.execution_time / 1000
                        if job_record.execution_time
                        else None,
                        metadata=JobMetadata(**job_record.job_metadata),
                    )
                return None
            except Exception as e:
                raise StorageError(f"Failed to get job state: {str(e)}")

    async def list_jobs(
        self,
        status: Optional[JobStatus] = None,
        job_type: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[JobResult]:
        """List jobs with optional filters"""
        async with self.session_factory() as session:
            try:
                query = select(JobRecord)

                # Apply filters
                if status:
                    query = query.where(JobRecord.status == status)
                if job_type:
                    query = query.where(JobRecord.job_type == job_type)
                if start_date:
                    query = query.where(JobRecord.created_at >= start_date)
                if end_date:
                    query = query.where(JobRecord.created_at <= end_date)

                # Apply pagination
                query = query.order_by(JobRecord.created_at.desc())
                query = query.limit(limit).offset(offset)

                result = await session.execute(query)
                records = result.scalars().all()

                return [
                    JobResult(
                        job_id=record.id,
                        status=record.status,
                        result=record.output_data,
                        error=record.error,
                        execution_time=record.execution_time / 1000
                        if record.execution_time
                        else None,
                        metadata=JobMetadata(**record.job_metadata),
                    )
                    for record in records
                ]
            except Exception as e:
                raise StorageError(f"Failed to list jobs: {str(e)}")

    async def cleanup_old_jobs(
        self, days: int, status: Optional[List[JobStatus]] = None
    ) -> int:
        """Clean up old job records"""
        async with self.session_factory() as session:
            async with session.begin():
                try:
                    cutoff_date = datetime.utcnow() - timedelta(days=days)
                    query = delete(JobRecord).where(JobRecord.created_at < cutoff_date)

                    if status:
                        query = query.where(JobRecord.status.in_(status))

                    result = await session.execute(query)
                    return result.rowcount
                except Exception as e:
                    raise StorageError(f"Failed to cleanup old jobs: {str(e)}")

    async def get_job_counts(
        self, job_type: Optional[str] = None
    ) -> Dict[JobStatus, int]:
        """Get counts of jobs by status"""
        async with self.session_factory() as session:
            try:
                query = select(JobRecord.status, func.count(JobRecord.id)).group_by(
                    JobRecord.status
                )

                if job_type:
                    query = query.where(JobRecord.job_type == job_type)

                result = await session.execute(query)
                return dict(result.fetchall())
            except Exception as e:
                raise StorageError(f"Failed to get job counts: {str(e)}")

    async def get_failed_jobs(
        self, max_retries: int, limit: int = 100
    ) -> List[JobResult]:
        """Get failed jobs eligible for retry"""
        async with self.session_factory() as session:
            try:
                query = (
                    select(JobRecord)
                    .where(
                        and_(
                            JobRecord.status == JobStatus.FAILED,
                            JobRecord.retries < max_retries,
                        )
                    )
                    .order_by(JobRecord.updated_at)
                    .limit(limit)
                )

                result = await session.execute(query)
                records = result.scalars().all()

                return [
                    JobResult(
                        job_id=record.id,
                        status=record.status,
                        result=record.output_data,
                        error=record.error,
                        execution_time=record.execution_time / 1000
                        if record.execution_time
                        else None,
                        metadata=JobMetadata(**record.job_metadata),
                    )
                    for record in records
                ]
            except Exception as e:
                raise StorageError(f"Failed to get failed jobs: {str(e)}")

    async def get_stalled_jobs(
        self, timeout_minutes: int, limit: int = 100
    ) -> List[JobResult]:
        """Get jobs that have been running too long"""
        async with self.session_factory() as session:
            try:
                stall_threshold = datetime.utcnow() - timedelta(minutes=timeout_minutes)
                query = (
                    select(JobRecord)
                    .where(
                        and_(
                            JobRecord.status == JobStatus.RUNNING,
                            JobRecord.started_at < stall_threshold,
                        )
                    )
                    .limit(limit)
                )

                result = await session.execute(query)
                records = result.scalars().all()

                return [
                    JobResult(
                        job_id=record.id,
                        status=record.status,
                        result=record.output_data,
                        error=record.error,
                        execution_time=record.execution_time / 1000
                        if record.execution_time
                        else None,
                        metadata=JobMetadata(**record.job_metadata),
                    )
                    for record in records
                ]
            except Exception as e:
                raise StorageError(f"Failed to get stalled jobs: {str(e)}")
