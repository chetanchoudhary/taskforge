from typing import Dict, Type, Optional, List
import importlib
import pkgutil
from pathlib import Path
import structlog

from taskforge.jobs.base import Job, JobConfig
from taskforge.core.config import settings

logger = structlog.get_logger()

class JobRegistry:
    """
    Central registry for job types and their configurations
    """
    
    def __init__(self):
        self._jobs: Dict[str, Type[Job]] = {}
        self._configs: Dict[str, JobConfig] = {}
    
    def register(self,
                job_class: Type[Job],
                config: Optional[JobConfig] = None) -> None:
        """
        Register a job type with optional configuration
        
        Args:
            job_class: Job class to register
            config: Optional job configuration
        """
        job_type = job_class.__name__
        
        if job_type in self._jobs:
            logger.warning(f"Job type {job_type} already registered, overwriting")
        
        self._jobs[job_type] = job_class
        self._configs[job_type] = config or JobConfig(
            max_retries=settings.max_retries,
            timeout_seconds=settings.default_job_timeout,
            queue_name=job_type
        )
        
        logger.info(
            "Registered job type",
            job_type=job_type,
            config=self._configs[job_type].dict()
        )
    
    def get_job_class(self, job_type: str) -> Type[Job]:
        """Get job class by type name"""
        if job_type not in self._jobs:
            raise ValueError(f"Unknown job type: {job_type}")
        return self._jobs[job_type]
    
    def get_job_config(self, job_type: str) -> Optional[JobConfig]:
        """Get job configuration"""
        return self._configs.get(job_type)
    
    def list_jobs(self) -> List[str]:
        """Get list of registered job types"""
        return list(self._jobs.keys())
    
    @classmethod
    def discover_jobs(cls, package_path: str) -> "JobRegistry":
        """
        Automatically discover and register job classes in a package
        
        Args:
            package_path: Import path to package containing job classes
            
        Returns:
            JobRegistry with discovered jobs
        """
        registry = cls()
        
        try:
            package = importlib.import_module(package_path)
        except ImportError as e:
            logger.error(
                "Failed to import job package",
                package=package_path,
                error=str(e)
            )
            return registry
        
        # Get package directory
        if hasattr(package, '__path__'):
            pkg_path = package.__path__[0]
        else:
            pkg_path = str(Path(package.__file__).parent)
        
        # Scan for job modules
        for _, name, _ in pkgutil.iter_modules([pkg_path]):
            try:
                module = importlib.import_module(f"{package_path}.{name}")
                
                # Look for Job subclasses
                for attr_name in dir(module):
                    attr = getattr(module, attr_name)
                    if (isinstance(attr, type) and 
                        issubclass(attr, Job) and 
                        attr != Job):
                        registry.register(attr)
                        
            except Exception as e:
                logger.error(
                    "Failed to load job module",
                    module=name,
                    error=str(e)
                )
                
        return registry
    
    def __len__(self) -> int:
        return len(self._jobs)
    
    def __contains__(self, job_type: str) -> bool:
        return job_type in self._jobs
    
    def __iter__(self):
        return iter(self._jobs.items())

# Global job registry
registry = JobRegistry()
