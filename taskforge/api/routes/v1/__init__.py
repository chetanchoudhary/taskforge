from fastapi import APIRouter

from .admin import router as admin_router
from .jobs import router as jobs_router
from .schedules import router as schedules_router
from .workflows import router as workflows_router

# Main v1 router
v1_router = APIRouter(prefix="/api/v1")

# Include all sub-routers
v1_router.include_router(jobs_router)
v1_router.include_router(workflows_router)
v1_router.include_router(schedules_router)
v1_router.include_router(admin_router)
