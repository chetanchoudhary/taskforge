from fastapi import APIRouter

from .v1 import v1_router

# Main API router that includes versioned routes
api_router = APIRouter()
api_router.include_router(v1_router)
