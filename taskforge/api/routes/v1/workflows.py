from fastapi import APIRouter, Depends, HTTPException, Path

from taskforge.api.dependencies import get_workflow_engine, verify_api_key
from taskforge.api.models import WorkflowCreate, WorkflowResponse
from taskforge.exceptions import WorkflowError

router = APIRouter(prefix="/workflows", tags=["Workflows"])


@router.post("/", response_model=WorkflowResponse)
async def create_workflow(
    workflow: WorkflowCreate,
    engine=Depends(get_workflow_engine),
    api_key: str = Depends(verify_api_key),
):
    """Create a new workflow"""
    try:
        result = await engine.create_workflow(
            name=workflow.name, nodes=workflow.nodes, context=workflow.context
        )
        return result
    except WorkflowError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/{workflow_id}/start")
async def start_workflow(
    workflow_id: str = Path(..., description="ID of the workflow to start"),
    engine=Depends(get_workflow_engine),
    api_key: str = Depends(verify_api_key),
):
    """Start workflow execution"""
    try:
        await engine.start_workflow(workflow_id)
        return {"status": "started"}
    except WorkflowError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/{workflow_id}", response_model=WorkflowResponse)
async def get_workflow(
    workflow_id: str = Path(..., description="ID of the workflow to retrieve"),
    engine=Depends(get_workflow_engine),
    api_key: str = Depends(verify_api_key),
):
    """Get workflow status"""
    workflow = await engine.get_workflow(workflow_id)
    if not workflow:
        raise HTTPException(status_code=404, detail=f"Workflow {workflow_id} not found")
    return workflow


@router.post("/{workflow_id}/cancel")
async def cancel_workflow(
    workflow_id: str = Path(..., description="ID of the workflow to cancel"),
    engine=Depends(get_workflow_engine),
    api_key: str = Depends(verify_api_key),
):
    """Cancel workflow execution"""
    try:
        await engine.cancel_workflow(workflow_id)
        return {"status": "cancelled"}
    except WorkflowError as e:
        raise HTTPException(status_code=400, detail=str(e))
