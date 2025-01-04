# flake8: noqa

import asyncio
import uuid
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

import jinja2
import structlog

from taskforge.exceptions import WorkflowError
from taskforge.jobs.base import JobStatus
from taskforge.orchestrator import JobOrchestrator
from taskforge.workflow.models import CallbackNode
from taskforge.workflow.models import DecisionNode
from taskforge.workflow.models import ParallelNode
from taskforge.workflow.models import TaskNode
from taskforge.workflow.models import WaitNode
from taskforge.workflow.models import Workflow
from taskforge.workflow.models import WorkflowNode
from taskforge.workflow.models import WorkflowNodeType
from taskforge.workflow.models import WorkflowStatus

logger = structlog.get_logger()


class WorkflowEngine:
    """Workflow execution engine"""

    def __init__(self, orchestrator: JobOrchestrator):
        self.orchestrator = orchestrator
        self.workflows: Dict[str, Workflow] = {}
        self.template_env = jinja2.Environment()
        self._running = False
        self._process_task: Optional[asyncio.Task] = None
        self._lock = asyncio.Lock()

    async def start(self):
        """Start the workflow engine"""
        async with self._lock:
            if self._running:
                return

            self._running = True
            self._process_task = asyncio.create_task(self._process_workflows())
            logger.info("Workflow engine started")

    async def stop(self):
        """Stop the workflow engine"""
        async with self._lock:
            if not self._running:
                return

            self._running = False
            if self._process_task:
                self._process_task.cancel()
                try:
                    await self._process_task
                except asyncio.CancelledError:
                    pass

            logger.info("Workflow engine stopped")

    async def create_workflow(
        self,
        name: str,
        nodes: Dict[str, WorkflowNode],
        context: Optional[Dict[str, Any]] = None,
    ) -> Workflow:
        """Create a new workflow"""
        workflow = Workflow(
            id=str(uuid.uuid4()), name=name, nodes=nodes, context=context or {}
        )

        async with self._lock:
            self.workflows[workflow.id] = workflow

        logger.info("Workflow created", workflow_id=workflow.id, name=name)

        return workflow

    async def start_workflow(self, workflow_id: str):
        """Start workflow execution"""
        async with self._lock:
            workflow = self.workflows.get(workflow_id)
            if not workflow:
                raise WorkflowError(f"Workflow {workflow_id} not found")

            if workflow.status != WorkflowStatus.PENDING:
                raise WorkflowError(
                    f"Workflow {workflow_id} is already {workflow.status}"
                )

            workflow.status = WorkflowStatus.RUNNING
            workflow.started_at = datetime.now(timezone.utc)

        logger.info("Workflow started", workflow_id=workflow_id)

    async def cancel_workflow(self, workflow_id: str):
        """Cancel workflow execution"""
        async with self._lock:
            workflow = self.workflows.get(workflow_id)
            if not workflow:
                raise WorkflowError(f"Workflow {workflow_id} not found")

            if workflow.status not in (WorkflowStatus.RUNNING, WorkflowStatus.WAITING):
                raise WorkflowError(
                    f"Workflow {workflow_id} cannot be cancelled in state {workflow.status}"
                )

            workflow.status = WorkflowStatus.CANCELLED

            # Cancel any running tasks
            for node in workflow.nodes.values():
                if (
                    isinstance(node, TaskNode)
                    and node.status == WorkflowStatus.RUNNING
                    and node.job_id
                ):
                    await self.orchestrator.cancel_job(node.job_id)

        logger.info("Workflow cancelled", workflow_id=workflow_id)

    async def _process_workflows(self):
        """Main workflow processing loop"""
        while self._running:
            try:
                async with self._lock:
                    # Process each running workflow
                    for workflow in self.workflows.values():
                        if workflow.status == WorkflowStatus.RUNNING:
                            await self._process_workflow(workflow)

            except Exception as e:
                logger.error("Error processing workflows", error=str(e), exc_info=True)

            await asyncio.sleep(1)

    async def _process_workflow(self, workflow: Workflow):
        """Process a single workflow"""
        # Find ready nodes
        ready_nodes = self._get_ready_nodes(workflow)

        # Process each ready node
        for node in ready_nodes:
            await self._process_node(workflow, node)

        # Check if workflow is complete
        if self._is_workflow_complete(workflow):
            workflow.status = WorkflowStatus.COMPLETED
            workflow.completed_at = datetime.now(timezone.utc)
            logger.info("Workflow completed", workflow_id=workflow.id)

    def _get_ready_nodes(self, workflow: Workflow) -> List[WorkflowNode]:
        """Get nodes that are ready to execute"""
        ready_nodes = []

        for node in workflow.nodes.values():
            if node.status != WorkflowStatus.PENDING:
                continue

            # Check if dependencies are satisfied
            deps_satisfied = all(
                workflow.nodes[dep].status == WorkflowStatus.COMPLETED
                for dep in node.depends_on
            )

            if deps_satisfied:
                ready_nodes.append(node)

        return ready_nodes

    async def _process_node(self, workflow: Workflow, node: WorkflowNode):
        """Process a single workflow node"""
        try:
            node.status = WorkflowStatus.RUNNING
            node.started_at = datetime.now(timezone.utc)

            if node.type == WorkflowNodeType.TASK:
                await self._process_task_node(workflow, node)
            elif node.type == WorkflowNodeType.DECISION:
                await self._process_decision_node(workflow, node)
            elif node.type == WorkflowNodeType.PARALLEL:
                await self._process_parallel_node(workflow, node)
            elif node.type == WorkflowNodeType.WAIT:
                await self._process_wait_node(workflow, node)
            elif node.type == WorkflowNodeType.CALLBACK:
                await self._process_callback_node(workflow, node)

        except Exception as e:
            node.status = WorkflowStatus.FAILED
            node.error = str(e)
            workflow.status = WorkflowStatus.FAILED
            workflow.error = f"Node {node.id} failed: {str(e)}"
            logger.error(
                "Workflow node failed",
                workflow_id=workflow.id,
                node_id=node.id,
                error=str(e),
            )

    async def _process_task_node(self, workflow: Workflow, node: TaskNode):
        """Process a task execution node"""
        # Evaluate template variables in input data
        input_data = self._evaluate_templates(node.input_data, workflow.context)

        # Submit job
        job_id = await self.orchestrator.submit_job(
            job_type=node.job_type,
            input_data=input_data,
            metadata={"workflow_id": workflow.id, "node_id": node.id},
        )

        node.job_id = job_id

        # Wait for job completion
        while True:
            job_state = await self.orchestrator.get_job_state(job_id)
            if job_state.status == JobStatus.COMPLETED:
                node.status = WorkflowStatus.COMPLETED
                workflow.context[node.id] = job_state.result
                break
            elif job_state.status == JobStatus.FAILED:
                raise WorkflowError(f"Job failed: {job_state.error}")

            await asyncio.sleep(1)

    async def _process_decision_node(self, workflow: Workflow, node: DecisionNode):
        """Process a decision node"""
        # Evaluate condition
        condition_result = self._evaluate_condition(node.condition, workflow.context)

        node.condition_result = condition_result
        node.status = WorkflowStatus.COMPLETED

        # Update workflow to follow appropriate branch
        branch = node.true_branch if condition_result else node.false_branch
        workflow.nodes[branch].depends_on = [node.id]

    async def _process_parallel_node(self, workflow: Workflow, node: ParallelNode):
        """Process a parallel execution node"""
        # Start all branches
        for branch in node.branches:
            for node_id in branch:
                workflow.nodes[node_id].depends_on = [node.id]

        node.status = WorkflowStatus.COMPLETED

    async def _process_wait_node(self, workflow: Workflow, node: WaitNode):
        """Process a wait/delay node"""
        if not node.wait_until:
            node.wait_until = datetime.now(timezone.utc) + timedelta(
                seconds=node.wait_time
            )

        if datetime.now(timezone.utc) >= node.wait_until:
            node.status = WorkflowStatus.COMPLETED

    async def _process_callback_node(self, workflow: Workflow, node: CallbackNode):
        """Process an external callback node"""
        if not node.callback_received:
            # Send callback request
            await self._send_callback_request(workflow, node)
            node.status = WorkflowStatus.WAITING
            return

        # Callback received, complete node
        node.status = WorkflowStatus.COMPLETED
        workflow.context[node.id] = node.callback_data

    def _evaluate_templates(
        self, data: Dict[str, Any], context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Evaluate template variables in data"""
        result = {}
        for key, value in data.items():
            if isinstance(value, str):
                template = self.template_env.from_string(value)
                result[key] = template.render(**context)
            elif isinstance(value, dict):
                result[key] = self._evaluate_templates(value, context)
            elif isinstance(value, list):
                result[key] = [
                    self._evaluate_templates(item, context)
                    if isinstance(item, dict)
                    else item
                    for item in value
                ]
            else:
                result[key] = value
        return result

    def _evaluate_condition(self, condition: str, context: Dict[str, Any]) -> bool:
        """Evaluate a condition expression"""
        template = self.template_env.from_string(
            f"{{% if {condition} %}}true{{% else %}}false{{% endif %}}"
        )
        return template.render(**context) == "true"

    async def _send_callback_request(self, workflow: Workflow, node: CallbackNode):
        """Send external callback request"""
        import aiohttp

        callback_data = {
            "workflow_id": workflow.id,
            "node_id": node.id,
            "callback_token": node.callback_token,
            "callback_url": f"/api/workflows/{workflow.id}/nodes/{node.id}/callback",
        }

        async with aiohttp.ClientSession() as session:
            try:
                async with session.post(
                    node.callback_url, json=callback_data, timeout=30
                ) as response:
                    response.raise_for_status()
            except Exception as e:
                raise WorkflowError(f"Failed to send callback request: {str(e)}")

    def _is_workflow_complete(self, workflow: Workflow) -> bool:
        """Check if workflow is complete"""
        return all(
            node.status in (WorkflowStatus.COMPLETED, WorkflowStatus.FAILED)
            for node in workflow.nodes.values()
        )
