# flake8: noqa


import asyncio
from datetime import datetime
from datetime import timedelta
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

import structlog

from taskforge.exceptions import WorkflowError
from taskforge.workflow.models import CallbackNode
from taskforge.workflow.models import DecisionNode
from taskforge.workflow.models import ParallelNode
from taskforge.workflow.models import TaskNode
from taskforge.workflow.models import WaitNode
from taskforge.workflow.models import Workflow
from taskforge.workflow.models import WorkflowNode
from taskforge.workflow.models import WorkflowStatus

logger = structlog.get_logger()


class WorkflowExecutor:
    """Executes workflow nodes"""

    def __init__(self, orchestrator):
        self.orchestrator = orchestrator
        self.logger = logger.bind(component="WorkflowExecutor")

    async def execute_node(self, workflow: Workflow, node: WorkflowNode) -> None:
        """Execute a single workflow node"""
        node.started_at = datetime.now(timezone.utc)
        node.status = WorkflowStatus.RUNNING

        try:
            if isinstance(node, TaskNode):
                await self._execute_task_node(workflow, node)
            elif isinstance(node, DecisionNode):
                await self._execute_decision_node(workflow, node)
            elif isinstance(node, ParallelNode):
                await self._execute_parallel_node(workflow, node)
            elif isinstance(node, WaitNode):
                await self._execute_wait_node(workflow, node)
            elif isinstance(node, CallbackNode):
                await self._execute_callback_node(workflow, node)
            else:
                raise WorkflowError(workflow.id, f"Unknown node type: {type(node)}")

            node.status = WorkflowStatus.COMPLETED
            node.completed_at = datetime.now(timezone.utc)

        except Exception as e:
            node.status = WorkflowStatus.FAILED
            node.error = str(e)
            node.completed_at = datetime.now(timezone.utc)
            raise WorkflowError(
                workflow.id, f"Node {node.id} execution failed: {str(e)}"
            )

    async def _execute_task_node(self, workflow: Workflow, node: TaskNode) -> None:
        """Execute a task node"""
        # Submit job
        job_id = await self.orchestrator.submit_job(
            job_type=node.job_type,
            input_data=self._resolve_variables(node.input_data, workflow.context),
            metadata={"workflow_id": workflow.id, "node_id": node.id},
        )

        node.job_id = job_id

        # Wait for completion
        while True:
            job_state = await self.orchestrator.get_job_state(job_id)

            if job_state.status == "completed":
                workflow.context[node.id] = job_state.result
                break
            elif job_state.status == "failed":
                raise WorkflowError(workflow.id, f"Job failed: {job_state.error}")

            await asyncio.sleep(1)

    async def _execute_decision_node(
        self, workflow: Workflow, node: DecisionNode
    ) -> None:
        """Execute a decision node"""
        result = self._evaluate_condition(node.condition, workflow.context)

        node.condition_result = result

        # Update dependencies for chosen branch
        branch_id = node.true_branch if result else node.false_branch
        workflow.nodes[branch_id].depends_on.append(node.id)

    async def _execute_parallel_node(
        self, workflow: Workflow, node: ParallelNode
    ) -> None:
        """Execute a parallel node"""
        tasks = []

        for branch in node.branches:
            for node_id in branch:
                # Add dependency
                workflow.nodes[node_id].depends_on.append(node.id)

                # Create execution task
                task = asyncio.create_task(
                    self.execute_node(workflow, workflow.nodes[node_id])
                )
                tasks.append(task)

        # Wait according to join policy
        if node.join_policy == "all":
            await asyncio.gather(*tasks)
        elif node.join_policy == "any":
            done, pending = await asyncio.wait(
                tasks, return_when=asyncio.FIRST_COMPLETED
            )
            for task in pending:
                task.cancel()
        else:
            # N of M completion
            n = int(node.join_policy)
            done, pending = await asyncio.wait(
                tasks, return_when=asyncio.N_COMPLETED(n)
            )
            for task in pending:
                task.cancel()

    async def _execute_wait_node(self, workflow: Workflow, node: WaitNode) -> None:
        """Execute a wait node"""
        if not node.wait_until:
            node.wait_until = datetime.now(timezone.utc) + timedelta(
                seconds=node.wait_time
            )

        while datetime.now(timezone.utc) < node.wait_until:
            await asyncio.sleep(1)

    async def _execute_callback_node(
        self, workflow: Workflow, node: CallbackNode
    ) -> None:
        """Execute a callback node"""
        if not node.callback_received:
            # Send callback request
            await self._send_callback(workflow, node)
            node.status = WorkflowStatus.WAITING
            return

        # Process callback data
        workflow.context[node.id] = node.callback_data

    def _resolve_variables(
        self, data: Dict[str, Any], context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Resolve variables in data using workflow context"""
        import jinja2

        result = {}
        env = jinja2.Environment()

        for key, value in data.items():
            if isinstance(value, str):
                template = env.from_string(value)
                result[key] = template.render(**context)
            elif isinstance(value, dict):
                result[key] = self._resolve_variables(value, context)
            elif isinstance(value, list):
                result[key] = [
                    self._resolve_variables(item, context)
                    if isinstance(item, dict)
                    else item
                    for item in value
                ]
            else:
                result[key] = value

        return result

    def _evaluate_condition(self, condition: str, context: Dict[str, Any]) -> bool:
        """Evaluate a condition using workflow context"""
        import jinja2

        env = jinja2.Environment()
        template = env.from_string(
            f"{{% if {condition} %}}true{{% else %}}false{{% endif %}}"
        )
        return template.render(**context) == "true"

    async def _send_callback(self, workflow: Workflow, node: CallbackNode) -> None:
        """Send callback request"""
        import aiohttp

        data = {
            "workflow_id": workflow.id,
            "node_id": node.id,
            "callback_token": node.callback_token,
            "callback_url": f"/api/workflows/{workflow.id}/callback/{node.id}",
        }

        async with aiohttp.ClientSession() as session:
            try:
                async with session.post(
                    node.callback_url, json=data, timeout=node.callback_timeout
                ) as response:
                    response.raise_for_status()
            except Exception as e:
                raise WorkflowError(workflow.id, f"Failed to send callback: {str(e)}")
