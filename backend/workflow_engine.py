from typing import Dict, Any, List, Tuple, Optional
from collections import deque
from datetime import datetime
from sqlalchemy.orm import Session
import models


class NodeStatus:
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


class WorkflowValidator:
    @staticmethod
    def validate_dag(dag_config: dict) -> Tuple[bool, Optional[str]]:
        nodes = dag_config.get("nodes", [])
        edges = dag_config.get("edges", [])

        if not nodes:
            return False, "DAG must contain at least one node"

        node_keys = {node.get("key") for node in nodes}
        if None in node_keys:
            return False, "All nodes must have a 'key' field"

        for edge in edges:
            source = edge.get("source")
            target = edge.get("target")
            if source not in node_keys:
                return False, f"Edge source '{source}' not found in nodes"
            if target not in node_keys:
                return False, f"Edge target '{target}' not found in nodes"

        if WorkflowValidator.detect_cycle(nodes, edges):
            return False, "DAG contains a cycle"

        return True, None

    @staticmethod
    def detect_cycle(nodes: List[dict], edges: List[dict]) -> bool:
        node_keys = [node["key"] for node in nodes]
        adjacency = {key: [] for key in node_keys}
        in_degree = {key: 0 for key in node_keys}

        for edge in edges:
            source = edge["source"]
            target = edge["target"]
            adjacency[source].append(target)
            in_degree[target] += 1

        queue = deque([key for key in node_keys if in_degree[key] == 0])
        visited_count = 0

        while queue:
            current = queue.popleft()
            visited_count += 1
            for neighbor in adjacency[current]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

        return visited_count != len(node_keys)

    @staticmethod
    def topological_sort(nodes: List[dict], edges: List[dict]) -> List[str]:
        node_keys = [node["key"] for node in nodes]
        adjacency = {key: [] for key in node_keys}
        in_degree = {key: 0 for key in node_keys}

        for edge in edges:
            source = edge["source"]
            target = edge["target"]
            adjacency[source].append(target)
            in_degree[target] += 1

        queue = deque([key for key in node_keys if in_degree[key] == 0])
        result = []

        while queue:
            current = queue.popleft()
            result.append(current)
            for neighbor in adjacency[current]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

        if len(result) != len(node_keys):
            raise ValueError("Graph contains a cycle, cannot perform topological sort")

        return result


class WorkflowExecutor:
    def __init__(self, workflow_id: int, db_session_factory):
        self.workflow_id = workflow_id
        self.db_session_factory = db_session_factory
        self.node_states: Dict[str, Dict[str, Any]] = {}
        self.workflow_execution: Optional[models.WorkflowExecution] = None

    def execute(self) -> Dict[str, Any]:
        db = self.db_session_factory()
        try:
            workflow = db.query(models.Workflow).filter(
                models.Workflow.id == self.workflow_id
            ).first()
            if not workflow:
                return {"success": False, "error": f"Workflow {self.workflow_id} not found"}

            dag_config = workflow.dag_config
            nodes = dag_config.get("nodes", [])
            edges = dag_config.get("edges", [])

            valid, error = WorkflowValidator.validate_dag(dag_config)
            if not valid:
                return {"success": False, "error": error}

            self.workflow_execution = models.WorkflowExecution(
                workflow_id=self.workflow_id,
                status=NodeStatus.RUNNING,
                start_time=datetime.utcnow(),
                node_states={}
            )
            db.add(self.workflow_execution)
            db.commit()
            db.refresh(self.workflow_execution)

            execution_order = WorkflowValidator.topological_sort(nodes, edges)

            node_map = {node["key"]: node for node in nodes}

            for node_key in execution_order:
                self.node_states[node_key] = {
                    "status": NodeStatus.PENDING,
                    "start_time": None,
                    "end_time": None,
                    "pipeline_id": node_map[node_key].get("pipeline_id"),
                    "error": None
                }

            self.workflow_execution.node_states = self.node_states
            db.commit()

            for node_key in execution_order:
                if self.node_states[node_key]["status"] == NodeStatus.SKIPPED:
                    continue

                pipeline_id = node_map[node_key].get("pipeline_id")
                if not pipeline_id:
                    self.node_states[node_key]["status"] = NodeStatus.FAILED
                    self.node_states[node_key]["end_time"] = datetime.utcnow().isoformat()
                    self.node_states[node_key]["error"] = "No pipeline_id specified for node"
                    self.mark_downstream_skipped(node_key)
                    self._update_execution_state(db)
                    continue

                success = self.execute_pipeline_node(node_key, pipeline_id)
                self._update_execution_state(db)

                if not success:
                    self.mark_downstream_skipped(node_key)
                    self._update_execution_state(db)
                    break

            all_completed = all(
                state["status"] == NodeStatus.COMPLETED
                for state in self.node_states.values()
            )
            any_failed = any(
                state["status"] == NodeStatus.FAILED
                for state in self.node_states.values()
            )

            if all_completed:
                self.workflow_execution.status = NodeStatus.COMPLETED
            elif any_failed:
                self.workflow_execution.status = NodeStatus.FAILED
            else:
                self.workflow_execution.status = NodeStatus.COMPLETED

            self.workflow_execution.end_time = datetime.utcnow()
            self.workflow_execution.node_states = self.node_states
            db.commit()

            return {
                "success": all_completed,
                "workflow_execution_id": self.workflow_execution.id,
                "node_states": self.node_states,
                "status": self.workflow_execution.status
            }

        except Exception as e:
            if self.workflow_execution:
                self.workflow_execution.status = NodeStatus.FAILED
                self.workflow_execution.end_time = datetime.utcnow()
                self.workflow_execution.error_log = str(e)
                self.workflow_execution.node_states = self.node_states
                db.commit()
            return {"success": False, "error": str(e)}
        finally:
            db.close()

    def execute_pipeline_node(self, node_key: str, pipeline_id: int) -> bool:
        db = self.db_session_factory()
        try:
            self.node_states[node_key]["status"] = NodeStatus.RUNNING
            self.node_states[node_key]["start_time"] = datetime.utcnow().isoformat()

            pipeline = db.query(models.Pipeline).filter(
                models.Pipeline.id == pipeline_id
            ).first()
            if not pipeline:
                self.node_states[node_key]["status"] = NodeStatus.FAILED
                self.node_states[node_key]["end_time"] = datetime.utcnow().isoformat()
                self.node_states[node_key]["error"] = f"Pipeline {pipeline_id} not found"
                return False

            execution = models.Execution(
                pipeline_id=pipeline_id,
                status=NodeStatus.RUNNING,
                start_time=datetime.utcnow()
            )
            db.add(execution)
            db.commit()
            db.refresh(execution)

            from dag_executor import DAGExecutor
            executor = DAGExecutor(pipeline.dag_config)

            success, result = executor.execute()

            execution.status = NodeStatus.COMPLETED if success else NodeStatus.FAILED
            execution.end_time = datetime.utcnow()
            execution.node_states = result.get("node_states")
            execution.total_rows = executor.get_total_rows()
            execution.success_rows = executor.get_success_rows()

            if not success:
                execution.error_log = result.get("error", "")

            db.commit()

            if success:
                self.node_states[node_key]["status"] = NodeStatus.COMPLETED
                self.node_states[node_key]["end_time"] = datetime.utcnow().isoformat()
                self.node_states[node_key]["execution_id"] = execution.id
                return True
            else:
                self.node_states[node_key]["status"] = NodeStatus.FAILED
                self.node_states[node_key]["end_time"] = datetime.utcnow().isoformat()
                self.node_states[node_key]["error"] = result.get("error", "Pipeline execution failed")
                self.node_states[node_key]["execution_id"] = execution.id
                return False

        except Exception as e:
            self.node_states[node_key]["status"] = NodeStatus.FAILED
            self.node_states[node_key]["end_time"] = datetime.utcnow().isoformat()
            self.node_states[node_key]["error"] = str(e)
            return False
        finally:
            db.close()

    def get_downstream_nodes(self, node_key: str) -> List[str]:
        db = self.db_session_factory()
        try:
            workflow = db.query(models.Workflow).filter(
                models.Workflow.id == self.workflow_id
            ).first()
            if not workflow:
                return []

            dag_config = workflow.dag_config
            edges = dag_config.get("edges", [])
            nodes = dag_config.get("nodes", [])
            node_keys = [node["key"] for node in nodes]

            adjacency = {key: [] for key in node_keys}
            for edge in edges:
                adjacency[edge["source"]].append(edge["target"])

            downstream = []
            visited = set()
            queue = deque([node_key])

            while queue:
                current = queue.popleft()
                for neighbor in adjacency[current]:
                    if neighbor not in visited:
                        visited.add(neighbor)
                        downstream.append(neighbor)
                        queue.append(neighbor)

            return downstream
        finally:
            db.close()

    def mark_downstream_skipped(self, failed_node: str):
        downstream_nodes = self.get_downstream_nodes(failed_node)
        for node_key in downstream_nodes:
            if self.node_states.get(node_key, {}).get("status") in [NodeStatus.PENDING, NodeStatus.RUNNING]:
                self.node_states[node_key]["status"] = NodeStatus.SKIPPED
                self.node_states[node_key]["end_time"] = datetime.utcnow().isoformat()
                self.node_states[node_key]["error"] = f"Skipped due to failure of upstream node '{failed_node}'"

    def _update_execution_state(self, db: Session):
        if self.workflow_execution:
            self.workflow_execution.node_states = self.node_states
            db.commit()


class PipelineDependencyManager:
    @staticmethod
    def add_dependency(upstream_pipeline_id: int, downstream_pipeline_id: int, db: Session) -> bool:
        if upstream_pipeline_id == downstream_pipeline_id:
            return False

        existing = db.query(models.WorkflowNode).filter(
            models.WorkflowNode.pipeline_id == downstream_pipeline_id
        ).first()

        if existing and existing.depends_on:
            if upstream_pipeline_id not in existing.depends_on:
                existing.depends_on.append(upstream_pipeline_id)
                db.commit()
        elif existing:
            existing.depends_on = [upstream_pipeline_id]
            db.commit()
        else:
            return False

        return True

    @staticmethod
    def remove_dependency(upstream_pipeline_id: int, downstream_pipeline_id: int, db: Session) -> bool:
        node = db.query(models.WorkflowNode).filter(
            models.WorkflowNode.pipeline_id == downstream_pipeline_id
        ).first()

        if not node or not node.depends_on:
            return False

        if upstream_pipeline_id in node.depends_on:
            node.depends_on.remove(upstream_pipeline_id)
            db.commit()
            return True

        return False

    @staticmethod
    def check_and_trigger_downstream(completed_pipeline_id: int, db: Session, scheduler) -> List[int]:
        triggered_pipelines = []

        downstream_nodes = db.query(models.WorkflowNode).filter(
            models.WorkflowNode.depends_on is not None
        ).all()

        for node in downstream_nodes:
            if not node.depends_on:
                continue

            if completed_pipeline_id not in node.depends_on:
                continue

            all_upstream_completed = True
            for upstream_id in node.depends_on:
                latest_execution = db.query(models.Execution).filter(
                    models.Execution.pipeline_id == upstream_id
                ).order_by(models.Execution.created_at.desc()).first()

                if not latest_execution or latest_execution.status != NodeStatus.COMPLETED:
                    all_upstream_completed = False
                    break

            if all_upstream_completed:
                pipeline = db.query(models.Pipeline).filter(
                    models.Pipeline.id == node.pipeline_id
                ).first()

                if pipeline:
                    scheduler.execute_pipeline(node.pipeline_id, db=db)
                    triggered_pipelines.append(node.pipeline_id)

        return triggered_pipelines
