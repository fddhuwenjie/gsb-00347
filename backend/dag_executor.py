import pandas as pd
import numpy as np
from typing import Dict, Any, List, Tuple, Optional
from collections import deque
from datetime import datetime
from sqlalchemy.orm import Session
from data_sources import DataSourceHandler
from transform_nodes import TransformExecutor
from output_targets import OutputExecutor
from data_quality import QualityRuleEngine
from lineage_tracker import LineageTracker
import models
import json


class NodeStatus:
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


CHECKPOINT_VERSION = 1


class DAGExecutor:
    def __init__(self, dag_config: Dict[str, Any], pipeline_id: int = None,
                 execution_id: int = None, db_session: Session = None):
        self.dag_config = dag_config
        self.pipeline_id = pipeline_id
        self.execution_id = execution_id
        self.db_session = db_session
        self.nodes = dag_config.get("nodes", [])
        self.edges = dag_config.get("edges", [])
        self.node_map = {node["id"]: node for node in self.nodes}
        self.node_outputs: Dict[str, Any] = {}
        self.node_states: Dict[str, Dict[str, Any]] = {}
        self.execution_order: List[str] = []
        self.performance_metrics: Dict[str, Dict[str, Any]] = {}
        self.quality_reports: List[Dict[str, Any]] = []
        self.completed_node_ids: set = set()
        self.output_written_nodes: set = set()
        self.lineage_tracker = LineageTracker(pipeline_id, execution_id) if pipeline_id else None
        self.quality_engine = QualityRuleEngine()
        self._adjacency = self._build_adjacency()
        self._skip_side_effects = False

    def _build_adjacency(self) -> Dict[str, List[str]]:
        adj = {node["id"]: [] for node in self.nodes}
        for edge in self.edges:
            adj[edge["source"]].append(edge["target"])
        return adj

    def topological_sort(self) -> List[str]:
        in_degree = {node["id"]: 0 for node in self.nodes}

        for edge in self.edges:
            in_degree[edge["target"]] += 1

        queue = deque([node_id for node_id, degree in in_degree.items() if degree == 0])
        result = []

        while queue:
            node_id = queue.popleft()
            result.append(node_id)

            for neighbor in self._adjacency[node_id]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

        if len(result) != len(self.nodes):
            raise ValueError("DAG has a cycle")

        return result

    def get_downstream_nodes(self, node_id: str) -> List[str]:
        downstream = []
        visited = set()
        queue = deque([node_id])

        while queue:
            current = queue.popleft()
            for neighbor in self._adjacency.get(current, []):
                if neighbor not in visited:
                    visited.add(neighbor)
                    downstream.append(neighbor)
                    queue.append(neighbor)

        return downstream

    def get_node_inputs(self, node_id: str) -> List[pd.DataFrame]:
        inputs = []
        for edge in self.edges:
            if edge["target"] == node_id:
                source_id = edge["source"]
                source_output = self.node_outputs.get(source_id)
                if source_output is not None:
                    if isinstance(source_output, list):
                        inputs.extend(source_output)
                    else:
                        inputs.append(source_output)
        return inputs

    def _estimate_memory_usage(self, df: Any) -> int:
        if isinstance(df, pd.DataFrame):
            return int(df.memory_usage(deep=True).sum())
        elif isinstance(df, list):
            return sum(self._estimate_memory_usage(item) for item in df)
        return 0

    def _calculate_throughput(self, rows: int, start_time: datetime, end_time: datetime) -> float:
        duration = (end_time - start_time).total_seconds()
        return rows / duration if duration > 0 else 0.0

    def _track_performance(self, node_id: str, start_time: datetime, end_time: datetime,
                           input_rows: int, output_rows: int, memory_peak: int):
        duration_ms = (end_time - start_time).total_seconds() * 1000
        throughput = self._calculate_throughput(output_rows, start_time, end_time)

        metric_data = {
            "node_id": node_id,
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "duration_ms": duration_ms,
            "input_rows": input_rows,
            "output_rows": output_rows,
            "throughput": round(throughput, 2),
            "memory_peak_bytes": memory_peak
        }
        self.performance_metrics[node_id] = metric_data

        if self.execution_id and self.db_session:
            existing = self.db_session.query(models.PerformanceMetric).filter_by(
                execution_id=self.execution_id,
                node_id=node_id
            ).first()
            if existing:
                existing.start_time = start_time
                existing.end_time = end_time
                existing.duration_ms = duration_ms
                existing.input_rows = input_rows
                existing.output_rows = output_rows
                existing.throughput = round(throughput, 2)
                existing.memory_peak_bytes = memory_peak
            else:
                metric = models.PerformanceMetric(
                    execution_id=self.execution_id,
                    node_id=node_id,
                    start_time=start_time,
                    end_time=end_time,
                    duration_ms=duration_ms,
                    input_rows=input_rows,
                    output_rows=output_rows,
                    throughput=round(throughput, 2),
                    memory_peak_bytes=memory_peak
                )
                self.db_session.add(metric)

    def _delete_quality_reports_for_node(self, node_id: str):
        if self.execution_id and self.db_session:
            self.db_session.query(models.QualityReport).filter_by(
                execution_id=self.execution_id,
                node_id=node_id
            ).delete()
            self.db_session.query(models.QuarantineRecord).filter_by(
                execution_id=self.execution_id,
                node_id=node_id
            ).delete()
        self.quality_reports = [r for r in self.quality_reports if r.get("node_id") != node_id]

    def _run_quality_checks(self, node_id: str, df: pd.DataFrame) -> Tuple[pd.DataFrame, bool]:
        if not self.pipeline_id or not self.db_session:
            return df, True

        rules = self.db_session.query(models.DataQualityRule).filter(
            models.DataQualityRule.pipeline_id == self.pipeline_id,
            models.DataQualityRule.node_id == node_id,
            models.DataQualityRule.is_enabled == True
        ).all()

        if not rules:
            return df, True

        current_df = df
        for rule in rules:
            check_result = self.quality_engine.check_rule(
                rule.rule_type, rule.rule_config, current_df
            )

            current_df, should_continue = self.quality_engine.apply_failure_strategy(
                rule.failure_strategy, current_df, check_result,
                self.execution_id, node_id, rule.id, self.db_session
            )

            self.quality_engine.generate_report(
                self.execution_id, node_id, rule, check_result, self.db_session
            )

            self.quality_reports.append({
                "rule_id": rule.id,
                "node_id": node_id,
                "rule_type": rule.rule_type,
                "pass_rate": check_result.pass_rate,
                "failed_records": check_result.failed_records
            })

            if not should_continue:
                return current_df, False

        return current_df, True

    def _track_lineage(self, node_id: str, node_type: str, config: Dict[str, Any],
                       inputs: List[pd.DataFrame], output: Any):
        if not self.lineage_tracker or self._skip_side_effects:
            return

        node = self.node_map.get(node_id, {})
        node_data = node.get("data", {})
        label = node_data.get("label", node_id)

        if node_type == "source":
            source_config = config.get("source_config", {})
            table_name = source_config.get("table_name", "unknown_source")
            if isinstance(output, pd.DataFrame):
                for col in output.columns:
                    self.lineage_tracker.track_field_mapping(
                        source_table=table_name,
                        source_column=col,
                        target_table=label,
                        target_column=col,
                        transform_ops=[{"node_id": node_id, "node_type": "source", "operation": "extract"}]
                    )

        elif node_type in TransformExecutor.NODE_TYPES:
            input_fields = []
            for df in inputs:
                if isinstance(df, pd.DataFrame):
                    input_fields.extend(df.columns.tolist())

            output_fields = []
            if isinstance(output, pd.DataFrame):
                output_fields = output.columns.tolist()
            elif isinstance(output, list) and len(output) > 0 and isinstance(output[0], pd.DataFrame):
                output_fields = output[0].columns.tolist()

            self.lineage_tracker.record_transform(
                node_id=node_id,
                node_type=node_type,
                input_fields=list(set(input_fields)),
                output_fields=list(set(output_fields)),
                operation=config
            )

        elif node_type == "output":
            output_config = config.get("output_config", {})
            table_name = output_config.get("table_name", "unknown_target")
            if inputs and isinstance(inputs[0], pd.DataFrame):
                for col in inputs[0].columns:
                    self.lineage_tracker.track_field_mapping(
                        source_table=label,
                        source_column=col,
                        target_table=table_name,
                        target_column=col,
                        transform_ops=[{"node_id": node_id, "node_type": "output", "operation": "load"}]
                    )

    def execute_node(self, node_id: str) -> Tuple[bool, Optional[str]]:
        node = self.node_map.get(node_id)
        if not node:
            return False, f"Node {node_id} not found"

        node_type = node.get("type")
        node_data = node.get("data", {})
        config = node_data.get("config", {})

        if node_id in self.completed_node_ids:
            return True, None

        start_time = datetime.now()
        self.node_states[node_id] = {
            "status": NodeStatus.RUNNING,
            "start_time": start_time.isoformat(),
            "input_rows": 0,
            "output_rows": 0,
            "error": None
        }
        self._save_checkpoint()

        memory_peak = 0

        try:
            df = None
            if node_type == "source":
                source_type = config.get("source_type")
                source_config = config.get("source_config", {})
                df = DataSourceHandler.read(source_type, source_config)
                self.node_outputs[node_id] = df
                self.node_states[node_id]["input_rows"] = len(df)
                self.node_states[node_id]["output_rows"] = len(df)
                memory_peak = self._estimate_memory_usage(df)

                df, should_continue = self._run_quality_checks(node_id, df)
                if not should_continue:
                    raise RuntimeError(f"Data quality check failed for node {node_id}")
                self.node_outputs[node_id] = df

                self._track_lineage(node_id, node_type, config, [], df)

            elif node_type in TransformExecutor.NODE_TYPES:
                inputs = self.get_node_inputs(node_id)
                total_input_rows = sum(len(df) for df in inputs)
                self.node_states[node_id]["input_rows"] = total_input_rows

                output = TransformExecutor.execute(node_type, inputs, config)
                self.node_outputs[node_id] = output

                if isinstance(output, list):
                    self.node_states[node_id]["output_rows"] = sum(len(df) for df in output)
                else:
                    self.node_states[node_id]["output_rows"] = len(output)

                memory_peak = self._estimate_memory_usage(output)

                if isinstance(output, pd.DataFrame):
                    output, should_continue = self._run_quality_checks(node_id, output)
                    if not should_continue:
                        raise RuntimeError(f"Data quality check failed for node {node_id}")
                    self.node_outputs[node_id] = output

                self._track_lineage(node_id, node_type, config, inputs, output)

            elif node_type == "output":
                inputs = self.get_node_inputs(node_id)
                if inputs:
                    df = inputs[0]
                    self.node_states[node_id]["input_rows"] = len(df)

                    df, should_continue = self._run_quality_checks(node_id, df)
                    if not should_continue:
                        raise RuntimeError(f"Data quality check failed for node {node_id}")

                    if node_id not in self.output_written_nodes:
                        output_type = config.get("output_type")
                        output_config = config.get("output_config", {})
                        rows_written, errors = OutputExecutor.write(
                            output_type, df, output_config,
                            execution_id=self.execution_id,
                            node_id=node_id
                        )
                        self.output_written_nodes.add(node_id)
                        self._save_checkpoint()
                    else:
                        rows_written = len(df)
                        errors = []

                    self.node_states[node_id]["output_rows"] = rows_written
                    if errors:
                        self.node_states[node_id]["warnings"] = errors

                    memory_peak = self._estimate_memory_usage(df)
                    self._track_lineage(node_id, node_type, config, inputs, df)
                else:
                    self.node_states[node_id]["output_rows"] = 0

            end_time = datetime.now()
            self.node_states[node_id]["status"] = NodeStatus.COMPLETED
            self.node_states[node_id]["end_time"] = end_time.isoformat()
            self.node_states[node_id]["error"] = None
            self.completed_node_ids.add(node_id)

            self._track_performance(
                node_id, start_time, end_time,
                self.node_states[node_id]["input_rows"],
                self.node_states[node_id]["output_rows"],
                memory_peak
            )

            self._save_checkpoint()

            return True, None

        except Exception as e:
            end_time = datetime.now()
            self.node_states[node_id]["status"] = NodeStatus.FAILED
            self.node_states[node_id]["end_time"] = end_time.isoformat()
            self.node_states[node_id]["error"] = str(e)

            self._track_performance(
                node_id, start_time, end_time,
                self.node_states[node_id]["input_rows"],
                self.node_states[node_id]["output_rows"],
                memory_peak
            )

            self._save_checkpoint()

            return False, str(e)

    def _serialize_dataframe(self, df: pd.DataFrame) -> Dict[str, Any]:
        dtype_map = {}
        for col in df.columns:
            dtype_map[col] = str(df[col].dtype)

        records = df.to_dict("records")

        for record in records:
            for col, val in record.items():
                if isinstance(val, (pd.Timestamp, datetime)):
                    record[col] = val.isoformat()
                elif isinstance(val, (np.integer,)):
                    record[col] = int(val)
                elif isinstance(val, (np.floating,)):
                    record[col] = float(val)
                elif isinstance(val, (np.bool_,)):
                    record[col] = bool(val)
                elif pd.isna(val):
                    record[col] = None

        return {
            "type": "dataframe",
            "columns": list(df.columns),
            "dtypes": dtype_map,
            "data": records
        }

    def _deserialize_dataframe(self, serialized: Dict[str, Any]) -> pd.DataFrame:
        columns = serialized["columns"]
        dtype_map = serialized.get("dtypes", {})
        records = serialized["data"]

        df = pd.DataFrame(records, columns=columns)

        for col in columns:
            if col in dtype_map:
                dtype_str = dtype_map[col]
                try:
                    if "datetime" in dtype_str:
                        df[col] = pd.to_datetime(df[col], errors="coerce")
                    elif dtype_str in ("int64", "Int64", "int32"):
                        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
                    elif dtype_str in ("float64", "float32"):
                        df[col] = pd.to_numeric(df[col], errors="coerce")
                    elif dtype_str in ("bool", "boolean"):
                        df[col] = df[col].astype("boolean")
                    elif dtype_str in ("object", "str", "string"):
                        df[col] = df[col].astype("object")
                except (ValueError, TypeError):
                    pass

        return df

    def _serialize_output(self, output: Any) -> Any:
        if isinstance(output, pd.DataFrame):
            return self._serialize_dataframe(output)
        elif isinstance(output, list):
            return {
                "type": "list",
                "data": [self._serialize_output(item) for item in output]
            }
        return output

    def _deserialize_output(self, serialized: Any) -> Any:
        if isinstance(serialized, dict):
            if serialized.get("type") == "dataframe":
                return self._deserialize_dataframe(serialized)
            elif serialized.get("type") == "list":
                return [self._deserialize_output(item) for item in serialized["data"]]
        return serialized

    def _create_checkpoint(self) -> Dict[str, Any]:
        serialized_outputs = {}
        for node_id in self.completed_node_ids:
            output = self.node_outputs.get(node_id)
            if output is not None:
                serialized_outputs[node_id] = self._serialize_output(output)

        return {
            "version": CHECKPOINT_VERSION,
            "node_states": {k: v for k, v in self.node_states.items()},
            "node_outputs": serialized_outputs,
            "completed_node_ids": list(self.completed_node_ids),
            "output_written_nodes": list(self.output_written_nodes),
            "performance_metrics": self.performance_metrics,
            "quality_reports": self.quality_reports
        }

    def _save_checkpoint(self):
        if self.execution_id and self.db_session:
            checkpoint = self._create_checkpoint()
            execution = self.db_session.query(models.Execution).filter_by(
                id=self.execution_id
            ).first()
            if execution:
                execution.checkpoint_data = checkpoint
                execution.node_states = self.node_states
                self.db_session.commit()

    def restore_from_checkpoint(self, checkpoint_data: Dict[str, Any]):
        if not checkpoint_data:
            return

        version = checkpoint_data.get("version", 0)
        if version != CHECKPOINT_VERSION:
            pass

        self.node_states = checkpoint_data.get("node_states", {})

        self.completed_node_ids = set(checkpoint_data.get("completed_node_ids", []))
        self.output_written_nodes = set(checkpoint_data.get("output_written_nodes", []))

        serialized_outputs = checkpoint_data.get("node_outputs", {})
        self.node_outputs = {}
        for node_id, serialized in serialized_outputs.items():
            self.node_outputs[node_id] = self._deserialize_output(serialized)

        self.performance_metrics = checkpoint_data.get("performance_metrics", {})
        self.quality_reports = checkpoint_data.get("quality_reports", [])

    @staticmethod
    def compute_nodes_to_rerun(dag_config: Dict[str, Any], checkpoint_data: Dict[str, Any]) -> set:
        nodes = dag_config.get("nodes", [])
        edges = dag_config.get("edges", [])
        adjacency = {node["id"]: [] for node in nodes}
        for edge in edges:
            adjacency.setdefault(edge["source"], []).append(edge["target"])

        node_states = checkpoint_data.get("node_states", {})
        to_rerun = set()
        for node_id, state in node_states.items():
            status = state.get("status")
            if status in (NodeStatus.FAILED, NodeStatus.PENDING, NodeStatus.RUNNING):
                to_rerun.add(node_id)
                visited = set()
                queue = deque([node_id])
                while queue:
                    current = queue.popleft()
                    for neighbor in adjacency.get(current, []):
                        if neighbor not in visited:
                            visited.add(neighbor)
                            to_rerun.add(neighbor)
                            queue.append(neighbor)

        if not to_rerun:
            in_degree = {node["id"]: 0 for node in nodes}
            for edge in edges:
                in_degree[edge["target"]] += 1
            queue = deque([nid for nid, d in in_degree.items() if d == 0])
            ordered = []
            while queue:
                nid = queue.popleft()
                ordered.append(nid)
                for neighbor in adjacency.get(nid, []):
                    in_degree[neighbor] -= 1
                    if in_degree[neighbor] == 0:
                        queue.append(neighbor)
            last_completed_idx = -1
            for i, nid in enumerate(ordered):
                if node_states.get(nid, {}).get("status") == NodeStatus.COMPLETED:
                    last_completed_idx = i
                else:
                    break
            to_rerun = set(ordered[last_completed_idx + 1:])

        return to_rerun

    def _get_nodes_to_rerun(self) -> set:
        to_rerun = set()
        for node_id, state in self.node_states.items():
            status = state.get("status")
            if status in (NodeStatus.FAILED, NodeStatus.PENDING, NodeStatus.RUNNING):
                to_rerun.add(node_id)
                downstream = self.get_downstream_nodes(node_id)
                to_rerun.update(downstream)

        if not to_rerun:
            ordered_nodes = self.topological_sort()
            last_completed_idx = -1
            for i, nid in enumerate(ordered_nodes):
                if self.node_states.get(nid, {}).get("status") == NodeStatus.COMPLETED:
                    last_completed_idx = i
                else:
                    break
            to_rerun = set(ordered_nodes[last_completed_idx + 1:])

        return to_rerun

    def _clear_stale_data_for_rerun(self, nodes_to_rerun: set):
        if self.execution_id and self.db_session:
            self.db_session.query(models.LineageRecord).filter_by(
                execution_id=self.execution_id
            ).delete(synchronize_session=False)

        for node_id in nodes_to_rerun:
            if node_id in self.completed_node_ids:
                self.completed_node_ids.discard(node_id)

            self.node_states[node_id] = {
                "status": NodeStatus.PENDING,
                "start_time": None,
                "end_time": None,
                "input_rows": 0,
                "output_rows": 0,
                "error": None
            }

            if node_id in self.node_outputs:
                del self.node_outputs[node_id]

            if self.execution_id and self.db_session:
                self.db_session.query(models.PerformanceMetric).filter_by(
                    execution_id=self.execution_id,
                    node_id=node_id
                ).delete(synchronize_session=False)

            self.performance_metrics.pop(node_id, None)
            self._delete_quality_reports_for_node(node_id)

    def _rebuild_lineage_for_completed_nodes(self):
        if not self.lineage_tracker:
            return
        for node_id in self.execution_order:
            if node_id not in self.completed_node_ids:
                continue
            node = self.node_map.get(node_id, {})
            node_type = node.get("type")
            config = node.get("data", {}).get("config", {})
            if node_type == "source":
                output = self.node_outputs.get(node_id)
                self._track_lineage(node_id, node_type, config, [], output)
            elif node_type == "output":
                inputs = self.get_node_inputs(node_id)
                self._track_lineage(node_id, node_type, config, inputs, inputs[0] if inputs else None)
            else:
                inputs = self.get_node_inputs(node_id)
                output = self.node_outputs.get(node_id)
                self._track_lineage(node_id, node_type, config, inputs, output)

    def execute(self, resume_from_checkpoint: Optional[Dict[str, Any]] = None) -> Tuple[bool, Dict[str, Any]]:
        self.execution_order = self.topological_sort()

        if resume_from_checkpoint:
            self.restore_from_checkpoint(resume_from_checkpoint)
            nodes_to_rerun = self._get_nodes_to_rerun()
            self._clear_stale_data_for_rerun(nodes_to_rerun)
            self._rebuild_lineage_for_completed_nodes()
            self._skip_side_effects = False
        else:
            nodes_to_rerun = set(self.execution_order)
            self._skip_side_effects = False

        for node_id in self.execution_order:
            if node_id in self.completed_node_ids:
                continue

            if resume_from_checkpoint and node_id not in nodes_to_rerun:
                state = self.node_states.get(node_id, {})
                if state.get("status") == NodeStatus.COMPLETED:
                    continue

            success, error = self.execute_node(node_id)
            if not success:
                if self.lineage_tracker and self.db_session:
                    self.lineage_tracker.save_to_db(self.db_session)
                if self.db_session:
                    self.db_session.commit()
                return False, {
                    "node_states": self.node_states,
                    "failed_node": node_id,
                    "error": error,
                    "checkpoint_data": self._create_checkpoint(),
                    "performance_metrics": self.performance_metrics,
                    "quality_reports": self.quality_reports
                }

        if self.lineage_tracker and self.db_session:
            self.lineage_tracker.save_to_db(self.db_session)
        if self.db_session:
            self.db_session.commit()

        return True, {
            "node_states": self.node_states,
            "checkpoint_data": self._create_checkpoint(),
            "performance_metrics": self.performance_metrics,
            "quality_reports": self.quality_reports
        }

    def get_total_rows(self) -> int:
        return sum(
            state.get("output_rows", 0)
            for state in self.node_states.values()
            if state.get("status") == NodeStatus.COMPLETED
        )

    def get_success_rows(self) -> int:
        return sum(
            state.get("output_rows", 0)
            for state in self.node_states.values()
            if state.get("status") == NodeStatus.COMPLETED
        )
