import pandas as pd
import numpy as np
from typing import Dict, Any, List, Tuple, Optional, Set
from collections import deque
from datetime import datetime
from sqlalchemy.orm import Session
from data_sources import DataSourceHandler
from transform_nodes import TransformExecutor
from output_targets import OutputExecutor
from data_quality import QualityRuleEngine
from lineage_tracker import LineageTracker
import models


CHECKPOINT_VERSION = 1


class NodeStatus:
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


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
        self.lineage_tracker = LineageTracker(pipeline_id, execution_id) if pipeline_id else None
        self.quality_engine = QualityRuleEngine()
        self._checkpoint_callback = None
        self._nodes_rerun: Set[str] = set()

    def set_checkpoint_callback(self, callback):
        self._checkpoint_callback = callback

    def topological_sort(self) -> List[str]:
        in_degree = {node["id"]: 0 for node in self.nodes}
        adjacency = {node["id"]: [] for node in self.nodes}

        for edge in self.edges:
            source = edge["source"]
            target = edge["target"]
            if source in adjacency and target in in_degree:
                adjacency[source].append(target)
                in_degree[target] += 1

        queue = deque([node_id for node_id, degree in in_degree.items() if degree == 0])
        result = []

        while queue:
            node_id = queue.popleft()
            result.append(node_id)

            for neighbor in adjacency[node_id]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

        if len(result) != len(self.nodes):
            raise ValueError("DAG has a cycle")

        return result

    def _get_downstream_set(self, node_ids: Set[str]) -> Set[str]:
        adjacency = {node["id"]: [] for node in self.nodes}
        for edge in self.edges:
            source = edge["source"]
            target = edge["target"]
            if source in adjacency:
                adjacency[source].append(target)

        downstream: Set[str] = set()
        queue = deque(node_ids)
        while queue:
            current = queue.popleft()
            for neighbor in adjacency.get(current, []):
                if neighbor not in downstream:
                    downstream.add(neighbor)
                    queue.append(neighbor)
        return downstream

    def get_node_inputs(self, node_id: str) -> List[pd.DataFrame]:
        inputs = []
        for edge in self.edges:
            if edge["target"] == node_id:
                source_id = edge["source"]
                source_output = self.node_outputs.get(source_id)
                if source_output is None:
                    continue

                source_handle = edge.get("sourceHandle")

                if isinstance(source_output, list):
                    if source_handle and source_handle.startswith("branch_"):
                        try:
                            idx = int(source_handle.split("_")[1])
                            if 0 <= idx < len(source_output):
                                inputs.append(source_output[idx])
                            else:
                                inputs.extend(source_output)
                        except (ValueError, IndexError):
                            inputs.extend(source_output)
                    else:
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

        self.performance_metrics[node_id] = {
            "node_id": node_id,
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "duration_ms": duration_ms,
            "input_rows": input_rows,
            "output_rows": output_rows,
            "throughput": round(throughput, 2),
            "memory_peak_bytes": memory_peak
        }

        if self.execution_id and self.db_session:
            self.db_session.query(models.PerformanceMetric).filter(
                models.PerformanceMetric.execution_id == self.execution_id,
                models.PerformanceMetric.node_id == node_id
            ).delete()
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

        if self.execution_id:
            self.db_session.query(models.QualityReport).filter(
                models.QualityReport.execution_id == self.execution_id,
                models.QualityReport.node_id == node_id
            ).delete()
            self.db_session.commit()

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

            self.quality_reports = [r for r in self.quality_reports if r.get("node_id") != node_id]
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
        if not self.lineage_tracker:
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
                        node_id=node_id,
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
                        node_id=node_id,
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

        start_time = datetime.now()
        self.node_states[node_id] = {
            "status": NodeStatus.RUNNING,
            "start_time": start_time.isoformat(),
            "end_time": None,
            "input_rows": 0,
            "output_rows": 0,
            "error": None
        }

        if node_id in self.node_outputs:
            del self.node_outputs[node_id]

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
                total_input_rows = sum(len(df) for df in inputs if isinstance(df, pd.DataFrame))
                self.node_states[node_id]["input_rows"] = total_input_rows

                output = TransformExecutor.execute(node_type, inputs, config)
                self.node_outputs[node_id] = output

                if isinstance(output, list):
                    self.node_states[node_id]["output_rows"] = sum(
                        len(d) for d in output if isinstance(d, pd.DataFrame)
                    )
                elif isinstance(output, pd.DataFrame):
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

                    output_type = config.get("output_type")
                    output_config = config.get("output_config", {})
                    rows_written, errors, write_skipped = OutputExecutor.write(
                        output_type, df, output_config,
                        execution_id=self.execution_id,
                        node_id=node_id
                    )
                    self.node_states[node_id]["output_rows"] = rows_written
                    if errors:
                        self.node_states[node_id]["warnings"] = errors
                    if write_skipped:
                        self.node_states[node_id]["write_skipped"] = True

                    memory_peak = self._estimate_memory_usage(df)
                    self._track_lineage(node_id, node_type, config, inputs, df)
                else:
                    self.node_states[node_id]["output_rows"] = 0

            end_time = datetime.now()
            self.node_states[node_id]["status"] = NodeStatus.COMPLETED
            self.node_states[node_id]["end_time"] = end_time.isoformat()
            self.node_states[node_id]["error"] = None

            self._track_performance(
                node_id, start_time, end_time,
                self.node_states[node_id]["input_rows"],
                self.node_states[node_id]["output_rows"],
                memory_peak
            )

            self._persist_checkpoint()

            return True, None

        except Exception as e:
            end_time = datetime.now()
            self.node_states[node_id]["status"] = NodeStatus.FAILED
            self.node_states[node_id]["end_time"] = end_time.isoformat()
            self.node_states[node_id]["error"] = str(e)
            if node_id in self.node_outputs:
                del self.node_outputs[node_id]

            self._track_performance(
                node_id, start_time, end_time,
                self.node_states[node_id]["input_rows"],
                self.node_states[node_id]["output_rows"],
                memory_peak
            )

            self._persist_checkpoint()

            return False, str(e)

    def _serialize_output(self, output: Any) -> Any:
        if isinstance(output, pd.DataFrame):
            return {
                "__type__": "dataframe",
                "data": output.to_dict("records"),
                "columns": output.columns.tolist(),
                "dtypes": {col: str(dtype) for col, dtype in output.dtypes.items()}
            }
        elif isinstance(output, list):
            return {
                "__type__": "list",
                "items": [self._serialize_output(item) for item in output]
            }
        return output

    def _deserialize_output(self, serialized: Any) -> Any:
        if isinstance(serialized, dict) and "__type__" in serialized:
            if serialized["__type__"] == "dataframe":
                columns = serialized.get("columns", [])
                data = serialized.get("data", [])
                df = pd.DataFrame(data, columns=columns) if columns else pd.DataFrame(data)
                dtypes = serialized.get("dtypes", {})
                for col, dtype_str in dtypes.items():
                    if col in df.columns:
                        try:
                            if "datetime" in dtype_str:
                                df[col] = pd.to_datetime(df[col], errors="coerce")
                            else:
                                df[col] = df[col].astype(dtype_str)
                        except (TypeError, ValueError):
                            pass
                return df
            elif serialized["__type__"] == "list":
                return [self._deserialize_output(item) for item in serialized.get("items", [])]
        return serialized

    def _create_checkpoint(self) -> Dict[str, Any]:
        serialized_outputs = {}
        for node_id, output in self.node_outputs.items():
            if self.node_states.get(node_id, {}).get("status") == NodeStatus.COMPLETED:
                serialized_outputs[node_id] = self._serialize_output(output)

        lineage_data = None
        if self.lineage_tracker:
            lineage_data = self.lineage_tracker.serialize()

        return {
            "version": CHECKPOINT_VERSION,
            "node_states": {
                node_id: dict(state) for node_id, state in self.node_states.items()
            },
            "node_outputs": serialized_outputs,
            "execution_order": list(self.execution_order),
            "performance_metrics": dict(self.performance_metrics),
            "quality_reports": list(self.quality_reports),
            "lineage": lineage_data
        }

    def _restore_from_checkpoint(self, checkpoint: Dict[str, Any]):
        if not checkpoint:
            return

        saved_states = checkpoint.get("node_states", {})
        self.node_states = {node_id: dict(state) for node_id, state in saved_states.items()}

        saved_outputs = checkpoint.get("node_outputs", {})
        self.node_outputs = {}
        for node_id, serialized in saved_outputs.items():
            if self.node_states.get(node_id, {}).get("status") == NodeStatus.COMPLETED:
                self.node_outputs[node_id] = self._deserialize_output(serialized)

        saved_metrics = checkpoint.get("performance_metrics", {})
        self.performance_metrics = dict(saved_metrics)

        saved_reports = checkpoint.get("quality_reports", [])
        self.quality_reports = list(saved_reports)

        lineage_data = checkpoint.get("lineage")
        if self.lineage_tracker and lineage_data:
            self.lineage_tracker.deserialize(lineage_data, self.pipeline_id, self.execution_id)

    def _persist_checkpoint(self):
        if self._checkpoint_callback:
            try:
                self._checkpoint_callback(self._create_checkpoint())
            except Exception:
                pass
        elif self.db_session and self.execution_id:
            try:
                execution = self.db_session.query(models.Execution).filter(
                    models.Execution.id == self.execution_id
                ).first()
                if execution:
                    execution.checkpoint_data = self._create_checkpoint()
                    execution.node_states = self.node_states
                    self.db_session.commit()
            except Exception:
                self.db_session.rollback()

    def _clean_rerun_db_records(self, rerun_nodes: Set[str]):
        if not self.execution_id or not self.db_session:
            return

        for node_id in rerun_nodes:
            self.db_session.query(models.PerformanceMetric).filter(
                models.PerformanceMetric.execution_id == self.execution_id,
                models.PerformanceMetric.node_id == node_id
            ).delete()

            self.db_session.query(models.QualityReport).filter(
                models.QualityReport.execution_id == self.execution_id,
                models.QualityReport.node_id == node_id
            ).delete()

        self.db_session.commit()

    def execute(self, resume: bool = False,
                checkpoint: Optional[Dict[str, Any]] = None) -> Tuple[bool, Dict[str, Any]]:
        self.execution_order = self.topological_sort()

        rerun_nodes: Set[str] = set()

        if resume and checkpoint:
            self._restore_from_checkpoint(checkpoint)

            failed_nodes = {
                node_id for node_id, state in self.node_states.items()
                if state.get("status") in (NodeStatus.FAILED, NodeStatus.RUNNING)
            }

            if not failed_nodes:
                failed_nodes = {
                    node_id for node_id in self.execution_order
                    if node_id not in self.node_states
                }

            rerun_nodes = failed_nodes | self._get_downstream_set(failed_nodes)

            for node_id in rerun_nodes:
                state = self.node_states.get(node_id, {})
                if state.get("status") in (NodeStatus.FAILED, NodeStatus.RUNNING):
                    state["status"] = NodeStatus.PENDING
                    state["error"] = None
                    state["end_time"] = None
                elif state.get("status") == NodeStatus.COMPLETED:
                    state["status"] = NodeStatus.PENDING
                    state["start_time"] = None
                    state["end_time"] = None
                    state["error"] = None
                    state.pop("write_skipped", None)
                    if node_id in self.node_outputs:
                        del self.node_outputs[node_id]

            self._nodes_rerun = rerun_nodes
            self._clean_rerun_db_records(rerun_nodes)

            if self.lineage_tracker:
                self.lineage_tracker.remove_nodes(rerun_nodes)

        for node_id in self.execution_order:
            state = self.node_states.get(node_id, {})
            if state.get("status") == NodeStatus.COMPLETED:
                continue
            if state.get("status") == NodeStatus.SKIPPED:
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
                    "quality_reports": self.quality_reports,
                    "rerun_nodes": sorted(rerun_nodes) if resume else []
                }

        if self.lineage_tracker and self.db_session:
            self.db_session.query(models.LineageRecord).filter(
                models.LineageRecord.execution_id == self.execution_id
            ).delete()
            self.lineage_tracker.save_to_db(self.db_session)
        if self.db_session:
            self.db_session.commit()

        return True, {
            "node_states": self.node_states,
            "checkpoint_data": self._create_checkpoint(),
            "performance_metrics": self.performance_metrics,
            "quality_reports": self.quality_reports,
            "rerun_nodes": sorted(rerun_nodes) if resume else []
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
