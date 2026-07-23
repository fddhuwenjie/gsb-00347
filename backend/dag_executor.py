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


class NodeStatus:
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


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

    def topological_sort(self) -> List[str]:
        in_degree = {node["id"]: 0 for node in self.nodes}
        adjacency = {node["id"]: [] for node in self.nodes}

        for edge in self.edges:
            source = edge["source"]
            target = edge["target"]
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

        start_time = datetime.now()
        self.node_states[node_id] = {
            "status": NodeStatus.RUNNING,
            "start_time": start_time.isoformat(),
            "input_rows": 0,
            "output_rows": 0,
            "error": None
        }

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

                    output_type = config.get("output_type")
                    output_config = config.get("output_config", {})
                    rows_written, errors = OutputExecutor.write(output_type, df, output_config)
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

            self._track_performance(
                node_id, start_time, end_time,
                self.node_states[node_id]["input_rows"],
                self.node_states[node_id]["output_rows"],
                memory_peak
            )

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

            return False, str(e)

    def execute(self, resume_from: Optional[str] = None,
                checkpoint_states: Optional[Dict[str, Any]] = None) -> Tuple[bool, Dict[str, Any]]:
        if checkpoint_states:
            self.node_states = checkpoint_states
            for node_id, state in checkpoint_states.items():
                if state.get("status") == NodeStatus.COMPLETED:
                    pass

        self.execution_order = self.topological_sort()
        start_index = 0

        if resume_from:
            if resume_from in self.execution_order:
                start_index = self.execution_order.index(resume_from)

        for node_id in self.execution_order[start_index:]:
            if self.node_states.get(node_id, {}).get("status") == NodeStatus.COMPLETED:
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

    def _create_checkpoint(self) -> Dict[str, Any]:
        return {
            "node_states": self.node_states,
            "node_outputs": {
                node_id: self._serialize_output(output)
                for node_id, output in self.node_outputs.items()
            },
            "performance_metrics": self.performance_metrics,
            "quality_reports": self.quality_reports
        }

    def _serialize_output(self, output: Any) -> Any:
        if isinstance(output, pd.DataFrame):
            return output.to_dict("records")
        elif isinstance(output, list):
            return [self._serialize_output(item) for item in output]
        return output

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
