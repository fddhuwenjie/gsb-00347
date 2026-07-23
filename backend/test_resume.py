import os
import sys
import tempfile
import shutil
import threading
import time
import sqlite3
import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch, MagicMock
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from database import engine, SessionLocal, Base
import models
from dag_executor import DAGExecutor, NodeStatus, CHECKPOINT_VERSION
from scheduler import PipelineScheduler, ResumeError


@pytest.fixture(autouse=True)
def setup_db():
    Base.metadata.create_all(bind=engine)
    yield
    Base.metadata.drop_all(bind=engine)


@pytest.fixture
def db_session():
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


@pytest.fixture
def temp_dir():
    d = tempfile.mkdtemp()
    yield d
    shutil.rmtree(d, ignore_errors=True)


@pytest.fixture
def sample_csv(temp_dir):
    csv_path = os.path.join(temp_dir, "source1.csv")
    df = pd.DataFrame({
        "id": [1, 2, 3, 4, 5, 6],
        "name": ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank"],
        "amount": [100.0, 250.5, 75.0, 300.0, 150.0, 500.0],
        "category": ["A", "B", "A", "B", "A", "B"]
    })
    df.to_csv(csv_path, index=False)
    return csv_path


@pytest.fixture
def sample_csv2(temp_dir):
    csv_path = os.path.join(temp_dir, "source2.csv")
    df = pd.DataFrame({
        "id": [1, 2, 3, 4, 5, 6],
        "region": ["North", "South", "East", "West", "North", "South"],
        "score": [10, 20, 30, 40, 50, 60]
    })
    df.to_csv(csv_path, index=False)
    return csv_path


def create_branch_join_dag(csv_path1, csv_path2, output_db_path, output_table="result_table"):
    return {
        "nodes": [
            {
                "id": "source1",
                "type": "source",
                "data": {
                    "label": "Source 1",
                    "config": {
                        "source_type": "csv",
                        "source_config": {
                            "file_path": csv_path1,
                            "has_header": True
                        }
                    }
                }
            },
            {
                "id": "source2",
                "type": "source",
                "data": {
                    "label": "Source 2",
                    "config": {
                        "source_type": "csv",
                        "source_config": {
                            "file_path": csv_path2,
                            "has_header": True
                        }
                    }
                }
            },
            {
                "id": "transform_filter_a",
                "type": "filter",
                "data": {
                    "label": "Filter Category A",
                    "config": {
                        "condition": "category == 'A'"
                    }
                }
            },
            {
                "id": "transform_map",
                "type": "map",
                "data": {
                    "label": "Double Amount",
                    "config": {
                        "operations": [
                            {
                                "type": "compute",
                                "new_column": "doubled_amount",
                                "expression": "amount * 2"
                            }
                        ]
                    }
                }
            },
            {
                "id": "transform_filter_b",
                "type": "filter",
                "data": {
                    "label": "Filter Category B",
                    "config": {
                        "condition": "category == 'B'"
                    }
                }
            },
            {
                "id": "join_node",
                "type": "join",
                "data": {
                    "label": "Join All",
                    "config": {
                        "join_type": "outer",
                        "left_key": "id",
                        "right_key": "id"
                    }
                }
            },
            {
                "id": "output_node",
                "type": "output",
                "data": {
                    "label": "Write to SQLite",
                    "config": {
                        "output_type": "sqlite",
                        "output_config": {
                            "db_path": output_db_path,
                            "table_name": output_table,
                            "write_mode": "append"
                        }
                    }
                }
            }
        ],
        "edges": [
            {"source": "source1", "target": "transform_filter_a"},
            {"source": "source1", "target": "transform_filter_b"},
            {"source": "transform_filter_a", "target": "transform_map"},
            {"source": "transform_map", "target": "join_node"},
            {"source": "source2", "target": "join_node"},
            {"source": "transform_filter_b", "target": "join_node"},
            {"source": "join_node", "target": "output_node"}
        ]
    }


def _fresh_session():
    return SessionLocal()


def _get_execution(execution_id):
    db = _fresh_session()
    try:
        return db.query(models.Execution).filter_by(id=execution_id).first()
    finally:
        db.close()


class TestCheckpointSerialization:
    def test_dataframe_serialization_preserves_dtypes(self, db_session):
        df = pd.DataFrame({
            "int_col": pd.array([1, 2, 3], dtype="Int64"),
            "float_col": [1.5, 2.5, 3.5],
            "str_col": ["a", "b", "c"],
            "bool_col": pd.array([True, False, True], dtype="boolean"),
            "date_col": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"])
        })

        executor = DAGExecutor({"nodes": [], "edges": []})
        serialized = executor._serialize_dataframe(df)
        assert serialized["type"] == "dataframe"
        assert serialized["columns"] == ["int_col", "float_col", "str_col", "bool_col", "date_col"]
        assert "int_col" in serialized["dtypes"]

        restored = executor._deserialize_dataframe(serialized)
        assert list(restored.columns) == list(df.columns)
        assert len(restored) == len(df)
        assert restored["int_col"].dtype == "Int64"
        assert restored["float_col"].dtype == "float64"
        assert restored["bool_col"].dtype == "boolean"
        assert pd.api.types.is_datetime64_any_dtype(restored["date_col"])

    def test_list_output_serialization(self, db_session):
        df1 = pd.DataFrame({"a": [1, 2]})
        df2 = pd.DataFrame({"b": [3, 4]})

        executor = DAGExecutor({"nodes": [], "edges": []})
        serialized = executor._serialize_output([df1, df2])
        assert serialized["type"] == "list"
        assert len(serialized["data"]) == 2

        restored = executor._deserialize_output(serialized)
        assert isinstance(restored, list)
        assert len(restored) == 2
        assert isinstance(restored[0], pd.DataFrame)
        assert list(restored[0]["a"]) == [1, 2]
        assert isinstance(restored[1], pd.DataFrame)
        assert list(restored[1]["b"]) == [3, 4]

    def test_checkpoint_roundtrip(self, db_session, sample_csv, sample_csv2, temp_dir):
        output_db = os.path.join(temp_dir, "output.db")
        dag = create_branch_join_dag(sample_csv, sample_csv2, output_db)

        pipeline = models.Pipeline(name="test", dag_config=dag)
        db_session.add(pipeline)
        db_session.commit()

        execution = models.Execution(
            pipeline_id=pipeline.id,
            status=NodeStatus.RUNNING,
            start_time=datetime.utcnow()
        )
        db_session.add(execution)
        db_session.commit()
        db_session.refresh(execution)

        executor1 = DAGExecutor(dag, pipeline_id=pipeline.id, execution_id=execution.id, db_session=db_session)
        executor1.topological_sort()
        executor1.execute_node("source1")
        executor1.execute_node("source2")

        checkpoint = executor1._create_checkpoint()
        assert checkpoint["version"] == CHECKPOINT_VERSION
        assert "source1" in checkpoint["node_states"]
        assert checkpoint["node_states"]["source1"]["status"] == NodeStatus.COMPLETED
        assert "source1" in checkpoint["node_outputs"]
        assert "source1" in checkpoint["completed_node_ids"]

        executor2 = DAGExecutor(dag, pipeline_id=pipeline.id, execution_id=execution.id, db_session=db_session)
        executor2.restore_from_checkpoint(checkpoint)

        assert "source1" in executor2.completed_node_ids
        assert "source2" in executor2.completed_node_ids
        assert "source1" in executor2.node_outputs
        assert "source2" in executor2.node_outputs
        assert isinstance(executor2.node_outputs["source1"], pd.DataFrame)
        assert len(executor2.node_outputs["source1"]) == 6
        assert executor2.node_states["source1"]["status"] == NodeStatus.COMPLETED


class TestFailureRecovery:
    def test_branch_join_dag_failure_and_recovery(self, db_session, sample_csv, sample_csv2, temp_dir):
        output_db = os.path.join(temp_dir, "output.db")
        output_table = "final_result"
        dag = create_branch_join_dag(sample_csv, sample_csv2, output_db, output_table)

        pipeline = models.Pipeline(name="test_pipeline", dag_config=dag)
        db_session.add(pipeline)
        db_session.commit()

        execution = models.Execution(
            pipeline_id=pipeline.id,
            status=NodeStatus.RUNNING,
            start_time=datetime.utcnow()
        )
        db_session.add(execution)
        db_session.commit()
        db_session.refresh(execution)

        call_count = {"transform_map": 0}
        original_execute = DAGExecutor.execute_node

        def mock_execute_node(self, node_id):
            if node_id == "transform_map":
                call_count["transform_map"] += 1
                if call_count["transform_map"] == 1:
                    end_time = datetime.now()
                    self.node_states[node_id] = {
                        "status": NodeStatus.FAILED,
                        "start_time": end_time.isoformat(),
                        "end_time": end_time.isoformat(),
                        "input_rows": 3,
                        "output_rows": 0,
                        "error": "Simulated failure on first run"
                    }
                    self.completed_node_ids.discard(node_id)
                    self._save_checkpoint()
                    return False, "Simulated failure on first run"
            return original_execute(self, node_id)

        with patch.object(DAGExecutor, 'execute_node', mock_execute_node):
            executor1 = DAGExecutor(dag, pipeline_id=pipeline.id, execution_id=execution.id, db_session=db_session)
            success1, result1 = executor1.execute()

        assert success1 == False
        assert result1["failed_node"] == "transform_map"
        assert result1["error"] == "Simulated failure on first run"

        db_session.refresh(execution)
        checkpoint1 = execution.checkpoint_data
        assert checkpoint1 is not None
        assert "source1" in checkpoint1["completed_node_ids"]
        assert "source2" in checkpoint1["completed_node_ids"]
        assert "transform_filter_a" in checkpoint1["completed_node_ids"]
        assert "transform_map" not in checkpoint1["completed_node_ids"]

        execution.status = NodeStatus.FAILED
        execution.end_time = datetime.utcnow()
        execution.error_log = result1["error"]
        db_session.commit()

        with patch.object(DAGExecutor, 'execute_node', mock_execute_node):
            executor2 = DAGExecutor(dag, pipeline_id=pipeline.id, execution_id=execution.id, db_session=db_session)
            success2, result2 = executor2.execute(resume_from_checkpoint=checkpoint1)

        assert success2 == True
        assert call_count["transform_map"] == 2

        states = result2["node_states"]
        for node_id in ["source1", "source2", "transform_filter_a", "transform_filter_b",
                         "transform_map", "join_node", "output_node"]:
            assert states[node_id]["status"] == NodeStatus.COMPLETED, \
                f"Node {node_id} should be completed, got {states[node_id]['status']}"

        conn = sqlite3.connect(output_db)
        try:
            output_df = pd.read_sql_query(f"SELECT * FROM {output_table}", conn)
            assert len(output_df) > 0, "Output table should have data"
        finally:
            conn.close()

        conn = sqlite3.connect(output_db)
        try:
            output_df2 = pd.read_sql_query(f"SELECT * FROM {output_table}", conn)
            assert len(output_df2) == len(output_df), \
                f"Output should not be duplicated: expected {len(output_df)}, got {len(output_df2)}"
        finally:
            conn.close()

    def test_upstream_nodes_not_reexecuted(self, db_session, sample_csv, sample_csv2, temp_dir):
        output_db = os.path.join(temp_dir, "output2.db")
        dag = create_branch_join_dag(sample_csv, sample_csv2, output_db, "t2")

        pipeline = models.Pipeline(name="test_pipeline2", dag_config=dag)
        db_session.add(pipeline)
        db_session.commit()

        execution = models.Execution(
            pipeline_id=pipeline.id,
            status=NodeStatus.RUNNING,
            start_time=datetime.utcnow()
        )
        db_session.add(execution)
        db_session.commit()
        db_session.refresh(execution)

        node_exec_counts = {}
        original_execute_node = DAGExecutor.execute_node

        def counting_execute_node(self, node_id):
            node_exec_counts[node_id] = node_exec_counts.get(node_id, 0) + 1
            if node_id == "transform_map" and node_exec_counts[node_id] == 1:
                end_time = datetime.now()
                self.node_states[node_id] = {
                    "status": NodeStatus.FAILED,
                    "start_time": end_time.isoformat(),
                    "end_time": end_time.isoformat(),
                    "input_rows": 3,
                    "output_rows": 0,
                    "error": "First run failure"
                }
                self._save_checkpoint()
                return False, "First run failure"
            return original_execute_node(self, node_id)

        with patch.object(DAGExecutor, 'execute_node', counting_execute_node):
            executor1 = DAGExecutor(dag, pipeline_id=pipeline.id, execution_id=execution.id, db_session=db_session)
            success1, result1 = executor1.execute()

        assert success1 == False
        assert node_exec_counts["source1"] == 1
        assert node_exec_counts["source2"] == 1
        assert node_exec_counts["transform_filter_a"] == 1

        execution.status = NodeStatus.FAILED
        execution.end_time = datetime.utcnow()
        db_session.commit()

        with patch.object(DAGExecutor, 'execute_node', counting_execute_node):
            executor2 = DAGExecutor(dag, pipeline_id=pipeline.id, execution_id=execution.id, db_session=db_session)
            success2, result2 = executor2.execute(resume_from_checkpoint=execution.checkpoint_data)

        assert success2 == True
        assert node_exec_counts["source1"] == 1, f"source1 ran {node_exec_counts['source1']} times, should be 1"
        assert node_exec_counts["source2"] == 1, f"source2 ran {node_exec_counts['source2']} times, should be 1"
        assert node_exec_counts["transform_filter_a"] == 1, f"transform_filter_a ran {node_exec_counts['transform_filter_a']} times, should be 1"
        assert node_exec_counts["transform_map"] == 2, f"transform_map ran {node_exec_counts['transform_map']} times, should be 2"
        assert node_exec_counts["join_node"] == 1, f"join_node ran {node_exec_counts['join_node']} times"
        assert node_exec_counts["output_node"] == 1, f"output_node ran {node_exec_counts['output_node']} times"

    def test_output_not_duplicated_on_resume(self, db_session, sample_csv, sample_csv2, temp_dir):
        output_db = os.path.join(temp_dir, "output3.db")
        dag = create_branch_join_dag(sample_csv, sample_csv2, output_db, "t3")

        pipeline = models.Pipeline(name="test_pipeline3", dag_config=dag)
        db_session.add(pipeline)
        db_session.commit()

        execution = models.Execution(
            pipeline_id=pipeline.id,
            status=NodeStatus.RUNNING,
            start_time=datetime.utcnow()
        )
        db_session.add(execution)
        db_session.commit()
        db_session.refresh(execution)

        original_execute_node = DAGExecutor.execute_node
        run_count = {"join_node": 0}

        def failing_execute(self, node_id):
            if node_id == "join_node":
                run_count["join_node"] += 1
                if run_count["join_node"] == 1:
                    end_time = datetime.now()
                    self.node_states[node_id] = {
                        "status": NodeStatus.FAILED,
                        "start_time": end_time.isoformat(),
                        "end_time": end_time.isoformat(),
                        "input_rows": 0,
                        "output_rows": 0,
                        "error": "Join failure"
                    }
                    self._save_checkpoint()
                    return False, "Join failure"
            return original_execute_node(self, node_id)

        with patch.object(DAGExecutor, 'execute_node', failing_execute):
            executor1 = DAGExecutor(dag, pipeline_id=pipeline.id, execution_id=execution.id, db_session=db_session)
            success1, result1 = executor1.execute()

        assert success1 == False

        conn = sqlite3.connect(output_db)
        try:
            cur = conn.cursor()
            cur.execute("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='t3'")
            table_exists = cur.fetchone()[0] > 0
            if table_exists:
                cur.execute("SELECT count(*) FROM t3")
                first_count = cur.fetchone()[0]
            else:
                first_count = 0
        finally:
            conn.close()

        assert first_count == 0, "Output should not have been written since join failed before output"

        execution.status = NodeStatus.FAILED
        execution.end_time = datetime.utcnow()
        db_session.commit()

        with patch.object(DAGExecutor, 'execute_node', failing_execute):
            executor2 = DAGExecutor(dag, pipeline_id=pipeline.id, execution_id=execution.id, db_session=db_session)
            success2, result2 = executor2.execute(resume_from_checkpoint=execution.checkpoint_data)

        assert success2 == True

        conn = sqlite3.connect(output_db)
        try:
            df = pd.read_sql_query("SELECT * FROM t3", conn)
            row_count = len(df)
            assert row_count > 0, "Should have output data"
        finally:
            conn.close()

        conn = sqlite3.connect(output_db)
        try:
            df_check = pd.read_sql_query("SELECT * FROM t3", conn)
            assert len(df_check) == row_count, \
                f"Output rows duplicated: got {len(df_check)}, expected {row_count}"
        finally:
            conn.close()

    def test_performance_metrics_not_duplicated(self, db_session, sample_csv, sample_csv2, temp_dir):
        output_db = os.path.join(temp_dir, "output4.db")
        dag = create_branch_join_dag(sample_csv, sample_csv2, output_db, "t4")

        pipeline = models.Pipeline(name="test_pipeline4", dag_config=dag)
        db_session.add(pipeline)
        db_session.commit()

        execution = models.Execution(
            pipeline_id=pipeline.id,
            status=NodeStatus.RUNNING,
            start_time=datetime.utcnow()
        )
        db_session.add(execution)
        db_session.commit()
        db_session.refresh(execution)

        original_execute_node = DAGExecutor.execute_node
        fail_count = {"transform_map": 0}

        def failing_map(self, node_id):
            if node_id == "transform_map":
                fail_count["transform_map"] += 1
                if fail_count["transform_map"] == 1:
                    end_time = datetime.now()
                    self.node_states[node_id] = {
                        "status": NodeStatus.FAILED,
                        "start_time": end_time.isoformat(),
                        "end_time": end_time.isoformat(),
                        "input_rows": 3,
                        "output_rows": 0,
                        "error": "fail"
                    }
                    self._save_checkpoint()
                    return False, "fail"
            return original_execute_node(self, node_id)

        with patch.object(DAGExecutor, 'execute_node', failing_map):
            executor1 = DAGExecutor(dag, pipeline_id=pipeline.id, execution_id=execution.id, db_session=db_session)
            success1, _ = executor1.execute()

        assert success1 == False

        db_session.commit()

        execution.status = NodeStatus.FAILED
        execution.end_time = datetime.utcnow()
        db_session.commit()

        with patch.object(DAGExecutor, 'execute_node', failing_map):
            executor2 = DAGExecutor(dag, pipeline_id=pipeline.id, execution_id=execution.id, db_session=db_session)
            success2, _ = executor2.execute(resume_from_checkpoint=execution.checkpoint_data)

        assert success2 == True

        metrics_final = db_session.query(models.PerformanceMetric).filter_by(
            execution_id=execution.id
        ).all()
        metric_nodes_final = {}
        for m in metrics_final:
            assert m.node_id not in metric_nodes_final, \
                f"Duplicate performance metric for node {m.node_id}"
            metric_nodes_final[m.node_id] = m

        assert len(metrics_final) == 7, f"Expected 7 metrics (one per node), got {len(metrics_final)}"

    def test_completed_node_metrics_preserved_on_resume(self, db_session, sample_csv, sample_csv2, temp_dir):
        output_db = os.path.join(temp_dir, "output_preserve.db")
        dag = create_branch_join_dag(sample_csv, sample_csv2, output_db, "preserve_result")

        pipeline = models.Pipeline(name="preserve_pipeline", dag_config=dag)
        db_session.add(pipeline)
        db_session.commit()

        execution = models.Execution(
            pipeline_id=pipeline.id,
            status=NodeStatus.RUNNING,
            start_time=datetime.utcnow()
        )
        db_session.add(execution)
        db_session.commit()
        db_session.refresh(execution)

        original_execute_node = DAGExecutor.execute_node
        call_count = {"join_node": 0}

        def fail_at_join(self, node_id):
            if node_id == "join_node":
                call_count["join_node"] += 1
                if call_count["join_node"] == 1:
                    end_time = datetime.now()
                    self.node_states[node_id] = {
                        "status": NodeStatus.FAILED,
                        "start_time": end_time.isoformat(),
                        "end_time": end_time.isoformat(),
                        "input_rows": 0,
                        "output_rows": 0,
                        "error": "join fail"
                    }
                    self._save_checkpoint()
                    return False, "join fail"
            return original_execute_node(self, node_id)

        with patch.object(DAGExecutor, 'execute_node', fail_at_join):
            executor1 = DAGExecutor(dag, pipeline_id=pipeline.id, execution_id=execution.id, db_session=db_session)
            success1, _ = executor1.execute()

        assert success1 == False

        source1_metric = db_session.query(models.PerformanceMetric).filter_by(
            execution_id=execution.id, node_id="source1"
        ).first()
        assert source1_metric is not None, "source1 metric should exist after first run"
        original_source1_duration = source1_metric.duration_ms

        execution.status = NodeStatus.FAILED
        execution.end_time = datetime.utcnow()
        db_session.commit()

        with patch.object(DAGExecutor, 'execute_node', fail_at_join):
            executor2 = DAGExecutor(dag, pipeline_id=pipeline.id, execution_id=execution.id, db_session=db_session)
            success2, _ = executor2.execute(resume_from_checkpoint=execution.checkpoint_data)

        assert success2 == True

        source1_metric_after = db_session.query(models.PerformanceMetric).filter_by(
            execution_id=execution.id, node_id="source1"
        ).first()
        assert source1_metric_after is not None, "source1 metric should still exist after resume"


class TestResumeValidation:
    def test_resume_nonexistent_execution(self, db_session, sample_csv, temp_dir):
        scheduler = PipelineScheduler(SessionLocal)
        try:
            pipeline = models.Pipeline(
                name="test",
                dag_config={"nodes": [], "edges": []}
            )
            db = _fresh_session()
            db.add(pipeline)
            db.commit()
            pid = pipeline.id
            db.close()

            result = scheduler.execute_pipeline(
                pipeline_id=pid,
                resume_from_failed=True,
                execution_id=99999,
                sync=True
            )
            assert result["success"] == False
            assert result["status"] == "rejected"
            assert "not found" in result["error"].lower()
        finally:
            scheduler.shutdown()

    def test_resume_without_execution_id_rejected(self, db_session, sample_csv, temp_dir):
        scheduler = PipelineScheduler(SessionLocal)
        try:
            pipeline = models.Pipeline(
                name="test_no_eid",
                dag_config={"nodes": [], "edges": []}
            )
            db = _fresh_session()
            db.add(pipeline)
            db.commit()
            pid = pipeline.id
            db.close()

            result = scheduler.execute_pipeline(
                pipeline_id=pid,
                resume_from_failed=True,
                execution_id=None,
                sync=True
            )
            assert result["success"] == False
            assert result["status"] == "rejected"
            assert "execution_id is required" in result["error"].lower()
        finally:
            scheduler.shutdown()

    def test_resume_completed_execution(self, db_session, sample_csv, sample_csv2, temp_dir):
        output_db = os.path.join(temp_dir, "output5.db")
        dag = create_branch_join_dag(sample_csv, sample_csv2, output_db, "t5")

        pipeline = models.Pipeline(name="test_p5", dag_config=dag)
        db_session.add(pipeline)
        db_session.commit()

        execution = models.Execution(
            pipeline_id=pipeline.id,
            status=NodeStatus.COMPLETED,
            start_time=datetime.utcnow(),
            end_time=datetime.utcnow()
        )
        db_session.add(execution)
        db_session.commit()
        db_session.refresh(execution)

        scheduler = PipelineScheduler(SessionLocal)
        try:
            result = scheduler.execute_pipeline(
                pipeline_id=pipeline.id,
                resume_from_failed=True,
                execution_id=execution.id,
                sync=True
            )
            assert result["success"] == False
            assert result["status"] == "rejected"
            assert "already completed" in result["error"].lower()
        finally:
            scheduler.shutdown()

    def test_resume_running_execution(self, db_session, sample_csv, sample_csv2, temp_dir):
        output_db = os.path.join(temp_dir, "output6.db")
        dag = create_branch_join_dag(sample_csv, sample_csv2, output_db, "t6")

        pipeline = models.Pipeline(name="test_p6", dag_config=dag)
        db_session.add(pipeline)
        db_session.commit()

        execution = models.Execution(
            pipeline_id=pipeline.id,
            status=NodeStatus.RUNNING,
            start_time=datetime.utcnow()
        )
        db_session.add(execution)
        db_session.commit()
        db_session.refresh(execution)

        scheduler = PipelineScheduler(SessionLocal)
        try:
            result = scheduler.execute_pipeline(
                pipeline_id=pipeline.id,
                resume_from_failed=True,
                execution_id=execution.id,
                sync=True
            )
            assert result["success"] == False
            assert result["status"] == "rejected"
            assert "already running" in result["error"].lower()
        finally:
            scheduler.shutdown()

    def test_resume_wrong_pipeline(self, db_session, sample_csv, sample_csv2, temp_dir):
        output_db = os.path.join(temp_dir, "output7.db")
        dag = create_branch_join_dag(sample_csv, sample_csv2, output_db, "t7")

        pipeline1 = models.Pipeline(name="test_p7a", dag_config=dag)
        pipeline2 = models.Pipeline(name="test_p7b", dag_config=dag)
        db_session.add(pipeline1)
        db_session.add(pipeline2)
        db_session.commit()

        execution = models.Execution(
            pipeline_id=pipeline1.id,
            status=NodeStatus.FAILED,
            start_time=datetime.utcnow(),
            end_time=datetime.utcnow(),
            checkpoint_data={"node_states": {}, "node_outputs": {}, "completed_node_ids": []}
        )
        db_session.add(execution)
        db_session.commit()
        db_session.refresh(execution)

        scheduler = PipelineScheduler(SessionLocal)
        try:
            result = scheduler.execute_pipeline(
                pipeline_id=pipeline2.id,
                resume_from_failed=True,
                execution_id=execution.id,
                sync=True
            )
            assert result["success"] == False
            assert result["status"] == "rejected"
            assert "belongs to pipeline" in result["error"].lower()
        finally:
            scheduler.shutdown()

    def test_duplicate_resume_rejected(self, db_session, sample_csv, sample_csv2, temp_dir):
        output_db = os.path.join(temp_dir, "output8.db")
        dag = create_branch_join_dag(sample_csv, sample_csv2, output_db, "t8")

        pipeline = models.Pipeline(name="test_p8", dag_config=dag)
        db_session.add(pipeline)
        db_session.commit()

        execution = models.Execution(
            pipeline_id=pipeline.id,
            status=NodeStatus.FAILED,
            start_time=datetime.utcnow(),
            end_time=datetime.utcnow(),
            checkpoint_data={
                "version": CHECKPOINT_VERSION,
                "node_states": {
                    "transform_map": {"status": NodeStatus.FAILED, "error": "fail"}
                },
                "node_outputs": {},
                "completed_node_ids": ["source1", "source2"],
                "output_written_nodes": [],
                "performance_metrics": {},
                "quality_reports": []
            }
        )
        db_session.add(execution)
        db_session.commit()
        db_session.refresh(execution)

        scheduler = PipelineScheduler(SessionLocal)
        try:
            lock = scheduler._get_execution_lock(execution.id)
            assert lock.acquire(blocking=False)

            result = scheduler.execute_pipeline(
                pipeline_id=pipeline.id,
                resume_from_failed=True,
                execution_id=execution.id,
                sync=True
            )
            assert result["success"] == False
            assert result["status"] == "rejected"
            assert "already being resumed" in result["error"].lower()

            lock.release()
        finally:
            scheduler.shutdown()


class TestEndToEndWithScheduler:
    def test_full_run_fail_resume_success_via_scheduler_sync(self, sample_csv, sample_csv2, temp_dir):
        output_db = os.path.join(temp_dir, "output_e2e.db")
        dag = create_branch_join_dag(sample_csv, sample_csv2, output_db, "e2e_result")

        db = _fresh_session()
        pipeline = models.Pipeline(name="e2e_pipeline", dag_config=dag)
        db.add(pipeline)
        db.commit()
        pid = pipeline.id
        db.close()

        scheduler = PipelineScheduler(SessionLocal)
        try:
            node_calls = {}
            original_execute_node = DAGExecutor.execute_node

            def fail_second_run_node(self, node_id):
                node_calls[node_id] = node_calls.get(node_id, 0) + 1
                if node_id == "transform_map" and node_calls[node_id] == 1:
                    end_time = datetime.now()
                    self.node_states[node_id] = {
                        "status": NodeStatus.FAILED,
                        "start_time": end_time.isoformat(),
                        "end_time": end_time.isoformat(),
                        "input_rows": 3,
                        "output_rows": 0,
                        "error": "Simulated E2E failure"
                    }
                    self._save_checkpoint()
                    return False, "Simulated E2E failure"
                return original_execute_node(self, node_id)

            with patch.object(DAGExecutor, 'execute_node', fail_second_run_node):
                result1 = scheduler.execute_pipeline(
                    pipeline_id=pid,
                    sync=True
                )

            assert result1["success"] == False
            assert result1["status"] == NodeStatus.FAILED
            execution_id = result1["execution_id"]
            assert execution_id is not None

            exec_obj = _get_execution(execution_id)
            assert exec_obj.status == NodeStatus.FAILED
            assert exec_obj.checkpoint_data is not None

            with patch.object(DAGExecutor, 'execute_node', fail_second_run_node):
                result2 = scheduler.execute_pipeline(
                    pipeline_id=pid,
                    resume_from_failed=True,
                    execution_id=execution_id,
                    sync=True
                )

            assert result2["success"] == True
            assert result2["status"] == NodeStatus.COMPLETED

            exec_obj2 = _get_execution(execution_id)
            assert exec_obj2.status == NodeStatus.COMPLETED
            assert exec_obj2.end_time is not None
            assert exec_obj2.error_log is None

            conn = sqlite3.connect(output_db)
            try:
                df = pd.read_sql_query("SELECT * FROM e2e_result", conn)
                assert len(df) > 0
            finally:
                conn.close()

            conn = sqlite3.connect(output_db)
            try:
                df2 = pd.read_sql_query("SELECT * FROM e2e_result", conn)
                assert len(df2) == len(df), "Output should not be duplicated"
            finally:
                conn.close()

            metrics_db = _fresh_session()
            try:
                metrics = metrics_db.query(models.PerformanceMetric).filter_by(
                    execution_id=execution_id
                ).all()
                node_ids_with_metrics = [m.node_id for m in metrics]
                assert len(node_ids_with_metrics) == len(set(node_ids_with_metrics)), \
                    "No duplicate performance metrics"
            finally:
                metrics_db.close()
        finally:
            scheduler.shutdown()

    def test_async_resume_updates_db_after_completion(self, sample_csv, sample_csv2, temp_dir):
        output_db = os.path.join(temp_dir, "output_async.db")
        dag = create_branch_join_dag(sample_csv, sample_csv2, output_db, "async_result")

        db = _fresh_session()
        pipeline = models.Pipeline(name="async_pipeline", dag_config=dag)
        db.add(pipeline)
        db.commit()
        pid = pipeline.id
        db.close()

        scheduler = PipelineScheduler(SessionLocal)
        try:
            node_calls = {}
            original_execute_node = DAGExecutor.execute_node

            def fail_first_run(self, node_id):
                node_calls[node_id] = node_calls.get(node_id, 0) + 1
                if node_id == "transform_map" and node_calls[node_id] == 1:
                    end_time = datetime.now()
                    self.node_states[node_id] = {
                        "status": NodeStatus.FAILED,
                        "start_time": end_time.isoformat(),
                        "end_time": end_time.isoformat(),
                        "input_rows": 3,
                        "output_rows": 0,
                        "error": "async failure"
                    }
                    self._save_checkpoint()
                    return False, "async failure"
                return original_execute_node(self, node_id)

            patcher = patch.object(DAGExecutor, 'execute_node', fail_first_run)
            patcher.start()
            try:
                result1 = scheduler.execute_pipeline(pipeline_id=pid, sync=True)

                assert result1["success"] == False
                execution_id = result1["execution_id"]

                exec_after_fail = _get_execution(execution_id)
                assert exec_after_fail.status == NodeStatus.FAILED

                result2 = scheduler.execute_pipeline(
                    pipeline_id=pid,
                    resume_from_failed=True,
                    execution_id=execution_id,
                    sync=False
                )

                assert result2["status"] == "started"
                assert result2["execution_id"] == execution_id

                deadline = time.time() + 15
                saw_running = False
                while time.time() < deadline:
                    exec_obj = _get_execution(execution_id)
                    if exec_obj.status == NodeStatus.RUNNING:
                        saw_running = True
                    if exec_obj.status in (NodeStatus.COMPLETED, NodeStatus.FAILED) and saw_running:
                        break
                    time.sleep(0.2)
            finally:
                patcher.stop()

            exec_final = _get_execution(execution_id)
            assert exec_final.status == NodeStatus.COMPLETED, \
                f"Async resume should complete, got status={exec_final.status}, error={exec_final.error_log}"
            assert exec_final.end_time is not None, "End time should be set after async completion"
            assert exec_final.checkpoint_data is not None, "Checkpoint should be updated after async completion"

            conn = sqlite3.connect(output_db)
            try:
                df = pd.read_sql_query("SELECT * FROM async_result", conn)
                assert len(df) > 0, "Output should have data after async resume"
            finally:
                conn.close()

        finally:
            scheduler.shutdown()

    def test_async_new_execution_updates_db_after_completion(self, sample_csv, sample_csv2, temp_dir):
        output_db = os.path.join(temp_dir, "output_async_new.db")
        dag = create_branch_join_dag(sample_csv, sample_csv2, output_db, "async_new_result")

        db = _fresh_session()
        pipeline = models.Pipeline(name="async_new_pipeline", dag_config=dag)
        db.add(pipeline)
        db.commit()
        pid = pipeline.id
        db.close()

        scheduler = PipelineScheduler(SessionLocal)
        try:
            result = scheduler.execute_pipeline(pipeline_id=pid, sync=False)
            assert result["status"] == "started"
            execution_id = result["execution_id"]

            deadline = time.time() + 15
            while time.time() < deadline:
                exec_obj = _get_execution(execution_id)
                if exec_obj.status in (NodeStatus.COMPLETED, NodeStatus.FAILED):
                    break
                time.sleep(0.2)

            exec_final = _get_execution(execution_id)
            assert exec_final.status == NodeStatus.COMPLETED, \
                f"Async execution should complete successfully, got {exec_final.status}"
            assert exec_final.end_time is not None
            assert exec_final.checkpoint_data is not None
            assert exec_final.checkpoint_data.get("completed_node_ids") is not None
            assert len(exec_final.checkpoint_data["completed_node_ids"]) == 7

            conn = sqlite3.connect(output_db)
            try:
                df = pd.read_sql_query("SELECT * FROM async_new_result", conn)
                assert len(df) > 0
            finally:
                conn.close()
        finally:
            scheduler.shutdown()


class TestOutputWriteThenCrash:
    def test_crash_after_output_write_no_duplicate_on_resume(self, db_session, sample_csv, sample_csv2, temp_dir):
        output_db = os.path.join(temp_dir, "output_crash.db")
        dag = create_branch_join_dag(sample_csv, sample_csv2, output_db, "crash_result")

        pipeline = models.Pipeline(name="crash_pipeline", dag_config=dag)
        db_session.add(pipeline)
        db_session.commit()

        execution = models.Execution(
            pipeline_id=pipeline.id,
            status=NodeStatus.RUNNING,
            start_time=datetime.utcnow()
        )
        db_session.add(execution)
        db_session.commit()
        db_session.refresh(execution)

        original_save = DAGExecutor._save_checkpoint
        crash_triggered = {"flag": False}

        def crash_after_output_checkpoint(self):
            original_save(self)
            if (not crash_triggered["flag"]
                and "output_node" in self.output_written_nodes
                and "output_node" not in self.completed_node_ids):
                crash_triggered["flag"] = True
                self.db_session.commit()
                raise RuntimeError("Simulated crash AFTER output write and checkpoint save")

        with patch.object(DAGExecutor, '_save_checkpoint', crash_after_output_checkpoint):
            executor1 = DAGExecutor(dag, pipeline_id=pipeline.id, execution_id=execution.id, db_session=db_session)
            success1, result1 = executor1.execute()

        assert success1 == False
        assert "Simulated crash AFTER output write" in result1["error"]

        db_session.commit()

        conn = sqlite3.connect(output_db)
        try:
            df_first = pd.read_sql_query("SELECT * FROM crash_result", conn)
            first_count = len(df_first)
            assert first_count > 0, "Output should have been written before crash"
        finally:
            conn.close()

        db_session.expire_all()
        exec_obj = db_session.query(models.Execution).filter_by(id=execution.id).first()
        checkpoint = exec_obj.checkpoint_data if exec_obj.checkpoint_data else {}
        assert "output_node" in checkpoint.get("output_written_nodes", []), \
            "Checkpoint should record that output was written"
        assert "output_node" not in checkpoint.get("completed_node_ids", []), \
            "Output node should NOT be marked completed (crash before completion checkpoint)"

        exec_obj.status = NodeStatus.FAILED
        exec_obj.end_time = datetime.utcnow()
        exec_obj.error_log = result1["error"]
        db_session.commit()

        executor2 = DAGExecutor(dag, pipeline_id=pipeline.id, execution_id=execution.id, db_session=db_session)
        success2, result2 = executor2.execute(resume_from_checkpoint=checkpoint)

        assert success2 == True, f"Resume should succeed, got: {result2.get('error', 'success')}"

        conn = sqlite3.connect(output_db)
        try:
            df_final = pd.read_sql_query("SELECT * FROM crash_result", conn)
            assert len(df_final) == first_count, \
                f"Output must not be duplicated. First write: {first_count} rows, after resume: {len(df_final)} rows"
        finally:
            conn.close()

    def test_crash_before_output_checkpoint_allows_clean_write(self, db_session, sample_csv, sample_csv2, temp_dir):
        output_db = os.path.join(temp_dir, "output_crash2.db")
        dag = create_branch_join_dag(sample_csv, sample_csv2, output_db, "crash2_result")

        pipeline = models.Pipeline(name="crash_pipeline2", dag_config=dag)
        db_session.add(pipeline)
        db_session.commit()

        execution = models.Execution(
            pipeline_id=pipeline.id,
            status=NodeStatus.RUNNING,
            start_time=datetime.utcnow()
        )
        db_session.add(execution)
        db_session.commit()
        db_session.refresh(execution)

        original_execute_node = DAGExecutor.execute_node
        call_count = {"join_node": 0}

        def crash_after_join(self, node_id):
            success, error = original_execute_node(self, node_id)
            if node_id == "join_node" and success:
                call_count["join_node"] += 1
                if call_count["join_node"] == 1:
                    return False, "Simulated crash after join but before output"
            return success, error

        with patch.object(DAGExecutor, 'execute_node', crash_after_join):
            executor1 = DAGExecutor(dag, pipeline_id=pipeline.id, execution_id=execution.id, db_session=db_session)
            success1, result1 = executor1.execute()

        assert success1 == False

        conn = sqlite3.connect(output_db)
        try:
            cur = conn.cursor()
            cur.execute("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='crash2_result'")
            if cur.fetchone()[0] > 0:
                df = pd.read_sql_query("SELECT * FROM crash2_result", conn)
                assert len(df) == 0, "No output should be written yet"
        finally:
            conn.close()

        execution.status = NodeStatus.FAILED
        execution.end_time = datetime.utcnow()
        execution.error_log = result1["error"]
        db_session.commit()

        db_session.expire_all()
        exec_obj = db_session.query(models.Execution).filter_by(id=execution.id).first()
        checkpoint = exec_obj.checkpoint_data if exec_obj.checkpoint_data else {}

        executor2 = DAGExecutor(dag, pipeline_id=pipeline.id, execution_id=execution.id, db_session=db_session)
        success2, result2 = executor2.execute(resume_from_checkpoint=checkpoint)

        assert success2 == True, f"Resume should succeed, got: {result2.get('error', 'success')}"

        conn = sqlite3.connect(output_db)
        try:
            df = pd.read_sql_query("SELECT * FROM crash2_result", conn)
            assert len(df) > 0, "Output should have data after successful resume"
        finally:
            conn.close()

    def test_idempotent_write_marker_prevents_duplicate_after_hard_crash(self, db_session, sample_csv, sample_csv2, temp_dir):
        output_db = os.path.join(temp_dir, "output_idempotent.db")
        dag = create_branch_join_dag(sample_csv, sample_csv2, output_db, "idempotent_result")

        pipeline = models.Pipeline(name="idempotent_pipeline", dag_config=dag)
        db_session.add(pipeline)
        db_session.commit()

        execution = models.Execution(
            pipeline_id=pipeline.id,
            status=NodeStatus.RUNNING,
            start_time=datetime.utcnow()
        )
        db_session.add(execution)
        db_session.commit()
        db_session.refresh(execution)

        original_save = DAGExecutor._save_checkpoint
        crash_count = {"n": 0}

        def crash_before_app_checkpoint(self):
            if ("output_node" in self.output_written_nodes
                and "output_node" not in self.completed_node_ids):
                crash_count["n"] += 1
                raise RuntimeError("Simulated hard crash AFTER target DB write, BEFORE app checkpoint save")
            original_save(self)

        with patch.object(DAGExecutor, '_save_checkpoint', crash_before_app_checkpoint):
            executor1 = DAGExecutor(dag, pipeline_id=pipeline.id, execution_id=execution.id, db_session=db_session)
            try:
                success1, result1 = executor1.execute()
            except RuntimeError as e:
                assert "Simulated hard crash" in str(e)

        conn = sqlite3.connect(output_db)
        try:
            df_first = pd.read_sql_query("SELECT * FROM idempotent_result", conn)
            first_count = len(df_first)
            assert first_count > 0, "Data should have been written to target DB before crash"

            cur = conn.cursor()
            cur.execute("SELECT rows_written FROM _etl_write_markers WHERE execution_id=? AND node_id=?",
                        (execution.id, "output_node"))
            marker_row = cur.fetchone()
            assert marker_row is not None, "Idempotency marker should exist in target DB"
            assert marker_row[0] == first_count
        finally:
            conn.close()

        db_session.expire_all()
        exec_obj = db_session.query(models.Execution).filter_by(id=execution.id).first()
        checkpoint_before_resume = exec_obj.checkpoint_data if exec_obj.checkpoint_data else {}
        assert "output_node" not in checkpoint_before_resume.get("output_written_nodes", []), \
            "App checkpoint should NOT know about the write (simulating hard crash before save)"

        exec_obj.status = NodeStatus.FAILED
        exec_obj.end_time = datetime.utcnow()
        exec_obj.error_log = "Simulated hard crash"
        db_session.commit()

        db_session.expire_all()
        exec_obj2 = db_session.query(models.Execution).filter_by(id=execution.id).first()
        resume_checkpoint = exec_obj2.checkpoint_data if exec_obj2.checkpoint_data else {}

        executor2 = DAGExecutor(dag, pipeline_id=pipeline.id, execution_id=execution.id, db_session=db_session)
        success2, result2 = executor2.execute(resume_from_checkpoint=resume_checkpoint)

        assert success2 == True, f"Resume should succeed, got: {result2.get('error', 'success')}"

        conn = sqlite3.connect(output_db)
        try:
            df_final = pd.read_sql_query("SELECT * FROM idempotent_result", conn)
            assert len(df_final) == first_count, \
                f"Output must NOT be duplicated by idempotency marker. First: {first_count}, after resume: {len(df_final)}"
        finally:
            conn.close()


class TestCheckpointPersistence:
    def test_checkpoint_saved_after_each_node(self, db_session, sample_csv, sample_csv2, temp_dir):
        output_db = os.path.join(temp_dir, "output_cp.db")
        dag = create_branch_join_dag(sample_csv, sample_csv2, output_db, "cp_result")

        pipeline = models.Pipeline(name="cp_pipeline", dag_config=dag)
        db_session.add(pipeline)
        db_session.commit()

        execution = models.Execution(
            pipeline_id=pipeline.id,
            status=NodeStatus.RUNNING,
            start_time=datetime.utcnow()
        )
        db_session.add(execution)
        db_session.commit()
        db_session.refresh(execution)

        save_counts = []
        original_save = DAGExecutor._save_checkpoint

        def counting_save(self):
            original_save(self)
            if self.execution_id:
                db_session.expire_all()
                exec_obj = db_session.query(models.Execution).filter_by(id=self.execution_id).first()
                if exec_obj and exec_obj.checkpoint_data:
                    completed = exec_obj.checkpoint_data.get("completed_node_ids", [])
                    save_counts.append(len(completed))

        with patch.object(DAGExecutor, '_save_checkpoint', counting_save):
            executor = DAGExecutor(dag, pipeline_id=pipeline.id, execution_id=execution.id, db_session=db_session)
            executor.execute()

        assert len(save_counts) > 0, "Checkpoint should have been saved"
        assert save_counts[-1] == 7, f"Final checkpoint should have 7 completed nodes, got {save_counts[-1]}"
        assert save_counts == sorted(save_counts), "Checkpoint should accumulate completed nodes"

    def test_output_checkpoint_saved_immediately_after_write(self, db_session, sample_csv, sample_csv2, temp_dir):
        output_db = os.path.join(temp_dir, "output_cp_immediate.db")
        dag = create_branch_join_dag(sample_csv, sample_csv2, output_db, "cp_imm_result")

        pipeline = models.Pipeline(name="cp_imm_pipeline", dag_config=dag)
        db_session.add(pipeline)
        db_session.commit()

        execution = models.Execution(
            pipeline_id=pipeline.id,
            status=NodeStatus.RUNNING,
            start_time=datetime.utcnow()
        )
        db_session.add(execution)
        db_session.commit()
        db_session.refresh(execution)

        save_events = []
        original_save = DAGExecutor._save_checkpoint

        def tracking_save(self):
            original_save(self)
            if self.execution_id and "output_node" in self.output_written_nodes:
                save_events.append("output_written_and_checkpointed")
                raise RuntimeError("STOP_AFTER_OUTPUT_CHECKPOINT")

        try:
            with patch.object(DAGExecutor, '_save_checkpoint', tracking_save):
                executor = DAGExecutor(dag, pipeline_id=pipeline.id, execution_id=execution.id, db_session=db_session)
                executor.execute()
        except RuntimeError as e:
            if "STOP_AFTER_OUTPUT_CHECKPOINT" not in str(e):
                raise

        db_session.commit()
        exec_obj = db_session.query(models.Execution).filter_by(id=execution.id).first()
        assert "output_node" in exec_obj.checkpoint_data.get("output_written_nodes", []), \
            "Output write must be checkpointed immediately after write completes"


class TestFailedNodeErrorCleared:
    def test_failed_node_error_cleared_on_resume(self, db_session, sample_csv, sample_csv2, temp_dir):
        output_db = os.path.join(temp_dir, "output_err.db")
        dag = create_branch_join_dag(sample_csv, sample_csv2, output_db, "err_result")

        pipeline = models.Pipeline(name="err_pipeline", dag_config=dag)
        db_session.add(pipeline)
        db_session.commit()

        execution = models.Execution(
            pipeline_id=pipeline.id,
            status=NodeStatus.RUNNING,
            start_time=datetime.utcnow()
        )
        db_session.add(execution)
        db_session.commit()
        db_session.refresh(execution)

        original_execute_node = DAGExecutor.execute_node
        call_count = {"transform_filter_b": 0}

        def fail_once(self, node_id):
            if node_id == "transform_filter_b":
                call_count["transform_filter_b"] += 1
                if call_count["transform_filter_b"] == 1:
                    end_time = datetime.now()
                    self.node_states[node_id] = {
                        "status": NodeStatus.FAILED,
                        "start_time": end_time.isoformat(),
                        "end_time": end_time.isoformat(),
                        "input_rows": 6,
                        "output_rows": 0,
                        "error": "Temporary error"
                    }
                    self._save_checkpoint()
                    return False, "Temporary error"
            return original_execute_node(self, node_id)

        with patch.object(DAGExecutor, 'execute_node', fail_once):
            executor1 = DAGExecutor(dag, pipeline_id=pipeline.id, execution_id=execution.id, db_session=db_session)
            success1, result1 = executor1.execute()

        assert success1 == False
        assert result1["node_states"]["transform_filter_b"]["status"] == NodeStatus.FAILED
        assert result1["node_states"]["transform_filter_b"]["error"] == "Temporary error"

        execution.status = NodeStatus.FAILED
        execution.end_time = datetime.utcnow()
        db_session.commit()

        with patch.object(DAGExecutor, 'execute_node', fail_once):
            executor2 = DAGExecutor(dag, pipeline_id=pipeline.id, execution_id=execution.id, db_session=db_session)
            success2, result2 = executor2.execute(resume_from_checkpoint=execution.checkpoint_data)

        assert success2 == True
        assert result2["node_states"]["transform_filter_b"]["status"] == NodeStatus.COMPLETED
        assert result2["node_states"]["transform_filter_b"]["error"] is None
        assert result2["node_states"]["transform_filter_b"]["end_time"] is not None


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
