import os
import sys
import json
import tempfile
import shutil
import threading
import sqlite3
from datetime import datetime
from unittest.mock import patch, MagicMock

import pytest
import pandas as pd
import requests

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "backend"))

os.environ["DATABASE_URL"] = "sqlite:///:memory:"

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import models
from database import Base
from dag_executor import DAGExecutor, NodeStatus
from scheduler import PipelineScheduler
from lineage_tracker import LineageTracker


@pytest.fixture
def db_engine():
    from sqlalchemy.pool import StaticPool
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool
    )
    Base.metadata.create_all(bind=engine)
    return engine


@pytest.fixture
def db_session_factory(db_engine):
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=db_engine)
    return SessionLocal


@pytest.fixture
def temp_dir():
    d = tempfile.mkdtemp()
    yield d
    shutil.rmtree(d, ignore_errors=True)


def _create_csv(path, df):
    df.to_csv(path, index=False)


def _build_branch_join_dag(temp_dir, output_db_path):
    src1_path = os.path.join(temp_dir, "sales.csv")
    src2_path = os.path.join(temp_dir, "regions.csv")

    sales_df = pd.DataFrame({
        "id": [1, 2, 3, 4, 5, 6],
        "region": ["AMER", "EMEA", "AMER", "APAC", "EMEA", "AMER"],
        "amount": [100.0, 200.0, 150.0, 300.0, 250.0, 175.0],
        "qty": [1, 2, 3, 1, 2, 5]
    })
    regions_df = pd.DataFrame({
        "region": ["AMER", "EMEA", "APAC"],
        "region_name": ["Americas", "Europe", "Asia Pacific"],
        "tax_rate": [0.07, 0.20, 0.10]
    })

    _create_csv(src1_path, sales_df)
    _create_csv(src2_path, regions_df)

    dag_config = {
        "nodes": [
            {
                "id": "src1", "type": "source",
                "data": {
                    "label": "Sales Source",
                    "config": {
                        "source_type": "csv",
                        "source_config": {"file_path": src1_path}
                    }
                }
            },
            {
                "id": "mp1", "type": "map",
                "data": {
                    "label": "Add Computed Column",
                    "config": {
                        "operations": [
                            {"type": "compute", "new_column": "total", "expression": "amount * qty"}
                        ]
                    }
                }
            },
            {
                "id": "br1", "type": "branch",
                "data": {
                    "label": "Split by Region",
                    "config": {
                        "branches": [
                            {"condition": "region == 'AMER'"},
                            {"condition": None}
                        ]
                    }
                }
            },
            {
                "id": "out1", "type": "output",
                "data": {
                    "label": "Write NA Sales",
                    "config": {
                        "output_type": "sqlite",
                        "output_config": {
                            "db_path": output_db_path,
                            "table_name": "na_sales",
                            "write_mode": "append"
                        }
                    }
                }
            },
            {
                "id": "src2", "type": "source",
                "data": {
                    "label": "Regions Source",
                    "config": {
                        "source_type": "csv",
                        "source_config": {"file_path": src2_path}
                    }
                }
            },
            {
                "id": "jn1", "type": "join",
                "data": {
                    "label": "Join with Regions",
                    "config": {
                        "join_type": "inner",
                        "left_key": "region",
                        "right_key": "region"
                    }
                }
            },
            {
                "id": "out2", "type": "output",
                "data": {
                    "label": "Write Joined Results",
                    "config": {
                        "output_type": "sqlite",
                        "output_config": {
                            "db_path": output_db_path,
                            "table_name": "joined_results",
                            "write_mode": "append"
                        }
                    }
                }
            }
        ],
        "edges": [
            {"source": "src1", "target": "mp1"},
            {"source": "mp1", "target": "br1"},
            {"source": "br1", "target": "out1", "sourceHandle": "branch_0"},
            {"source": "br1", "target": "jn1", "sourceHandle": "branch_1"},
            {"source": "src2", "target": "jn1"},
            {"source": "jn1", "target": "out2"}
        ]
    }
    return dag_config


class TestCheckpointSerialization:
    def test_dataframe_round_trip_preserves_dtypes(self):
        df = pd.DataFrame({
            "a": [1, 2, 3],
            "b": [1.5, 2.5, 3.5],
            "c": ["x", "y", "z"]
        })
        df["a"] = df["a"].astype("int64")
        df["b"] = df["b"].astype("float64")

        executor = DAGExecutor({"nodes": [], "edges": []})
        serialized = executor._serialize_output(df)
        assert serialized["__type__"] == "dataframe"
        assert serialized["dtypes"]["a"] == "int64"

        restored = executor._deserialize_output(serialized)
        assert isinstance(restored, pd.DataFrame)
        assert list(restored.columns) == ["a", "b", "c"]
        assert restored["a"].dtype == "int64"
        assert restored["b"].dtype == "float64"
        assert len(restored) == 3

    def test_list_of_dataframes_round_trip(self):
        df1 = pd.DataFrame({"x": [1, 2]})
        df2 = pd.DataFrame({"y": [3, 4]})
        executor = DAGExecutor({"nodes": [], "edges": []})
        serialized = executor._serialize_output([df1, df2])
        assert serialized["__type__"] == "list"
        restored = executor._deserialize_output(serialized)
        assert isinstance(restored, list)
        assert len(restored) == 2
        assert isinstance(restored[0], pd.DataFrame)
        assert list(restored[0].columns) == ["x"]
        assert list(restored[1].columns) == ["y"]

    def test_checkpoint_includes_version_and_all_fields(self):
        dag_config = {"nodes": [], "edges": []}
        executor = DAGExecutor(dag_config)
        executor.node_states["n1"] = {"status": NodeStatus.COMPLETED}
        executor.node_outputs["n1"] = pd.DataFrame({"a": [1]})
        cp = executor._create_checkpoint()
        assert cp["version"] == 1
        assert "node_states" in cp
        assert "node_outputs" in cp
        assert "lineage" in cp
        assert "performance_metrics" in cp
        assert "quality_reports" in cp


class TestDownstreamComputation:
    def test_downstream_closure(self):
        dag = {
            "nodes": [
                {"id": "a"}, {"id": "b"}, {"id": "c"},
                {"id": "d"}, {"id": "e"}
            ],
            "edges": [
                {"source": "a", "target": "b"},
                {"source": "b", "target": "c"},
                {"source": "a", "target": "d"},
                {"source": "c", "target": "e"}
            ]
        }
        executor = DAGExecutor(dag)
        downstream = executor._get_downstream_set({"b"})
        assert downstream == {"c", "e"}
        assert "d" not in downstream

    def test_downstream_closure_multiple_roots(self):
        dag = {
            "nodes": [{"id": n} for n in ["a", "b", "c", "d"]],
            "edges": [
                {"source": "a", "target": "c"},
                {"source": "b", "target": "c"},
                {"source": "c", "target": "d"}
            ]
        }
        executor = DAGExecutor(dag)
        downstream = executor._get_downstream_set({"a", "b"})
        assert downstream == {"c", "d"}


class TestBranchJoinDAGRecovery:
    def test_full_resume_flow_with_branch_and_join(self, db_session_factory, temp_dir):
        from transform_nodes import JoinNode

        output_db = os.path.join(temp_dir, "output.db")
        dag_config = _build_branch_join_dag(temp_dir, output_db)

        db = db_session_factory()
        pipeline = models.Pipeline(
            name="Test Branch-Join Pipeline",
            dag_config=dag_config
        )
        db.add(pipeline)
        db.commit()
        db.refresh(pipeline)
        db.close()

        scheduler = PipelineScheduler(db_session_factory)
        try:
            source_read_count = {"src1": 0, "src2": 0}
            output_write_count = {"out1": 0, "out2": 0}
            join_call_count = [0]
            original_read = None
            original_write = None
            original_join_execute = JoinNode.execute

            def counting_read(source_type, config, data_source_id=None, db_session=None):
                if source_type == "csv":
                    fp = config.get("file_path", "")
                    if "sales.csv" in fp:
                        source_read_count["src1"] += 1
                    elif "regions.csv" in fp:
                        source_read_count["src2"] += 1
                return original_read(source_type, config, data_source_id, db_session)

            def counting_write(target_type, df, config, **kwargs):
                table = config.get("table_name", "")
                if table == "na_sales":
                    output_write_count["out1"] += 1
                elif table == "joined_results":
                    output_write_count["out2"] += 1
                return original_write(target_type, df, config, **kwargs)

            def fail_first_join(self_join, dfs, config):
                join_call_count[0] += 1
                if join_call_count[0] == 1:
                    raise RuntimeError("Simulated join failure on first run")
                return original_join_execute(self_join, dfs, config)

            import data_sources
            import output_targets
            original_read = data_sources.DataSourceHandler.read
            original_write = output_targets.OutputExecutor.write

            with patch.object(data_sources.DataSourceHandler, 'read', side_effect=counting_read), \
                 patch.object(output_targets.OutputExecutor, 'write', side_effect=counting_write), \
                 patch.object(JoinNode, 'execute', fail_first_join):

                result1 = scheduler.execute_pipeline(
                    pipeline.id, synchronous=True
                )

                assert result1["status"] == "failed", f"First run should fail, got {result1}"
                assert join_call_count[0] == 1

                result2 = scheduler.execute_pipeline(
                    pipeline.id,
                    resume_from_failed=True,
                    execution_id=result1["execution_id"],
                    synchronous=True
                )

            assert result2["status"] == "completed", f"Resume should succeed, got {result2}"
            assert join_call_count[0] == 2, f"Join should have been called twice total (1 fail + 1 success), got {join_call_count[0]}"

            assert source_read_count["src1"] == 1, f"src1 read {source_read_count['src1']} times, expected 1"
            assert source_read_count["src2"] == 1, f"src2 read {source_read_count['src2']} times, expected 1"
            assert output_write_count["out1"] == 1, f"out1 written {output_write_count['out1']} times, expected 1"
            assert output_write_count["out2"] == 1, f"out2 written {output_write_count['out2']} times, expected 1"

            db = db_session_factory()
            execution = db.query(models.Execution).filter(
                models.Execution.id == result1["execution_id"]
            ).first()
            assert execution.status == "completed"
            assert execution.error_log is None
            assert execution.end_time is not None

            states = execution.node_states
            assert states["jn1"]["status"] == NodeStatus.COMPLETED
            assert states["jn1"]["error"] is None
            assert states["jn1"]["end_time"] is not None
            assert states["out2"]["status"] == NodeStatus.COMPLETED

            metrics = db.query(models.PerformanceMetric).filter(
                models.PerformanceMetric.execution_id == execution.id
            ).all()
            metric_nodes = [m.node_id for m in metrics]
            assert len(metric_nodes) == 7, f"Expected 7 metrics, got {len(metric_nodes)}: {metric_nodes}"
            for nid in ["src1", "src2", "mp1", "br1", "out1", "jn1", "out2"]:
                assert metric_nodes.count(nid) == 1, f"Node {nid} should have exactly 1 metric, got {metric_nodes.count(nid)}"

            lineage_count = db.query(models.LineageRecord).filter(
                models.LineageRecord.execution_id == execution.id
            ).count()
            assert lineage_count > 0, "Should have lineage records"

            db.close()

            conn = sqlite3.connect(output_db)
            na_sales = pd.read_sql_query("SELECT * FROM na_sales", conn)
            joined = pd.read_sql_query("SELECT * FROM joined_results", conn)
            conn.close()

            assert len(na_sales) == 3, f"NA sales should have 3 rows (regions=NA), got {len(na_sales)}"
            assert set(na_sales["region"].unique()) == {"AMER"}
            assert len(joined) == 3, f"Joined results should have 3 rows (EMEA + APAC non-AMER), got {len(joined)}"
            assert "region_name" in joined.columns, "Join should include region_name"
            assert "tax_rate" in joined.columns, "Join should include tax_rate"
            assert "total" in joined.columns, "Map column should be preserved"

            non_amer_regions = set(joined["region"].unique())
            assert non_amer_regions == {"EMEA", "APAC"}, f"Expected EMEA/APAC, got {non_amer_regions}"

            expected_totals = {"EU": 400.0 + 500.0, "APAC": 300.0}
            for _, row in joined.iterrows():
                assert abs(row["total"] - row["amount"] * row["qty"]) < 0.01
        finally:
            scheduler.shutdown()

    def test_duplicate_resume_rejected(self, db_session_factory, temp_dir):
        output_db = os.path.join(temp_dir, "output.db")
        dag_config = _build_branch_join_dag(temp_dir, output_db)

        db = db_session_factory()
        pipeline = models.Pipeline(name="Test Duplicate", dag_config=dag_config)
        db.add(pipeline)
        db.commit()
        db.refresh(pipeline)
        db.close()

        scheduler = PipelineScheduler(db_session_factory)
        try:
            from transform_nodes import JoinNode
            import data_sources
            import output_targets

            join_call_count = [0]
            original_join = JoinNode.execute

            def fail_first_join(self, dfs, config):
                join_call_count[0] += 1
                if join_call_count[0] == 1:
                    raise RuntimeError("Join failure")
                return original_join(self, dfs, config)

            with patch.object(JoinNode, 'execute', fail_first_join):
                result1 = scheduler.execute_pipeline(pipeline.id, synchronous=True)
                assert result1["status"] == "failed"
                exec_id = result1["execution_id"]

                result2 = scheduler.execute_pipeline(
                    pipeline.id, resume_from_failed=True,
                    execution_id=exec_id, synchronous=True
                )
            assert result2["status"] == "completed"

            result3 = scheduler.execute_pipeline(
                pipeline.id, resume_from_failed=True,
                execution_id=exec_id, synchronous=True
            )
            assert result3["status"] == "already_completed", \
                f"Duplicate resume of completed execution should be rejected, got {result3}"

            result4 = scheduler.execute_pipeline(
                pipeline.id + 999, resume_from_failed=True,
                execution_id=exec_id, synchronous=True
            )
            assert result4["status"] in ("error", "not_found"), \
                f"Non-existent pipeline should return error, got {result4}"

            db2 = db_session_factory()
            other_pipeline = models.Pipeline(name="Other", dag_config=dag_config)
            db2.add(other_pipeline)
            db2.commit()
            db2.refresh(other_pipeline)
            other_pid = other_pipeline.id
            db2.close()

            result4b = scheduler.execute_pipeline(
                other_pid, resume_from_failed=True,
                execution_id=exec_id, synchronous=True
            )
            assert result4b["status"] == "pipeline_mismatch", \
                f"Execution from different pipeline should be rejected, got {result4b}"

            result5 = scheduler.execute_pipeline(
                pipeline.id, resume_from_failed=True,
                execution_id=999999, synchronous=True
            )
            assert result5["status"] == "not_found"

            result6 = scheduler.execute_pipeline(
                pipeline.id, resume_from_failed=True,
                execution_id=None, synchronous=True
            )
            assert result6["status"] == "missing_execution_id"
        finally:
            scheduler.shutdown()

    def test_concurrent_resume_rejected(self, db_session_factory, temp_dir):
        output_db = os.path.join(temp_dir, "output.db")
        dag_config = _build_branch_join_dag(temp_dir, output_db)

        db = db_session_factory()
        pipeline = models.Pipeline(name="Test Concurrent", dag_config=dag_config)
        db.add(pipeline)
        db.commit()
        db.refresh(pipeline)
        db.close()

        scheduler = PipelineScheduler(db_session_factory)
        try:
            from transform_nodes import JoinNode
            original_join = JoinNode.execute

            join_call_count = [0]
            def fail_first_slow(self, dfs, config):
                join_call_count[0] += 1
                if join_call_count[0] == 1:
                    import time
                    time.sleep(0.3)
                    raise RuntimeError("Slow join failure")
                import time
                time.sleep(0.8)
                return original_join(self, dfs, config)

            with patch.object(JoinNode, 'execute', fail_first_slow):
                result1 = scheduler.execute_pipeline(pipeline.id, synchronous=True)
            assert result1["status"] == "failed"
            exec_id = result1["execution_id"]

            results = []
            lock_acquired = threading.Event()

            def slow_resume():
                import time
                with patch.object(JoinNode, 'execute', fail_first_slow):
                    r = scheduler.execute_pipeline(
                        pipeline.id, resume_from_failed=True,
                        execution_id=exec_id, synchronous=True
                    )
                    results.append(r)

            t1 = threading.Thread(target=slow_resume)
            t1.start()

            import time
            time.sleep(0.15)

            t2_result = scheduler.execute_pipeline(
                pipeline.id, resume_from_failed=True,
                execution_id=exec_id, synchronous=True
            )

            t1.join()

            statuses = [r["status"] for r in results] + [t2_result["status"]]
            assert "completed" in statuses, f"At least one resume should succeed: {statuses}"
            assert "concurrent_resume" in statuses or "already_running" in statuses, \
                f"One concurrent resume should be rejected: {statuses}"
        finally:
            scheduler.shutdown()

    def test_resume_with_running_execution_rejected(self, db_session_factory, temp_dir):
        output_db = os.path.join(temp_dir, "output.db")
        dag_config = _build_branch_join_dag(temp_dir, output_db)

        db = db_session_factory()
        pipeline = models.Pipeline(name="Test Running", dag_config=dag_config)
        db.add(pipeline)
        db.commit()
        db.refresh(pipeline)
        pid = pipeline.id

        execution = models.Execution(
            pipeline_id=pid, status="running",
            start_time=datetime.utcnow()
        )
        db.add(execution)
        db.commit()
        db.refresh(execution)
        eid = execution.id
        db.close()

        scheduler = PipelineScheduler(db_session_factory)
        try:
            result = scheduler.execute_pipeline(
                pid, resume_from_failed=True,
                execution_id=eid, synchronous=True
            )
            assert result["status"] == "already_running"
        finally:
            scheduler.shutdown()

    def test_output_not_duplicated_after_resume(self, db_session_factory, temp_dir):
        output_db = os.path.join(temp_dir, "output_nodup.db")
        dag_config = _build_branch_join_dag(temp_dir, output_db)

        db = db_session_factory()
        pipeline = models.Pipeline(name="Test NoDup", dag_config=dag_config)
        db.add(pipeline)
        db.commit()
        db.refresh(pipeline)
        db.close()

        scheduler = PipelineScheduler(db_session_factory)
        try:
            from transform_nodes import JoinNode
            import data_sources
            import output_targets

            join_calls = [0]
            original_join = JoinNode.execute

            def fail_once_join(self, dfs, config):
                join_calls[0] += 1
                if join_calls[0] == 1:
                    raise RuntimeError("fail once")
                return original_join(self, dfs, config)

            with patch.object(JoinNode, 'execute', fail_once_join):
                r1 = scheduler.execute_pipeline(pipeline.id, synchronous=True)
                assert r1["status"] == "failed"

                r2 = scheduler.execute_pipeline(
                    pipeline.id, resume_from_failed=True,
                    execution_id=r1["execution_id"], synchronous=True
                )
            assert r2["status"] == "completed"

            conn = sqlite3.connect(output_db)
            na_count = pd.read_sql_query("SELECT COUNT(*) as c FROM na_sales", conn).iloc[0]["c"]
            joined_count = pd.read_sql_query("SELECT COUNT(*) as c FROM joined_results", conn).iloc[0]["c"]
            conn.close()

            assert na_count == 3, f"na_sales should have exactly 3 rows, got {na_count}"
            assert joined_count == 3, f"joined_results should have exactly 3 rows, got {joined_count}"

            db = db_session_factory()
            metrics = db.query(models.PerformanceMetric).filter(
                models.PerformanceMetric.execution_id == r1["execution_id"]
            ).all()
            node_counts = {}
            for m in metrics:
                node_counts[m.node_id] = node_counts.get(m.node_id, 0) + 1
            for nid, count in node_counts.items():
                assert count == 1, f"Node {nid} has {count} performance metrics (expected 1)"

            lineage_count = db.query(models.LineageRecord).filter(
                models.LineageRecord.execution_id == r1["execution_id"]
            ).count()
            assert lineage_count > 0

            db.close()
        finally:
            scheduler.shutdown()

    def test_fresh_execution_ignores_stale_checkpoint(self, db_session_factory, temp_dir):
        output_db = os.path.join(temp_dir, "output_fresh.db")
        dag_config = _build_branch_join_dag(temp_dir, output_db)

        db = db_session_factory()
        pipeline = models.Pipeline(name="Test Fresh", dag_config=dag_config)
        db.add(pipeline)
        db.commit()
        db.refresh(pipeline)
        db.close()

        scheduler = PipelineScheduler(db_session_factory)
        try:
            result = scheduler.execute_pipeline(pipeline.id, synchronous=True)
            assert result["status"] == "completed"

            db = db_session_factory()
            exec_row = db.query(models.Execution).filter(
                models.Execution.id == result["execution_id"]
            ).first()
            assert exec_row.status == "completed"
            assert exec_row.checkpoint_data is not None
            states = exec_row.node_states
            for nid in ["src1", "src2", "mp1", "br1", "out1", "jn1", "out2"]:
                assert states[nid]["status"] == NodeStatus.COMPLETED
            db.close()
        finally:
            scheduler.shutdown()


class TestLineageTracker:
    def test_serialize_deserialize_roundtrip(self):
        tracker = LineageTracker(pipeline_id=1, execution_id=100)
        tracker.track_field_mapping(
            node_id="src1",
            source_table="sales",
            source_column="id",
            target_table="Sales Source",
            target_column="id",
            transform_ops=[{"node_id": "src1", "node_type": "source", "operation": "extract"}]
        )
        tracker.record_transform(
            node_id="jn1",
            node_type="join",
            input_fields=["id", "region"],
            output_fields=["id", "region", "region_name"],
            operation={"join_type": "inner"}
        )

        data = tracker.serialize()
        assert len(data["field_mappings"]) == 1
        assert len(data["transform_records"]) == 1
        assert len(data["lineage_records"]) == 1

        tracker2 = LineageTracker(pipeline_id=1, execution_id=100)
        tracker2.deserialize(data, pipeline_id=1, execution_id=100)
        assert len(tracker2._lineage_records) == 1
        assert len(tracker2._field_mappings) == 1
        assert tracker2._field_mappings[0]["node_id"] == "src1"

    def test_remove_nodes(self):
        tracker = LineageTracker(pipeline_id=1, execution_id=100)
        tracker.track_field_mapping("src1", "a", "b", "c", "d", [])
        tracker.track_field_mapping("out1", "e", "f", "g", "h", [])
        tracker.track_field_mapping("out2", "i", "j", "k", "l", [])

        assert len(tracker._lineage_records) == 3
        tracker.remove_nodes({"out1"})
        assert len(tracker._lineage_records) == 2
        remaining_nodes = [m["node_id"] for m in tracker._field_mappings]
        assert "out1" not in remaining_nodes
        assert "src1" in remaining_nodes
        assert "out2" in remaining_nodes


class TestSchedulerValidation:
    def test_nonexistent_execution_id(self, db_session_factory, temp_dir):
        output_db = os.path.join(temp_dir, "output_val.db")
        dag_config = _build_branch_join_dag(temp_dir, output_db)

        db = db_session_factory()
        pipeline = models.Pipeline(name="Val", dag_config=dag_config)
        db.add(pipeline)
        db.commit()
        db.refresh(pipeline)
        db.close()

        scheduler = PipelineScheduler(db_session_factory)
        try:
            r = scheduler.execute_pipeline(
                pipeline.id, resume_from_failed=True,
                execution_id=987654, synchronous=True
            )
            assert r["status"] == "not_found"
        finally:
            scheduler.shutdown()

    def test_new_execution_creates_record(self, db_session_factory, temp_dir):
        output_db = os.path.join(temp_dir, "output_new.db")
        dag_config = _build_branch_join_dag(temp_dir, output_db)

        db = db_session_factory()
        pipeline = models.Pipeline(name="New", dag_config=dag_config)
        db.add(pipeline)
        db.commit()
        db.refresh(pipeline)
        db.close()

        scheduler = PipelineScheduler(db_session_factory)
        try:
            r = scheduler.execute_pipeline(pipeline.id, synchronous=True)
            assert r["status"] == "completed"
            assert r["execution_id"] is not None

            db = db_session_factory()
            exec_count = db.query(models.Execution).filter(
                models.Execution.pipeline_id == pipeline.id
            ).count()
            assert exec_count == 1
            db.close()
        finally:
            scheduler.shutdown()


class TestOutputIdempotency:
    def test_sqlite_marker_prevents_duplicate_write(self, temp_dir):
        from output_targets import SQLiteOutput, MARKER_TABLE
        output_db = os.path.join(temp_dir, "idempotent.db")
        target = SQLiteOutput()
        df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        config = {"db_path": output_db, "table_name": "test_tbl", "write_mode": "append"}

        rows1, errs1, skip1 = target.write(df, config, execution_id=1, node_id="n1")
        assert rows1 == 3
        assert skip1 is False

        rows2, errs2, skip2 = target.write(df, config, execution_id=1, node_id="n1")
        assert rows2 == 3
        assert skip2 is True

        conn = sqlite3.connect(output_db)
        cnt = pd.read_sql_query("SELECT COUNT(*) as c FROM test_tbl", conn).iloc[0]["c"]
        marker_cnt = pd.read_sql_query(
            f"SELECT COUNT(*) as c FROM {MARKER_TABLE}", conn
        ).iloc[0]["c"]
        conn.close()
        assert cnt == 3, f"Expected 3 rows after duplicate write, got {cnt}"
        assert marker_cnt == 1

    def test_sqlite_marker_different_execution_separate(self, temp_dir):
        from output_targets import SQLiteOutput
        output_db = os.path.join(temp_dir, "idempotent2.db")
        target = SQLiteOutput()
        df = pd.DataFrame({"a": [1, 2]})
        config = {"db_path": output_db, "table_name": "t2", "write_mode": "append"}

        target.write(df, config, execution_id=1, node_id="n1")
        target.write(df, config, execution_id=2, node_id="n1")
        target.write(df, config, execution_id=1, node_id="n1")
        target.write(df, config, execution_id=2, node_id="n1")

        conn = sqlite3.connect(output_db)
        cnt = pd.read_sql_query("SELECT COUNT(*) as c FROM t2", conn).iloc[0]["c"]
        conn.close()
        assert cnt == 4, f"Different execution/node pairs should each write once, got {cnt}"

    def test_crash_between_write_and_checkpoint(self, db_session_factory, temp_dir):
        output_db = os.path.join(temp_dir, "crash_recovery.db")
        dag_config = _build_branch_join_dag(temp_dir, output_db)

        db = db_session_factory()
        pipeline = models.Pipeline(name="Crash Test", dag_config=dag_config)
        db.add(pipeline)
        db.commit()
        db.refresh(pipeline)
        db.close()

        scheduler = PipelineScheduler(db_session_factory)
        try:
            from transform_nodes import JoinNode
            from output_targets import MARKER_TABLE
            original_join = JoinNode.execute
            join_calls = [0]

            def fail_then_ok(self, dfs, config):
                join_calls[0] += 1
                if join_calls[0] == 1:
                    raise RuntimeError("fail first")
                return original_join(self, dfs, config)

            with patch.object(JoinNode, 'execute', fail_then_ok):
                r1 = scheduler.execute_pipeline(pipeline.id, synchronous=True)
            assert r1["status"] == "failed"
            exec_id = r1["execution_id"]

            conn = sqlite3.connect(output_db)
            tables = pd.read_sql_query(
                "SELECT name FROM sqlite_master WHERE type='table'", conn
            )["name"].tolist()
            if "na_sales" in tables:
                na_count = pd.read_sql_query(
                    "SELECT COUNT(*) as c FROM na_sales", conn
                ).iloc[0]["c"]
            else:
                na_count = 0
            conn.close()
            assert na_count == 3, f"out1 should have written 3 AMER rows before join failed, got {na_count}"

            db = db_session_factory()
            exec_row = db.query(models.Execution).filter(
                models.Execution.id == exec_id
            ).first()
            cp = exec_row.checkpoint_data
            cp["node_states"]["out1"]["status"] = NodeStatus.RUNNING
            cp["node_states"]["out1"]["end_time"] = None
            cp["node_states"]["out1"]["error"] = None
            if "out1" in cp.get("node_outputs", {}):
                del cp["node_outputs"]["out1"]
            exec_row.checkpoint_data = cp
            exec_row.node_states = cp["node_states"]
            db.commit()
            db.close()

            r2 = scheduler.execute_pipeline(
                pipeline.id, resume_from_failed=True,
                execution_id=exec_id, synchronous=True
            )
            assert r2["status"] == "completed", f"Resume should complete, got {r2}"

            conn = sqlite3.connect(output_db)
            na_count = pd.read_sql_query(
                "SELECT COUNT(*) as c FROM na_sales", conn
            ).iloc[0]["c"]
            joined_count = pd.read_sql_query(
                "SELECT COUNT(*) as c FROM joined_results", conn
            ).iloc[0]["c"]
            na_marker = pd.read_sql_query(
                f"SELECT rows_written FROM {MARKER_TABLE} WHERE execution_id = ? AND node_id = ?",
                conn, params=(exec_id, "out1")
            )
            conn.close()

            assert na_count == 3, f"out1 must NOT duplicate rows after resume, got {na_count}"
            assert joined_count == 3, f"out2 should have 3 joined rows, got {joined_count}"
            assert len(na_marker) == 1, "out1 marker should exist"
            assert na_marker.iloc[0]["rows_written"] == 3
        finally:
            scheduler.shutdown()

    def test_output_node_idempotent_marker_in_target_db(self, db_session_factory, temp_dir):
        output_db = os.path.join(temp_dir, "marker_persist.db")
        dag_config = _build_branch_join_dag(temp_dir, output_db)

        db = db_session_factory()
        pipeline = models.Pipeline(name="Marker Test", dag_config=dag_config)
        db.add(pipeline)
        db.commit()
        db.refresh(pipeline)
        db.close()

        scheduler = PipelineScheduler(db_session_factory)
        try:
            r1 = scheduler.execute_pipeline(pipeline.id, synchronous=True)
            assert r1["status"] == "completed"
            exec_id = r1["execution_id"]

            from output_targets import MARKER_TABLE
            conn = sqlite3.connect(output_db)
            markers = pd.read_sql_query(
                f"SELECT node_id, rows_written FROM {MARKER_TABLE} WHERE execution_id = ?",
                conn, params=(exec_id,)
            )
            conn.close()
            marker_nodes = set(markers["node_id"].tolist())
            assert "out1" in marker_nodes
            assert "out2" in marker_nodes

            db = db_session_factory()
            exec_row = db.query(models.Execution).filter(
                models.Execution.id == exec_id
            ).first()
            for nid in ["out1", "out2"]:
                exec_row.node_states[nid]["status"] = NodeStatus.RUNNING
                exec_row.node_states[nid]["end_time"] = None
            exec_row.checkpoint_data["node_states"] = exec_row.node_states
            if "out1" in exec_row.checkpoint_data.get("node_outputs", {}):
                del exec_row.checkpoint_data["node_outputs"]["out1"]
            if "out2" in exec_row.checkpoint_data.get("node_outputs", {}):
                del exec_row.checkpoint_data["node_outputs"]["out2"]
            exec_row.status = "failed"
            db.commit()
            db.close()

            r2 = scheduler.execute_pipeline(
                pipeline.id, resume_from_failed=True,
                execution_id=exec_id, synchronous=True
            )
            assert r2["status"] == "completed"

            conn = sqlite3.connect(output_db)
            na_count = pd.read_sql_query(
                "SELECT COUNT(*) as c FROM na_sales", conn
            ).iloc[0]["c"]
            joined_count = pd.read_sql_query(
                "SELECT COUNT(*) as c FROM joined_results", conn
            ).iloc[0]["c"]
            conn.close()

            assert na_count == 3, f"out1 rows should remain 3 after simulated crash+resume, got {na_count}"
            assert joined_count == 3, f"out2 rows should remain 3, got {joined_count}"
        finally:
            scheduler.shutdown()


class TestCSVIdempotency:
    def test_csv_marker_file_prevents_rewrite(self, temp_dir):
        from output_targets import CSVOutput
        export_dir = os.path.join(temp_dir, "exports")
        os.makedirs(export_dir, exist_ok=True)
        with patch("output_targets.EXPORT_DIR", export_dir):
            target = CSVOutput()
            df = pd.DataFrame({"x": [1, 2, 3]})
            config = {"filename": "test_export.csv"}

            r1 = target.write(df, config, execution_id=10, node_id="node_a")
            assert r1[2] is False

            r2 = target.write(df, config, execution_id=10, node_id="node_a")
            assert r2[2] is True
            assert r2[0] == 3

            fpath = os.path.join(export_dir, "test_export.csv")
            mpath = os.path.join(export_dir, "test_export.csv.etl_marker")
            assert os.path.exists(fpath)
            assert os.path.exists(mpath)
            written_df = pd.read_csv(fpath)
            assert len(written_df) == 3


class TestHTTPIdempotency:
    def _mock_response(self, status_code=200):
        resp = MagicMock()
        resp.status_code = status_code
        if status_code >= 400:
            resp.raise_for_status.side_effect = requests.exceptions.HTTPError(
                f"{status_code} Error", response=resp
            )
        else:
            resp.raise_for_status.return_value = None
        return resp

    def test_first_200_not_skipped(self):
        from output_targets import HTTPAPIOutput
        target = HTTPAPIOutput()
        df = pd.DataFrame({"v": [1, 2]})
        config = {"url": "https://example.com/api/ingest", "batch_size": 100}

        mock_resp = self._mock_response(200)
        with patch("output_targets.requests.post", return_value=mock_resp) as mock_post:
            rows, errors, skipped = target.write(df, config, execution_id=1, node_id="n1")

        assert rows == 2
        assert skipped is False, "First successful 200 should NOT be marked as skipped"
        assert errors == []
        assert mock_post.call_count == 1
        call_headers = mock_post.call_args.kwargs.get("headers", {})
        assert call_headers.get("X-Idempotency-Key") == "1-n1"

    def test_409_treated_as_idempotent_skip_no_retry(self):
        from output_targets import HTTPAPIOutput
        target = HTTPAPIOutput()
        df = pd.DataFrame({"v": [1, 2, 3]})
        config = {"url": "https://example.com/api/ingest", "max_retries": 3}

        mock_resp = self._mock_response(409)
        with patch("output_targets.requests.post", return_value=mock_resp) as mock_post:
            rows, errors, skipped = target.write(df, config, execution_id=1, node_id="n1")

        assert rows == 3, f"409 should count rows as already-written, got {rows}"
        assert skipped is True, "409 Conflict should be recognized as idempotent skip"
        assert errors == [], f"409 should not produce errors, got {errors}"
        assert mock_post.call_count == 1, "409 must NOT trigger retries"

    def test_409_does_not_trigger_retries_across_batches(self):
        from output_targets import HTTPAPIOutput
        target = HTTPAPIOutput()
        df = pd.DataFrame({"v": list(range(10))})
        config = {"url": "https://example.com/api", "batch_size": 3, "max_retries": 5}

        mock_resp = self._mock_response(409)
        with patch("output_targets.requests.post", return_value=mock_resp) as mock_post:
            rows, errors, skipped = target.write(df, config, execution_id=99, node_id="abc")

        assert rows == 10
        assert skipped is True
        assert mock_post.call_count == 4, f"10 rows / batch_size=3 → 4 batches, got {mock_post.call_count}"
        for call in mock_post.call_args_list:
            hdr = call.kwargs.get("headers", {})
            assert hdr.get("X-Idempotency-Key") == "99-abc"

    def test_server_error_retries_then_succeeds(self):
        from output_targets import HTTPAPIOutput
        target = HTTPAPIOutput()
        df = pd.DataFrame({"v": [1, 2]})
        config = {"url": "https://example.com/api", "max_retries": 3}

        responses = [
            self._mock_response(500),
            self._mock_response(502),
            self._mock_response(200),
        ]
        with patch("output_targets.requests.post", side_effect=responses) as mock_post:
            rows, errors, skipped = target.write(df, config, execution_id=1, node_id="n1")

        assert rows == 2
        assert skipped is False, "Eventual 200 after retries should NOT be skipped"
        assert errors == []
        assert mock_post.call_count == 3

    def test_no_idempotency_key_without_execution_context(self):
        from output_targets import HTTPAPIOutput
        target = HTTPAPIOutput()
        df = pd.DataFrame({"v": [1]})
        config = {"url": "https://example.com/api"}

        mock_resp = self._mock_response(200)
        with patch("output_targets.requests.post", return_value=mock_resp) as mock_post:
            rows, errors, skipped = target.write(df, config)

        assert rows == 1
        assert skipped is False
        call_headers = mock_post.call_args.kwargs.get("headers", {})
        assert "X-Idempotency-Key" not in call_headers

    def test_409_without_idempotency_key_still_skips(self):
        from output_targets import HTTPAPIOutput
        target = HTTPAPIOutput()
        df = pd.DataFrame({"v": [1]})
        config = {"url": "https://example.com/api"}

        mock_resp = self._mock_response(409)
        with patch("output_targets.requests.post", return_value=mock_resp) as mock_post:
            rows, errors, skipped = target.write(df, config)

        assert rows == 1
        assert skipped is True
        assert errors == []
        assert mock_post.call_count == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
