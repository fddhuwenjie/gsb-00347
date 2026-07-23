"""
Automated tests for the ETL failure-recovery / checkpoint-resume mechanism.

The core scenario builds a DAG that contains a branch (fan-out) and a merge
(join) node:

        ┌──> H (map: age -> int) ──> OUT_H (output "high_out")
    S ──┤                        └──> J (join on id) ──> OUT (output "final_out")
        └──> L (map: rename) ──> MID (map: compute, FAILS ONCE) ──┘

The middle transform ``MID`` fails on the first attempt and succeeds on the
resume. The tests assert:

* upstream nodes (S, H, L) and the already-completed output (OUT_H) execute /
  write exactly once across both runs (no duplicate side effects),
* the failing node reruns and its downstream (J, OUT) run once, landing the
  final data exactly once, with correct content and column types,
* a duplicate / concurrent / unknown / cross-pipeline resume is rejected.
"""
import os
import time
import tempfile
import sqlite3
from collections import defaultdict

import pandas as pd
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import models
from database import Base
from scheduler import PipelineScheduler
from dag_executor import DAGExecutor
from transform_nodes import MapNode
from output_targets import (
    OutputExecutor, SQLiteOutput, CSVOutput, HTTPAPIOutput,
    LocalCommitLog, COMMIT_MARKER_TABLE,
)


# --- injected-failure state (shared across both scheduler calls) -------------
FAIL_STATE = {}
EXEC_COUNTS = defaultdict(int)
WRITE_COUNTS = defaultdict(int)


@pytest.fixture
def workspace(tmp_path):
    """Temp CSV source + temp SQLite target + isolated metadata DB."""
    csv_path = tmp_path / "customers.csv"
    df = pd.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "name": ["a", "b", "c", "d", "e"],
        "age": [28, 35, 22, 41, 30],
        "city": ["bj", "sh", "gz", "sz", "hz"],
    })
    df.to_csv(csv_path, index=False)

    target_db = tmp_path / "target.db"

    meta_db = tmp_path / "meta.db"
    engine = create_engine(
        f"sqlite:///{meta_db}", connect_args={"check_same_thread": False}
    )
    Base.metadata.create_all(bind=engine)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

    return {
        "csv_path": str(csv_path),
        "target_db": str(target_db),
        "SessionLocal": SessionLocal,
    }


def build_dag(csv_path, target_db):
    return {
        "nodes": [
            {"id": "S", "type": "source", "data": {"config": {
                "source_type": "csv",
                "source_config": {"file_path": csv_path},
            }}},
            {"id": "H", "type": "map", "data": {"config": {
                "operations": [
                    {"type": "type_convert", "column": "age", "target_type": "int"}
                ],
            }}},
            {"id": "L", "type": "map", "data": {"config": {
                "operations": [
                    {"type": "rename", "old_name": "city", "new_name": "location"}
                ],
            }}},
            {"id": "MID", "type": "map", "data": {"config": {
                "operations": [
                    {"type": "compute", "new_column": "age_bracket",
                     "expression": "age * 2"}
                ],
                "fail_once_token": "MID",
            }}},
            {"id": "J", "type": "join", "data": {"config": {
                "join_type": "inner", "left_key": "id", "right_key": "id",
            }}},
            {"id": "OUT_H", "type": "output", "data": {"config": {
                "output_type": "sqlite",
                "output_config": {"db_path": target_db, "table_name": "high_out",
                                  "write_mode": "replace"},
            }}},
            {"id": "OUT", "type": "output", "data": {"config": {
                "output_type": "sqlite",
                "output_config": {"db_path": target_db, "table_name": "final_out",
                                  "write_mode": "replace"},
            }}},
        ],
        # H before MID as J inputs so the left side of the join is H.
        "edges": [
            {"source": "S", "target": "H"},
            {"source": "S", "target": "L"},
            {"source": "H", "target": "OUT_H"},
            {"source": "L", "target": "MID"},
            {"source": "H", "target": "J"},
            {"source": "MID", "target": "J"},
            {"source": "J", "target": "OUT"},
        ],
    }


@pytest.fixture
def instrument(monkeypatch):
    """Count node executions / output writes and inject a one-time failure."""
    FAIL_STATE.clear()
    EXEC_COUNTS.clear()
    WRITE_COUNTS.clear()

    orig_exec_node = DAGExecutor.execute_node

    def counting_exec_node(self, node_id):
        EXEC_COUNTS[node_id] += 1
        return orig_exec_node(self, node_id)

    monkeypatch.setattr(DAGExecutor, "execute_node", counting_exec_node)

    orig_map = MapNode.execute

    def flaky_map(self, df, config):
        token = config.get("fail_once_token")
        if token and not FAIL_STATE.get(token):
            FAIL_STATE[token] = True
            raise RuntimeError(f"injected failure for {token}")
        return orig_map(self, df, config)

    monkeypatch.setattr(MapNode, "execute", flaky_map)

    orig_write = OutputExecutor.write.__func__

    def counting_write(cls, target_type, df, config, idempotency_key=None):
        WRITE_COUNTS[config.get("table_name")] += 1
        return orig_write(cls, target_type, df, config,
                          idempotency_key=idempotency_key)

    monkeypatch.setattr(OutputExecutor, "write", classmethod(counting_write))


def _wait(SessionLocal, execution_id, timeout=20.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        db = SessionLocal()
        try:
            ex = db.query(models.Execution).filter(
                models.Execution.id == execution_id
            ).first()
            if ex and ex.status in ("completed", "failed"):
                return ex.status
        finally:
            db.close()
        time.sleep(0.1)
    raise AssertionError(f"execution {execution_id} did not finish in {timeout}s")


def test_resume_no_duplicate_side_effects(workspace, instrument):
    SessionLocal = workspace["SessionLocal"]
    scheduler = PipelineScheduler(SessionLocal)
    try:
        db = SessionLocal()
        pipeline = models.Pipeline(
            name="branch-merge",
            dag_config=build_dag(workspace["csv_path"], workspace["target_db"]),
        )
        db.add(pipeline)
        db.commit()
        db.refresh(pipeline)
        pipeline_id = pipeline.id
        db.close()

        # --- Run 1: MID fails, so J/OUT never run, OUT_H completes once. ------
        res1 = scheduler.execute_pipeline(pipeline_id)
        assert res1["status"] == "started"
        exec_id = res1["execution_id"]
        assert _wait(SessionLocal, exec_id) == "failed"

        db = SessionLocal()
        ex = db.query(models.Execution).get(exec_id)
        states = ex.node_states
        assert states["S"]["status"] == "completed"
        assert states["H"]["status"] == "completed"
        assert states["L"]["status"] == "completed"
        assert states["OUT_H"]["status"] == "completed"
        assert states["MID"]["status"] == "failed"
        assert ex.checkpoint_data is not None
        assert "H" in ex.checkpoint_data["node_outputs"]
        db.close()

        # After run 1: OUT_H wrote once, final_out not written yet.
        assert WRITE_COUNTS["high_out"] == 1
        assert WRITE_COUNTS["final_out"] == 0

        # --- Resume: only MID + downstream (J, OUT) should run. ---------------
        res2 = scheduler.execute_pipeline(
            pipeline_id, resume_from_failed=True, execution_id=exec_id
        )
        assert res2["status"] == "started"
        assert res2["resumed"] is True
        assert _wait(SessionLocal, exec_id) == "completed"

        # Upstream + already-completed output executed exactly once total.
        assert EXEC_COUNTS["S"] == 1
        assert EXEC_COUNTS["H"] == 1
        assert EXEC_COUNTS["L"] == 1
        assert EXEC_COUNTS["OUT_H"] == 1
        # Failing node ran twice (fail + success); merge/output ran once.
        assert EXEC_COUNTS["MID"] == 2
        assert EXEC_COUNTS["J"] == 1
        assert EXEC_COUNTS["OUT"] == 1

        # Completed output not rewritten; final output landed exactly once.
        assert WRITE_COUNTS["high_out"] == 1
        assert WRITE_COUNTS["final_out"] == 1

        # Failed node's error/end_time updated to the new (successful) result.
        db = SessionLocal()
        ex = db.query(models.Execution).get(exec_id)
        assert ex.status == "completed"
        assert ex.error_log in (None, "")
        assert ex.node_states["MID"]["status"] == "completed"
        assert ex.node_states["MID"]["error"] is None
        db.close()

        # --- Final data content correct ---------------------------------------
        conn = sqlite3.connect(workspace["target_db"])
        final = pd.read_sql_query("SELECT * FROM final_out", conn)
        high = pd.read_sql_query("SELECT * FROM high_out", conn)
        conn.close()

        assert len(final) == 5                       # inner join on unique id
        assert "age_bracket" in final.columns        # from restored L -> MID path
        assert len(high) == 5
        assert sorted(high["age"].tolist()) == [22, 28, 30, 35, 41]

        # --- Duplicate resume of a now-completed execution is rejected. -------
        res3 = scheduler.execute_pipeline(
            pipeline_id, resume_from_failed=True, execution_id=exec_id
        )
        assert res3["status"] == "rejected"
        assert res3["reason"] == "already_completed"
        # No extra writes triggered by the rejected resume.
        assert WRITE_COUNTS["final_out"] == 1
    finally:
        scheduler.shutdown()


def test_resume_rejections(workspace, instrument):
    SessionLocal = workspace["SessionLocal"]
    scheduler = PipelineScheduler(SessionLocal)
    try:
        db = SessionLocal()
        p1 = models.Pipeline(name="p1", dag_config=build_dag(
            workspace["csv_path"], workspace["target_db"]))
        p2 = models.Pipeline(name="p2", dag_config=build_dag(
            workspace["csv_path"], workspace["target_db"]))
        db.add_all([p1, p2])
        db.commit()
        db.refresh(p1)
        db.refresh(p2)
        p1_id, p2_id = p1.id, p2.id

        # A genuinely failed execution belonging to p1.
        failed = models.Execution(pipeline_id=p1_id, status="failed")
        db.add(failed)
        # A running execution belonging to p1.
        running = models.Execution(pipeline_id=p1_id, status="running")
        db.add(running)
        # A completed execution belonging to p1.
        done = models.Execution(pipeline_id=p1_id, status="completed")
        db.add(done)
        db.commit()
        failed_id, running_id, done_id = failed.id, running.id, done.id
        db.close()

        # Unknown execution_id is never silently executed as a new task.
        r = scheduler.execute_pipeline(p1_id, resume_from_failed=True,
                                       execution_id=999999)
        assert r["status"] == "rejected" and r["reason"] == "execution_not_found"

        # Resume without an execution_id.
        r = scheduler.execute_pipeline(p1_id, resume_from_failed=True)
        assert r["status"] == "rejected" and r["reason"] == "execution_id_required"

        # Resume request must match the execution's pipeline.
        r = scheduler.execute_pipeline(p2_id, resume_from_failed=True,
                                       execution_id=failed_id)
        assert r["status"] == "rejected" and r["reason"] == "pipeline_mismatch"

        # Running executions cannot be resumed.
        r = scheduler.execute_pipeline(p1_id, resume_from_failed=True,
                                       execution_id=running_id)
        assert r["status"] == "rejected" and r["reason"] == "already_running"

        # Completed executions cannot be resumed.
        r = scheduler.execute_pipeline(p1_id, resume_from_failed=True,
                                       execution_id=done_id)
        assert r["status"] == "rejected" and r["reason"] == "already_completed"

        # Concurrent resume of the same failed execution: second call rejected.
        scheduler.running_executions[failed_id] = object()
        r = scheduler.execute_pipeline(p1_id, resume_from_failed=True,
                                       execution_id=failed_id)
        assert r["status"] == "rejected" and r["reason"] == "already_running"
        scheduler.running_executions.pop(failed_id, None)

        # execution_id supplied without resume flag is rejected (not run as new).
        r = scheduler.execute_pipeline(p1_id, execution_id=failed_id)
        assert r["status"] == "rejected" and r["reason"] == "execution_id_not_allowed"
    finally:
        scheduler.shutdown()


def test_sqlite_output_idempotent_key(tmp_path):
    """Same idempotency_key writes rows exactly once even if called twice."""
    target = tmp_path / "t.db"
    df = pd.DataFrame({"id": [1, 2, 3], "v": ["a", "b", "c"]})
    config = {"db_path": str(target), "table_name": "tbl", "write_mode": "replace"}
    out = SQLiteOutput()

    rows1, err1 = out.write(df, config, idempotency_key="exec:1:node:OUT")
    assert rows1 == 3 and err1 == []

    # A second call with the SAME key (simulating a resume after the checkpoint
    # save was lost) must NOT insert the rows again.
    rows2, err2 = out.write(df, config, idempotency_key="exec:1:node:OUT")
    assert rows2 == 3  # reported count preserved
    assert err2 == []

    conn = sqlite3.connect(str(target))
    count = conn.execute("SELECT COUNT(*) FROM tbl").fetchone()[0]
    marker_count = conn.execute(
        f"SELECT COUNT(*) FROM {COMMIT_MARKER_TABLE}").fetchone()[0]
    conn.close()

    assert count == 3           # rows written exactly once
    assert marker_count == 1    # single commit marker


def test_output_write_survives_checkpoint_loss(workspace, monkeypatch):
    """
    Reproduce the interrupt window: the output node writes+commits to the target
    DB, then the checkpoint save (or the whole process) dies BEFORE the output
    node is marked completed. On resume the output node re-runs, but the
    idempotency marker must stop it from writing the rows a second time.
    """
    FAIL_STATE.clear()
    EXEC_COUNTS.clear()
    WRITE_COUNTS.clear()

    SessionLocal = workspace["SessionLocal"]
    target_db = workspace["target_db"]

    # Count how many times rows are physically inserted (vs. skipped by marker).
    orig_sqlite_write = SQLiteOutput.write
    physical_writes = defaultdict(int)

    def tracking_write(self, df, config, idempotency_key=None):
        conn_before = sqlite3.connect(target_db)
        try:
            existed = conn_before.execute(
                f"SELECT 1 FROM {COMMIT_MARKER_TABLE} WHERE idempotency_key = ?",
                (idempotency_key,)
            ).fetchone() if _marker_table_exists(conn_before) else None
        finally:
            conn_before.close()
        result = orig_sqlite_write(self, df, config, idempotency_key=idempotency_key)
        if not existed:
            physical_writes[config.get("table_name")] += 1
        return result

    monkeypatch.setattr(SQLiteOutput, "write", tracking_write)

    dag = {
        "nodes": [
            {"id": "S", "type": "source", "data": {"config": {
                "source_type": "csv",
                "source_config": {"file_path": workspace["csv_path"]},
            }}},
            {"id": "OUT", "type": "output", "data": {"config": {
                "output_type": "sqlite",
                "output_config": {"db_path": target_db, "table_name": "sink",
                                  "write_mode": "replace"},
            }}},
        ],
        "edges": [{"source": "S", "target": "OUT"}],
    }

    db = SessionLocal()
    pipeline = models.Pipeline(name="crash-window", dag_config=dag)
    db.add(pipeline)
    db.commit()
    db.refresh(pipeline)
    pipeline_id = pipeline.id

    execution = models.Execution(pipeline_id=pipeline_id, status="running")
    db.add(execution)
    db.commit()
    db.refresh(execution)
    exec_id = execution.id
    db.close()

    # --- Attempt 1: run the executor directly, then simulate a crash right
    #     after OUT commits to the target DB but before the checkpoint is saved.
    db = SessionLocal()

    class CrashAfterOutput(Exception):
        pass

    def crashing_checkpoint(checkpoint_data, node_states):
        # OUT already committed its rows to the target DB at this point.
        if node_states.get("OUT", {}).get("status") == "completed":
            raise CrashAfterOutput("checkpoint save lost after output commit")

    executor = DAGExecutor(
        dag, pipeline_id=pipeline_id, execution_id=exec_id, db_session=db,
        checkpoint_callback=crashing_checkpoint,
    )
    with pytest.raises(CrashAfterOutput):
        executor.execute()
    db.rollback()  # metadata checkpoint update is lost, mimicking a crash
    db.close()

    # The rows were physically written once; the metadata checkpoint did NOT
    # record OUT as completed (it was rolled back).
    assert physical_writes["sink"] == 1
    conn = sqlite3.connect(target_db)
    assert conn.execute("SELECT COUNT(*) FROM sink").fetchone()[0] == 5
    conn.close()

    db = SessionLocal()
    ex = db.query(models.Execution).get(exec_id)
    ex.status = "failed"
    db.commit()
    # node_states has no committed OUT completion.
    assert (ex.node_states or {}).get("OUT", {}).get("status") != "completed"
    db.close()

    # --- Resume: OUT re-runs, but the idempotency marker prevents a 2nd write.
    scheduler = PipelineScheduler(SessionLocal)
    try:
        res = scheduler.execute_pipeline(
            pipeline_id, resume_from_failed=True, execution_id=exec_id
        )
        assert res["status"] == "started"
        assert _wait(SessionLocal, exec_id) == "completed"
    finally:
        scheduler.shutdown()

    # OUT executed again on resume, but produced NO second physical write.
    assert physical_writes["sink"] == 1
    conn = sqlite3.connect(target_db)
    total = conn.execute("SELECT COUNT(*) FROM sink").fetchone()[0]
    markers = conn.execute(
        f"SELECT COUNT(*) FROM {COMMIT_MARKER_TABLE}").fetchone()[0]
    conn.close()
    assert total == 5      # data landed exactly once despite the crash + resume
    assert markers == 1

    db = SessionLocal()
    ex = db.query(models.Execution).get(exec_id)
    assert ex.status == "completed"
    assert ex.node_states["OUT"]["status"] == "completed"
    db.close()


def _marker_table_exists(conn) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (COMMIT_MARKER_TABLE,)
    ).fetchone()
    return row is not None


@pytest.fixture
def isolated_commit_log(tmp_path, monkeypatch):
    """Point CSV/HTTP outputs at a per-test commit-log DB and export dir."""
    import output_targets as ot
    log_db = str(tmp_path / "commit_log.db")
    export_dir = tmp_path / "exports"
    export_dir.mkdir()
    monkeypatch.setattr(ot, "COMMIT_LOG_DB", log_db)
    monkeypatch.setattr(ot, "EXPORT_DIR", str(export_dir))
    monkeypatch.setattr(ot.LocalCommitLog, "__init__",
                        lambda self, db_path=log_db: setattr(self, "db_path", db_path))
    return {"log_db": log_db, "export_dir": export_dir}


def test_csv_output_idempotent_after_checkpoint_loss(isolated_commit_log):
    """
    Auto-named CSV must not emit a second file when the output node re-runs on
    resume (the interrupt window where the write committed but the checkpoint was
    lost).
    """
    export_dir = isolated_commit_log["export_dir"]
    df = pd.DataFrame({"id": [1, 2, 3], "v": ["a", "b", "c"]})
    # No filename -> auto-named. Previously this produced a NEW timestamped file
    # on every call.
    config = {}
    out = CSVOutput()
    key = "exec:7:node:OUT_CSV"

    rows1, err1 = out.write(df, config, idempotency_key=key)
    assert rows1 == 3 and err1 == []
    files_after_first = sorted(os.listdir(export_dir))
    assert len([f for f in files_after_first if f.endswith(".csv")]) == 1

    # Resume re-runs the output node with the SAME key.
    rows2, err2 = out.write(df, config, idempotency_key=key)
    assert rows2 == 3 and err2 == []

    csv_files = [f for f in os.listdir(export_dir) if f.endswith(".csv")]
    assert len(csv_files) == 1  # still exactly one file, not duplicated

    written = pd.read_csv(os.path.join(export_dir, csv_files[0]))
    assert len(written) == 3
    assert written["id"].tolist() == [1, 2, 3]


def test_http_output_idempotent_after_checkpoint_loss(isolated_commit_log, monkeypatch):
    """HTTP output must not re-POST batches when the node re-runs on resume."""
    posts = []

    class FakeResp:
        def raise_for_status(self):
            return None

    def fake_post(url, json=None, headers=None, timeout=None):
        posts.append({"url": url, "rows": len(json),
                      "idem": (headers or {}).get("Idempotency-Key")})
        return FakeResp()

    import output_targets as ot
    monkeypatch.setattr(ot.requests, "post", fake_post)

    df = pd.DataFrame({"id": list(range(1, 6))})
    config = {"url": "http://example/ingest", "batch_size": 2}
    out = HTTPAPIOutput()
    key = "exec:9:node:OUT_HTTP"

    rows1, err1 = out.write(df, config, idempotency_key=key)
    assert rows1 == 5 and err1 == []
    first_round_posts = len(posts)
    assert first_round_posts == 3  # 2 + 2 + 1
    # Every batch carried a stable per-batch Idempotency-Key header.
    assert all(p["idem"] for p in posts)

    # Resume re-runs the node with the same key -> no additional POSTs.
    rows2, err2 = out.write(df, config, idempotency_key=key)
    assert rows2 == 5 and err2 == []
    assert len(posts) == first_round_posts  # no new POSTs on resume


def test_http_output_partial_failure_resumes_missing_batches(isolated_commit_log, monkeypatch):
    """
    If some batches succeed and one fails, resume only re-POSTs the missing
    batches (no duplicate delivery of the already-committed ones).
    """
    posts = []
    fail_state = {"fail_batch_index": 1, "should_fail": True}

    class FakeResp:
        def raise_for_status(self):
            return None

    def fake_post(url, json=None, headers=None, timeout=None):
        idem = (headers or {}).get("Idempotency-Key", "")
        # Fail batch index 1 on the first run only.
        if fail_state["should_fail"] and idem.endswith(":batch:1"):
            raise RuntimeError("simulated network error")
        posts.append({"rows": len(json), "idem": idem})
        return FakeResp()

    import output_targets as ot
    monkeypatch.setattr(ot.requests, "post", fake_post)

    df = pd.DataFrame({"id": list(range(1, 7))})  # 3 batches of 2
    config = {"url": "http://example/ingest", "batch_size": 2,
              "error_strategy": "skip", "max_retries": 0}
    out = HTTPAPIOutput()
    key = "exec:11:node:OUT_HTTP"

    rows1, err1 = out.write(df, config, idempotency_key=key)
    # Batches 0 and 2 delivered; batch 1 failed.
    assert err1 != []
    delivered_first = {p["idem"] for p in posts}
    assert f"{key}:batch:0" in delivered_first
    assert f"{key}:batch:2" in delivered_first
    assert f"{key}:batch:1" not in delivered_first

    # Now let the previously-failing batch succeed and resume.
    fail_state["should_fail"] = False
    posts.clear()
    rows2, err2 = out.write(df, config, idempotency_key=key)
    assert err2 == []
    delivered_second = {p["idem"] for p in posts}
    # Only the missing batch is re-sent; the committed ones are NOT re-POSTed.
    assert delivered_second == {f"{key}:batch:1"}


def test_all_output_types_receive_idempotency_key(monkeypatch, workspace):
    """Regression: DAGExecutor threads a per-(execution,node) key to every
    output type, not just SQLite."""
    seen = {}

    orig = OutputExecutor.write.__func__

    def spy(cls, target_type, df, config, idempotency_key=None):
        seen[config.get("table_name") or config.get("url") or config.get("filename") or target_type] = idempotency_key
        # Avoid real side effects for csv/http here.
        if target_type == "sqlite":
            return orig(cls, target_type, df, config, idempotency_key=idempotency_key)
        return len(df), []

    monkeypatch.setattr(OutputExecutor, "write", classmethod(spy))

    dag = {
        "nodes": [
            {"id": "S", "type": "source", "data": {"config": {
                "source_type": "csv",
                "source_config": {"file_path": workspace["csv_path"]}}}},
            {"id": "OUT_DB", "type": "output", "data": {"config": {
                "output_type": "sqlite",
                "output_config": {"db_path": workspace["target_db"],
                                  "table_name": "t1", "write_mode": "replace"}}}},
            {"id": "OUT_CSV", "type": "output", "data": {"config": {
                "output_type": "csv", "output_config": {"filename": "x.csv"}}}},
            {"id": "OUT_HTTP", "type": "output", "data": {"config": {
                "output_type": "http_api",
                "output_config": {"url": "http://example/x"}}}},
        ],
        "edges": [
            {"source": "S", "target": "OUT_DB"},
            {"source": "S", "target": "OUT_CSV"},
            {"source": "S", "target": "OUT_HTTP"},
        ],
    }
    executor = DAGExecutor(dag, pipeline_id=1, execution_id=42)
    ok, _ = executor.execute()
    assert ok
    assert seen["t1"] == "exec:42:node:OUT_DB"
    assert seen["x.csv"] == "exec:42:node:OUT_CSV"
    assert seen["http://example/x"] == "exec:42:node:OUT_HTTP"
