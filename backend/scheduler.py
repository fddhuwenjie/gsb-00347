import threading
from typing import Dict, Any, Optional
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from sqlalchemy.orm import Session
import models
import schemas
from dag_executor import DAGExecutor


class ResumeError(Exception):
    def __init__(self, code: str, message: str):
        self.code = code
        self.message = message
        super().__init__(message)


class PipelineScheduler:
    def __init__(self, db_session_factory):
        self.db_session_factory = db_session_factory
        self.scheduler = BackgroundScheduler()
        self.running_executions: Dict[int, threading.Thread] = {}
        self._active_locks: Dict[int, threading.Lock] = {}
        self._global_lock = threading.Lock()
        self._start_scheduler()

    def _start_scheduler(self):
        self.scheduler.start()
        self._load_scheduled_pipelines()

    def _load_scheduled_pipelines(self):
        db = self.db_session_factory()
        try:
            pipelines = db.query(models.Pipeline).filter(
                models.Pipeline.is_scheduled == True,
                models.Pipeline.cron_expression.isnot(None)
            ).all()

            for pipeline in pipelines:
                self._add_scheduled_job(pipeline)
        finally:
            db.close()

    def _add_scheduled_job(self, pipeline: models.Pipeline):
        job_id = f"pipeline_{pipeline.id}"
        if self.scheduler.get_job(job_id):
            self.scheduler.remove_job(job_id)

        trigger = CronTrigger.from_crontab(pipeline.cron_expression)
        self.scheduler.add_job(
            self._execute_scheduled_pipeline,
            trigger=trigger,
            id=job_id,
            args=[pipeline.id],
            replace_existing=True
        )

    def _execute_scheduled_pipeline(self, pipeline_id: int):
        self.execute_pipeline(pipeline_id)

    def update_schedule(self, pipeline_id: int, cron_expression: str, is_scheduled: bool):
        db = self.db_session_factory()
        try:
            pipeline = db.query(models.Pipeline).filter(
                models.Pipeline.id == pipeline_id
            ).first()
            if not pipeline:
                raise ValueError(f"Pipeline {pipeline_id} not found")

            pipeline.cron_expression = cron_expression
            pipeline.is_scheduled = is_scheduled
            db.commit()

            job_id = f"pipeline_{pipeline.id}"
            if is_scheduled and cron_expression:
                self._add_scheduled_job(pipeline)
            elif self.scheduler.get_job(job_id):
                self.scheduler.remove_job(job_id)

            return pipeline
        finally:
            db.close()

    def _get_execution_lock(self, execution_id: int) -> threading.Lock:
        with self._global_lock:
            if execution_id not in self._active_locks:
                self._active_locks[execution_id] = threading.Lock()
            return self._active_locks[execution_id]

    def _validate_resume(self, db: Session, pipeline_id: int,
                         execution_id: Optional[int]) -> models.Execution:
        if execution_id is None:
            raise ResumeError("missing_execution_id",
                              "execution_id is required for resume")

        execution = db.query(models.Execution).filter(
            models.Execution.id == execution_id
        ).first()

        if not execution:
            raise ResumeError("not_found",
                              f"Execution {execution_id} not found")

        if execution.pipeline_id != pipeline_id:
            raise ResumeError("pipeline_mismatch",
                              f"Execution {execution_id} belongs to pipeline "
                              f"{execution.pipeline_id}, not {pipeline_id}")

        if execution.status == "running":
            raise ResumeError("already_running",
                              f"Execution {execution_id} is already running")

        if execution.status == "completed":
            raise ResumeError("already_completed",
                              f"Execution {execution_id} has already completed successfully")

        if execution.status == "pending":
            raise ResumeError("not_started",
                              f"Execution {execution_id} has not been started yet")

        if execution.status != "failed":
            raise ResumeError("not_resumable",
                              f"Execution {execution_id} is in '{execution.status}' state; "
                              f"only failed executions can be resumed")

        if not execution.checkpoint_data:
            raise ResumeError("no_checkpoint",
                              f"Execution {execution_id} has no checkpoint to resume from")

        lock = self._get_execution_lock(execution_id)
        if not lock.acquire(blocking=False):
            raise ResumeError("concurrent_resume",
                              f"Execution {execution_id} is already being resumed by another request")

        with self._global_lock:
            if execution_id in self.running_executions:
                lock.release()
                raise ResumeError("already_running",
                                  f"Execution {execution_id} is already running")

        return execution

    def execute_pipeline(self, pipeline_id: int, resume_from_failed: bool = False,
                         execution_id: Optional[int] = None, db: Optional[Session] = None,
                         synchronous: bool = False) -> Dict[str, Any]:
        setup_db = db if db is not None else self.db_session_factory()
        try:
            pipeline = setup_db.query(models.Pipeline).filter(
                models.Pipeline.id == pipeline_id
            ).first()
            if not pipeline:
                return {"status": "error", "message": f"Pipeline {pipeline_id} not found"}

            checkpoint = None
            resume = False
            dag_config = pipeline.dag_config

            if resume_from_failed:
                try:
                    execution = self._validate_resume(setup_db, pipeline_id, execution_id)
                except ResumeError as re:
                    return {"status": re.code, "message": re.message}

                resume = True
                checkpoint = execution.checkpoint_data
                execution.status = "running"
                execution.error_log = None
                execution.end_time = None
                setup_db.commit()
                lock = self._get_execution_lock(execution.id)
            else:
                if execution_id is not None:
                    existing = setup_db.query(models.Execution).filter(
                        models.Execution.id == execution_id
                    ).first()
                    if not existing:
                        return {"status": "not_found",
                                "message": f"Execution {execution_id} not found"}

                    lock = self._get_execution_lock(execution_id)
                    if not lock.acquire(blocking=False):
                        return {"status": "already_running",
                                "message": f"Execution {execution_id} is already running"}
                    with self._global_lock:
                        if execution_id in self.running_executions:
                            lock.release()
                            return {"status": "already_running",
                                    "message": f"Execution {execution_id} is already running"}

                    if existing.status == "running":
                        lock.release()
                        return {"status": "already_running",
                                "message": f"Execution {execution_id} is already running"}
                    if existing.status == "completed":
                        lock.release()
                        return {"status": "already_completed",
                                "message": f"Execution {execution_id} has already completed"}
                    execution = existing
                    execution.status = "running"
                    setup_db.commit()
                else:
                    execution = models.Execution(
                        pipeline_id=pipeline_id,
                        status="running",
                        start_time=datetime.utcnow()
                    )
                    setup_db.add(execution)
                    setup_db.commit()
                    setup_db.refresh(execution)
                    lock = self._get_execution_lock(execution.id)
                    if not lock.acquire(blocking=False):
                        return {"status": "already_running",
                                "message": f"Execution {execution.id} is already running"}

            exec_id = execution.id
            pipe_id = pipeline_id

            def run(db_session=None):
                owns_session = False
                if db_session is None:
                    thread_db = self.db_session_factory()
                    owns_session = True
                else:
                    thread_db = db_session
                try:
                    thread_exec = thread_db.query(models.Execution).filter(
                        models.Execution.id == exec_id
                    ).first()

                    executor = DAGExecutor(
                        dag_config,
                        pipeline_id=pipe_id,
                        execution_id=exec_id,
                        db_session=thread_db
                    )

                    success, result = executor.execute(
                        resume=resume,
                        checkpoint=checkpoint
                    )

                    thread_exec.status = "completed" if success else "failed"
                    thread_exec.end_time = datetime.utcnow()
                    thread_exec.node_states = result.get("node_states")
                    thread_exec.checkpoint_data = result.get("checkpoint_data")
                    thread_exec.total_rows = executor.get_total_rows()
                    thread_exec.success_rows = executor.get_success_rows()

                    if not success:
                        thread_exec.error_log = result.get("error", "")
                    else:
                        thread_exec.error_log = None

                    thread_db.commit()

                except Exception as e:
                    thread_db.rollback()
                    try:
                        thread_exec = thread_db.query(models.Execution).filter(
                            models.Execution.id == exec_id
                        ).first()
                        if thread_exec:
                            thread_exec.status = "failed"
                            thread_exec.end_time = datetime.utcnow()
                            thread_exec.error_log = str(e)
                            thread_db.commit()
                    except Exception:
                        thread_db.rollback()
                finally:
                    with self._global_lock:
                        if exec_id in self.running_executions:
                            del self.running_executions[exec_id]
                    lk = self._active_locks.get(exec_id)
                    if lk:
                        try:
                            lk.release()
                        except RuntimeError:
                            pass
                    if owns_session:
                        thread_db.close()

            if synchronous:
                self.running_executions[exec_id] = threading.current_thread()
                try:
                    run(db_session=setup_db)
                finally:
                    with self._global_lock:
                        self.running_executions.pop(exec_id, None)
                setup_db.expire_all()
                final_exec = setup_db.query(models.Execution).filter(
                    models.Execution.id == exec_id
                ).first()
                return {
                    "status": final_exec.status if final_exec else "unknown",
                    "execution_id": exec_id,
                    "message": "Pipeline execution completed"
                }

            thread = threading.Thread(target=run)
            with self._global_lock:
                self.running_executions[exec_id] = thread
            thread.start()

            return {
                "status": "started",
                "execution_id": exec_id,
                "message": "Pipeline execution started"
            }

        finally:
            if db is None:
                setup_db.close()

    def shutdown(self):
        self.scheduler.shutdown()
