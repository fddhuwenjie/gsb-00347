import threading
from typing import Dict, Any, Optional
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from sqlalchemy.orm import Session
import models
import schemas
from dag_executor import DAGExecutor


class PipelineScheduler:
    def __init__(self, db_session_factory):
        self.db_session_factory = db_session_factory
        self.scheduler = BackgroundScheduler()
        self.running_executions: Dict[int, threading.Thread] = {}
        self._lock = threading.Lock()
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
        db = self.db_session_factory()
        try:
            pipeline = db.query(models.Pipeline).filter(models.Pipeline.id == pipeline_id).first()
            if pipeline:
                self.execute_pipeline(pipeline_id, db=db)
        finally:
            db.close()

    def update_schedule(self, pipeline_id: int, cron_expression: str, is_scheduled: bool):
        db = self.db_session_factory()
        try:
            pipeline = db.query(models.Pipeline).filter(models.Pipeline.id == pipeline_id).first()
            if not pipeline:
                raise ValueError(f"Pipeline {pipeline_id} not found")

            pipeline.cron_expression = cron_expression
            pipeline.is_scheduled = is_scheduled
            db.commit()

            job_id = f"pipeline_{pipeline_id}"
            if is_scheduled and cron_expression:
                self._add_scheduled_job(pipeline)
            elif self.scheduler.get_job(job_id):
                self.scheduler.remove_job(job_id)

            return pipeline
        finally:
            db.close()

    def execute_pipeline(self, pipeline_id: int, resume_from_failed: bool = False,
                         execution_id: Optional[int] = None, db: Optional[Session] = None) -> Dict[str, Any]:
        # Validate the request up-front on the caller's thread so the API can
        # return a definite, synchronous result instead of silently spawning
        # work. Resume requests go through a strict state machine.
        validation_db = db if db is not None else self.db_session_factory()
        close_validation_db = db is None
        try:
            pipeline = validation_db.query(models.Pipeline).filter(
                models.Pipeline.id == pipeline_id
            ).first()
            if not pipeline:
                return {"status": "rejected", "reason": "pipeline_not_found",
                        "message": f"Pipeline {pipeline_id} not found"}

            target_execution_id = None

            if resume_from_failed:
                if execution_id is None:
                    return {"status": "rejected", "reason": "execution_id_required",
                            "message": "Resume requires an execution_id"}

                execution = validation_db.query(models.Execution).filter(
                    models.Execution.id == execution_id
                ).first()

                # An unknown execution_id is never treated as a brand new run.
                if not execution:
                    return {"status": "rejected", "reason": "execution_not_found",
                            "message": f"Execution {execution_id} not found"}

                # Resume may only act on a failed execution of the SAME pipeline.
                if execution.pipeline_id != pipeline_id:
                    return {"status": "rejected", "reason": "pipeline_mismatch",
                            "message": f"Execution {execution_id} does not belong "
                                       f"to pipeline {pipeline_id}"}

                if execution.status == "running":
                    return {"status": "rejected", "reason": "already_running",
                            "message": f"Execution {execution_id} is currently running"}

                if execution.status == "completed":
                    return {"status": "rejected", "reason": "already_completed",
                            "message": f"Execution {execution_id} already completed"}

                if execution.status != "failed":
                    return {"status": "rejected", "reason": "not_resumable",
                            "message": f"Execution {execution_id} is not in a "
                                       f"resumable (failed) state"}

                # Guard against duplicate clicks / concurrent resume of the same
                # execution: only one worker thread per execution at a time.
                with self._lock:
                    if execution_id in self.running_executions:
                        return {"status": "rejected", "reason": "already_running",
                                "message": f"Execution {execution_id} is already "
                                           f"being resumed"}
                    execution.status = "running"
                    validation_db.commit()
                    self.running_executions[execution_id] = None
                target_execution_id = execution_id
            else:
                if execution_id is not None:
                    return {"status": "rejected", "reason": "execution_id_not_allowed",
                            "message": "execution_id is only valid with "
                                       "resume_from_failed=true"}
                execution = models.Execution(
                    pipeline_id=pipeline_id,
                    status="running",
                    start_time=datetime.utcnow()
                )
                validation_db.add(execution)
                validation_db.commit()
                validation_db.refresh(execution)
                with self._lock:
                    self.running_executions[execution.id] = None
                target_execution_id = execution.id
        finally:
            if close_validation_db:
                validation_db.close()

        def run():
            local_db = self.db_session_factory() if db is None else db
            execution = None
            try:
                pipeline = local_db.query(models.Pipeline).filter(
                    models.Pipeline.id == pipeline_id
                ).first()
                if not pipeline:
                    return

                execution = local_db.query(models.Execution).filter(
                    models.Execution.id == target_execution_id
                ).first()
                if not execution:
                    return

                def _save_checkpoint(checkpoint_data, node_states):
                    # Persist the last complete checkpoint after each successful
                    # node so an unexpected crash never rewinds past it.
                    execution.node_states = node_states
                    execution.checkpoint_data = checkpoint_data
                    local_db.commit()

                executor = DAGExecutor(
                    pipeline.dag_config,
                    pipeline_id=pipeline_id,
                    execution_id=execution.id,
                    db_session=local_db,
                    checkpoint_callback=_save_checkpoint
                )

                resume_checkpoint = None
                resume_states = None
                if resume_from_failed:
                    resume_checkpoint = execution.checkpoint_data
                    resume_states = execution.node_states

                success, result = executor.execute(
                    checkpoint_states=resume_states if not resume_checkpoint else None,
                    checkpoint_data=resume_checkpoint
                )

                execution.status = "completed" if success else "failed"
                execution.end_time = datetime.utcnow()
                execution.node_states = result.get("node_states")
                execution.checkpoint_data = result.get("checkpoint_data")
                execution.total_rows = executor.get_total_rows()
                execution.success_rows = executor.get_success_rows()

                if success:
                    execution.error_log = None
                else:
                    execution.error_log = result.get("error", "")

                local_db.commit()

            except Exception as e:
                if execution:
                    execution.status = "failed"
                    execution.end_time = datetime.utcnow()
                    execution.error_log = str(e)
                    local_db.commit()
            finally:
                if db is None:
                    local_db.close()
                with self._lock:
                    self.running_executions.pop(target_execution_id, None)

        thread = threading.Thread(target=run)
        with self._lock:
            self.running_executions[target_execution_id] = thread
        thread.start()

        return {
            "status": "started",
            "execution_id": target_execution_id,
            "resumed": resume_from_failed,
            "message": "Pipeline execution started"
        }

    def shutdown(self):
        self.scheduler.shutdown()
