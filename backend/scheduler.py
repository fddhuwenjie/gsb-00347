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
        def run():
            local_db = self.db_session_factory() if db is None else db
            try:
                pipeline = local_db.query(models.Pipeline).filter(
                    models.Pipeline.id == pipeline_id
                ).first()
                if not pipeline:
                    return

                if execution_id:
                    execution = local_db.query(models.Execution).filter(
                        models.Execution.id == execution_id
                    ).first()
                else:
                    execution = models.Execution(
                        pipeline_id=pipeline_id,
                        status="running",
                        start_time=datetime.utcnow()
                    )
                    local_db.add(execution)
                    local_db.commit()
                    local_db.refresh(execution)

                executor = DAGExecutor(
                    pipeline.dag_config,
                    pipeline_id=pipeline_id,
                    execution_id=execution.id,
                    db_session=local_db
                )
                resume_node = None
                checkpoint_states = None

                if resume_from_failed and execution.node_states:
                    checkpoint_states = execution.node_states
                    for node_id, state in execution.node_states.items():
                        if state.get("status") == "failed":
                            resume_node = node_id
                            break

                success, result = executor.execute(
                    resume_from=resume_node,
                    checkpoint_states=checkpoint_states
                )

                execution.status = "completed" if success else "failed"
                execution.end_time = datetime.utcnow()
                execution.node_states = result.get("node_states")
                execution.checkpoint_data = result.get("checkpoint_data")
                execution.total_rows = executor.get_total_rows()
                execution.success_rows = executor.get_success_rows()

                if not success:
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
                if execution and execution.id in self.running_executions:
                    del self.running_executions[execution.id]

        thread = threading.Thread(target=run)
        thread.start()

        return {"status": "started", "message": "Pipeline execution started"}

    def shutdown(self):
        self.scheduler.shutdown()
