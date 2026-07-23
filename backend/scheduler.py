import threading
import time
from typing import Dict, Any, Optional
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from sqlalchemy.orm import Session
import models
import schemas
from dag_executor import DAGExecutor, NodeStatus


class ResumeError(Exception):
    pass


class PipelineScheduler:
    def __init__(self, db_session_factory):
        self.db_session_factory = db_session_factory
        self.scheduler = BackgroundScheduler()
        self.running_executions: Dict[int, threading.Thread] = {}
        self._execution_locks: Dict[int, threading.Lock] = {}
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
        try:
            self.execute_pipeline(pipeline_id, sync=True)
        except Exception:
            pass

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
            if execution_id not in self._execution_locks:
                self._execution_locks[execution_id] = threading.Lock()
            return self._execution_locks[execution_id]

    def _validate_resume_request(self, pipeline_id: int, execution_id: Optional[int],
                                  db: Session) -> models.Execution:
        if execution_id is None:
            raise ResumeError("execution_id is required for resume")

        execution = db.query(models.Execution).filter(
            models.Execution.id == execution_id
        ).first()

        if not execution:
            raise ResumeError(f"Execution {execution_id} not found")

        if execution.pipeline_id != pipeline_id:
            raise ResumeError(
                f"Execution {execution_id} belongs to pipeline {execution.pipeline_id}, "
                f"not pipeline {pipeline_id}"
            )

        if execution.status == NodeStatus.RUNNING:
            raise ResumeError(f"Execution {execution_id} is already running")

        if execution.status == NodeStatus.COMPLETED:
            raise ResumeError(f"Execution {execution_id} has already completed")

        if execution.status != NodeStatus.FAILED:
            raise ResumeError(
                f"Execution {execution_id} is in status '{execution.status}', "
                f"can only resume failed executions"
            )

        return execution

    def execute_pipeline(self, pipeline_id: int, resume_from_failed: bool = False,
                         execution_id: Optional[int] = None, db: Optional[Session] = None,
                         sync: bool = False) -> Dict[str, Any]:
        if resume_from_failed:
            if execution_id is None:
                return {
                    "success": False,
                    "error": "execution_id is required when resume_from_failed is true",
                    "status": "rejected"
                }
            return self._resume_execution(pipeline_id, execution_id, sync=sync)
        else:
            return self._start_new_execution(pipeline_id, sync=sync)

    def _start_new_execution(self, pipeline_id: int, sync: bool = False) -> Dict[str, Any]:
        setup_db = self.db_session_factory()
        try:
            pipeline = setup_db.query(models.Pipeline).filter(
                models.Pipeline.id == pipeline_id
            ).first()
            if not pipeline:
                return {"success": False, "error": f"Pipeline {pipeline_id} not found"}

            execution = models.Execution(
                pipeline_id=pipeline_id,
                status=NodeStatus.RUNNING,
                start_time=datetime.utcnow()
            )
            setup_db.add(execution)
            setup_db.commit()
            setup_db.refresh(execution)
            exec_id = execution.id
            setup_db.close()
        except Exception as e:
            setup_db.close()
            return {"success": False, "error": str(e)}

        def run_in_thread():
            thread_db = self.db_session_factory()
            try:
                pipeline = thread_db.query(models.Pipeline).filter(
                    models.Pipeline.id == pipeline_id
                ).first()
                exec_obj = thread_db.query(models.Execution).filter(
                    models.Execution.id == exec_id
                ).first()
                if not pipeline:
                    exec_obj.status = NodeStatus.FAILED
                    exec_obj.error_log = f"Pipeline {pipeline_id} not found"
                    exec_obj.end_time = datetime.utcnow()
                    thread_db.commit()
                    return

                executor = DAGExecutor(
                    pipeline.dag_config,
                    pipeline_id=pipeline_id,
                    execution_id=exec_id,
                    db_session=thread_db
                )

                success, result = executor.execute()

                exec_obj.status = NodeStatus.COMPLETED if success else NodeStatus.FAILED
                exec_obj.end_time = datetime.utcnow()
                exec_obj.node_states = result.get("node_states")
                exec_obj.checkpoint_data = result.get("checkpoint_data")
                exec_obj.total_rows = executor.get_total_rows()
                exec_obj.success_rows = executor.get_success_rows()

                if not success:
                    exec_obj.error_log = result.get("error", "")

                thread_db.commit()

            except Exception as e:
                try:
                    thread_db.rollback()
                    exec_obj = thread_db.query(models.Execution).filter(
                        models.Execution.id == exec_id
                    ).first()
                    if exec_obj:
                        exec_obj.status = NodeStatus.FAILED
                        exec_obj.end_time = datetime.utcnow()
                        exec_obj.error_log = str(e)
                        thread_db.commit()
                except Exception:
                    pass
            finally:
                with self._global_lock:
                    self.running_executions.pop(exec_id, None)
                thread_db.close()

        with self._global_lock:
            self.running_executions[exec_id] = None

        if sync:
            run_in_thread()
            result_db = self.db_session_factory()
            try:
                exec_obj = result_db.query(models.Execution).filter(
                    models.Execution.id == exec_id
                ).first()
                return {
                    "success": exec_obj.status == NodeStatus.COMPLETED,
                    "execution_id": exec_id,
                    "status": exec_obj.status,
                    "error": exec_obj.error_log,
                    "node_states": exec_obj.node_states
                }
            finally:
                result_db.close()
        else:
            thread = threading.Thread(target=run_in_thread)
            with self._global_lock:
                self.running_executions[exec_id] = thread
            thread.start()
            return {"status": "started", "execution_id": exec_id, "message": "Pipeline execution started"}

    def _resume_execution(self, pipeline_id: int, execution_id: int,
                           sync: bool = False) -> Dict[str, Any]:
        validate_db = self.db_session_factory()
        try:
            execution = self._validate_resume_request(pipeline_id, execution_id, validate_db)

            lock = self._get_execution_lock(execution_id)
            if not lock.acquire(blocking=False):
                return {
                    "success": False,
                    "error": f"Execution {execution_id} is already being resumed by another request",
                    "status": "rejected"
                }

            try:
                with self._global_lock:
                    if execution_id in self.running_executions:
                        existing_thread = self.running_executions[execution_id]
                        if existing_thread is not None and existing_thread.is_alive():
                            lock.release()
                            return {
                                "success": False,
                                "error": f"Execution {execution_id} is already running",
                                "status": "rejected"
                            }

                checkpoint_data = execution.checkpoint_data
                if not checkpoint_data:
                    lock.release()
                    return {
                        "success": False,
                        "error": f"Execution {execution_id} has no checkpoint data, cannot resume",
                        "status": "rejected"
                    }

                pipeline_obj = validate_db.query(models.Pipeline).filter(
                    models.Pipeline.id == pipeline_id
                ).first()
                if not pipeline_obj:
                    lock.release()
                    return {
                        "success": False,
                        "error": f"Pipeline {pipeline_id} not found",
                        "status": "rejected"
                    }

                nodes_to_rerun = DAGExecutor.compute_nodes_to_rerun(
                    pipeline_obj.dag_config, checkpoint_data
                )
                failed_nodes = [
                    nid for nid, state in checkpoint_data.get("node_states", {}).items()
                    if state.get("status") == NodeStatus.FAILED
                ]
                if not failed_nodes:
                    lock.release()
                    return {
                        "success": False,
                        "error": f"Execution {execution_id} has no failed nodes in checkpoint",
                        "status": "rejected"
                    }

                validate_db.close()

                def run_resume_in_thread():
                    thread_db = self.db_session_factory()
                    try:
                        exec_obj = thread_db.query(models.Execution).filter(
                            models.Execution.id == execution_id
                        ).first()
                        pipeline = thread_db.query(models.Pipeline).filter(
                            models.Pipeline.id == pipeline_id
                        ).first()

                        exec_obj.status = NodeStatus.RUNNING
                        exec_obj.start_time = datetime.utcnow()
                        exec_obj.end_time = None
                        exec_obj.error_log = None
                        thread_db.commit()

                        executor = DAGExecutor(
                            pipeline.dag_config,
                            pipeline_id=pipeline_id,
                            execution_id=execution_id,
                            db_session=thread_db
                        )

                        success, result = executor.execute(
                            resume_from_checkpoint=checkpoint_data
                        )

                        exec_obj.status = NodeStatus.COMPLETED if success else NodeStatus.FAILED
                        exec_obj.end_time = datetime.utcnow()
                        exec_obj.node_states = result.get("node_states")
                        exec_obj.checkpoint_data = result.get("checkpoint_data")
                        exec_obj.total_rows = executor.get_total_rows()
                        exec_obj.success_rows = executor.get_success_rows()

                        if not success:
                            exec_obj.error_log = result.get("error", "")

                        thread_db.commit()

                    except Exception as e:
                        try:
                            thread_db.rollback()
                            exec_obj = thread_db.query(models.Execution).filter(
                                models.Execution.id == execution_id
                            ).first()
                            if exec_obj:
                                exec_obj.status = NodeStatus.FAILED
                                exec_obj.end_time = datetime.utcnow()
                                exec_obj.error_log = str(e)
                                thread_db.commit()
                        except Exception:
                            pass
                    finally:
                        with self._global_lock:
                            self.running_executions.pop(execution_id, None)
                        thread_db.close()

                with self._global_lock:
                    self.running_executions[execution_id] = None

                if sync:
                    lock.release()
                    run_resume_in_thread()
                    result_db = self.db_session_factory()
                    try:
                        exec_obj = result_db.query(models.Execution).filter(
                            models.Execution.id == execution_id
                        ).first()
                        return {
                            "success": exec_obj.status == NodeStatus.COMPLETED,
                            "execution_id": execution_id,
                            "status": exec_obj.status,
                            "error": exec_obj.error_log,
                            "node_states": exec_obj.node_states
                        }
                    finally:
                        result_db.close()
                else:
                    thread = threading.Thread(target=run_resume_in_thread)
                    with self._global_lock:
                        self.running_executions[execution_id] = thread
                    thread.start()
                    lock.release()
                    return {
                        "status": "started",
                        "execution_id": execution_id,
                        "message": "Pipeline resume started"
                    }
            except Exception:
                lock.release()
                raise
        except ResumeError as e:
            validate_db.close()
            return {"success": False, "error": str(e), "status": "rejected"}
        except Exception as e:
            validate_db.close()
            return {"success": False, "error": str(e), "status": "error"}

    def shutdown(self):
        self.scheduler.shutdown()
