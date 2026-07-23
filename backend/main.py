import os
from fastapi import FastAPI, Depends, HTTPException, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from typing import List, Optional
import json

import models
import schemas
from database import engine, get_db, SessionLocal
from data_sources import DataSourceHandler, save_upload_file
from scheduler import PipelineScheduler
from pipeline_templates import get_all_templates, create_pipeline_from_template
from lineage_tracker import LineageTracker, ReverseLineageQuery
from workflow_engine import WorkflowValidator, WorkflowExecutor, PipelineDependencyManager

models.Base.metadata.create_all(bind=engine)

app = FastAPI(title="ETL Pipeline System", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

scheduler = PipelineScheduler(SessionLocal)


@app.on_event("shutdown")
def shutdown_event():
    scheduler.shutdown()


@app.get("/")
def read_root():
    return {"message": "ETL Pipeline System API"}


@app.post("/data/preview", response_model=schemas.DataPreviewResponse)
def preview_data(request: schemas.DataPreviewRequest):
    try:
        columns, data, total_count = DataSourceHandler.preview(
            request.source_type, request.config, request.limit
        )
        return {"columns": columns, "data": data, "total_count": total_count}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/upload/csv")
async def upload_csv(file: UploadFile = File(...)):
    try:
        file_path = await save_upload_file(file)
        return {"file_path": file_path, "filename": file.filename}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/pipelines", response_model=List[schemas.Pipeline])
def list_pipelines(skip: int = 0, limit: int = 100, is_template: bool = None, db: Session = Depends(get_db)):
    query = db.query(models.Pipeline)
    if is_template is not None:
        query = query.filter(models.Pipeline.is_template == is_template)
    pipelines = query.offset(skip).limit(limit).all()
    return pipelines


@app.get("/pipelines/{pipeline_id}", response_model=schemas.Pipeline)
def get_pipeline(pipeline_id: int, db: Session = Depends(get_db)):
    pipeline = db.query(models.Pipeline).filter(models.Pipeline.id == pipeline_id).first()
    if not pipeline:
        raise HTTPException(status_code=404, detail="Pipeline not found")
    return pipeline


@app.post("/pipelines", response_model=schemas.Pipeline)
def create_pipeline(pipeline: schemas.PipelineCreate, db: Session = Depends(get_db)):
    db_pipeline = models.Pipeline(**pipeline.dict())
    db.add(db_pipeline)
    db.commit()
    db.refresh(db_pipeline)
    return db_pipeline


@app.put("/pipelines/{pipeline_id}", response_model=schemas.Pipeline)
def update_pipeline(pipeline_id: int, pipeline_update: schemas.PipelineUpdate, db: Session = Depends(get_db)):
    pipeline = db.query(models.Pipeline).filter(models.Pipeline.id == pipeline_id).first()
    if not pipeline:
        raise HTTPException(status_code=404, detail="Pipeline not found")

    update_data = pipeline_update.dict(exclude_unset=True)
    for key, value in update_data.items():
        setattr(pipeline, key, value)

    db.commit()
    db.refresh(pipeline)
    return pipeline


@app.delete("/pipelines/{pipeline_id}")
def delete_pipeline(pipeline_id: int, db: Session = Depends(get_db)):
    pipeline = db.query(models.Pipeline).filter(models.Pipeline.id == pipeline_id).first()
    if not pipeline:
        raise HTTPException(status_code=404, detail="Pipeline not found")
    db.delete(pipeline)
    db.commit()
    return {"message": "Pipeline deleted"}


@app.get("/templates")
def list_templates():
    return get_all_templates()


@app.post("/templates/create")
def create_from_template(request: schemas.TemplateCreateRequest, db: Session = Depends(get_db)):
    try:
        pipeline_data = create_pipeline_from_template(request.template_type, request.name)
        db_pipeline = models.Pipeline(**pipeline_data)
        db.add(db_pipeline)
        db.commit()
        db.refresh(db_pipeline)
        return db_pipeline
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/execute")
def execute_pipeline(request: schemas.ExecuteRequest):
    try:
        result = scheduler.execute_pipeline(
            request.pipeline_id,
            resume_from_failed=request.resume_from_failed,
            execution_id=request.execution_id
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/scheduler")
def update_scheduler(request: schemas.SchedulerUpdateRequest):
    try:
        scheduler.update_schedule(
            request.pipeline_id,
            request.cron_expression,
            request.is_scheduled
        )
        return {"message": "Schedule updated"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/executions", response_model=List[schemas.Execution])
def list_executions(pipeline_id: int = None, skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    query = db.query(models.Execution)
    if pipeline_id:
        query = query.filter(models.Execution.pipeline_id == pipeline_id)
    executions = query.order_by(models.Execution.created_at.desc()).offset(skip).limit(limit).all()
    return executions


@app.get("/executions/{execution_id}", response_model=schemas.Execution)
def get_execution(execution_id: int, db: Session = Depends(get_db)):
    execution = db.query(models.Execution).filter(models.Execution.id == execution_id).first()
    if not execution:
        raise HTTPException(status_code=404, detail="Execution not found")
    return execution


@app.get("/data/files")
def list_uploaded_files():
    upload_dir = os.getenv("UPLOAD_DIR", "./uploads")
    if not os.path.exists(upload_dir):
        return []
    files = []
    for f in os.listdir(upload_dir):
        if f.endswith('.csv'):
            file_path = os.path.join(upload_dir, f)
            files.append({
                "name": f,
                "path": file_path,
                "size": os.path.getsize(file_path)
            })
    return files


@app.get("/data/sqlite/tables")
def list_sqlite_tables(db_path: str):
    import sqlite3
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [row[0] for row in cursor.fetchall()]
        conn.close()
        return {"tables": tables}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/data/sqlite/cdc/enable")
def enable_sqlite_cdc(request: dict, db: Session = Depends(get_db)):
    try:
        db_path = request.get("db_path")
        table_name = request.get("table_name")
        data_source_id = request.get("data_source_id")
        success = DataSourceHandler.enable_sqlite_cdc(db_path, table_name, data_source_id)
        return {"success": success, "message": "CDC enabled successfully" if success else "CDC enable failed"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/data/sqlite/cdc/disable")
def disable_sqlite_cdc(request: dict):
    try:
        db_path = request.get("db_path")
        table_name = request.get("table_name")
        success = DataSourceHandler.disable_sqlite_cdc(db_path, table_name)
        return {"success": success, "message": "CDC disabled successfully" if success else "CDC disable failed"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/quality/rules", response_model=List[schemas.DataQualityRule])
def list_quality_rules(pipeline_id: Optional[int] = None, node_id: Optional[str] = None,
                        skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    query = db.query(models.DataQualityRule)
    if pipeline_id:
        query = query.filter(models.DataQualityRule.pipeline_id == pipeline_id)
    if node_id:
        query = query.filter(models.DataQualityRule.node_id == node_id)
    rules = query.offset(skip).limit(limit).all()
    return rules


@app.get("/quality/rules/{rule_id}", response_model=schemas.DataQualityRule)
def get_quality_rule(rule_id: int, db: Session = Depends(get_db)):
    rule = db.query(models.DataQualityRule).filter(models.DataQualityRule.id == rule_id).first()
    if not rule:
        raise HTTPException(status_code=404, detail="Quality rule not found")
    return rule


@app.post("/quality/rules", response_model=schemas.DataQualityRule)
def create_quality_rule(rule: schemas.DataQualityRuleCreate, db: Session = Depends(get_db)):
    db_rule = models.DataQualityRule(**rule.dict())
    db.add(db_rule)
    db.commit()
    db.refresh(db_rule)
    return db_rule


@app.put("/quality/rules/{rule_id}", response_model=schemas.DataQualityRule)
def update_quality_rule(rule_id: int, rule_update: schemas.DataQualityRuleUpdate, db: Session = Depends(get_db)):
    rule = db.query(models.DataQualityRule).filter(models.DataQualityRule.id == rule_id).first()
    if not rule:
        raise HTTPException(status_code=404, detail="Quality rule not found")
    update_data = rule_update.dict(exclude_unset=True)
    for key, value in update_data.items():
        setattr(rule, key, value)
    db.commit()
    db.refresh(rule)
    return rule


@app.delete("/quality/rules/{rule_id}")
def delete_quality_rule(rule_id: int, db: Session = Depends(get_db)):
    rule = db.query(models.DataQualityRule).filter(models.DataQualityRule.id == rule_id).first()
    if not rule:
        raise HTTPException(status_code=404, detail="Quality rule not found")
    db.delete(rule)
    db.commit()
    return {"message": "Quality rule deleted"}


@app.get("/quality/reports", response_model=List[schemas.QualityReport])
def list_quality_reports(execution_id: Optional[int] = None, pipeline_id: Optional[int] = None,
                          skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    query = db.query(models.QualityReport)
    if execution_id:
        query = query.filter(models.QualityReport.execution_id == execution_id)
    if pipeline_id:
        query = query.join(models.Execution).filter(models.Execution.pipeline_id == pipeline_id)
    reports = query.order_by(models.QualityReport.created_at.desc()).offset(skip).limit(limit).all()
    return reports


@app.get("/quality/reports/{report_id}", response_model=schemas.QualityReport)
def get_quality_report(report_id: int, db: Session = Depends(get_db)):
    report = db.query(models.QualityReport).filter(models.QualityReport.id == report_id).first()
    if not report:
        raise HTTPException(status_code=404, detail="Quality report not found")
    return report


@app.get("/quality/quarantine", response_model=List[schemas.QuarantineRecord])
def list_quarantine_records(execution_id: Optional[int] = None, node_id: Optional[str] = None,
                              skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    query = db.query(models.QuarantineRecord)
    if execution_id:
        query = query.filter(models.QuarantineRecord.execution_id == execution_id)
    if node_id:
        query = query.filter(models.QuarantineRecord.node_id == node_id)
    records = query.order_by(models.QuarantineRecord.created_at.desc()).offset(skip).limit(limit).all()
    return records


@app.get("/watermarks", response_model=List[schemas.Watermark])
def list_watermarks(data_source_id: Optional[int] = None, skip: int = 0, limit: int = 100,
                     db: Session = Depends(get_db)):
    query = db.query(models.Watermark)
    if data_source_id:
        query = query.filter(models.Watermark.data_source_id == data_source_id)
    watermarks = query.order_by(models.Watermark.updated_at.desc()).offset(skip).limit(limit).all()
    return watermarks


@app.get("/watermarks/{watermark_id}", response_model=schemas.Watermark)
def get_watermark(watermark_id: int, db: Session = Depends(get_db)):
    watermark = db.query(models.Watermark).filter(models.Watermark.id == watermark_id).first()
    if not watermark:
        raise HTTPException(status_code=404, detail="Watermark not found")
    return watermark


@app.post("/watermarks", response_model=schemas.Watermark)
def create_watermark(watermark: schemas.WatermarkCreate, db: Session = Depends(get_db)):
    db_watermark = models.Watermark(**watermark.dict())
    db.add(db_watermark)
    db.commit()
    db.refresh(db_watermark)
    return db_watermark


@app.put("/watermarks/{watermark_id}", response_model=schemas.Watermark)
def update_watermark(watermark_id: int, watermark_update: schemas.WatermarkUpdate, db: Session = Depends(get_db)):
    watermark = db.query(models.Watermark).filter(models.Watermark.id == watermark_id).first()
    if not watermark:
        raise HTTPException(status_code=404, detail="Watermark not found")
    watermark.watermark_value = watermark_update.watermark_value
    db.commit()
    db.refresh(watermark)
    return watermark


@app.get("/performance/metrics", response_model=List[schemas.PerformanceMetric])
def list_performance_metrics(execution_id: Optional[int] = None, node_id: Optional[str] = None,
                              skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    query = db.query(models.PerformanceMetric)
    if execution_id:
        query = query.filter(models.PerformanceMetric.execution_id == execution_id)
    if node_id:
        query = query.filter(models.PerformanceMetric.node_id == node_id)
    metrics = query.order_by(models.PerformanceMetric.created_at.desc()).offset(skip).limit(limit).all()
    return metrics


@app.post("/performance/compare", response_model=schemas.PerformanceComparisonResponse)
def compare_performance(request: schemas.PerformanceComparisonRequest, db: Session = Depends(get_db)):
    metrics_1 = db.query(models.PerformanceMetric).filter(
        models.PerformanceMetric.execution_id == request.execution_id_1
    ).all()
    metrics_2 = db.query(models.PerformanceMetric).filter(
        models.PerformanceMetric.execution_id == request.execution_id_2
    ).all()

    comparison = {}
    metric_map_1 = {m.node_id: m for m in metrics_1}
    metric_map_2 = {m.node_id: m for m in metrics_2}
    all_nodes = set(metric_map_1.keys()) | set(metric_map_2.keys())

    for node_id in all_nodes:
        m1 = metric_map_1.get(node_id)
        m2 = metric_map_2.get(node_id)
        comparison[node_id] = {
            "duration_ms_diff": (m2.duration_ms - m1.duration_ms) if m1 and m2 else None,
            "throughput_diff": (m2.throughput - m1.throughput) if m1 and m2 else None,
            "memory_diff": (m2.memory_peak_bytes - m1.memory_peak_bytes) if m1 and m2 else None,
            "output_rows_diff": (m2.output_rows - m1.output_rows) if m1 and m2 else None
        }

    return {
        "execution_1": metrics_1,
        "execution_2": metrics_2,
        "comparison": comparison
    }


@app.get("/workflows", response_model=List[schemas.Workflow])
def list_workflows(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    workflows = db.query(models.Workflow).offset(skip).limit(limit).all()
    return workflows


@app.get("/workflows/{workflow_id}", response_model=schemas.Workflow)
def get_workflow(workflow_id: int, db: Session = Depends(get_db)):
    workflow = db.query(models.Workflow).filter(models.Workflow.id == workflow_id).first()
    if not workflow:
        raise HTTPException(status_code=404, detail="Workflow not found")
    return workflow


@app.post("/workflows", response_model=schemas.Workflow)
def create_workflow(workflow: schemas.WorkflowCreate, db: Session = Depends(get_db)):
    validator = WorkflowValidator()
    valid, error = validator.validate_dag(workflow.dag_config)
    if not valid:
        raise HTTPException(status_code=400, detail=f"Invalid workflow DAG: {error}")

    db_workflow = models.Workflow(**workflow.dict())
    db.add(db_workflow)
    db.commit()
    db.refresh(db_workflow)

    nodes = workflow.dag_config.get("nodes", [])
    for node in nodes:
        node_key = node.get("id")
        pipeline_id = node.get("data", {}).get("pipeline_id")
        depends_on = []
        for edge in workflow.dag_config.get("edges", []):
            if edge.get("target") == node_key:
                depends_on.append(edge.get("source"))

        if pipeline_id:
            db_node = models.WorkflowNode(
                workflow_id=db_workflow.id,
                pipeline_id=pipeline_id,
                node_key=node_key,
                depends_on=depends_on if depends_on else None
            )
            db.add(db_node)

    db.commit()
    return db_workflow


@app.put("/workflows/{workflow_id}", response_model=schemas.Workflow)
def update_workflow(workflow_id: int, workflow_update: schemas.WorkflowUpdate, db: Session = Depends(get_db)):
    workflow = db.query(models.Workflow).filter(models.Workflow.id == workflow_id).first()
    if not workflow:
        raise HTTPException(status_code=404, detail="Workflow not found")

    if workflow_update.dag_config is not None:
        validator = WorkflowValidator()
        valid, error = validator.validate_dag(workflow_update.dag_config)
        if not valid:
            raise HTTPException(status_code=400, detail=f"Invalid workflow DAG: {error}")

    update_data = workflow_update.dict(exclude_unset=True)
    for key, value in update_data.items():
        setattr(workflow, key, value)
    db.commit()
    db.refresh(workflow)
    return workflow


@app.delete("/workflows/{workflow_id}")
def delete_workflow(workflow_id: int, db: Session = Depends(get_db)):
    workflow = db.query(models.Workflow).filter(models.Workflow.id == workflow_id).first()
    if not workflow:
        raise HTTPException(status_code=404, detail="Workflow not found")
    db.delete(workflow)
    db.commit()
    return {"message": "Workflow deleted"}


@app.post("/workflows/execute")
def execute_workflow(request: schemas.WorkflowExecuteRequest):
    try:
        executor = WorkflowExecutor(request.workflow_id, SessionLocal)
        result = executor.execute()
        return result
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/workflows/executions", response_model=List[schemas.WorkflowExecution])
def list_workflow_executions(workflow_id: Optional[int] = None, skip: int = 0, limit: int = 100,
                              db: Session = Depends(get_db)):
    query = db.query(models.WorkflowExecution)
    if workflow_id:
        query = query.filter(models.WorkflowExecution.workflow_id == workflow_id)
    executions = query.order_by(models.WorkflowExecution.created_at.desc()).offset(skip).limit(limit).all()
    return executions


@app.get("/workflows/executions/{execution_id}", response_model=schemas.WorkflowExecution)
def get_workflow_execution(execution_id: int, db: Session = Depends(get_db)):
    execution = db.query(models.WorkflowExecution).filter(models.WorkflowExecution.id == execution_id).first()
    if not execution:
        raise HTTPException(status_code=404, detail="Workflow execution not found")
    return execution


@app.post("/pipeline-dependencies")
def add_pipeline_dependency(request: dict, db: Session = Depends(get_db)):
    try:
        upstream_id = request.get("upstream_pipeline_id")
        downstream_id = request.get("downstream_pipeline_id")
        manager = PipelineDependencyManager()
        success = manager.add_dependency(upstream_id, downstream_id, db)
        return {"success": success}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.delete("/pipeline-dependencies")
def remove_pipeline_dependency(request: dict, db: Session = Depends(get_db)):
    try:
        upstream_id = request.get("upstream_pipeline_id")
        downstream_id = request.get("downstream_pipeline_id")
        manager = PipelineDependencyManager()
        success = manager.remove_dependency(upstream_id, downstream_id, db)
        return {"success": success}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/lineage/graph", response_model=schemas.LineageGraph)
def get_lineage_graph(pipeline_id: Optional[int] = None, execution_id: Optional[int] = None,
                       db: Session = Depends(get_db)):
    query = db.query(models.LineageRecord)
    if pipeline_id:
        query = query.filter(models.LineageRecord.pipeline_id == pipeline_id)
    if execution_id:
        query = query.filter(models.LineageRecord.execution_id == execution_id)
    records = query.all()

    tracker = LineageTracker(pipeline_id, execution_id)
    graph = tracker.build_lineage_graph(db)
    return graph


@app.post("/lineage/reverse")
def get_reverse_lineage(request: schemas.ReverseLineageRequest, db: Session = Depends(get_db)):
    try:
        records = ReverseLineageQuery.find_sources(request.target_table, request.target_column, db)
        return {
            "target_table": request.target_table,
            "target_column": request.target_column,
            "sources": records
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/lineage/records", response_model=List[schemas.LineageRecord])
def list_lineage_records(pipeline_id: Optional[int] = None, execution_id: Optional[int] = None,
                          skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    query = db.query(models.LineageRecord)
    if pipeline_id:
        query = query.filter(models.LineageRecord.pipeline_id == pipeline_id)
    if execution_id:
        query = query.filter(models.LineageRecord.execution_id == execution_id)
    records = query.offset(skip).limit(limit).all()
    return records
