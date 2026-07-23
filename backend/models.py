from sqlalchemy import Column, Integer, String, Text, DateTime, ForeignKey, JSON, Boolean, Float
from sqlalchemy.orm import relationship
from datetime import datetime
from database import Base


class Pipeline(Base):
    __tablename__ = "pipelines"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False)
    description = Column(Text, nullable=True)
    dag_config = Column(JSON, nullable=False)
    is_template = Column(Boolean, default=False)
    template_type = Column(String(50), nullable=True)
    cron_expression = Column(String(100), nullable=True)
    is_scheduled = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    executions = relationship("Execution", back_populates="pipeline", cascade="all, delete-orphan")
    quality_rules = relationship("DataQualityRule", back_populates="pipeline", cascade="all, delete-orphan")
    lineage_records = relationship("LineageRecord", back_populates="pipeline", cascade="all, delete-orphan")
    workflow_nodes = relationship("WorkflowNode", back_populates="pipeline")


class Execution(Base):
    __tablename__ = "executions"

    id = Column(Integer, primary_key=True, index=True)
    pipeline_id = Column(Integer, ForeignKey("pipelines.id"), nullable=False)
    status = Column(String(50), default="pending")
    start_time = Column(DateTime, nullable=True)
    end_time = Column(DateTime, nullable=True)
    total_rows = Column(Integer, default=0)
    success_rows = Column(Integer, default=0)
    error_log = Column(Text, nullable=True)
    node_states = Column(JSON, nullable=True)
    checkpoint_data = Column(JSON, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    pipeline = relationship("Pipeline", back_populates="executions")
    quality_reports = relationship("QualityReport", back_populates="execution", cascade="all, delete-orphan")
    performance_metrics = relationship("PerformanceMetric", back_populates="execution", cascade="all, delete-orphan")
    workflow_executions = relationship("WorkflowExecution", back_populates="execution")


class DataSource(Base):
    __tablename__ = "data_sources"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False)
    type = Column(String(50), nullable=False)
    config = Column(JSON, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    watermarks = relationship("Watermark", back_populates="data_source", cascade="all, delete-orphan")


class DataQualityRule(Base):
    __tablename__ = "data_quality_rules"

    id = Column(Integer, primary_key=True, index=True)
    pipeline_id = Column(Integer, ForeignKey("pipelines.id"), nullable=False)
    node_id = Column(String(100), nullable=False)
    rule_type = Column(String(50), nullable=False)
    rule_config = Column(JSON, nullable=False)
    failure_strategy = Column(String(50), default="mark")
    is_enabled = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    pipeline = relationship("Pipeline", back_populates="quality_rules")


class QualityReport(Base):
    __tablename__ = "quality_reports"

    id = Column(Integer, primary_key=True, index=True)
    execution_id = Column(Integer, ForeignKey("executions.id"), nullable=False)
    rule_id = Column(Integer, ForeignKey("data_quality_rules.id"), nullable=True)
    node_id = Column(String(100), nullable=False)
    rule_type = Column(String(50), nullable=False)
    rule_config = Column(JSON, nullable=False)
    total_records = Column(Integer, default=0)
    passed_records = Column(Integer, default=0)
    failed_records = Column(Integer, default=0)
    pass_rate = Column(Float, default=0.0)
    violation_samples = Column(JSON, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    execution = relationship("Execution", back_populates="quality_reports")


class QuarantineRecord(Base):
    __tablename__ = "quarantine_records"

    id = Column(Integer, primary_key=True, index=True)
    execution_id = Column(Integer, ForeignKey("executions.id"), nullable=False)
    node_id = Column(String(100), nullable=False)
    rule_id = Column(Integer, ForeignKey("data_quality_rules.id"), nullable=True)
    record_data = Column(JSON, nullable=False)
    violation_reason = Column(Text, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)


class Watermark(Base):
    __tablename__ = "watermarks"

    id = Column(Integer, primary_key=True, index=True)
    data_source_id = Column(Integer, ForeignKey("data_sources.id"), nullable=False)
    source_type = Column(String(50), nullable=False)
    watermark_type = Column(String(50), nullable=False)
    watermark_value = Column(JSON, nullable=False)
    last_sync_time = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    data_source = relationship("DataSource", back_populates="watermarks")


class ChangeLog(Base):
    __tablename__ = "change_logs"

    id = Column(Integer, primary_key=True, index=True)
    data_source_id = Column(Integer, ForeignKey("data_sources.id"), nullable=False)
    table_name = Column(String(255), nullable=False)
    operation_type = Column(String(20), nullable=False)
    record_id = Column(String(255), nullable=False)
    record_data = Column(JSON, nullable=True)
    changed_at = Column(DateTime, default=datetime.utcnow)
    synced = Column(Boolean, default=False)


class PerformanceMetric(Base):
    __tablename__ = "performance_metrics"

    id = Column(Integer, primary_key=True, index=True)
    execution_id = Column(Integer, ForeignKey("executions.id"), nullable=False)
    node_id = Column(String(100), nullable=False)
    start_time = Column(DateTime, nullable=True)
    end_time = Column(DateTime, nullable=True)
    duration_ms = Column(Float, default=0)
    input_rows = Column(Integer, default=0)
    output_rows = Column(Integer, default=0)
    throughput = Column(Float, default=0.0)
    memory_peak_bytes = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)

    execution = relationship("Execution", back_populates="performance_metrics")


class Workflow(Base):
    __tablename__ = "workflows"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False)
    description = Column(Text, nullable=True)
    dag_config = Column(JSON, nullable=False)
    is_enabled = Column(Boolean, default=True)
    cron_expression = Column(String(100), nullable=True)
    is_scheduled = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    nodes = relationship("WorkflowNode", back_populates="workflow", cascade="all, delete-orphan")
    executions = relationship("WorkflowExecution", back_populates="workflow", cascade="all, delete-orphan")


class WorkflowNode(Base):
    __tablename__ = "workflow_nodes"

    id = Column(Integer, primary_key=True, index=True)
    workflow_id = Column(Integer, ForeignKey("workflows.id"), nullable=False)
    pipeline_id = Column(Integer, ForeignKey("pipelines.id"), nullable=False)
    node_key = Column(String(100), nullable=False)
    depends_on = Column(JSON, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    workflow = relationship("Workflow", back_populates="nodes")
    pipeline = relationship("Pipeline", back_populates="workflow_nodes")


class WorkflowExecution(Base):
    __tablename__ = "workflow_executions"

    id = Column(Integer, primary_key=True, index=True)
    workflow_id = Column(Integer, ForeignKey("workflows.id"), nullable=False)
    status = Column(String(50), default="pending")
    start_time = Column(DateTime, nullable=True)
    end_time = Column(DateTime, nullable=True)
    execution_id = Column(Integer, ForeignKey("executions.id"), nullable=True)
    node_states = Column(JSON, nullable=True)
    error_log = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    workflow = relationship("Workflow", back_populates="executions")
    execution = relationship("Execution", back_populates="workflow_executions")


class LineageRecord(Base):
    __tablename__ = "lineage_records"

    id = Column(Integer, primary_key=True, index=True)
    pipeline_id = Column(Integer, ForeignKey("pipelines.id"), nullable=False)
    execution_id = Column(Integer, ForeignKey("executions.id"), nullable=True)
    source_table = Column(String(255), nullable=False)
    source_column = Column(String(255), nullable=False)
    target_table = Column(String(255), nullable=False)
    target_column = Column(String(255), nullable=False)
    transform_path = Column(JSON, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

    pipeline = relationship("Pipeline", back_populates="lineage_records")
