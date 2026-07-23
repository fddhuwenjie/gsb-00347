from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any, Tuple
from datetime import datetime


class DataSourceBase(BaseModel):
    name: str
    type: str
    config: Dict[str, Any]


class DataSourceCreate(DataSourceBase):
    pass


class DataSource(DataSourceBase):
    id: int
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class PipelineBase(BaseModel):
    name: str
    description: Optional[str] = None
    dag_config: Dict[str, Any]
    is_template: bool = False
    template_type: Optional[str] = None
    cron_expression: Optional[str] = None
    is_scheduled: bool = False


class PipelineCreate(PipelineBase):
    pass


class PipelineUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    dag_config: Optional[Dict[str, Any]] = None
    cron_expression: Optional[str] = None
    is_scheduled: Optional[bool] = None


class Pipeline(PipelineBase):
    id: int
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class ExecutionBase(BaseModel):
    pipeline_id: int
    status: str = "pending"


class ExecutionCreate(ExecutionBase):
    pass


class Execution(ExecutionBase):
    id: int
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    total_rows: int = 0
    success_rows: int = 0
    error_log: Optional[str] = None
    node_states: Optional[Dict[str, Any]] = None
    created_at: datetime

    class Config:
        from_attributes = True


class DataPreviewRequest(BaseModel):
    source_type: str
    config: Dict[str, Any]
    limit: int = 10


class DataPreviewResponse(BaseModel):
    columns: List[str]
    data: List[Dict[str, Any]]
    total_count: int


class ExecuteRequest(BaseModel):
    pipeline_id: int
    resume_from_failed: bool = False
    execution_id: Optional[int] = None


class NodeConfig(BaseModel):
    node_id: str
    node_type: str
    config: Dict[str, Any]


class TemplateCreateRequest(BaseModel):
    template_type: str
    name: str


class SchedulerUpdateRequest(BaseModel):
    pipeline_id: int
    cron_expression: str
    is_scheduled: bool = True


class DataQualityRuleBase(BaseModel):
    pipeline_id: int
    node_id: str
    rule_type: str
    rule_config: Dict[str, Any]
    failure_strategy: str = "mark"
    is_enabled: bool = True


class DataQualityRuleCreate(DataQualityRuleBase):
    pass


class DataQualityRuleUpdate(BaseModel):
    rule_config: Optional[Dict[str, Any]] = None
    failure_strategy: Optional[str] = None
    is_enabled: Optional[bool] = None


class DataQualityRule(DataQualityRuleBase):
    id: int
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class QualityReportBase(BaseModel):
    execution_id: int
    rule_id: Optional[int]
    node_id: str
    rule_type: str
    rule_config: Dict[str, Any]
    total_records: int
    passed_records: int
    failed_records: int
    pass_rate: float
    violation_samples: Optional[List[Dict[str, Any]]] = None


class QualityReportCreate(QualityReportBase):
    pass


class QualityReport(QualityReportBase):
    id: int
    created_at: datetime

    class Config:
        from_attributes = True


class QualityCheckResult(BaseModel):
    passed: bool
    total_records: int
    passed_records: int
    failed_records: int
    pass_rate: float
    violation_samples: List[Dict[str, Any]]
    cleaned_data: Optional[Any] = None
    quarantined_data: Optional[List[Dict[str, Any]]] = None


class WatermarkBase(BaseModel):
    data_source_id: int
    source_type: str
    watermark_type: str
    watermark_value: Dict[str, Any]


class WatermarkCreate(WatermarkBase):
    pass


class WatermarkUpdate(BaseModel):
    watermark_value: Dict[str, Any]


class Watermark(WatermarkBase):
    id: int
    last_sync_time: Optional[datetime] = None
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class IncrementalConfig(BaseModel):
    enabled: bool = False
    mode: Optional[str] = None
    timestamp_field: Optional[str] = None
    id_field: Optional[str] = None
    cdc_enabled: bool = False


class PerformanceMetricBase(BaseModel):
    execution_id: int
    node_id: str
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    duration_ms: float = 0
    input_rows: int = 0
    output_rows: int = 0
    throughput: float = 0.0
    memory_peak_bytes: int = 0


class PerformanceMetricCreate(PerformanceMetricBase):
    pass


class PerformanceMetric(PerformanceMetricBase):
    id: int
    created_at: datetime

    class Config:
        from_attributes = True


class PerformanceComparisonRequest(BaseModel):
    execution_id_1: int
    execution_id_2: int


class PerformanceComparisonResponse(BaseModel):
    execution_1: List[PerformanceMetric]
    execution_2: List[PerformanceMetric]
    comparison: Dict[str, Any]


class WorkflowBase(BaseModel):
    name: str
    description: Optional[str] = None
    dag_config: Dict[str, Any]
    is_enabled: bool = True
    cron_expression: Optional[str] = None
    is_scheduled: bool = False


class WorkflowCreate(WorkflowBase):
    pass


class WorkflowUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    dag_config: Optional[Dict[str, Any]] = None
    is_enabled: Optional[bool] = None
    cron_expression: Optional[str] = None
    is_scheduled: Optional[bool] = None


class Workflow(WorkflowBase):
    id: int
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class WorkflowNodeBase(BaseModel):
    workflow_id: int
    pipeline_id: int
    node_key: str
    depends_on: Optional[List[str]] = None


class WorkflowNodeCreate(WorkflowNodeBase):
    pass


class WorkflowNode(WorkflowNodeBase):
    id: int
    created_at: datetime

    class Config:
        from_attributes = True


class WorkflowExecutionBase(BaseModel):
    workflow_id: int
    status: str = "pending"


class WorkflowExecutionCreate(WorkflowExecutionBase):
    pass


class WorkflowExecution(WorkflowExecutionBase):
    id: int
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    execution_id: Optional[int] = None
    node_states: Optional[Dict[str, Any]] = None
    error_log: Optional[str] = None
    created_at: datetime

    class Config:
        from_attributes = True


class WorkflowExecuteRequest(BaseModel):
    workflow_id: int


class LineageRecordBase(BaseModel):
    pipeline_id: int
    execution_id: Optional[int] = None
    source_table: str
    source_column: str
    target_table: str
    target_column: str
    transform_path: List[Dict[str, Any]]


class LineageRecordCreate(LineageRecordBase):
    pass


class LineageRecord(LineageRecordBase):
    id: int
    created_at: datetime

    class Config:
        from_attributes = True


class LineageNode(BaseModel):
    id: str
    name: str
    type: str


class LineageLink(BaseModel):
    source: str
    target: str
    value: int
    transform: Optional[Dict[str, Any]] = None


class LineageGraph(BaseModel):
    nodes: List[LineageNode]
    links: List[LineageLink]


class ReverseLineageRequest(BaseModel):
    target_table: str
    target_column: str


class QuarantineRecordBase(BaseModel):
    execution_id: int
    node_id: str
    rule_id: Optional[int] = None
    record_data: Dict[str, Any]
    violation_reason: str


class QuarantineRecordCreate(QuarantineRecordBase):
    pass


class QuarantineRecord(QuarantineRecordBase):
    id: int
    created_at: datetime

    class Config:
        from_attributes = True
