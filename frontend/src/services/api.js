import axios from 'axios';

const API_BASE_URL = process.env.REACT_APP_API_URL || 'http://localhost:8347';

const api = axios.create({
  baseURL: API_BASE_URL,
  timeout: 30000,
});

export const dataSources = {
  preview: (sourceType, config, limit = 10) =>
    api.post('/data/preview', { source_type: sourceType, config, limit }),
  
  uploadCSV: (file) => {
    const formData = new FormData();
    formData.append('file', file);
    return api.post('/upload/csv', formData, {
      headers: { 'Content-Type': 'multipart/form-data' },
    });
  },

  listFiles: () => api.get('/data/files'),
  
  listTables: (dbPath) => api.get(`/data/sqlite/tables?db_path=${dbPath}`),
};

export const pipelines = {
  list: (isTemplate = null) => {
    let url = '/pipelines';
    if (isTemplate !== null) {
      url += `?is_template=${isTemplate}`;
    }
    return api.get(url);
  },
  
  get: (id) => api.get(`/pipelines/${id}`),
  
  create: (data) => api.post('/pipelines', data),
  
  update: (id, data) => api.put(`/pipelines/${id}`, data),
  
  delete: (id) => api.delete(`/pipelines/${id}`),
};

export const templates = {
  list: () => api.get('/templates'),
  
  createFromTemplate: (templateType, name) =>
    api.post('/templates/create', { template_type: templateType, name }),
};

export const executions = {
  list: (pipelineId = null) => {
    let url = '/executions';
    if (pipelineId) {
      url += `?pipeline_id=${pipelineId}`;
    }
    return api.get(url);
  },
  
  get: (id) => api.get(`/executions/${id}`),
  
  execute: (pipelineId, resumeFromFailed = false, executionId = null) =>
    api.post('/execute', {
      pipeline_id: pipelineId,
      resume_from_failed: resumeFromFailed,
      execution_id: executionId,
    }),
};

export const scheduler = {
  update: (pipelineId, cronExpression, isScheduled = true) =>
    api.post('/scheduler', {
      pipeline_id: pipelineId,
      cron_expression: cronExpression,
      is_scheduled: isScheduled,
    }),
};

export const qualityRules = {
  list: (pipelineId = null, nodeId = null) => {
    let url = '/quality/rules';
    const params = new URLSearchParams();
    if (pipelineId) params.append('pipeline_id', pipelineId);
    if (nodeId) params.append('node_id', nodeId);
    const queryString = params.toString();
    if (queryString) url += `?${queryString}`;
    return api.get(url);
  },
  get: (id) => api.get(`/quality/rules/${id}`),
  create: (data) => api.post('/quality/rules', data),
  update: (id, data) => api.put(`/quality/rules/${id}`, data),
  delete: (id) => api.delete(`/quality/rules/${id}`),
};

export const qualityReports = {
  list: (executionId = null, pipelineId = null) => {
    let url = '/quality/reports';
    const params = new URLSearchParams();
    if (executionId) params.append('execution_id', executionId);
    if (pipelineId) params.append('pipeline_id', pipelineId);
    const queryString = params.toString();
    if (queryString) url += `?${queryString}`;
    return api.get(url);
  },
  get: (id) => api.get(`/quality/reports/${id}`),
};

export const quarantine = {
  list: (executionId = null, nodeId = null) => {
    let url = '/quality/quarantine';
    const params = new URLSearchParams();
    if (executionId) params.append('execution_id', executionId);
    if (nodeId) params.append('node_id', nodeId);
    const queryString = params.toString();
    if (queryString) url += `?${queryString}`;
    return api.get(url);
  },
};

export const watermarks = {
  list: (dataSourceId = null) => {
    let url = '/watermarks';
    if (dataSourceId) url += `?data_source_id=${dataSourceId}`;
    return api.get(url);
  },
  get: (id) => api.get(`/watermarks/${id}`),
  create: (data) => api.post('/watermarks', data),
  update: (id, data) => api.put(`/watermarks/${id}`, data),
};

export const performance = {
  listMetrics: (executionId = null, nodeId = null) => {
    let url = '/performance/metrics';
    const params = new URLSearchParams();
    if (executionId) params.append('execution_id', executionId);
    if (nodeId) params.append('node_id', nodeId);
    const queryString = params.toString();
    if (queryString) url += `?${queryString}`;
    return api.get(url);
  },
  compare: (executionId1, executionId2) =>
    api.post('/performance/compare', {
      execution_id_1: executionId1,
      execution_id_2: executionId2,
    }),
};

export const workflows = {
  list: () => api.get('/workflows'),
  get: (id) => api.get(`/workflows/${id}`),
  create: (data) => api.post('/workflows', data),
  update: (id, data) => api.put(`/workflows/${id}`, data),
  delete: (id) => api.delete(`/workflows/${id}`),
  execute: (workflowId) => api.post('/workflows/execute', { workflow_id: workflowId }),
  listExecutions: (workflowId = null) => {
    let url = '/workflows/executions';
    if (workflowId) url += `?workflow_id=${workflowId}`;
    return api.get(url);
  },
  getExecution: (id) => api.get(`/workflows/executions/${id}`),
};

export const lineage = {
  getGraph: (pipelineId = null, executionId = null) => {
    let url = '/lineage/graph';
    const params = new URLSearchParams();
    if (pipelineId) params.append('pipeline_id', pipelineId);
    if (executionId) params.append('execution_id', executionId);
    const queryString = params.toString();
    if (queryString) url += `?${queryString}`;
    return api.get(url);
  },
  reverse: (targetTable, targetColumn) =>
    api.post('/lineage/reverse', {
      target_table: targetTable,
      target_column: targetColumn,
    }),
  listRecords: (pipelineId = null, executionId = null) => {
    let url = '/lineage/records';
    const params = new URLSearchParams();
    if (pipelineId) params.append('pipeline_id', pipelineId);
    if (executionId) params.append('execution_id', executionId);
    const queryString = params.toString();
    if (queryString) url += `?${queryString}`;
    return api.get(url);
  },
};

export const cdc = {
  enable: (dbPath, tableName, dataSourceId) =>
    api.post('/data/sqlite/cdc/enable', {
      db_path: dbPath,
      table_name: tableName,
      data_source_id: dataSourceId,
    }),
  disable: (dbPath, tableName) =>
    api.post('/data/sqlite/cdc/disable', {
      db_path: dbPath,
      table_name: tableName,
    }),
};

export const pipelineDependencies = {
  add: (upstreamId, downstreamId) =>
    api.post('/pipeline-dependencies', {
      upstream_pipeline_id: upstreamId,
      downstream_pipeline_id: downstreamId,
    }),
  remove: (upstreamId, downstreamId) =>
    api.delete('/pipeline-dependencies', {
      data: {
        upstream_pipeline_id: upstreamId,
        downstream_pipeline_id: downstreamId,
      },
    }),
};

export default api;
