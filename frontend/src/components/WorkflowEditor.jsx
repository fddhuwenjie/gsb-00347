import React, { useState, useCallback, useRef, useEffect } from 'react';
import ReactFlow, {
  ReactFlowProvider,
  addEdge,
  useNodesState,
  useEdgesState,
  Controls,
  Background,
  MiniMap,
  useReactFlow,
  Handle,
  Position,
} from 'reactflow';
import 'reactflow/dist/style.css';
import {
  Button,
  Space,
  Card,
  Layout,
  message,
  Modal,
  Input,
  Tag,
  Table,
  Descriptions,
} from 'antd';
import {
  PlayCircleOutlined,
  SaveOutlined,
  DeleteOutlined,
  HistoryOutlined,
  ArrowLeftOutlined,
  ExperimentOutlined,
} from '@ant-design/icons';
import { pipelines, workflows } from '../services/api';
import dayjs from 'dayjs';

const { Sider, Content } = Layout;

const PipelineNode = ({ data, selected }) => {
  const status = data.status;

  const getStatusColor = (status) => {
    const colorMap = {
      pending: '#d9d9d9',
      running: '#1890ff',
      completed: '#52c41a',
      failed: '#ff4d4f',
      skipped: '#faad14',
    };
    return colorMap[status] || '#d9d9d9';
  };

  const getStatusText = (status) => {
    const textMap = {
      pending: '等待中',
      running: '运行中',
      completed: '已完成',
      failed: '失败',
      skipped: '已跳过',
    };
    return textMap[status] || status;
  };

  return (
    <div
      style={{
        position: 'relative',
        padding: '12px 16px',
        border: `2px solid ${selected ? '#1890ff' : '#e8e8e8'}`,
        borderRadius: '8px',
        background: '#fff',
        minWidth: '160px',
        boxShadow: selected ? '0 2px 8px rgba(24, 144, 255, 0.2)' : '0 1px 3px rgba(0,0,0,0.1)',
      }}
    >
      <Handle
        type="target"
        position={Position.Left}
        style={{ background: '#1890ff', width: 10, height: 10 }}
      />

      <div style={{ display: 'flex', flexDirection: 'column', gap: '6px' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
          <ExperimentOutlined style={{ color: '#722ed1' }} />
          <span
            style={{
              fontWeight: 600,
              fontSize: '14px',
              color: '#262626',
              overflow: 'hidden',
              textOverflow: 'ellipsis',
              whiteSpace: 'nowrap',
              maxWidth: '120px',
            }}
            title={data.label}
          >
            {data.label}
          </span>
        </div>
        {status && (
          <Tag
            color={getStatusColor(status)}
            style={{ margin: 0, fontSize: '12px' }}
          >
            {getStatusText(status)}
          </Tag>
        )}
      </div>

      <Handle
        type="source"
        position={Position.Right}
        style={{ background: '#1890ff', width: 10, height: 10 }}
      />
    </div>
  );
};

const nodeTypes = {
  pipeline: PipelineNode,
};

const WorkflowEditor = ({ workflowId, onSave, onBack }) => {
  const reactFlowWrapper = useRef(null);
  const [nodes, setNodes, onNodesChange] = useNodesState([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState([]);
  const [selectedNode, setSelectedNode] = useState(null);
  const [saveModalVisible, setSaveModalVisible] = useState(false);
  const [workflowName, setWorkflowName] = useState('');
  const [workflowDescription, setWorkflowDescription] = useState('');
  const [nodeStates, setNodeStates] = useState({});
  const [pipelineList, setPipelineList] = useState([]);
  const [loadingPipelines, setLoadingPipelines] = useState(false);
  const [historyModalVisible, setHistoryModalVisible] = useState(false);
  const [executionHistory, setExecutionHistory] = useState([]);
  const [loadingHistory, setLoadingHistory] = useState(false);
  const [detailModalVisible, setDetailModalVisible] = useState(false);
  const [selectedExecution, setSelectedExecution] = useState(null);
  const { screenToFlowPosition } = useReactFlow();

  useEffect(() => {
    loadPipelines();
  }, []);

  useEffect(() => {
    if (workflowId) {
      loadWorkflow(workflowId);
    }
  }, [workflowId]);

  const loadPipelines = async () => {
    setLoadingPipelines(true);
    try {
      const response = await pipelines.list();
      setPipelineList(response.data || []);
    } catch (error) {
      message.error('加载管道列表失败');
    } finally {
      setLoadingPipelines(false);
    }
  };

  const loadWorkflow = async (id) => {
    try {
      const response = await workflows.get(id);
      const workflow = response.data;
      if (workflow.dag_config) {
        const loadedNodes = (workflow.dag_config.nodes || []).map((node) => ({
          ...node,
          type: 'pipeline',
          data: {
            ...node.data,
            status: nodeStates[node.id]?.status,
          },
        }));
        setNodes(loadedNodes);
        setEdges(workflow.dag_config.edges || []);
        setWorkflowName(workflow.name);
        setWorkflowDescription(workflow.description || '');
      }
    } catch (error) {
      message.error('加载工作流失败');
    }
  };

  const detectCycle = (nodes, edges) => {
    const nodeIds = nodes.map((n) => n.id);
    const adjacency = {};
    const inDegree = {};

    nodeIds.forEach((id) => {
      adjacency[id] = [];
      inDegree[id] = 0;
    });

    edges.forEach((edge) => {
      if (adjacency[edge.source]) {
        adjacency[edge.source].push(edge.target);
        inDegree[edge.target]++;
      }
    });

    const queue = nodeIds.filter((id) => inDegree[id] === 0);
    let visitedCount = 0;

    while (queue.length > 0) {
      const current = queue.shift();
      visitedCount++;
      adjacency[current].forEach((neighbor) => {
        inDegree[neighbor]--;
        if (inDegree[neighbor] === 0) {
          queue.push(neighbor);
        }
      });
    }

    return visitedCount !== nodeIds.length;
  };

  const onConnect = useCallback(
    (params) => {
      const tempEdges = [...edges, params];
      if (detectCycle(nodes, tempEdges)) {
        message.error('检测到循环依赖，无法创建此连接');
        return;
      }
      setEdges((eds) => addEdge(params, eds));
    },
    [setEdges, nodes, edges]
  );

  const onDragOver = useCallback((event) => {
    event.preventDefault();
    event.dataTransfer.dropEffect = 'move';
  }, []);

  const onDrop = useCallback(
    (event) => {
      event.preventDefault();

      const pipelineData = event.dataTransfer.getData('application/workflow-pipeline');
      if (!pipelineData) return;

      const pipeline = JSON.parse(pipelineData);

      const position = screenToFlowPosition({
        x: event.clientX,
        y: event.clientY,
      });

      const newNode = {
        id: `pipeline_${pipeline.id}_${Date.now()}`,
        type: 'pipeline',
        position,
        data: {
          label: pipeline.name,
          pipelineId: pipeline.id,
          key: `pipeline_${pipeline.id}_${Date.now()}`,
        },
      };

      setNodes((nds) => nds.concat(newNode));
    },
    [screenToFlowPosition, setNodes]
  );

  const onNodeClick = useCallback(
    (event, node) => {
      const nodeWithStatus = {
        ...node,
        data: {
          ...node.data,
          status: nodeStates[node.id]?.status,
        },
      };
      setSelectedNode(nodeWithStatus);
    },
    [nodeStates]
  );

  const onPaneClick = useCallback(() => {
    setSelectedNode(null);
  }, []);

  const onDeleteSelected = useCallback(() => {
    if (!selectedNode) return;
    setNodes((nds) => nds.filter((node) => node.id !== selectedNode.id));
    setEdges((eds) =>
      eds.filter(
        (edge) => edge.source !== selectedNode.id && edge.target !== selectedNode.id
      )
    );
    setSelectedNode(null);
  }, [selectedNode, setNodes, setEdges]);

  const handleSave = async () => {
    if (!workflowName.trim()) {
      message.warning('请输入工作流名称');
      return;
    }

    if (nodes.length === 0) {
      message.warning('工作流至少需要一个节点');
      return;
    }

    if (detectCycle(nodes, edges)) {
      message.error('检测到循环依赖，请修正后再保存');
      return;
    }

    const dagNodes = nodes.map((node) => ({
      id: node.id,
      type: node.type,
      position: node.position,
      data: node.data,
      key: node.data.key || node.id,
      pipeline_id: node.data.pipelineId,
    }));

    const dagConfig = {
      nodes: dagNodes,
      edges,
    };

    try {
      if (workflowId) {
        await workflows.update(workflowId, {
          name: workflowName,
          description: workflowDescription,
          dag_config: dagConfig,
        });
        message.success('保存成功');
      } else {
        const response = await workflows.create({
          name: workflowName,
          description: workflowDescription,
          dag_config: dagConfig,
          is_enabled: true,
        });
        message.success('创建成功');
        if (onSave) {
          onSave(response.data);
        }
      }
      setSaveModalVisible(false);
    } catch (error) {
      message.error('保存失败: ' + (error.response?.data?.detail || error.message));
    }
  };

  const handleExecute = async () => {
    if (!workflowId) {
      message.warning('请先保存工作流');
      return;
    }

    if (detectCycle(nodes, edges)) {
      message.error('检测到循环依赖，无法执行');
      return;
    }

    try {
      await workflows.execute(workflowId);
      message.success('工作流执行已开始');
      monitorExecution(workflowId);
    } catch (error) {
      message.error('执行失败: ' + (error.response?.data?.detail || error.message));
    }
  };

  const monitorExecution = async (workflowId) => {
    const checkStatus = async () => {
      try {
        const response = await workflows.listExecutions(workflowId);
        const latest = response.data[0];
        if (latest && latest.node_states) {
          setNodeStates(latest.node_states);

          setNodes((nds) =>
            nds.map((node) => ({
              ...node,
              data: {
                ...node.data,
                status: latest.node_states[node.data.key || node.id]?.status,
              },
            }))
          );

          if (latest.status === 'running') {
            setTimeout(checkStatus, 1000);
          }
        }
      } catch (error) {
        console.error('检查执行状态失败', error);
      }
    };
    checkStatus();
  };

  const handleViewHistory = async () => {
    if (!workflowId) {
      message.warning('请先保存工作流');
      return;
    }

    setLoadingHistory(true);
    try {
      const response = await workflows.listExecutions(workflowId);
      setExecutionHistory(response.data || []);
      setHistoryModalVisible(true);
    } catch (error) {
      message.error('获取执行历史失败');
    } finally {
      setLoadingHistory(false);
    }
  };

  const handleViewExecutionDetail = (execution) => {
    setSelectedExecution(execution);
    setDetailModalVisible(true);
  };

  const getStatusColor = (status) => {
    const colorMap = {
      pending: 'default',
      running: 'processing',
      completed: 'success',
      failed: 'error',
      skipped: 'warning',
    };
    return colorMap[status] || 'default';
  };

  const getStatusText = (status) => {
    const textMap = {
      pending: '等待中',
      running: '运行中',
      completed: '已完成',
      failed: '失败',
      skipped: '已跳过',
    };
    return textMap[status] || status;
  };

  const historyColumns = [
    {
      title: '执行ID',
      dataIndex: 'id',
      key: 'id',
      width: 80,
    },
    {
      title: '状态',
      dataIndex: 'status',
      key: 'status',
      render: (status) => (
        <Tag color={getStatusColor(status)}>{getStatusText(status)}</Tag>
      ),
    },
    {
      title: '开始时间',
      dataIndex: 'start_time',
      key: 'start_time',
      render: (date) => (date ? dayjs(date).format('YYYY-MM-DD HH:mm:ss') : '-'),
    },
    {
      title: '结束时间',
      dataIndex: 'end_time',
      key: 'end_time',
      render: (date) => (date ? dayjs(date).format('YYYY-MM-DD HH:mm:ss') : '-'),
    },
    {
      title: '操作',
      key: 'actions',
      render: (_, record) => (
        <Button
          type="text"
          onClick={() => handleViewExecutionDetail(record)}
        >
          详情
        </Button>
      ),
    },
  ];

  const sidebarWidth = 280;

  return (
    <Layout style={{ height: '100%' }}>
      <Sider
        width={sidebarWidth}
        theme="light"
        style={{ borderRight: '1px solid #e8e8e8' }}
      >
        <Card title="管道列表" size="small" style={{ border: 'none' }} loading={loadingPipelines}>
          <Space direction="vertical" style={{ width: '100%' }}>
            {pipelineList.length === 0 ? (
              <div style={{ color: '#999', textAlign: 'center', padding: '20px 0' }}>
                暂无可用管道
              </div>
            ) : (
              pipelineList.map((pipeline) => (
                <div
                  key={pipeline.id}
                  draggable
                  onDragStart={(event) => {
                    event.dataTransfer.setData(
                      'application/workflow-pipeline',
                      JSON.stringify(pipeline)
                    );
                    event.dataTransfer.effectAllowed = 'move';
                  }}
                  style={{
                    padding: '10px 12px',
                    border: '1px solid #d9d9d9',
                    borderRadius: '4px',
                    cursor: 'grab',
                    display: 'flex',
                    alignItems: 'center',
                    gap: '8px',
                    background: '#fafafa',
                    transition: 'all 0.2s',
                  }}
                  onMouseEnter={(e) => {
                    e.currentTarget.style.borderColor = '#1890ff';
                    e.currentTarget.style.background = '#e6f7ff';
                  }}
                  onMouseLeave={(e) => {
                    e.currentTarget.style.borderColor = '#d9d9d9';
                    e.currentTarget.style.background = '#fafafa';
                  }}
                >
                  <ExperimentOutlined style={{ color: '#722ed1' }} />
                  <span
                    style={{
                      overflow: 'hidden',
                      textOverflow: 'ellipsis',
                      whiteSpace: 'nowrap',
                      flex: 1,
                    }}
                    title={pipeline.name}
                  >
                    {pipeline.name}
                  </span>
                </div>
              ))
            )}
          </Space>
        </Card>

        <Card title="操作" size="small" style={{ border: 'none', marginTop: 8 }}>
          <Space direction="vertical" style={{ width: '100%' }}>
            <Button
              type="primary"
              icon={<PlayCircleOutlined />}
              onClick={handleExecute}
              block
            >
              执行工作流
            </Button>
            <Button
              icon={<SaveOutlined />}
              onClick={() => setSaveModalVisible(true)}
              block
            >
              {workflowId ? '保存修改' : '保存工作流'}
            </Button>
            <Button
              icon={<HistoryOutlined />}
              onClick={handleViewHistory}
              block
            >
              执行历史
            </Button>
            <Button
              danger
              icon={<DeleteOutlined />}
              onClick={onDeleteSelected}
              disabled={!selectedNode}
              block
            >
              删除选中节点
            </Button>
            {onBack && (
              <Button icon={<ArrowLeftOutlined />} onClick={onBack} block>
                返回列表
              </Button>
            )}
          </Space>
        </Card>

        {selectedNode && (
          <Card title="节点信息" size="small" style={{ border: 'none', marginTop: 8 }}>
            <div style={{ padding: '8px 0' }}>
              <div style={{ marginBottom: '8px' }}>
                <span style={{ color: '#888', fontSize: '12px' }}>节点名称</span>
                <div style={{ fontWeight: 500 }}>{selectedNode.data.label}</div>
              </div>
              <div style={{ marginBottom: '8px' }}>
                <span style={{ color: '#888', fontSize: '12px' }}>管道ID</span>
                <div>{selectedNode.data.pipelineId}</div>
              </div>
              {selectedNode.data.status && (
                <div>
                  <span style={{ color: '#888', fontSize: '12px' }}>执行状态</span>
                  <div>
                    <Tag color={getStatusColor(selectedNode.data.status)}>
                      {getStatusText(selectedNode.data.status)}
                    </Tag>
                  </div>
                </div>
              )}
            </div>
          </Card>
        )}
      </Sider>

      <Content>
        <div ref={reactFlowWrapper} style={{ width: '100%', height: '100%' }}>
          <ReactFlow
            nodes={nodes}
            edges={edges}
            onNodesChange={onNodesChange}
            onEdgesChange={onEdgesChange}
            onConnect={onConnect}
            onDrop={onDrop}
            onDragOver={onDragOver}
            onNodeClick={onNodeClick}
            onPaneClick={onPaneClick}
            nodeTypes={nodeTypes}
            fitView
          >
            <Background />
            <Controls />
            <MiniMap
              nodeColor={(node) => {
                const status = node.data.status;
                if (status === 'completed') return '#52c41a';
                if (status === 'running') return '#1890ff';
                if (status === 'failed') return '#ff4d4f';
                if (status === 'skipped') return '#faad14';
                return '#e8e8e8';
              }}
            />
          </ReactFlow>
        </div>
      </Content>

      <Modal
        title={workflowId ? '保存工作流' : '创建工作流'}
        open={saveModalVisible}
        onOk={handleSave}
        onCancel={() => setSaveModalVisible(false)}
        width={500}
      >
        <div style={{ marginBottom: 16 }}>
          <div style={{ marginBottom: 8, fontWeight: 500 }}>工作流名称</div>
          <Input
            placeholder="请输入工作流名称"
            value={workflowName}
            onChange={(e) => setWorkflowName(e.target.value)}
          />
        </div>
        <div>
          <div style={{ marginBottom: 8, fontWeight: 500 }}>描述（可选）</div>
          <Input.TextArea
            placeholder="请输入工作流描述"
            value={workflowDescription}
            onChange={(e) => setWorkflowDescription(e.target.value)}
            rows={3}
          />
        </div>
      </Modal>

      <Modal
        title="执行历史"
        open={historyModalVisible}
        onCancel={() => setHistoryModalVisible(false)}
        footer={[
          <Button key="close" onClick={() => setHistoryModalVisible(false)}>
            关闭
          </Button>,
        ]}
        width={900}
      >
        <Table
          columns={historyColumns}
          dataSource={executionHistory}
          rowKey="id"
          loading={loadingHistory}
          pagination={{ pageSize: 10 }}
        />
      </Modal>

      <Modal
        title={`执行详情 #${selectedExecution?.id}`}
        open={detailModalVisible}
        onCancel={() => setDetailModalVisible(false)}
        footer={[
          <Button key="close" onClick={() => setDetailModalVisible(false)}>
            关闭
          </Button>,
        ]}
        width={800}
      >
        {selectedExecution && (
          <>
            <Descriptions column={2} bordered size="small" style={{ marginBottom: 16 }}>
              <Descriptions.Item label="状态">
                <Tag color={getStatusColor(selectedExecution.status)}>
                  {getStatusText(selectedExecution.status)}
                </Tag>
              </Descriptions.Item>
              <Descriptions.Item label="工作流ID">
                {selectedExecution.workflow_id}
              </Descriptions.Item>
              <Descriptions.Item label="开始时间">
                {selectedExecution.start_time
                  ? dayjs(selectedExecution.start_time).format('YYYY-MM-DD HH:mm:ss')
                  : '-'}
              </Descriptions.Item>
              <Descriptions.Item label="结束时间">
                {selectedExecution.end_time
                  ? dayjs(selectedExecution.end_time).format('YYYY-MM-DD HH:mm:ss')
                  : '-'}
              </Descriptions.Item>
            </Descriptions>

            <Card title="节点状态" size="small">
              <Space direction="vertical" style={{ width: '100%' }}>
                {Object.entries(selectedExecution.node_states || {}).map(
                  ([nodeKey, state]) => (
                    <div
                      key={nodeKey}
                      style={{
                        padding: '10px 12px',
                        border: '1px solid #e8e8e8',
                        borderRadius: 4,
                        display: 'flex',
                        justifyContent: 'space-between',
                        alignItems: 'center',
                      }}
                    >
                      <Space>
                        <span
                          style={{
                            display: 'inline-block',
                            width: 8,
                            height: 8,
                            borderRadius: '50%',
                            background:
                              state.status === 'completed'
                                ? '#52c41a'
                                : state.status === 'running'
                                ? '#1890ff'
                                : state.status === 'failed'
                                ? '#ff4d4f'
                                : state.status === 'skipped'
                                ? '#faad14'
                                : '#d9d9d9',
                          }}
                        />
                        <span style={{ fontWeight: 500 }}>{nodeKey}</span>
                      </Space>
                      <Space>
                        <Tag color={getStatusColor(state.status)}>
                          {getStatusText(state.status)}
                        </Tag>
                        {state.error && (
                          <Tag color="red" title={state.error}>
                            错误
                          </Tag>
                        )}
                      </Space>
                    </div>
                  )
                )}
              </Space>
            </Card>

            {selectedExecution.error_log && (
              <Card
                title="错误日志"
                size="small"
                type="inner"
                style={{ marginTop: 16 }}
              >
                <pre
                  style={{
                    whiteSpace: 'pre-wrap',
                    color: '#ff4d4f',
                    margin: 0,
                    maxHeight: '200px',
                    overflowY: 'auto',
                  }}
                >
                  {selectedExecution.error_log}
                </pre>
              </Card>
            )}
          </>
        )}
      </Modal>
    </Layout>
  );
};

const WorkflowEditorWithProvider = (props) => (
  <ReactFlowProvider>
    <WorkflowEditor {...props} />
  </ReactFlowProvider>
);

export default WorkflowEditorWithProvider;
