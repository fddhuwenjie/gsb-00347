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
} from 'reactflow';
import 'reactflow/dist/style.css';
import { Button, Space, Card, Layout, message, Modal, Input } from 'antd';
import {
  PlayCircleOutlined,
  SaveOutlined,
  DeleteOutlined,
  PlusOutlined,
  DatabaseOutlined,
  FilterOutlined,
  ArrowRightOutlined,
  BarChartOutlined,
  SortAscendingOutlined,
  DeleteRowOutlined,
  LinkOutlined,
  BranchesOutlined,
  ExportOutlined,
} from '@ant-design/icons';
import CustomNode from './CustomNode';
import NodeConfigPanel from './NodeConfigPanel';
import { pipelines, executions } from '../services/api';

const { Sider, Content } = Layout;

const nodeTypes = {
  source: CustomNode,
  filter: CustomNode,
  map: CustomNode,
  aggregate: CustomNode,
  sort: CustomNode,
  deduplicate: CustomNode,
  join: CustomNode,
  branch: CustomNode,
  output: CustomNode,
};

const nodeTypeConfig = {
  source: { icon: <DatabaseOutlined />, label: '数据源', color: '#52c41a' },
  filter: { icon: <FilterOutlined />, label: '过滤', color: '#1890ff' },
  map: { icon: <ArrowRightOutlined />, label: '映射', color: '#722ed1' },
  aggregate: { icon: <BarChartOutlined />, label: '聚合', color: '#eb2f96' },
  sort: { icon: <SortAscendingOutlined />, label: '排序', color: '#fa8c16' },
  deduplicate: { icon: <DeleteRowOutlined />, label: '去重', color: '#13c2c2' },
  join: { icon: <LinkOutlined />, label: 'JOIN', color: '#fa541c' },
  branch: { icon: <BranchesOutlined />, label: '分支', color: '#a0d911' },
  output: { icon: <ExportOutlined />, label: '输出', color: '#faad14' },
};

const DAGEditor = ({ pipelineId, onSave, onBack }) => {
  const reactFlowWrapper = useRef(null);
  const [nodes, setNodes, onNodesChange] = useNodesState([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState([]);
  const [selectedNode, setSelectedNode] = useState(null);
  const [saveModalVisible, setSaveModalVisible] = useState(false);
  const [pipelineName, setPipelineName] = useState('');
  const [nodeStates, setNodeStates] = useState({});
  const { screenToFlowPosition } = useReactFlow();

  useEffect(() => {
    if (pipelineId) {
      loadPipeline(pipelineId);
    }
  }, [pipelineId]);

  const loadPipeline = async (id) => {
    try {
      const response = await pipelines.get(id);
      const pipeline = response.data;
      if (pipeline.dag_config) {
        setNodes(pipeline.dag_config.nodes || []);
        setEdges(pipeline.dag_config.edges || []);
        setPipelineName(pipeline.name);
      }
    } catch (error) {
      message.error('加载管道失败');
    }
  };

  const onConnect = useCallback(
    (params) => setEdges((eds) => addEdge(params, eds)),
    [setEdges]
  );

  const onDragOver = useCallback((event) => {
    event.preventDefault();
    event.dataTransfer.dropEffect = 'move';
  }, []);

  const onDrop = useCallback(
    (event) => {
      event.preventDefault();

      const type = event.dataTransfer.getData('application/reactflow');
      if (!type) return;

      const position = screenToFlowPosition({
        x: event.clientX,
        y: event.clientY,
      });

      const newNode = {
        id: `${type}_${Date.now()}`,
        type,
        position,
        data: { 
          label: nodeTypeConfig[type]?.label || type,
          config: {},
        },
      };

      setNodes((nds) => nds.concat(newNode));
    },
    [screenToFlowPosition, setNodes]
  );

  const onNodeClick = useCallback((event, node) => {
    const nodeWithStatus = {
      ...node,
      data: {
        ...node.data,
        status: nodeStates[node.id]?.status,
      },
    };
    setSelectedNode(nodeWithStatus);
  }, [nodeStates]);

  const onPaneClick = useCallback(() => {
    setSelectedNode(null);
  }, []);

  const onNodeUpdate = useCallback((nodeId, newNode) => {
    setNodes((nds) =>
      nds.map((node) => (node.id === nodeId ? newNode : node))
    );
    setSelectedNode(newNode);
  }, [setNodes]);

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

  const handleExecute = async () => {
    if (!pipelineId) {
      message.warning('请先保存管道');
      return;
    }

    try {
      await executions.execute(pipelineId);
      message.success('执行已开始');
      monitorExecution(pipelineId);
    } catch (error) {
      message.error('执行失败: ' + (error.response?.data?.detail || error.message));
    }
  };

  const monitorExecution = async (pipelineId) => {
    const checkStatus = async () => {
      try {
        const response = await executions.list(pipelineId);
        const latest = response.data[0];
        if (latest && latest.node_states) {
          setNodeStates(latest.node_states);
          
          setNodes((nds) =>
            nds.map((node) => ({
              ...node,
              data: {
                ...node.data,
                status: latest.node_states[node.id]?.status,
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

  const handleSave = async () => {
    if (!pipelineName.trim()) {
      message.warning('请输入管道名称');
      return;
    }

    const dagConfig = {
      nodes,
      edges,
    };

    try {
      if (pipelineId) {
        await pipelines.update(pipelineId, {
          name: pipelineName,
          dag_config: dagConfig,
        });
        message.success('保存成功');
      } else {
        const response = await pipelines.create({
          name: pipelineName,
          dag_config: dagConfig,
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

  const sidebarWidth = 280;
  const configPanelWidth = 320;

  return (
    <Layout style={{ height: '100%' }}>
      <Sider
        width={sidebarWidth}
        theme="light"
        style={{ borderRight: '1px solid #e8e8e8' }}
      >
        <Card title="节点面板" size="small" style={{ border: 'none' }}>
          <Space direction="vertical" style={{ width: '100%' }}>
            {Object.entries(nodeTypeConfig).map(([type, config]) => (
              <div
                key={type}
                draggable
                onDragStart={(event) => {
                  event.dataTransfer.setData('application/reactflow', type);
                  event.dataTransfer.effectAllowed = 'move';
                }}
                style={{
                  padding: '8px 12px',
                  border: `1px solid ${config.color}`,
                  borderRadius: '4px',
                  cursor: 'grab',
                  display: 'flex',
                  alignItems: 'center',
                  gap: '8px',
                  background: `${config.color}10`,
                }}
              >
                <span style={{ color: config.color }}>{config.icon}</span>
                <span>{config.label}</span>
              </div>
            ))}
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
              执行管道
            </Button>
            <Button
              icon={<SaveOutlined />}
              onClick={() => setSaveModalVisible(true)}
              block
            >
              {pipelineId ? '保存修改' : '保存管道'}
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
              <Button onClick={onBack} block>
                返回列表
              </Button>
            )}
          </Space>
        </Card>
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
            <MiniMap />
          </ReactFlow>
        </div>
      </Content>

      {selectedNode && (
        <Sider
          width={configPanelWidth}
          theme="light"
          style={{ borderLeft: '1px solid #e8e8e8' }}
        >
          <NodeConfigPanel
            node={selectedNode}
            onUpdate={onNodeUpdate}
            onClose={() => setSelectedNode(null)}
            pipelineId={pipelineId}
          />
        </Sider>
      )}

      <Modal
        title="保存管道"
        open={saveModalVisible}
        onOk={handleSave}
        onCancel={() => setSaveModalVisible(false)}
      >
        <Input
          placeholder="请输入管道名称"
          value={pipelineName}
          onChange={(e) => setPipelineName(e.target.value)}
        />
      </Modal>
    </Layout>
  );
};

const DAGEditorWithProvider = (props) => (
  <ReactFlowProvider>
    <DAGEditor {...props} />
  </ReactFlowProvider>
);

export default DAGEditorWithProvider;
