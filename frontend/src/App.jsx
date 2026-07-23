import React, { useState } from 'react';
import { Layout, Menu, Typography } from 'antd';
import {
  DatabaseOutlined,
  PlayCircleOutlined,
  BranchesOutlined,
  ShareAltOutlined,
} from '@ant-design/icons';
import PipelineList from './components/PipelineList';
import DAGEditor from './components/DAGEditor';
import ExecutionHistory from './components/ExecutionHistory';
import WorkflowEditor from './components/WorkflowEditor';
import LineageGraph from './components/LineageGraph';
import WorkflowList from './components/WorkflowList';
import { templates } from './services/api';

const { Header, Content } = Layout;
const { Title } = Typography;

const App = () => {
  const [currentPage, setCurrentPage] = useState('pipelines');
  const [selectedPipelineId, setSelectedPipelineId] = useState(null);
  const [selectedWorkflowId, setSelectedWorkflowId] = useState(null);
  const [viewMode, setViewMode] = useState('list');
  const [workflowViewMode, setWorkflowViewMode] = useState('list');

  const handleEditPipeline = (pipelineId) => {
    setSelectedPipelineId(pipelineId);
    setViewMode('editor');
  };

  const handleCreatePipeline = () => {
    setSelectedPipelineId(null);
    setViewMode('editor');
  };

  const handleViewHistory = (pipelineId) => {
    setSelectedPipelineId(pipelineId);
    setViewMode('history');
  };

  const handleCreateFromTemplate = async (templateType, name) => {
    return await templates.createFromTemplate(templateType, name);
  };

  const handleBackToList = () => {
    setViewMode('list');
    setSelectedPipelineId(null);
  };

  const handleSavePipeline = (pipeline) => {
    setSelectedPipelineId(pipeline.id);
  };

  const handleEditWorkflow = (workflowId) => {
    setSelectedWorkflowId(workflowId);
    setWorkflowViewMode('editor');
  };

  const handleCreateWorkflow = () => {
    setSelectedWorkflowId(null);
    setWorkflowViewMode('editor');
  };

  const handleWorkflowBackToList = () => {
    setWorkflowViewMode('list');
    setSelectedWorkflowId(null);
  };

  const handleSaveWorkflow = (workflow) => {
    setSelectedWorkflowId(workflow.id);
  };

  const menuItems = [
    {
      key: 'pipelines',
      icon: <DatabaseOutlined />,
      label: '数据管道',
      onClick: () => {
        setCurrentPage('pipelines');
        setViewMode('list');
        setSelectedPipelineId(null);
      },
    },
    {
      key: 'workflows',
      icon: <BranchesOutlined />,
      label: '工作流编排',
      onClick: () => {
        setCurrentPage('workflows');
        setWorkflowViewMode('list');
        setSelectedWorkflowId(null);
      },
    },
    {
      key: 'lineage',
      icon: <ShareAltOutlined />,
      label: '数据血缘',
      onClick: () => {
        setCurrentPage('lineage');
      },
    },
    {
      key: 'executions',
      icon: <PlayCircleOutlined />,
      label: '执行记录',
      onClick: () => {
        setCurrentPage('executions');
      },
    },
  ];

  const renderContent = () => {
    if (currentPage === 'pipelines') {
      if (viewMode === 'editor') {
        return (
          <DAGEditor
            pipelineId={selectedPipelineId}
            onSave={handleSavePipeline}
            onBack={handleBackToList}
          />
        );
      }
      if (viewMode === 'history') {
        return (
          <ExecutionHistory
            pipelineId={selectedPipelineId}
            onBack={handleBackToList}
          />
        );
      }
      return (
        <PipelineList
          onEdit={handleEditPipeline}
          onViewHistory={handleViewHistory}
          onCreate={handleCreatePipeline}
          onCreateFromTemplate={handleCreateFromTemplate}
        />
      );
    }

    if (currentPage === 'workflows') {
      if (workflowViewMode === 'editor') {
        return (
          <WorkflowEditor
            workflowId={selectedWorkflowId}
            onSave={handleSaveWorkflow}
            onBack={handleWorkflowBackToList}
          />
        );
      }
      return (
        <WorkflowList
          onEdit={handleEditWorkflow}
          onCreate={handleCreateWorkflow}
        />
      );
    }

    if (currentPage === 'lineage') {
      return (
        <div style={{ padding: 24 }}>
          <h2>数据血缘追踪</h2>
          <LineageGraph />
        </div>
      );
    }

    if (currentPage === 'executions') {
      return (
        <div style={{ padding: 24 }}>
          <h2>执行记录</h2>
          <ExecutionHistory pipelineId={null} onBack={() => {}} />
        </div>
      );
    }

    return null;
  };

  return (
    <Layout style={{ height: '100vh' }}>
      <Header
        style={{
          background: '#001529',
          display: 'flex',
          alignItems: 'center',
          padding: '0 24px',
        }}
      >
        <Title level={4} style={{ color: 'white', margin: 0, marginRight: 48 }}>
          <DatabaseOutlined style={{ marginRight: 8 }} />
          ETL 数据管道编排系统
        </Title>
        <Menu
          theme="dark"
          mode="horizontal"
          selectedKeys={[currentPage]}
          items={menuItems}
          style={{ background: 'transparent', border: 'none', flex: 1 }}
        />
      </Header>
      <Layout>
        <Content style={{ background: '#f0f2f5', overflow: 'auto' }}>
          {renderContent()}
        </Content>
      </Layout>
    </Layout>
  );
};

export default App;
