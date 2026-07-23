import React, { useState, useEffect } from 'react';
import { Table, Button, Space, Tag, Modal, message, Popconfirm } from 'antd';
import {
  PlusOutlined,
  EditOutlined,
  DeleteOutlined,
  PlayCircleOutlined,
  HistoryOutlined,
  FileTextOutlined,
} from '@ant-design/icons';
import { pipelines, executions } from '../services/api';
import dayjs from 'dayjs';

const PipelineList = ({ onEdit, onViewHistory, onCreate, onCreateFromTemplate }) => {
  const [data, setData] = useState([]);
  const [loading, setLoading] = useState(false);
  const [templateModalVisible, setTemplateModalVisible] = useState(false);
  const [templates, setTemplates] = useState([]);

  useEffect(() => {
    fetchPipelines();
  }, []);

  const fetchPipelines = async () => {
    setLoading(true);
    try {
      const response = await pipelines.list(false);
      setData(response.data);
    } catch (error) {
      message.error('获取管道列表失败');
    } finally {
      setLoading(false);
    }
  };

  const handleDelete = async (id) => {
    try {
      await pipelines.delete(id);
      message.success('删除成功');
      fetchPipelines();
    } catch (error) {
      message.error('删除失败');
    }
  };

  const handleExecute = async (id) => {
    try {
      await executions.execute(id);
      message.success('执行已开始');
    } catch (error) {
      message.error('执行失败');
    }
  };

  const columns = [
    {
      title: '管道名称',
      dataIndex: 'name',
      key: 'name',
      render: (text, record) => (
        <Space>
          <FileTextOutlined />
          <span>{text}</span>
          {record.is_scheduled && <Tag color="blue">定时任务</Tag>}
        </Space>
      ),
    },
    {
      title: '描述',
      dataIndex: 'description',
      key: 'description',
      ellipsis: true,
    },
    {
      title: '节点数',
      key: 'nodes',
      render: (_, record) => record.dag_config?.nodes?.length || 0,
    },
    {
      title: '创建时间',
      dataIndex: 'created_at',
      key: 'created_at',
      render: (date) => dayjs(date).format('YYYY-MM-DD HH:mm'),
    },
    {
      title: '操作',
      key: 'actions',
      render: (_, record) => (
        <Space>
          <Button
            type="text"
            icon={<EditOutlined />}
            onClick={() => onEdit(record.id)}
          >
            编辑
          </Button>
          <Button
            type="text"
            icon={<PlayCircleOutlined />}
            onClick={() => handleExecute(record.id)}
          >
            执行
          </Button>
          <Button
            type="text"
            icon={<HistoryOutlined />}
            onClick={() => onViewHistory(record.id)}
          >
            历史
          </Button>
          <Popconfirm
            title="确定要删除这个管道吗？"
            onConfirm={() => handleDelete(record.id)}
            okText="确定"
            cancelText="取消"
          >
            <Button type="text" danger icon={<DeleteOutlined />}>
              删除
            </Button>
          </Popconfirm>
        </Space>
      ),
    },
  ];

  return (
    <div style={{ padding: 24 }}>
      <div style={{ marginBottom: 16, display: 'flex', justifyContent: 'space-between' }}>
        <h2>数据管道列表</h2>
        <Space>
          <Button
            type="primary"
            icon={<PlusOutlined />}
            onClick={() => setTemplateModalVisible(true)}
          >
            从模板创建
          </Button>
          <Button icon={<PlusOutlined />} onClick={onCreate}>
            新建管道
          </Button>
        </Space>
      </div>

      <Table
        columns={columns}
        dataSource={data}
        rowKey="id"
        loading={loading}
        pagination={{ pageSize: 10 }}
      />

      <TemplateSelectModal
        visible={templateModalVisible}
        onCancel={() => setTemplateModalVisible(false)}
        onSelect={async (templateType, name) => {
          try {
            const response = await onCreateFromTemplate(templateType, name);
            setTemplateModalVisible(false);
            message.success('创建成功');
            fetchPipelines();
            onEdit(response.data.id);
          } catch (error) {
            message.error('创建失败');
          }
        }}
      />
    </div>
  );
};

const TemplateSelectModal = ({ visible, onCancel, onSelect }) => {
  const [selectedTemplate, setSelectedTemplate] = useState(null);
  const [pipelineName, setPipelineName] = useState('');

  const templateList = [
    {
      type: 'csv_cleaning',
      name: 'CSV清洗入库',
      description: '从CSV文件读取数据，经过清洗后写入SQLite数据库',
    },
    {
      type: 'multi_table_join',
      name: '多表JOIN聚合报表',
      description: '从多个SQLite表读取数据，JOIN后聚合生成报表',
    },
    {
      type: 'api_sync',
      name: 'API数据同步',
      description: '从HTTP API获取数据，经过分支处理后同步到不同目标',
    },
  ];

  const handleConfirm = () => {
    if (!selectedTemplate || !pipelineName.trim()) {
      message.warning('请选择模板并输入管道名称');
      return;
    }
    onSelect(selectedTemplate, pipelineName);
  };

  return (
    <Modal
      title="选择管道模板"
      open={visible}
      onCancel={onCancel}
      onOk={handleConfirm}
      width={600}
    >
      <div style={{ marginBottom: 16 }}>
        <h4>选择模板</h4>
        <Space direction="vertical" style={{ width: '100%' }}>
          {templateList.map((tpl) => (
            <div
              key={tpl.type}
              style={{
                padding: 12,
                border: `1px solid ${selectedTemplate === tpl.type ? '#1890ff' : '#e8e8e8'}`,
                borderRadius: 4,
                cursor: 'pointer',
                background: selectedTemplate === tpl.type ? '#e6f7ff' : 'white',
              }}
              onClick={() => setSelectedTemplate(tpl.type)}
            >
              <div style={{ fontWeight: 500 }}>{tpl.name}</div>
              <div style={{ color: '#666', fontSize: 12 }}>{tpl.description}</div>
            </div>
          ))}
        </Space>
      </div>
      <div>
        <h4>管道名称</h4>
        <input
          type="text"
          className="ant-input"
          placeholder="请输入管道名称"
          value={pipelineName}
          onChange={(e) => setPipelineName(e.target.value)}
        />
      </div>
    </Modal>
  );
};

export default PipelineList;
