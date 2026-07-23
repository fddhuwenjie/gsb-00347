import React, { useState, useEffect } from 'react';
import { Table, Button, Space, Tag, Card, Modal, message, Popconfirm } from 'antd';
import {
  PlusOutlined,
  EditOutlined,
  DeleteOutlined,
  PlayCircleOutlined,
  HistoryOutlined,
} from '@ant-design/icons';
import { workflows } from '../services/api';
import dayjs from 'dayjs';

const WorkflowList = ({ onEdit, onCreate }) => {
  const [data, setData] = useState([]);
  const [loading, setLoading] = useState(false);
  const [historyVisible, setHistoryVisible] = useState(false);
  const [selectedWorkflow, setSelectedWorkflow] = useState(null);
  const [executionHistory, setExecutionHistory] = useState([]);
  const [historyLoading, setHistoryLoading] = useState(false);

  useEffect(() => {
    fetchWorkflows();
  }, []);

  const fetchWorkflows = async () => {
    setLoading(true);
    try {
      const response = await workflows.list();
      setData(response.data);
    } catch (error) {
      message.error('获取工作流列表失败');
    } finally {
      setLoading(false);
    }
  };

  const handleExecute = async (workflow) => {
    try {
      await workflows.execute(workflow.id);
      message.success('工作流执行已启动');
    } catch (error) {
      message.error('执行失败: ' + (error.response?.data?.detail || error.message));
    }
  };

  const handleDelete = async (workflowId) => {
    try {
      await workflows.delete(workflowId);
      message.success('删除成功');
      fetchWorkflows();
    } catch (error) {
      message.error('删除失败: ' + (error.response?.data?.detail || error.message));
    }
  };

  const handleViewHistory = async (workflow) => {
    setSelectedWorkflow(workflow);
    setHistoryLoading(true);
    try {
      const response = await workflows.listExecutions(workflow.id);
      setExecutionHistory(response.data);
      setHistoryVisible(true);
    } catch (error) {
      message.error('获取执行历史失败');
    } finally {
      setHistoryLoading(false);
    }
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

  const columns = [
    {
      title: 'ID',
      dataIndex: 'id',
      key: 'id',
      width: 80,
    },
    {
      title: '名称',
      dataIndex: 'name',
      key: 'name',
    },
    {
      title: '描述',
      dataIndex: 'description',
      key: 'description',
      ellipsis: true,
    },
    {
      title: '状态',
      dataIndex: 'is_enabled',
      key: 'is_enabled',
      render: (enabled) => (
        <Tag color={enabled ? 'success' : 'default'}>
          {enabled ? '启用' : '禁用'}
        </Tag>
      ),
    },
    {
      title: '调度',
      dataIndex: 'is_scheduled',
      key: 'is_scheduled',
      render: (scheduled, record) => (
        scheduled ? (
          <Tag color="blue">{record.cron_expression}</Tag>
        ) : (
          <Tag color="default">未调度</Tag>
        )
      ),
    },
    {
      title: '创建时间',
      dataIndex: 'created_at',
      key: 'created_at',
      render: (date) => date ? dayjs(date).format('YYYY-MM-DD HH:mm:ss') : '-',
    },
    {
      title: '操作',
      key: 'actions',
      width: 280,
      render: (_, record) => (
        <Space>
          <Button
            type="text"
            icon={<PlayCircleOutlined />}
            onClick={() => handleExecute(record)}
          >
            执行
          </Button>
          <Button
            type="text"
            icon={<EditOutlined />}
            onClick={() => onEdit(record.id)}
          >
            编辑
          </Button>
          <Button
            type="text"
            icon={<HistoryOutlined />}
            onClick={() => handleViewHistory(record)}
          >
            历史
          </Button>
          <Popconfirm
            title="确定要删除这个工作流吗？"
            onConfirm={() => handleDelete(record.id)}
            okText="确定"
            cancelText="取消"
          >
            <Button
              type="text"
              danger
              icon={<DeleteOutlined />}
            >
              删除
            </Button>
          </Popconfirm>
        </Space>
      ),
    },
  ];

  const historyColumns = [
    {
      title: 'ID',
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
      render: (date) => date ? dayjs(date).format('YYYY-MM-DD HH:mm:ss') : '-',
    },
    {
      title: '结束时间',
      dataIndex: 'end_time',
      key: 'end_time',
      render: (date) => date ? dayjs(date).format('YYYY-MM-DD HH:mm:ss') : '-',
    },
  ];

  return (
    <div style={{ padding: 24 }}>
      <Card
        title="工作流列表"
        extra={
          <Button
            type="primary"
            icon={<PlusOutlined />}
            onClick={onCreate}
          >
            创建工作流
          </Button>
        }
      >
        <Table
          columns={columns}
          dataSource={data}
          rowKey="id"
          loading={loading}
          pagination={{ pageSize: 10 }}
        />
      </Card>

      <Modal
        title={`执行历史 - ${selectedWorkflow?.name}`}
        open={historyVisible}
        onCancel={() => setHistoryVisible(false)}
        footer={[
          <Button key="close" onClick={() => setHistoryVisible(false)}>
            关闭
          </Button>,
        ]}
        width={800}
      >
        <Table
          columns={historyColumns}
          dataSource={executionHistory}
          rowKey="id"
          loading={historyLoading}
          pagination={{ pageSize: 10 }}
        />
      </Modal>
    </div>
  );
};

export default WorkflowList;
