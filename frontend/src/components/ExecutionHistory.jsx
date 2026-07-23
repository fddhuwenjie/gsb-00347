import React, { useState, useEffect } from 'react';
import { Table, Button, Space, Tag, Card, Descriptions, Modal, message, Progress, Tabs, Statistic, Row, Col, List, Empty } from 'antd';
import {
  ArrowLeftOutlined,
  ReloadOutlined,
  InfoCircleOutlined,
  CheckCircleOutlined,
  WarningOutlined,
} from '@ant-design/icons';
import { executions } from '../services/api';
import dayjs from 'dayjs';
import PerformanceTab from './PerformanceTab';

const ExecutionHistory = ({ pipelineId, onBack }) => {
  const [data, setData] = useState([]);
  const [loading, setLoading] = useState(false);
  const [detailModalVisible, setDetailModalVisible] = useState(false);
  const [selectedExecution, setSelectedExecution] = useState(null);
  const [qualityReportsData, setQualityReportsData] = useState([]);
  const [qualityLoading, setQualityLoading] = useState(false);

  useEffect(() => {
    fetchExecutions();
  }, [pipelineId]);

  const fetchExecutions = async () => {
    setLoading(true);
    try {
      const response = await executions.list(pipelineId);
      setData(response.data);
    } catch (error) {
      message.error('获取执行历史失败');
    } finally {
      setLoading(false);
    }
  };

  const handleResume = async (execution) => {
    try {
      await executions.execute(pipelineId, true, execution.id);
      message.success('断点续跑已开始');
      fetchExecutions();
    } catch (error) {
      message.error('启动失败');
    }
  };

  const handleViewDetail = (execution) => {
    setSelectedExecution(execution);
    setDetailModalVisible(true);
  };

  const getStatusColor = (status) => {
    const colorMap = {
      pending: 'default',
      running: 'processing',
      completed: 'success',
      failed: 'error',
    };
    return colorMap[status] || 'default';
  };

  const getStatusText = (status) => {
    const textMap = {
      pending: '等待中',
      running: '运行中',
      completed: '已完成',
      failed: '失败',
    };
    return textMap[status] || status;
  };

  const columns = [
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
      render: (status) => <Tag color={getStatusColor(status)}>{getStatusText(status)}</Tag>,
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
      title: '处理行数',
      key: 'rows',
      render: (_, record) => `${record.success_rows || 0} / ${record.total_rows || 0}`,
    },
    {
      title: '成功率',
      key: 'success_rate',
      render: (_, record) => {
        const rate = record.total_rows > 0 
          ? Math.round((record.success_rows / record.total_rows) * 100) 
          : 0;
        return (
          <Progress 
            percent={rate} 
            size="small" 
            status={record.status === 'failed' ? 'exception' : 'normal'}
          />
        );
      },
    },
    {
      title: '操作',
      key: 'actions',
      render: (_, record) => (
        <Space>
          <Button
            type="text"
            icon={<InfoCircleOutlined />}
            onClick={() => handleViewDetail(record)}
          >
            详情
          </Button>
          {record.status === 'failed' && (
            <Button
              type="text"
              icon={<ReloadOutlined />}
              onClick={() => handleResume(record)}
            >
              断点续跑
            </Button>
          )}
        </Space>
      ),
    },
  ];

  return (
    <div style={{ padding: 24 }}>
      <div style={{ marginBottom: 16, display: 'flex', alignItems: 'center', gap: 16 }}>
        <Button icon={<ArrowLeftOutlined />} onClick={onBack}>
          返回
        </Button>
        <h2>执行历史</h2>
      </div>

      <Table
        columns={columns}
        dataSource={data}
        rowKey="id"
        loading={loading}
        pagination={{ pageSize: 10 }}
      />

      <ExecutionDetailModal
        visible={detailModalVisible}
        execution={selectedExecution}
        onCancel={() => setDetailModalVisible(false)}
      />
    </div>
  );
};

const ExecutionDetailModal = ({ visible, execution, onCancel }) => {
  const [qualityReports, setQualityReports] = useState([]);
  const [qualityLoading, setQualityLoading] = useState(false);

  useEffect(() => {
    if (visible && execution) {
      fetchQualityReports();
    }
  }, [visible, execution]);

  const fetchQualityReports = async () => {
    if (!execution) return;
    setQualityLoading(true);
    try {
      const response = await qualityReports.list(execution.id);
      setQualityReports(response.data);
    } catch (error) {
      console.error('获取质量报告失败', error);
    } finally {
      setQualityLoading(false);
    }
  };

  if (!execution) return null;

  const nodeStates = execution.node_states || {};

  const getRuleTypeName = (type) => {
    const names = {
      'non_null_rate': '非空率检查',
      'uniqueness': '唯一性检查',
      'range_check': '范围校验',
      'regex_match': '正则匹配',
    };
    return names[type] || type;
  };

  const tabItems = [
    {
      key: 'nodes',
      label: '节点状态',
      children: (
        <Card size="small" style={{ marginTop: 16 }}>
          <Space direction="vertical" style={{ width: '100%' }}>
            {Object.entries(nodeStates).map(([nodeId, state]) => (
              <div
                key={nodeId}
                style={{
                  padding: '8px 12px',
                  border: '1px solid #e8e8e8',
                  borderRadius: 4,
                  display: 'flex',
                  justifyContent: 'space-between',
                  alignItems: 'center',
                }}
              >
                <Space>
                  <span className={`node-status status-${state.status || 'pending'}`} />
                  <span style={{ fontWeight: 500 }}>{nodeId}</span>
                </Space>
                <Space>
                  <span style={{ color: '#666', fontSize: 12 }}>
                    输入: {state.input_rows || 0}
                  </span>
                  <span style={{ color: '#666', fontSize: 12 }}>
                    输出: {state.output_rows || 0}
                  </span>
                  {state.error && (
                    <Tag color="red" title={state.error}>
                      错误
                    </Tag>
                  )}
                </Space>
              </div>
            ))}
          </Space>
        </Card>
      ),
    },
    {
      key: 'quality',
      label: '数据质量报告',
      children: (
        <Card size="small" style={{ marginTop: 16 }} loading={qualityLoading}>
          {qualityReports.length === 0 ? (
            <Empty description="暂无质量报告" />
          ) : (
            <Space direction="vertical" style={{ width: '100%' }}>
              {qualityReports.map((report) => (
                <Card
                  key={report.id}
                  size="small"
                  type="inner"
                  title={
                    <Space>
                      <Tag color={report.pass_rate >= 100 ? 'green' : report.pass_rate >= 80 ? 'orange' : 'red'}>
                        {getRuleTypeName(report.rule_type)}
                      </Tag>
                      <span style={{ color: '#666', fontSize: 12 }}>节点: {report.node_id}</span>
                    </Space>
                  }
                  extra={
                    <Space>
                      <Progress
                        type="circle"
                        size="small"
                        percent={Math.round(report.pass_rate)}
                        status={report.pass_rate >= 100 ? 'success' : report.pass_rate >= 80 ? 'normal' : 'exception'}
                      />
                    </Space>
                  }
                >
                  <Row gutter={16}>
                    <Col span={6}>
                      <Statistic title="总记录" value={report.total_records} />
                    </Col>
                    <Col span={6}>
                      <Statistic title="通过" value={report.passed_records} valueStyle={{ color: '#52c41a' }} prefix={<CheckCircleOutlined />} />
                    </Col>
                    <Col span={6}>
                      <Statistic title="失败" value={report.failed_records} valueStyle={{ color: '#ff4d4f' }} prefix={<WarningOutlined />} />
                    </Col>
                    <Col span={6}>
                      <Statistic title="通过率" value={report.pass_rate.toFixed(2)} suffix="%" />
                    </Col>
                  </Row>
                  {report.violation_samples && report.violation_samples.length > 0 && (
                    <Card title="违规样本" size="small" type="inner" style={{ marginTop: 8 }}>
                      <List
                        size="small"
                        dataSource={report.violation_samples.slice(0, 5)}
                        renderItem={(item, idx) => (
                          <List.Item>
                            <Space>
                              <Tag color="red">#{idx + 1}</Tag>
                              <code style={{ fontSize: 11 }}>{JSON.stringify(item).substring(0, 100)}...</code>
                            </Space>
                          </List.Item>
                        )}
                      />
                    </Card>
                  )}
                </Card>
              ))}
            </Space>
          )}
        </Card>
      ),
    },
    {
      key: 'performance',
      label: '性能分析',
      children: <PerformanceTab executionId={execution.id} />,
    },
  ];

  return (
    <Modal
      title={`执行详情 #${execution.id}`}
      open={visible}
      onCancel={onCancel}
      footer={[
        <Button key="close" onClick={onCancel}>
          关闭
        </Button>,
      ]}
      width={900}
    >
      <Descriptions column={2} bordered size="small" style={{ marginBottom: 16 }}>
        <Descriptions.Item label="状态">
          <Tag color={execution.status === 'completed' ? 'success' : execution.status === 'failed' ? 'error' : 'processing'}>
            {execution.status === 'completed' ? '已完成' : execution.status === 'failed' ? '失败' : execution.status}
          </Tag>
        </Descriptions.Item>
        <Descriptions.Item label="处理行数">
          {execution.success_rows || 0} / {execution.total_rows || 0}
        </Descriptions.Item>
        <Descriptions.Item label="开始时间">
          {execution.start_time ? dayjs(execution.start_time).format('YYYY-MM-DD HH:mm:ss') : '-'}
        </Descriptions.Item>
        <Descriptions.Item label="结束时间">
          {execution.end_time ? dayjs(execution.end_time).format('YYYY-MM-DD HH:mm:ss') : '-'}
        </Descriptions.Item>
      </Descriptions>

      <Tabs defaultActiveKey="nodes" items={tabItems} />

      {execution.error_log && (
        <Card title="错误日志" size="small" type="inner" style={{ marginTop: 16 }}>
          <pre style={{ whiteSpace: 'pre-wrap', color: '#ff4d4f', margin: 0 }}>
            {execution.error_log}
          </pre>
        </Card>
      )}
    </Modal>
  );
};

export default ExecutionHistory;
