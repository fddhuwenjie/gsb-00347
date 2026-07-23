import React, { useState, useEffect } from 'react';
import { Tabs, Card, Statistic, Row, Col, Progress, Select, Table, Empty, Spin, message, Space } from 'antd';
import { ArrowUpOutlined, ArrowDownOutlined, MinusOutlined } from '@ant-design/icons';
import { performance, executions } from '../services/api';

const { Option } = Select;

const PerformanceTab = ({ executionId }) => {
  const [loading, setLoading] = useState(false);
  const [metrics, setMetrics] = useState(null);
  const [compareLoading, setCompareLoading] = useState(false);
  const [compareExecutionId, setCompareExecutionId] = useState(null);
  const [compareData, setCompareData] = useState(null);
  const [executionList, setExecutionList] = useState([]);

  useEffect(() => {
    if (executionId) {
      fetchMetrics();
    }
  }, [executionId]);

  useEffect(() => {
    fetchExecutionList();
  }, []);

  const fetchMetrics = async () => {
    setLoading(true);
    try {
      const response = await performance.listMetrics(executionId);
      setMetrics(response.data);
    } catch (error) {
      message.error('获取性能指标失败');
    } finally {
      setLoading(false);
    }
  };

  const fetchExecutionList = async () => {
    try {
      const response = await executions.list();
      setExecutionList(response.data || []);
    } catch (error) {
      message.error('获取执行列表失败');
    }
  };

  const fetchCompareData = async (execId2) => {
    if (!execId2 || execId2 === executionId) {
      setCompareData(null);
      return;
    }
    setCompareLoading(true);
    try {
      const response = await performance.compare(executionId, execId2);
      setCompareData(response.data);
    } catch (error) {
      message.error('获取对比数据失败');
    } finally {
      setCompareLoading(false);
    }
  };

  const handleCompareChange = (value) => {
    setCompareExecutionId(value);
    fetchCompareData(value);
  };

  const overviewTab = (
    <Spin spinning={loading}>
      {metrics && metrics.node_metrics && metrics.node_metrics.length > 0 ? (
        <Space direction="vertical" size="large" style={{ width: '100%' }}>
          <Row gutter={16}>
            <Col span={6}>
              <Card>
                <Statistic
                  title="总耗时"
                  value={metrics.total_duration || 0}
                  suffix="ms"
                  precision={2}
                />
              </Card>
            </Col>
            <Col span={6}>
              <Card>
                <Statistic
                  title="总处理行数"
                  value={metrics.total_rows || 0}
                />
              </Card>
            </Col>
            <Col span={6}>
              <Card>
                <Statistic
                  title="平均吞吐量"
                  value={metrics.avg_throughput || 0}
                  suffix="行/秒"
                  precision={2}
                />
              </Card>
            </Col>
            <Col span={6}>
              <Card>
                <Statistic
                  title="内存峰值"
                  value={metrics.peak_memory || 0}
                  suffix="MB"
                  precision={2}
                />
              </Card>
            </Col>
          </Row>

          <Card title="瀑布图 - 节点处理耗时" size="small">
            <WaterfallChart data={metrics.node_metrics} />
          </Card>

          <Card title="数据吞吐量" size="small">
            <ThroughputChart data={metrics.node_metrics} />
          </Card>

          <Card title="内存峰值估算" size="small">
            <MemoryChart data={metrics.node_metrics} />
          </Card>

          <Card title="节点详情" size="small">
            <NodeMetricsTable data={metrics.node_metrics} />
          </Card>
        </Space>
      ) : (
        <Empty description="暂无性能数据" />
      )}
    </Spin>
  );

  const compareTab = (
    <Space direction="vertical" size="large" style={{ width: '100%' }}>
      <Card size="small">
        <Space align="center">
          <span>选择对比执行ID：</span>
          <Select
            placeholder="请选择要对比的执行"
            style={{ width: 300 }}
            value={compareExecutionId}
            onChange={handleCompareChange}
            showSearch
            optionFilterProp="children"
          >
            {executionList
              .filter((item) => item.id !== executionId)
              .map((item) => (
                <Option key={item.id} value={item.id}>
                  #{item.id} - {item.status === 'completed' ? '已完成' : item.status}
                </Option>
              ))}
          </Select>
        </Space>
      </Card>

      <Spin spinning={compareLoading}>
        {compareData ? (
          <CompareView data={compareData} execId1={executionId} execId2={compareExecutionId} />
        ) : (
          <Empty description="请选择要对比的执行ID" />
        )}
      </Spin>
    </Space>
  );

  return (
    <div style={{ padding: 16 }}>
      <Tabs defaultActiveKey="overview" items={[
        { key: 'overview', label: '概览', children: overviewTab },
        { key: 'compare', label: '对比', children: compareTab },
      ]} />
    </div>
  );
};

const WaterfallChart = ({ data }) => {
  if (!data || data.length === 0) return <Empty description="暂无数据" />;

  const maxDuration = Math.max(...data.map((item) => item.duration || 0), 1);
  const totalDuration = data.reduce((sum, item) => sum + (item.duration || 0), 0);

  return (
    <div style={{ padding: '16px 0' }}>
      <div style={{ display: 'flex', alignItems: 'center', marginBottom: 8, paddingLeft: 120 }}>
        <div style={{ flex: 1, display: 'flex', justifyContent: 'space-between', fontSize: 12, color: '#666' }}>
          <span>0 ms</span>
          <span>{Math.round(maxDuration / 2)} ms</span>
          <span>{Math.round(maxDuration)} ms</span>
        </div>
      </div>
      {data.map((item, index) => {
        const widthPercent = ((item.duration || 0) / maxDuration) * 100;
        const percentOfTotal = totalDuration > 0 ? ((item.duration || 0) / totalDuration) * 100 : 0;
        return (
          <div key={item.node_id || index} style={{ display: 'flex', alignItems: 'center', marginBottom: 8 }}>
            <div style={{ width: 120, textAlign: 'right', paddingRight: 12, fontSize: 13, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
              {item.node_name || item.node_id}
            </div>
            <div style={{ flex: 1, display: 'flex', alignItems: 'center' }}>
              <div
                style={{
                  height: 24,
                  width: `${widthPercent}%`,
                  minWidth: widthPercent > 0 ? 4 : 0,
                  background: `linear-gradient(90deg, #1890ff 0%, #69c0ff 100%)`,
                  borderRadius: 4,
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'flex-end',
                  paddingRight: 8,
                  color: '#fff',
                  fontSize: 12,
                  fontWeight: 500,
                  transition: 'width 0.3s ease',
                }}
                title={`${item.duration || 0} ms (${percentOfTotal.toFixed(1)}%)`}
              >
                {widthPercent > 10 && `${item.duration || 0} ms`}
              </div>
              {widthPercent <= 10 && (
                <span style={{ marginLeft: 8, fontSize: 12, color: '#666' }}>
                  {item.duration || 0} ms
                </span>
              )}
            </div>
            <div style={{ width: 80, textAlign: 'right', fontSize: 12, color: '#888', paddingLeft: 8 }}>
              {percentOfTotal.toFixed(1)}%
            </div>
          </div>
        );
      })}
    </div>
  );
};

const ThroughputChart = ({ data }) => {
  if (!data || data.length === 0) return <Empty description="暂无数据" />;

  const maxThroughput = Math.max(...data.map((item) => item.throughput || 0), 1);
  const chartHeight = 200;
  const padding = { top: 20, right: 20, bottom: 40, left: 60 };
  const width = 100;

  const points = data.map((item, index) => {
    const x = padding.left + (index * (width - padding.left - padding.right)) / (data.length - 1 || 1);
    const y = padding.top + chartHeight - ((item.throughput || 0) / maxThroughput) * (chartHeight - padding.top - padding.bottom);
    return { x, y, value: item.throughput || 0, nodeName: item.node_name || item.node_id };
  });

  const pathD = points.map((p, i) => `${i === 0 ? 'M' : 'L'} ${p.x} ${p.y}`).join(' ');
  const areaD = `${pathD} L ${points[points.length - 1].x} ${padding.top + chartHeight - padding.bottom} L ${points[0].x} ${padding.top + chartHeight - padding.bottom} Z`;

  return (
    <div style={{ padding: '16px 0', overflowX: 'auto' }}>
      <div style={{ minWidth: Math.max(data.length * 80 + 100, 400), position: 'relative', height: chartHeight + 60 }}>
        <svg width="100%" height={chartHeight + 60} style={{ overflow: 'visible' }}>
          {[0, 0.25, 0.5, 0.75, 1].map((ratio, i) => (
            <g key={i}>
              <line
                x1={padding.left}
                y1={padding.top + (chartHeight - padding.top - padding.bottom) * (1 - ratio)}
                x2={width - padding.right}
                y2={padding.top + (chartHeight - padding.top - padding.bottom) * (1 - ratio)}
                stroke="#f0f0f0"
                strokeWidth="1"
              />
              <text
                x={padding.left - 8}
                y={padding.top + (chartHeight - padding.top - padding.bottom) * (1 - ratio) + 4}
                fontSize="11"
                fill="#999"
                textAnchor="end"
              >
                {Math.round(maxThroughput * ratio)}
              </text>
            </g>
          ))}

          <defs>
            <linearGradient id="throughputGradient" x1="0%" y1="0%" x2="0%" y2="100%">
              <stop offset="0%" stopColor="#52c41a" stopOpacity="0.3" />
              <stop offset="100%" stopColor="#52c41a" stopOpacity="0.05" />
            </linearGradient>
          </defs>

          <path d={areaD} fill="url(#throughputGradient)" />

          <path d={pathD} fill="none" stroke="#52c41a" strokeWidth="2" />

          {points.map((p, i) => (
            <g key={i}>
              <circle cx={p.x} cy={p.y} r="4" fill="#52c41a" stroke="#fff" strokeWidth="2" />
              <text
                x={p.x}
                y={p.y - 10}
                fontSize="10"
                fill="#52c41a"
                textAnchor="middle"
                fontWeight="500"
              >
                {Math.round(p.value)}
              </text>
              <text
                x={p.x}
                y={chartHeight + 35}
                fontSize="10"
                fill="#666"
                textAnchor="middle"
              >
                {p.nodeName}
              </text>
            </g>
          ))}

          <text
            x={padding.left - 40}
            y={chartHeight / 2}
            fontSize="11"
            fill="#999"
            textAnchor="middle"
            transform={`rotate(-90, ${padding.left - 40}, ${chartHeight / 2})`}
          >
            行/秒
          </text>
        </svg>
      </div>
    </div>
  );
};

const MemoryChart = ({ data }) => {
  if (!data || data.length === 0) return <Empty description="暂无数据" />;

  const maxMemory = Math.max(...data.map((item) => item.peak_memory || 0), 1);
  const chartHeight = 200;
  const barWidth = Math.max(30, Math.min(60, 300 / data.length));
  const padding = { top: 20, right: 20, bottom: 40, left: 60 };

  return (
    <div style={{ padding: '16px 0', overflowX: 'auto' }}>
      <div style={{ minWidth: Math.max(data.length * (barWidth + 20) + 100, 400), position: 'relative', height: chartHeight + 60 }}>
        <svg width="100%" height={chartHeight + 60} style={{ overflow: 'visible' }}>
          {[0, 0.25, 0.5, 0.75, 1].map((ratio, i) => (
            <g key={i}>
              <line
                x1={padding.left}
                y1={padding.top + (chartHeight - padding.top - padding.bottom) * (1 - ratio)}
                x2={data.length * (barWidth + 20) + padding.left}
                y2={padding.top + (chartHeight - padding.top - padding.bottom) * (1 - ratio)}
                stroke="#f0f0f0"
                strokeWidth="1"
              />
              <text
                x={padding.left - 8}
                y={padding.top + (chartHeight - padding.top - padding.bottom) * (1 - ratio) + 4}
                fontSize="11"
                fill="#999"
                textAnchor="end"
              >
                {Math.round(maxMemory * ratio)}
              </text>
            </g>
          ))}

          {data.map((item, index) => {
            const memory = item.peak_memory || 0;
            const barHeight = (memory / maxMemory) * (chartHeight - padding.top - padding.bottom);
            const x = padding.left + index * (barWidth + 20);
            const y = padding.top + (chartHeight - padding.top - padding.bottom) - barHeight;
            return (
              <g key={item.node_id || index}>
                <rect
                  x={x}
                  y={y}
                  width={barWidth}
                  height={barHeight}
                  fill={`hsl(${280 - (memory / maxMemory) * 60}, 70%, 60%)`}
                  rx="4"
                  ry="4"
                />
                <text
                  x={x + barWidth / 2}
                  y={y - 6}
                  fontSize="10"
                  fill="#722ed1"
                  textAnchor="middle"
                  fontWeight="500"
                >
                  {memory.toFixed(1)}
                </text>
                <text
                  x={x + barWidth / 2}
                  y={chartHeight + 35}
                  fontSize="10"
                  fill="#666"
                  textAnchor="middle"
                >
                  {item.node_name || item.node_id}
                </text>
              </g>
            );
          })}

          <text
            x={padding.left - 40}
            y={chartHeight / 2}
            fontSize="11"
            fill="#999"
            textAnchor="middle"
            transform={`rotate(-90, ${padding.left - 40}, ${chartHeight / 2})`}
          >
            MB
          </text>
        </svg>
      </div>
    </div>
  );
};

const NodeMetricsTable = ({ data }) => {
  const columns = [
    {
      title: '节点名称',
      dataIndex: 'node_name',
      key: 'node_name',
      render: (text, record) => text || record.node_id,
    },
    {
      title: '处理耗时',
      dataIndex: 'duration',
      key: 'duration',
      render: (val) => `${val || 0} ms`,
      sorter: (a, b) => (a.duration || 0) - (b.duration || 0),
    },
    {
      title: '吞吐量',
      dataIndex: 'throughput',
      key: 'throughput',
      render: (val) => `${(val || 0).toFixed(2)} 行/秒`,
      sorter: (a, b) => (a.throughput || 0) - (b.throughput || 0),
    },
    {
      title: '内存峰值',
      dataIndex: 'peak_memory',
      key: 'peak_memory',
      render: (val) => `${(val || 0).toFixed(2)} MB`,
      sorter: (a, b) => (a.peak_memory || 0) - (b.peak_memory || 0),
    },
    {
      title: '输入行数',
      dataIndex: 'input_rows',
      key: 'input_rows',
      render: (val) => val || 0,
    },
    {
      title: '输出行数',
      dataIndex: 'output_rows',
      key: 'output_rows',
      render: (val) => val || 0,
    },
  ];

  return (
    <Table
      columns={columns}
      dataSource={data}
      rowKey={(record) => record.node_id || Math.random()}
      pagination={false}
      size="small"
    />
  );
};

const CompareView = ({ data, execId1, execId2 }) => {
  if (!data) return null;

  const renderDiff = (value1, value2, unit = '') => {
    const diff = value2 - value1;
    const percent = value1 > 0 ? ((diff / value1) * 100).toFixed(1) : 0;

    let icon, color;
    if (diff > 0) {
      icon = <ArrowUpOutlined />;
      color = '#ff4d4f';
    } else if (diff < 0) {
      icon = <ArrowDownOutlined />;
      color = '#52c41a';
    } else {
      icon = <MinusOutlined />;
      color = '#8c8c8c';
    }

    return (
      <span style={{ color }}>
        {icon} {diff > 0 ? '+' : ''}{diff.toFixed(2)}{unit} ({percent > 0 ? '+' : ''}{percent}%)
      </span>
    );
  };

  const summaryColumns = [
    {
      title: '指标',
      dataIndex: 'metric',
      key: 'metric',
      width: 150,
      render: (text) => <strong>{text}</strong>,
    },
    {
      title: `执行 #${execId1}`,
      dataIndex: 'value1',
      key: 'value1',
      render: (val, record) => `${val}${record.unit || ''}`,
    },
    {
      title: `执行 #${execId2}`,
      dataIndex: 'value2',
      key: 'value2',
      render: (val, record) => `${val}${record.unit || ''}`,
    },
    {
      title: '差异',
      dataIndex: 'diff',
      key: 'diff',
      render: (_, record) => renderDiff(record.value1, record.value2, record.unit),
    },
  ];

  const summaryData = [
    {
      metric: '总耗时',
      value1: data.exec1_total_duration || 0,
      value2: data.exec2_total_duration || 0,
      unit: ' ms',
    },
    {
      metric: '总处理行数',
      value1: data.exec1_total_rows || 0,
      value2: data.exec2_total_rows || 0,
      unit: '',
    },
    {
      metric: '平均吞吐量',
      value1: (data.exec1_avg_throughput || 0).toFixed(2),
      value2: (data.exec2_avg_throughput || 0).toFixed(2),
      unit: ' 行/秒',
    },
    {
      metric: '内存峰值',
      value1: (data.exec1_peak_memory || 0).toFixed(2),
      value2: (data.exec2_peak_memory || 0).toFixed(2),
      unit: ' MB',
    },
  ];

  const nodeColumns = [
    {
      title: '节点',
      dataIndex: 'node_name',
      key: 'node_name',
      width: 150,
      render: (text, record) => text || record.node_id,
    },
    {
      title: `#${execId1} 耗时`,
      dataIndex: 'exec1_duration',
      key: 'exec1_duration',
      render: (val) => `${val || 0} ms`,
    },
    {
      title: `#${execId2} 耗时`,
      dataIndex: 'exec2_duration',
      key: 'exec2_duration',
      render: (val) => `${val || 0} ms`,
    },
    {
      title: '耗时差异',
      key: 'duration_diff',
      render: (_, record) => renderDiff(record.exec1_duration || 0, record.exec2_duration || 0, ' ms'),
    },
    {
      title: `#${execId1} 吞吐量`,
      dataIndex: 'exec1_throughput',
      key: 'exec1_throughput',
      render: (val) => `${(val || 0).toFixed(2)} 行/秒`,
    },
    {
      title: `#${execId2} 吞吐量`,
      dataIndex: 'exec2_throughput',
      key: 'exec2_throughput',
      render: (val) => `${(val || 0).toFixed(2)} 行/秒`,
    },
    {
      title: '吞吐量差异',
      key: 'throughput_diff',
      render: (_, record) => renderDiff(record.exec1_throughput || 0, record.exec2_throughput || 0, ' 行/秒'),
    },
    {
      title: `#${execId1} 输出行数`,
      dataIndex: 'exec1_output_rows',
      key: 'exec1_output_rows',
      render: (val) => val || 0,
    },
    {
      title: `#${execId2} 输出行数`,
      dataIndex: 'exec2_output_rows',
      key: 'exec2_output_rows',
      render: (val) => val || 0,
    },
  ];

  return (
    <Space direction="vertical" size="large" style={{ width: '100%' }}>
      <Row gutter={16}>
        <Col span={12}>
          <Card title={`执行 #${execId1}`} size="small">
            <Space direction="vertical">
              <Progress
                percent={data.exec1_success_rate || 0}
                format={(percent) => `成功率 ${percent}%`}
              />
              <Statistic
                title="总耗时"
                value={data.exec1_total_duration || 0}
                suffix="ms"
                precision={2}
              />
              <Statistic
                title="吞吐量"
                value={data.exec1_avg_throughput || 0}
                suffix="行/秒"
                precision={2}
              />
            </Space>
          </Card>
        </Col>
        <Col span={12}>
          <Card title={`执行 #${execId2}`} size="small">
            <Space direction="vertical">
              <Progress
                percent={data.exec2_success_rate || 0}
                format={(percent) => `成功率 ${percent}%`}
              />
              <Statistic
                title="总耗时"
                value={data.exec2_total_duration || 0}
                suffix="ms"
                precision={2}
              />
              <Statistic
                title="吞吐量"
                value={data.exec2_avg_throughput || 0}
                suffix="行/秒"
                precision={2}
              />
            </Space>
          </Card>
        </Col>
      </Row>

      <Card title="总体对比" size="small">
        <Table
          columns={summaryColumns}
          dataSource={summaryData}
          rowKey="metric"
          pagination={false}
          size="small"
        />
      </Card>

      <Card title="节点级对比" size="small">
        <Table
          columns={nodeColumns}
          dataSource={data.node_comparisons || []}
          rowKey={(record) => record.node_id || Math.random()}
          pagination={{ pageSize: 10 }}
          size="small"
          scroll={{ x: 1000 }}
        />
      </Card>
    </Space>
  );
};

export default PerformanceTab;
