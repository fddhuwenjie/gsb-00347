import React, { useState, useEffect, useMemo, useRef } from 'react';
import { Card, Input, Button, Modal, Descriptions, Tag, Empty, Space, Typography, Spin, message } from 'antd';
import { SearchOutlined, ReloadOutlined, ArrowRightOutlined } from '@ant-design/icons';
import { lineage } from '../services/api';

const { Title, Text } = Typography;

const NODE_COLORS = {
  source: '#52c41a',
  transform: '#1890ff',
  target: '#fa8c16',
};

const NODE_LABELS = {
  source: '数据源字段',
  transform: '变换操作',
  target: '目标字段',
};

const TRANSFORM_LABELS = {
  'filter': '过滤',
  'map/rename': '字段重命名',
  'map/type_convert': '类型转换',
  'map/compute': '计算字段',
  'aggregate': '聚合',
  'join': 'JOIN',
  'deduplicate': '去重',
};

const LineageGraph = ({ pipelineId, executionId }) => {
  const [graphData, setGraphData] = useState({ nodes: [], links: [] });
  const [loading, setLoading] = useState(false);
  const [selectedNode, setSelectedNode] = useState(null);
  const [modalVisible, setModalVisible] = useState(false);
  const [highlightedNodes, setHighlightedNodes] = useState(new Set());
  const [highlightedLinks, setHighlightedLinks] = useState(new Set());
  const [reverseTable, setReverseTable] = useState('');
  const [reverseColumn, setReverseColumn] = useState('');
  const [reverseLoading, setReverseLoading] = useState(false);
  const [reverseResult, setReverseResult] = useState(null);
  const [nodePositions, setNodePositions] = useState({});
  const graphRef = useRef(null);

  useEffect(() => {
    if (pipelineId || executionId) {
      loadGraph();
    }
  }, [pipelineId, executionId]);

  const loadGraph = async () => {
    setLoading(true);
    try {
      const response = await lineage.getGraph(pipelineId, executionId);
      setGraphData(response.data);
      setReverseResult(null);
    } catch (error) {
      message.error('加载血缘图失败: ' + (error.response?.data?.detail || error.message));
    } finally {
      setLoading(false);
    }
  };

  const { sourceNodes, transformNodes, targetNodes } = useMemo(() => {
    const sources = [];
    const transforms = [];
    const targets = [];

    graphData.nodes.forEach((node) => {
      if (node.type === 'source') sources.push(node);
      else if (node.type === 'transform') transforms.push(node);
      else if (node.type === 'target') targets.push(node);
    });

    return { sourceNodes: sources, transformNodes: transforms, targetNodes: targets };
  }, [graphData]);

  useEffect(() => {
    const positions = {};
    const nodeHeight = 60;
    const gap = 20;

    sourceNodes.forEach((node, index) => {
      positions[node.id] = {
        x: 50,
        y: index * (nodeHeight + gap) + 40,
      };
    });

    transformNodes.forEach((node, index) => {
      positions[node.id] = {
        x: 400,
        y: index * (nodeHeight + gap) + 40,
      };
    });

    targetNodes.forEach((node, index) => {
      positions[node.id] = {
        x: 750,
        y: index * (nodeHeight + gap) + 40,
      };
    });

    setNodePositions(positions);
  }, [sourceNodes, transformNodes, targetNodes]);

  const getConnectedNodes = (nodeId) => {
    const connected = new Set([nodeId]);
    const linkSet = new Set();

    const traverseForward = (id) => {
      graphData.links.forEach((link) => {
        if (link.source === id) {
          connected.add(link.target);
          linkSet.add(`${link.source}->${link.target}`);
          traverseForward(link.target);
        }
      });
    };

    const traverseBackward = (id) => {
      graphData.links.forEach((link) => {
        if (link.target === id) {
          connected.add(link.source);
          linkSet.add(`${link.source}->${link.target}`);
          traverseBackward(link.source);
        }
      });
    };

    traverseForward(nodeId);
    traverseBackward(nodeId);

    return { nodes: connected, links: linkSet };
  };

  const handleNodeClick = (node) => {
    const { nodes, links } = getConnectedNodes(node.id);
    setSelectedNode(node);
    setHighlightedNodes(nodes);
    setHighlightedLinks(links);
    setModalVisible(true);
  };

  const handleBackgroundClick = () => {
    setHighlightedNodes(new Set());
    setHighlightedLinks(new Set());
  };

  const getTransformPath = (node) => {
    const path = [];
    let currentId = node.id;

    const collectBackward = (id) => {
      const incomingLinks = graphData.links.filter((l) => l.target === id);
      incomingLinks.forEach((link) => {
        const sourceNode = graphData.nodes.find((n) => n.id === link.source);
        if (sourceNode) {
          if (link.transform) {
            path.unshift({
              node: sourceNode,
              transform: link.transform,
            });
          } else {
            path.unshift({ node: sourceNode, transform: null });
          }
          collectBackward(link.source);
        }
      });
    };

    const collectForward = (id) => {
      const outgoingLinks = graphData.links.filter((l) => l.source === id);
      outgoingLinks.forEach((link) => {
        const targetNode = graphData.nodes.find((n) => n.id === link.target);
        if (targetNode) {
          if (link.transform) {
            path.push({
              node: targetNode,
              transform: link.transform,
            });
          } else {
            path.push({ node: targetNode, transform: null });
          }
          collectForward(link.target);
        }
      });
    };

    collectBackward(currentId);
    path.push({ node, transform: null });
    collectForward(currentId);

    return path;
  };

  const handleReverseTrace = async () => {
    if (!reverseTable.trim() || !reverseColumn.trim()) {
      message.warning('请输入目标表和字段名');
      return;
    }

    setReverseLoading(true);
    try {
      const response = await lineage.reverse(reverseTable.trim(), reverseColumn.trim());
      setReverseResult(response.data);
      if (response.data.sources && response.data.sources.length === 0) {
        message.info('未找到数据来源');
      }
    } catch (error) {
      message.error('反向追踪失败: ' + (error.response?.data?.detail || error.message));
      setReverseResult(null);
    } finally {
      setReverseLoading(false);
    }
  };

  const generateLinkPath = (link) => {
    const sourcePos = nodePositions[link.source];
    const targetPos = nodePositions[link.target];

    if (!sourcePos || !targetPos) return '';

    const sourceX = sourcePos.x + 180;
    const sourceY = sourcePos.y + 30;
    const targetX = targetPos.x;
    const targetY = targetPos.y + 30;

    const midX = (sourceX + targetX) / 2;

    return `M ${sourceX} ${sourceY} C ${midX} ${sourceY}, ${midX} ${targetY}, ${targetX} ${targetY}`;
  };

  const graphWidth = 1000;
  const graphHeight = Math.max(
    sourceNodes.length * 80 + 80,
    transformNodes.length * 80 + 80,
    targetNodes.length * 80 + 80,
    400
  );

  const hasData = graphData.nodes.length > 0;

  return (
    <Card
      title="字段级血缘关系"
      extra={
        <Button icon={<ReloadOutlined />} onClick={loadGraph} loading={loading}>
          刷新
        </Button>
      }
      style={{ height: '100%', display: 'flex', flexDirection: 'column' }}
    >
      <Card size="small" style={{ marginBottom: 16 }} title="反向追踪">
        <Space.Compact style={{ width: '100%' }}>
          <Input
            placeholder="目标表名"
            value={reverseTable}
            onChange={(e) => setReverseTable(e.target.value)}
            style={{ width: '30%' }}
          />
          <Input
            placeholder="目标字段名"
            value={reverseColumn}
            onChange={(e) => setReverseColumn(e.target.value)}
            style={{ width: '30%' }}
          />
          <Button
            type="primary"
            icon={<SearchOutlined />}
            onClick={handleReverseTrace}
            loading={reverseLoading}
          >
            追踪来源
          </Button>
        </Space.Compact>

        {reverseResult && reverseResult.sources && reverseResult.sources.length > 0 && (
          <Card size="small" style={{ marginTop: 16, background: '#f6ffed' }}>
            <Title level={5} style={{ marginTop: 0 }}>
              数据来源路径: {reverseResult.target_table}.{reverseResult.target_column}
            </Title>
            {reverseResult.sources.map((record, idx) => (
              <div key={idx} style={{ marginBottom: 8, padding: 8, background: 'white', borderRadius: 4 }}>
                <Space direction="vertical" style={{ width: '100%' }}>
                  <Space>
                    <Tag color="green">源</Tag>
                    <Text strong>{record.source_table}.{record.source_column}</Text>
                    <ArrowRightOutlined />
                    {record.transform_path && record.transform_path.map((op, opIdx) => (
                      <React.Fragment key={opIdx}>
                        <Tag color="blue">{TRANSFORM_LABELS[op.type] || op.type}</Tag>
                        <ArrowRightOutlined />
                      </React.Fragment>
                    ))}
                    <Tag color="orange">目标</Tag>
                    <Text strong>{record.target_table}.{record.target_column}</Text>
                  </Space>
                </Space>
              </div>
            ))}
          </Card>
        )}
      </Card>

      <Card
        size="small"
        style={{ flex: 1, overflow: 'auto' }}
        bodyStyle={{ padding: 0 }}
        onClick={handleBackgroundClick}
      >
        {loading ? (
          <div style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: 300 }}>
            <Spin size="large" />
          </div>
        ) : !hasData ? (
          <div style={{ padding: 40 }}>
            <Empty description="暂无血缘数据" />
          </div>
        ) : (
          <div ref={graphRef} style={{ position: 'relative', width: graphWidth, height: graphHeight }}>
            <svg
              width={graphWidth}
              height={graphHeight}
              style={{ position: 'absolute', top: 0, left: 0, pointerEvents: 'none' }}
            >
              <defs>
                {graphData.links.map((link, idx) => {
                  const sourceNode = graphData.nodes.find((n) => n.id === link.source);
                  const isHighlighted = highlightedLinks.has(`${link.source}->${link.target}`);
                  const hasHighlight = highlightedNodes.size > 0;
                  const opacity = hasHighlight ? (isHighlighted ? 1 : 0.15) : 0.5;
                  const color = sourceNode ? NODE_COLORS[sourceNode.type] : '#ccc';

                  return (
                    <linearGradient key={idx} id={`gradient-${idx}`} x1="0%" y1="0%" x2="100%" y2="0%">
                      <stop offset="0%" stopColor={color} stopOpacity={opacity} />
                      <stop offset="100%" stopColor={NODE_COLORS.target} stopOpacity={opacity} />
                    </linearGradient>
                  );
                })}
              </defs>

              {graphData.links.map((link, idx) => {
                const isHighlighted = highlightedLinks.has(`${link.source}->${link.target}`);
                const hasHighlight = highlightedNodes.size > 0;
                const opacity = hasHighlight ? (isHighlighted ? 1 : 0.15) : 0.5;
                const strokeWidth = isHighlighted ? 3 : 2;
                const path = generateLinkPath(link);

                if (!path) return null;

                return (
                  <path
                    key={idx}
                    d={path}
                    fill="none"
                    stroke={`url(#gradient-${idx})`}
                    strokeWidth={strokeWidth}
                    strokeOpacity={opacity}
                    style={{ transition: 'all 0.3s ease' }}
                  />
                );
              })}
            </svg>

            <div style={{ position: 'absolute', left: 50, top: 10, width: 180, textAlign: 'center' }}>
              <Tag color={NODE_COLORS.source} style={{ fontSize: 14, padding: '4px 12px' }}>
                {NODE_LABELS.source}
              </Tag>
            </div>
            <div style={{ position: 'absolute', left: 400, top: 10, width: 180, textAlign: 'center' }}>
              <Tag color={NODE_COLORS.transform} style={{ fontSize: 14, padding: '4px 12px' }}>
                {NODE_LABELS.transform}
              </Tag>
            </div>
            <div style={{ position: 'absolute', left: 750, top: 10, width: 180, textAlign: 'center' }}>
              <Tag color={NODE_COLORS.target} style={{ fontSize: 14, padding: '4px 12px' }}>
                {NODE_LABELS.target}
              </Tag>
            </div>

            {[...sourceNodes, ...transformNodes, ...targetNodes].map((node) => {
              const pos = nodePositions[node.id];
              if (!pos) return null;

              const isHighlighted = highlightedNodes.has(node.id);
              const hasHighlight = highlightedNodes.size > 0;
              const opacity = hasHighlight ? (isHighlighted ? 1 : 0.3) : 1;

              return (
                <div
                  key={node.id}
                  onClick={(e) => {
                    e.stopPropagation();
                    handleNodeClick(node);
                  }}
                  style={{
                    position: 'absolute',
                    left: pos.x,
                    top: pos.y,
                    width: 180,
                    padding: '10px 12px',
                    background: 'white',
                    border: `2px solid ${NODE_COLORS[node.type]}`,
                    borderRadius: 6,
                    cursor: 'pointer',
                    opacity,
                    boxShadow: isHighlighted ? `0 0 12px ${NODE_COLORS[node.type]}` : '0 2px 8px rgba(0,0,0,0.1)',
                    transition: 'all 0.3s ease',
                    zIndex: isHighlighted ? 10 : 1,
                  }}
                >
                  <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginBottom: 4 }}>
                    <Tag color={NODE_COLORS[node.type]} style={{ margin: 0 }}>
                      {NODE_LABELS[node.type]}
                    </Tag>
                  </div>
                  <div
                    style={{
                      fontSize: 12,
                      color: '#333',
                      wordBreak: 'break-all',
                      fontWeight: isHighlighted ? 600 : 400,
                    }}
                    title={node.name}
                  >
                    {node.name}
                  </div>
                </div>
              );
            })}
          </div>
        )}
      </Card>

      <Modal
        title="节点详情"
        open={modalVisible}
        onCancel={() => {
          setModalVisible(false);
          setSelectedNode(null);
        }}
        footer={[
          <Button key="close" onClick={() => setModalVisible(false)}>
            关闭
          </Button>,
        ]}
        width={700}
      >
        {selectedNode && (
          <Space direction="vertical" style={{ width: '100%' }}>
            <Descriptions bordered size="small" column={2}>
              <Descriptions.Item label="节点ID">
                <Text code>{selectedNode.id}</Text>
              </Descriptions.Item>
              <Descriptions.Item label="节点类型">
                <Tag color={NODE_COLORS[selectedNode.type]}>
                  {NODE_LABELS[selectedNode.type]}
                </Tag>
              </Descriptions.Item>
              <Descriptions.Item label="名称" span={2}>
                {selectedNode.name}
              </Descriptions.Item>
            </Descriptions>

            <Card size="small" title="完整变换路径">
              <Space direction="vertical" style={{ width: '100%' }}>
                {getTransformPath(selectedNode).map((item, idx) => (
                  <div
                    key={idx}
                    style={{
                      display: 'flex',
                      alignItems: 'center',
                      gap: 8,
                      padding: '8px 12px',
                      background: item.node.id === selectedNode.id ? '#e6f7ff' : '#fafafa',
                      borderRadius: 4,
                    }}
                  >
                    <Tag color={NODE_COLORS[item.node.type]}>
                      {NODE_LABELS[item.node.type]}
                    </Tag>
                    <Text strong>{item.node.name}</Text>
                    {item.transform && (
                      <>
                        <ArrowRightOutlined style={{ color: '#1890ff' }} />
                        <Tag color="blue">
                          {TRANSFORM_LABELS[item.transform.type] || item.transform.type}
                        </Tag>
                        <Text type="secondary" style={{ fontSize: 12 }}>
                          {Object.entries(item.transform)
                            .filter(([k]) => k !== 'type')
                            .map(([k, v]) => `${k}: ${JSON.stringify(v)}`)
                            .join(' | ')}
                        </Text>
                      </>
                    )}
                  </div>
                ))}
              </Space>
            </Card>
          </Space>
        )}
      </Modal>
    </Card>
  );
};

export default LineageGraph;
