import React, { useState, useEffect } from 'react';
import { Card, Form, Input, Select, Button, Space, Divider, InputNumber, Switch, Collapse, Table, message, Modal } from 'antd';
import { PlusOutlined, DeleteOutlined, EyeOutlined, SafetyOutlined } from '@ant-design/icons';
import { dataSources } from '../services/api';
import DataQualityRulePanel from './DataQualityRulePanel';

const { TextArea } = Input;
const { Option } = Select;
const { Panel } = Collapse;

const NodeConfigPanel = ({ node, onUpdate, onClose, pipelineId }) => {
  const [form] = Form.useForm();
  const [previewVisible, setPreviewVisible] = useState(false);
  const [previewData, setPreviewData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [qualityPanelVisible, setQualityPanelVisible] = useState(false);

  const nodeType = node?.type;
  const config = node?.data?.config || {};

  useEffect(() => {
    if (node) {
      form.setFieldsValue({
        label: node.data?.label || '',
        ...config,
      });
    }
  }, [node, form]);

  const handleValuesChange = (changedValues, allValues) => {
    const { label, ...configValues } = allValues;
    onUpdate(node.id, {
      ...node,
      data: {
        ...node.data,
        label: label || node.data?.label,
        config: configValues,
      },
    });
  };

  const handlePreview = async () => {
    if (nodeType !== 'source') return;
    
    try {
      setLoading(true);
      const values = form.getFieldsValue();
      const sourceConfig = values.source_config || {};
      
      const response = await dataSources.preview(values.source_type, sourceConfig, 10);
      setPreviewData(response.data);
      setPreviewVisible(true);
    } catch (error) {
      message.error('预览失败: ' + (error.response?.data?.detail || error.message));
    } finally {
      setLoading(false);
    }
  };

  const renderSourceConfig = () => (
    <Collapse defaultActiveKey={['config']}>
      <Panel header="数据源配置" key="config">
        <Form.Item name="source_type" label="数据源类型">
          <Select>
            <Option value="csv">CSV文件</Option>
            <Option value="sqlite">SQLite数据库</Option>
            <Option value="http_api">HTTP API</Option>
          </Select>
        </Form.Item>

        <Form.Item shouldUpdate noStyle>
          {({ getFieldValue }) => {
            const sourceType = getFieldValue('source_type');
            if (sourceType === 'csv') {
              return (
                <>
                  <Form.Item name={['source_config', 'file_path']} label="文件路径">
                    <Input placeholder="/path/to/file.csv" />
                  </Form.Item>
                  <Form.Item name={['source_config', 'delimiter']} label="分隔符">
                    <Input defaultValue="," />
                  </Form.Item>
                  <Form.Item name={['source_config', 'encoding']} label="编码">
                    <Input defaultValue="utf-8" />
                  </Form.Item>
                  <Form.Item name={['source_config', 'has_header']} label="包含表头" valuePropName="checked">
                    <Switch defaultChecked />
                  </Form.Item>
                </>
              );
            }
            if (sourceType === 'sqlite') {
              return (
                <>
                  <Form.Item name={['source_config', 'db_path']} label="数据库路径">
                    <Input placeholder="/path/to/database.db" />
                  </Form.Item>
                  <Form.Item name={['source_config', 'table_name']} label="表名">
                    <Input placeholder="table_name" />
                  </Form.Item>
                  <Form.Item name={['source_config', 'query']} label="SQL查询(可选)">
                    <TextArea rows={3} placeholder="SELECT * FROM table WHERE ..." />
                  </Form.Item>
                </>
              );
            }
            if (sourceType === 'http_api') {
              return (
                <>
                  <Form.Item name={['source_config', 'url']} label="API URL">
                    <Input placeholder="https://api.example.com/data" />
                  </Form.Item>
                  <Form.Item name={['source_config', 'data_path']} label="数据路径">
                    <Input placeholder="data.items" />
                  </Form.Item>
                </>
              );
            }
            return null;
          }}
        </Form.Item>

        <Form.Item shouldUpdate noStyle>
          {({ getFieldValue }) => {
            const sourceType = getFieldValue('source_type');
            if (!sourceType) return null;
            return (
              <Button 
                type="dashed" 
                icon={<EyeOutlined />} 
                onClick={handlePreview}
                loading={loading}
              >
                预览数据
              </Button>
            );
          }}
        </Form.Item>
      </Panel>

      <Panel header="增量同步配置" key="incremental">
        <Form.Item name={['source_config', 'incremental', 'enabled']} label="启用增量同步" valuePropName="checked">
          <Switch />
        </Form.Item>

        <Form.Item shouldUpdate noStyle>
          {({ getFieldValue }) => {
            const enabled = getFieldValue(['source_config', 'incremental', 'enabled']);
            if (!enabled) return null;

            return (
              <>
                <Form.Item name={['source_config', 'incremental', 'mode']} label="增量模式">
                  <Select>
                    <Option value="timestamp">基于时间戳</Option>
                    <Option value="auto_increment_id">基于自增ID</Option>
                  </Select>
                </Form.Item>

                <Form.Item shouldUpdate noStyle>
                  {({ getFieldValue }) => {
                    const mode = getFieldValue(['source_config', 'incremental', 'mode']);
                    if (mode === 'timestamp') {
                      return (
                        <Form.Item name={['source_config', 'incremental', 'timestamp_field']} label="时间戳字段">
                          <Input placeholder="created_at / updated_at" />
                        </Form.Item>
                      );
                    }
                    if (mode === 'auto_increment_id') {
                      return (
                        <Form.Item name={['source_config', 'incremental', 'id_field']} label="自增ID字段">
                          <Input placeholder="id" />
                        </Form.Item>
                      );
                    }
                    return null;
                  }}
                </Form.Item>

                {getFieldValue('source_type') === 'sqlite' && (
                  <Form.Item name={['source_config', 'incremental', 'cdc_enabled']} label="启用CDC(变更捕获)" valuePropName="checked">
                    <Switch />
                  </Form.Item>
                )}
              </>
            );
          }}
        </Form.Item>
      </Panel>
    </Collapse>
  );

  const renderFilterConfig = () => (
    <Form.Item name="condition" label="过滤条件" rules={[{ required: true }]}>
      <TextArea 
        rows={3} 
        placeholder="例如: age > 18 AND status == 'active'" 
      />
    </Form.Item>
  );

  const renderMapConfig = () => (
    <Form.List name="operations">
      {(fields, { add, remove }) => (
        <>
          {fields.map(({ key, name, ...restField }) => (
            <Card key={key} size="small" style={{ marginBottom: 8 }}>
              <Space direction="vertical" style={{ width: '100%' }}>
                <Form.Item {...restField} name={[name, 'type']} label="操作类型">
                  <Select>
                    <Option value="rename">字段重命名</Option>
                    <Option value="type_convert">类型转换</Option>
                    <Option value="compute">计算字段</Option>
                  </Select>
                </Form.Item>

                <Form.Item shouldUpdate noStyle>
                  {({ getFieldValue }) => {
                    const opType = getFieldValue(['operations', name, 'type']);
                    if (opType === 'rename') {
                      return (
                        <Space>
                          <Form.Item {...restField} name={[name, 'old_name']} label="原字段名">
                            <Input />
                          </Form.Item>
                          <Form.Item {...restField} name={[name, 'new_name']} label="新字段名">
                            <Input />
                          </Form.Item>
                        </Space>
                      );
                    }
                    if (opType === 'type_convert') {
                      return (
                        <Space>
                          <Form.Item {...restField} name={[name, 'column']} label="字段名">
                            <Input />
                          </Form.Item>
                          <Form.Item {...restField} name={[name, 'target_type']} label="目标类型">
                            <Select>
                              <Option value="int">整数</Option>
                              <Option value="float">浮点数</Option>
                              <Option value="string">字符串</Option>
                              <Option value="bool">布尔</Option>
                              <Option value="datetime">日期</Option>
                            </Select>
                          </Form.Item>
                        </Space>
                      );
                    }
                    if (opType === 'compute') {
                      return (
                        <Space>
                          <Form.Item {...restField} name={[name, 'new_column']} label="新字段名">
                            <Input />
                          </Form.Item>
                          <Form.Item {...restField} name={[name, 'expression']} label="表达式">
                            <Input placeholder="price * quantity" />
                          </Form.Item>
                        </Space>
                      );
                    }
                    return null;
                  }}
                </Form.Item>

                <Button 
                  type="text" 
                  danger 
                  icon={<DeleteOutlined />} 
                  onClick={() => remove(name)}
                >
                  删除
                </Button>
              </Space>
            </Card>
          ))}
          <Button type="dashed" onClick={() => add()} block icon={<PlusOutlined />}>
            添加操作
          </Button>
        </>
      )}
    </Form.List>
  );

  const renderAggregateConfig = () => (
    <>
      <Form.Item name="group_by" label="分组字段">
        <Select mode="tags" placeholder="选择分组字段" />
      </Form.Item>
      <Form.List name="aggregations">
        {(fields, { add, remove }) => (
          <>
            {fields.map(({ key, name, ...restField }) => (
              <Card key={key} size="small" style={{ marginBottom: 8 }}>
                <Space>
                  <Form.Item {...restField} name={[name, 'column']} label="字段">
                    <Input />
                  </Form.Item>
                  <Form.Item {...restField} name={[name, 'function']} label="函数">
                    <Select>
                      <Option value="sum">SUM</Option>
                      <Option value="avg">AVG</Option>
                      <Option value="count">COUNT</Option>
                      <Option value="min">MIN</Option>
                      <Option value="max">MAX</Option>
                    </Select>
                  </Form.Item>
                  <Form.Item {...restField} name={[name, 'alias']} label="别名">
                    <Input />
                  </Form.Item>
                  <Button 
                    type="text" 
                    danger 
                    icon={<DeleteOutlined />} 
                    onClick={() => remove(name)}
                  >
                    删除
                  </Button>
                </Space>
              </Card>
            ))}
            <Button type="dashed" onClick={() => add()} block icon={<PlusOutlined />}>
              添加聚合
            </Button>
          </>
        )}
      </Form.List>
    </>
  );

  const renderSortConfig = () => (
    <Form.List name="sort_by">
      {(fields, { add, remove }) => (
        <>
          {fields.map(({ key, name, ...restField }) => (
            <Card key={key} size="small" style={{ marginBottom: 8 }}>
              <Space>
                <Form.Item {...restField} name={[name, 'column']} label="字段">
                  <Input />
                </Form.Item>
                <Form.Item {...restField} name={[name, 'ascending']} label="排序" valuePropName="checked">
                  <Switch checkedChildren="升序" unCheckedChildren="降序" defaultChecked />
                </Form.Item>
                <Button 
                  type="text" 
                  danger 
                  icon={<DeleteOutlined />} 
                  onClick={() => remove(name)}
                >
                  删除
                </Button>
              </Space>
            </Card>
          ))}
          <Button type="dashed" onClick={() => add()} block icon={<PlusOutlined />}>
            添加排序
          </Button>
        </>
      )}
    </Form.List>
  );

  const renderDeduplicateConfig = () => (
    <>
      <Form.Item name="columns" label="去重字段">
        <Select mode="tags" placeholder="留空则对所有字段去重" />
      </Form.Item>
      <Form.Item name="keep" label="保留策略">
        <Select>
          <Option value="first">保留第一个</Option>
          <Option value="last">保留最后一个</Option>
        </Select>
      </Form.Item>
    </>
  );

  const renderJoinConfig = () => (
    <>
      <Form.Item name="join_type" label="连接类型">
        <Select>
          <Option value="inner">INNER JOIN</Option>
          <Option value="left">LEFT JOIN</Option>
          <Option value="right">RIGHT JOIN</Option>
          <Option value="outer">FULL OUTER JOIN</Option>
        </Select>
      </Form.Item>
      <Form.Item name="left_key" label="左表连接键">
        <Input />
      </Form.Item>
      <Form.Item name="right_key" label="右表连接键">
        <Input />
      </Form.Item>
    </>
  );

  const renderBranchConfig = () => (
    <Form.List name="branches">
      {(fields, { add, remove }) => (
        <>
          {fields.map(({ key, name, ...restField }) => (
            <Card key={key} size="small" style={{ marginBottom: 8 }} title={`分支 ${name + 1}`}>
              <Space direction="vertical" style={{ width: '100%' }}>
                <Form.Item {...restField} name={[name, 'name']} label="分支名称">
                  <Input />
                </Form.Item>
                <Form.Item {...restField} name={[name, 'condition']} label="条件">
                  <Input placeholder="留空为默认分支" />
                </Form.Item>
                {fields.length > 1 && (
                  <Button 
                    type="text" 
                    danger 
                    icon={<DeleteOutlined />} 
                    onClick={() => remove(name)}
                  >
                    删除
                  </Button>
                )}
              </Space>
            </Card>
          ))}
          <Button type="dashed" onClick={() => add()} block icon={<PlusOutlined />}>
            添加分支
          </Button>
        </>
      )}
    </Form.List>
  );

  const renderOutputConfig = () => (
    <Collapse defaultActiveKey={['config']}>
      <Panel header="输出配置" key="config">
        <Form.Item name="output_type" label="输出类型">
          <Select>
            <Option value="sqlite">SQLite数据库</Option>
            <Option value="csv">CSV文件</Option>
            <Option value="http_api">HTTP API</Option>
          </Select>
        </Form.Item>

        <Form.Item shouldUpdate noStyle>
          {({ getFieldValue }) => {
            const outputType = getFieldValue('output_type');
            if (outputType === 'sqlite') {
              return (
                <>
                  <Form.Item name={['output_config', 'db_path']} label="数据库路径">
                    <Input placeholder="/path/to/database.db" />
                  </Form.Item>
                  <Form.Item name={['output_config', 'table_name']} label="表名">
                    <Input placeholder="table_name" />
                  </Form.Item>
                  <Form.Item name={['output_config', 'write_mode']} label="写入模式">
                    <Select>
                      <Option value="append">追加</Option>
                      <Option value="replace">替换</Option>
                    </Select>
                  </Form.Item>
                </>
              );
            }
            if (outputType === 'csv') {
              return (
                <>
                  <Form.Item name={['output_config', 'filename']} label="文件名">
                    <Input placeholder="output.csv" />
                  </Form.Item>
                  <Form.Item name={['output_config', 'delimiter']} label="分隔符">
                    <Input defaultValue="," />
                  </Form.Item>
                </>
              );
            }
            if (outputType === 'http_api') {
              return (
                <>
                  <Form.Item name={['output_config', 'url']} label="API URL">
                    <Input placeholder="https://api.example.com/upload" />
                  </Form.Item>
                </>
              );
            }
            return null;
          }}
        </Form.Item>

        <Form.Item name={['output_config', 'batch_size']} label="批量大小">
          <InputNumber min={1} defaultValue={1000} />
        </Form.Item>
        <Form.Item name={['output_config', 'error_strategy']} label="错误处理策略">
          <Select>
            <Option value="skip">跳过错误</Option>
            <Option value="abort">中止执行</Option>
            <Option value="retry">重试</Option>
          </Select>
        </Form.Item>
      </Panel>
    </Collapse>
  );

  const renderConfigByType = () => {
    switch (nodeType) {
      case 'source': return renderSourceConfig();
      case 'filter': return renderFilterConfig();
      case 'map': return renderMapConfig();
      case 'aggregate': return renderAggregateConfig();
      case 'sort': return renderSortConfig();
      case 'deduplicate': return renderDeduplicateConfig();
      case 'join': return renderJoinConfig();
      case 'branch': return renderBranchConfig();
      case 'output': return renderOutputConfig();
      default: return null;
    }
  };

  if (!node) {
    return (
      <Card title="节点配置">
        <p style={{ color: '#999' }}>请选择一个节点进行配置</p>
      </Card>
    );
  }

  return (
    <>
      <Card 
        title={`节点配置 - ${node.data?.label || nodeType}`}
        extra={<Button type="text" onClick={onClose}>关闭</Button>}
      >
        <Form
          form={form}
          layout="vertical"
          initialValues={{
            label: node.data?.label || '',
            ...config,
          }}
          onValuesChange={handleValuesChange}
        >
          <Form.Item name="label" label="节点名称">
            <Input />
          </Form.Item>
          <Divider />
          {renderConfigByType()}
        </Form>

        {pipelineId && (
          <>
            <Divider />
            <Space>
              <Button
                icon={<SafetyOutlined />}
                onClick={() => setQualityPanelVisible(true)}
              >
                数据质量规则
              </Button>
            </Space>
          </>
        )}
      </Card>

      <Modal
        title="数据预览"
        open={previewVisible}
        onCancel={() => setPreviewVisible(false)}
        footer={null}
        width={800}
      >
        {previewData && (
          <>
            <p>共 {previewData.total_count} 条数据，显示前10条</p>
            <Table
              dataSource={previewData.data}
              columns={previewData.columns.map(col => ({
                title: col,
                dataIndex: col,
                key: col,
              }))}
              size="small"
              pagination={false}
              scroll={{ x: true }}
            />
          </>
        )}
      </Modal>

      <Modal
        title="数据质量规则配置"
        open={qualityPanelVisible}
        onCancel={() => setQualityPanelVisible(false)}
        footer={null}
        width={900}
        destroyOnClose
      >
        {pipelineId && node && (
          <DataQualityRulePanel
            pipelineId={pipelineId}
            nodeId={node.id}
            onClose={() => setQualityPanelVisible(false)}
          />
        )}
      </Modal>
    </>
  );
};

export default NodeConfigPanel;
