import React, { useState, useEffect } from 'react';
import { Table, Button, Modal, Form, Select, InputNumber, Input, Switch, Space, Popconfirm, message, Tag, Card } from 'antd';
import { PlusOutlined, EditOutlined, DeleteOutlined, CloseOutlined } from '@ant-design/icons';
import { qualityRules } from '../services/api';

const { Option } = Select;


const RULE_TYPE_OPTIONS = [
  { value: 'non_null_rate', label: '非空率检查' },
  { value: 'uniqueness', label: '唯一性检查' },
  { value: 'range_check', label: '范围校验' },
  { value: 'regex_match', label: '正则匹配' },
];

const FAILURE_STRATEGY_OPTIONS = [
  { value: 'mark', label: '标记脏数据继续执行' },
  { value: 'quarantine', label: '隔离到quarantine表' },
  { value: 'abort', label: '中止管道' },
];

const DataQualityRulePanel = ({ pipelineId, nodeId, onClose }) => {
  const [rules, setRules] = useState([]);
  const [loading, setLoading] = useState(false);
  const [modalVisible, setModalVisible] = useState(false);
  const [editingRule, setEditingRule] = useState(null);
  const [form] = Form.useForm();

  useEffect(() => {
    fetchRules();
  }, [pipelineId, nodeId]);

  const fetchRules = async () => {
    if (!pipelineId || !nodeId) return;
    
    setLoading(true);
    try {
      const response = await qualityRules.list(pipelineId, nodeId);
      setRules(response.data);
    } catch (error) {
      message.error('获取规则列表失败: ' + (error.response?.data?.detail || error.message));
    } finally {
      setLoading(false);
    }
  };

  const handleAdd = () => {
    setEditingRule(null);
    form.resetFields();
    setModalVisible(true);
  };

  const handleEdit = (rule) => {
    setEditingRule(rule);
    form.setFieldsValue({
      rule_type: rule.rule_type,
      failure_strategy: rule.failure_strategy,
      is_enabled: rule.is_enabled,
      ...rule.rule_config,
    });
    setModalVisible(true);
  };

  const handleDelete = async (id) => {
    try {
      await qualityRules.delete(id);
      message.success('删除成功');
      fetchRules();
    } catch (error) {
      message.error('删除失败: ' + (error.response?.data?.detail || error.message));
    }
  };

  const handleToggleEnabled = async (rule, checked) => {
    try {
      await qualityRules.update(rule.id, { is_enabled: checked });
      message.success(checked ? '已启用' : '已禁用');
      fetchRules();
    } catch (error) {
      message.error('操作失败: ' + (error.response?.data?.detail || error.message));
    }
  };

  const handleModalOk = async () => {
    try {
      const values = await form.validateFields();
      const { rule_type, failure_strategy, is_enabled, ...ruleConfig } = values;

      const payload = {
        pipeline_id: pipelineId,
        node_id: nodeId,
        rule_type,
        failure_strategy,
        is_enabled,
        rule_config: ruleConfig,
      };

      if (editingRule) {
        await qualityRules.update(editingRule.id, {
          rule_config: ruleConfig,
          failure_strategy,
          is_enabled,
        });
        message.success('更新成功');
      } else {
        await qualityRules.create(payload);
        message.success('创建成功');
      }

      setModalVisible(false);
      fetchRules();
    } catch (error) {
      if (error.errorFields) return;
      message.error('保存失败: ' + (error.response?.data?.detail || error.message));
    }
  };

  const getRuleTypeText = (type) => {
    const option = RULE_TYPE_OPTIONS.find(o => o.value === type);
    return option ? option.label : type;
  };

  const getStrategyText = (strategy) => {
    const option = FAILURE_STRATEGY_OPTIONS.find(o => o.value === strategy);
    return option ? option.label : strategy;
  };

  const getRuleConfigText = (rule) => {
    const { rule_type, rule_config } = rule;
    switch (rule_type) {
      case 'non_null_rate':
        return `字段: ${rule_config.field}, 最大null率: ${rule_config.max_null_percent}%`;
      case 'uniqueness':
        return `字段: ${Array.isArray(rule_config.fields) ? rule_config.fields.join(', ') : rule_config.fields}`;
      case 'range_check':
        return `字段: ${rule_config.field}, 范围: [${rule_config.min_value}, ${rule_config.max_value}]`;
      case 'regex_match':
        return `字段: ${rule_config.field}, 模式: ${rule_config.pattern}`;
      default:
        return JSON.stringify(rule_config);
    }
  };

  const columns = [
    {
      title: 'ID',
      dataIndex: 'id',
      key: 'id',
      width: 60,
    },
    {
      title: '规则类型',
      dataIndex: 'rule_type',
      key: 'rule_type',
      width: 120,
      render: (type) => <Tag color="blue">{getRuleTypeText(type)}</Tag>,
    },
    {
      title: '规则配置',
      dataIndex: 'rule_config',
      key: 'rule_config',
      render: (_, record) => getRuleConfigText(record),
    },
    {
      title: '失败策略',
      dataIndex: 'failure_strategy',
      key: 'failure_strategy',
      width: 160,
      render: (strategy) => (
        <Tag color={strategy === 'abort' ? 'red' : strategy === 'quarantine' ? 'orange' : 'green'}>
          {getStrategyText(strategy)}
        </Tag>
      ),
    },
    {
      title: '状态',
      dataIndex: 'is_enabled',
      key: 'is_enabled',
      width: 100,
      render: (enabled) => (
        <Tag color={enabled ? 'success' : 'default'}>
          {enabled ? '已启用' : '已禁用'}
        </Tag>
      ),
    },
    {
      title: '启用',
      dataIndex: 'is_enabled',
      key: 'toggle',
      width: 80,
      render: (enabled, record) => (
        <Switch
          checked={enabled}
          onChange={(checked) => handleToggleEnabled(record, checked)}
        />
      ),
    },
    {
      title: '操作',
      key: 'actions',
      width: 150,
      render: (_, record) => (
        <Space>
          <Button
            type="text"
            icon={<EditOutlined />}
            onClick={() => handleEdit(record)}
          >
            编辑
          </Button>
          <Popconfirm
            title="确定要删除这条规则吗？"
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

  const renderDynamicFields = () => (
    <Form.Item shouldUpdate noStyle>
      {({ getFieldValue }) => {
        const ruleType = getFieldValue('rule_type');
        if (!ruleType) return null;

        switch (ruleType) {
          case 'non_null_rate':
            return (
              <>
                <Form.Item
                  name="field"
                  label="字段名"
                  rules={[{ required: true, message: '请输入字段名' }]}
                >
                  <Input placeholder="例如: email" />
                </Form.Item>
                <Form.Item
                  name="max_null_percent"
                  label="最大null百分比"
                  rules={[{ required: true, message: '请输入最大null百分比' }]}
                >
                  <InputNumber
                    min={0}
                    max={100}
                    style={{ width: '100%' }}
                    placeholder="0-100"
                    addonAfter="%"
                  />
                </Form.Item>
              </>
            );
          case 'uniqueness':
            return (
              <Form.Item
                name="fields"
                label="检查字段"
                rules={[{ required: true, message: '请选择字段' }]}
              >
                <Select
                  mode="tags"
                  placeholder="输入字段名后按回车添加"
                  tokenSeparators={[',']}
                />
              </Form.Item>
            );
          case 'range_check':
            return (
              <>
                <Form.Item
                  name="field"
                  label="字段名"
                  rules={[{ required: true, message: '请输入字段名' }]}
                >
                  <Input placeholder="例如: age" />
                </Form.Item>
                <Space style={{ width: '100%' }}>
                  <Form.Item
                    name="min_value"
                    label="最小值"
                    rules={[{ required: true, message: '请输入最小值' }]}
                    style={{ flex: 1 }}
                  >
                    <InputNumber style={{ width: '100%' }} placeholder="最小值" />
                  </Form.Item>
                  <Form.Item
                    name="max_value"
                    label="最大值"
                    rules={[{ required: true, message: '请输入最大值' }]}
                    style={{ flex: 1 }}
                  >
                    <InputNumber style={{ width: '100%' }} placeholder="最大值" />
                  </Form.Item>
                </Space>
              </>
            );
          case 'regex_match':
            return (
              <>
                <Form.Item
                  name="field"
                  label="字段名"
                  rules={[{ required: true, message: '请输入字段名' }]}
                >
                  <Input placeholder="例如: phone" />
                </Form.Item>
                <Form.Item
                  name="pattern"
                  label="正则表达式"
                  rules={[{ required: true, message: '请输入正则表达式' }]}
                >
                  <Input placeholder="例如: ^1[3-9]\d{9}$" />
                </Form.Item>
              </>
            );
          default:
            return null;
        }
      }}
    </Form.Item>
  );

  return (
    <>
      <Card
        title="数据质量规则"
        extra={
          <Space>
            <Button
              type="primary"
              icon={<PlusOutlined />}
              onClick={handleAdd}
            >
              添加规则
            </Button>
            <Button
              type="text"
              icon={<CloseOutlined />}
              onClick={onClose}
            >
              关闭
            </Button>
          </Space>
        }
      >
        <Table
          columns={columns}
          dataSource={rules}
          rowKey="id"
          loading={loading}
          pagination={{ pageSize: 10 }}
          locale={{ emptyText: '暂无规则，点击"添加规则"创建' }}
        />
      </Card>

      <Modal
        title={editingRule ? '编辑规则' : '添加规则'}
        open={modalVisible}
        onOk={handleModalOk}
        onCancel={() => setModalVisible(false)}
        width={600}
        okText="保存"
        cancelText="取消"
      >
        <Form
          form={form}
          layout="vertical"
          initialValues={{
            rule_type: 'non_null_rate',
            failure_strategy: 'mark',
            is_enabled: true,
          }}
        >
          <Form.Item
            name="rule_type"
            label="规则类型"
            rules={[{ required: true, message: '请选择规则类型' }]}
          >
            <Select>
              {RULE_TYPE_OPTIONS.map(option => (
                <Option key={option.value} value={option.value}>
                  {option.label}
                </Option>
              ))}
            </Select>
          </Form.Item>

          {renderDynamicFields()}

          <Form.Item
            name="failure_strategy"
            label="失败策略"
            rules={[{ required: true, message: '请选择失败策略' }]}
          >
            <Select>
              {FAILURE_STRATEGY_OPTIONS.map(option => (
                <Option key={option.value} value={option.value}>
                  {option.label}
                </Option>
              ))}
            </Select>
          </Form.Item>

          <Form.Item
            name="is_enabled"
            label="启用状态"
            valuePropName="checked"
          >
            <Switch checkedChildren="启用" unCheckedChildren="禁用" />
          </Form.Item>
        </Form>
      </Modal>
    </>
  );
};

export default DataQualityRulePanel;
