import React from 'react';
import { Handle, Position } from 'reactflow';

const nodeTypeLabels = {
  source: '数据源',
  filter: '过滤',
  map: '映射',
  aggregate: '聚合',
  sort: '排序',
  deduplicate: '去重',
  join: 'JOIN',
  branch: '分支',
  output: '输出',
};

const CustomNode = ({ data, selected, type }) => {
  const label = data.label || nodeTypeLabels[type] || type;
  const status = data.status;

  return (
    <div style={{ position: 'relative' }}>
      {type !== 'source' && type !== 'output' && (
        <Handle
          type="target"
          position={Position.Left}
          style={{ background: '#1890ff' }}
        />
      )}
      
      {type === 'join' && (
        <Handle
          type="target"
          position={Position.Left}
          id="join_right"
          style={{ background: '#1890ff', top: '75%' }}
        />
      )}

      <div style={{ padding: '4px 8px' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '4px' }}>
          {status && (
            <span className={`node-status status-${status}`} />
          )}
          <span className="node-label" title={label}>{label}</span>
        </div>
        <div className="node-type">{nodeTypeLabels[type] || type}</div>
      </div>

      {type !== 'output' && (
        <Handle
          type="source"
          position={Position.Right}
          style={{ background: '#1890ff' }}
        />
      )}

      {type === 'branch' && (
        <>
          <Handle
            type="source"
            position={Position.Right}
            id="branch_0"
            style={{ background: '#52c41a', top: '25%' }}
          />
          <Handle
            type="source"
            position={Position.Right}
            id="branch_1"
            style={{ background: '#faad14', top: '75%' }}
          />
        </>
      )}
    </div>
  );
};

export default CustomNode;
