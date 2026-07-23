from typing import Dict, Any, List, Optional
from models import LineageRecord
from schemas import LineageGraph, LineageNode, LineageLink
from datetime import datetime


SUPPORTED_TRANSFORMS = {
    "filter": ["condition"],
    "map/rename": ["old_name", "new_name"],
    "map/type_convert": ["column", "target_type"],
    "map/compute": ["new_column", "expression"],
    "aggregate": ["group_by", "aggregations"],
    "join": ["join_type", "left_key", "right_key"],
    "deduplicate": ["columns", "keep"]
}


class LineageTracker:
    def __init__(self, pipeline_id: int, execution_id: Optional[int] = None):
        self.pipeline_id = pipeline_id
        self.execution_id = execution_id
        self._field_mappings: List[Dict[str, Any]] = []
        self._transform_records: List[Dict[str, Any]] = []
        self._lineage_records: List[LineageRecord] = []

    def track_field_mapping(
        self,
        source_table: str,
        source_column: str,
        target_table: str,
        target_column: str,
        transform_ops: List[Dict[str, Any]]
    ):
        validated_ops = []
        for op in transform_ops:
            op_type = op.get("type")
            if op_type not in SUPPORTED_TRANSFORMS:
                continue
            required_fields = SUPPORTED_TRANSFORMS[op_type]
            validated_op = {"type": op_type}
            for field in required_fields:
                if field in op:
                    validated_op[field] = op[field]
            validated_ops.append(validated_op)

        mapping = {
            "source_table": source_table,
            "source_column": source_column,
            "target_table": target_table,
            "target_column": target_column,
            "transform_path": validated_ops
        }
        self._field_mappings.append(mapping)

        lineage_record = LineageRecord(
            pipeline_id=self.pipeline_id,
            execution_id=self.execution_id,
            source_table=source_table,
            source_column=source_column,
            target_table=target_table,
            target_column=target_column,
            transform_path=validated_ops,
            created_at=datetime.utcnow()
        )
        self._lineage_records.append(lineage_record)

    def record_transform(
        self,
        node_id: str,
        node_type: str,
        input_fields: List[str],
        output_fields: List[str],
        operation: Dict[str, Any]
    ):
        transform_record = {
            "node_id": node_id,
            "node_type": node_type,
            "input_fields": input_fields,
            "output_fields": output_fields,
            "operation": operation,
            "timestamp": datetime.utcnow()
        }
        self._transform_records.append(transform_record)

    def build_lineage_graph(self, db_session) -> LineageGraph:
        records = db_session.query(LineageRecord).filter(
            LineageRecord.pipeline_id == self.pipeline_id
        ).all()
        if self.execution_id:
            records = [r for r in records if r.execution_id == self.execution_id]
        return LineageGraphBuilder.build_sankey_data(records)

    def save_to_db(self, db_session):
        for record in self._lineage_records:
            db_session.add(record)
        db_session.flush()


class LineageGraphBuilder:
    @staticmethod
    def build_sankey_data(lineage_records: List[LineageRecord]) -> LineageGraph:
        nodes_dict: Dict[str, LineageNode] = {}
        links_dict: Dict[str, Dict[str, Any]] = {}

        for record in lineage_records:
            source_node_id = f"source:{record.source_table}.{record.source_column}"
            if source_node_id not in nodes_dict:
                nodes_dict[source_node_id] = LineageNode(
                    id=source_node_id,
                    name=f"{record.source_table}.{record.source_column}",
                    type="source"
                )

            target_node_id = f"target:{record.target_table}.{record.target_column}"
            if target_node_id not in nodes_dict:
                nodes_dict[target_node_id] = LineageNode(
                    id=target_node_id,
                    name=f"{record.target_table}.{record.target_column}",
                    type="target"
                )

            if record.transform_path:
                prev_node_id = source_node_id
                for i, transform in enumerate(record.transform_path):
                    transform_type = transform.get("type", "transform")
                    transform_node_id = f"transform:{record.id}:{i}"
                    if transform_node_id not in nodes_dict:
                        nodes_dict[transform_node_id] = LineageNode(
                            id=transform_node_id,
                            name=transform_type,
                            type="transform"
                        )

                    link_key = f"{prev_node_id}->{transform_node_id}"
                    if link_key not in links_dict:
                        links_dict[link_key] = {
                            "source": prev_node_id,
                            "target": transform_node_id,
                            "value": 1,
                            "transform": transform
                        }
                    else:
                        links_dict[link_key]["value"] += 1

                    prev_node_id = transform_node_id

                final_link_key = f"{prev_node_id}->{target_node_id}"
                if final_link_key not in links_dict:
                    links_dict[final_link_key] = {
                        "source": prev_node_id,
                        "target": target_node_id,
                        "value": 1,
                        "transform": None
                    }
                else:
                    links_dict[final_link_key]["value"] += 1
            else:
                direct_link_key = f"{source_node_id}->{target_node_id}"
                if direct_link_key not in links_dict:
                    links_dict[direct_link_key] = {
                        "source": source_node_id,
                        "target": target_node_id,
                        "value": 1,
                        "transform": None
                    }
                else:
                    links_dict[direct_link_key]["value"] += 1

        nodes = list(nodes_dict.values())
        links = [
            LineageLink(
                source=link["source"],
                target=link["target"],
                value=link["value"],
                transform=link["transform"]
            )
            for link in links_dict.values()
        ]

        return LineageGraph(nodes=nodes, links=links)


class ReverseLineageQuery:
    @staticmethod
    def find_sources(target_table: str, target_column: str, db_session) -> List[LineageRecord]:
        results = []
        visited = set()

        def _traverse(current_table: str, current_column: str, path: List[LineageRecord]):
            records = db_session.query(LineageRecord).filter(
                LineageRecord.target_table == current_table,
                LineageRecord.target_column == current_column
            ).all()

            if not records:
                results.extend(path)
                return

            for record in records:
                record_key = (record.source_table, record.source_column, record.target_table, record.target_column)
                if record_key in visited:
                    continue
                visited.add(record_key)

                new_path = path + [record]
                _traverse(record.source_table, record.source_column, new_path)

        _traverse(target_table, target_column, [])
        return results
