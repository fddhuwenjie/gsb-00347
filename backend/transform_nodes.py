import pandas as pd
import numpy as np
from typing import Dict, Any, List, Tuple
import re


class TransformNode:
    def execute(self, df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        raise NotImplementedError


class FilterNode(TransformNode):
    def execute(self, df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        condition = config.get("condition", "")
        if not condition:
            return df

        condition = self._sanitize_condition(condition)
        try:
            return df.query(condition)
        except Exception as e:
            raise ValueError(f"Invalid filter condition: {str(e)}")

    def _sanitize_condition(self, condition: str) -> str:
        condition = condition.replace("&&", "&").replace("||", "|")
        condition = re.sub(r'\bAND\b', '&', condition, flags=re.IGNORECASE)
        condition = re.sub(r'\bOR\b', '|', condition, flags=re.IGNORECASE)
        return condition


class MapNode(TransformNode):
    def execute(self, df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        operations = config.get("operations", [])
        result_df = df.copy()

        for op in operations:
            op_type = op.get("type")
            if op_type == "rename":
                old_name = op.get("old_name")
                new_name = op.get("new_name")
                if old_name in result_df.columns:
                    result_df = result_df.rename(columns={old_name: new_name})
            elif op_type == "type_convert":
                column = op.get("column")
                target_type = op.get("target_type")
                if column in result_df.columns:
                    result_df[column] = self._convert_type(result_df[column], target_type)
            elif op_type == "compute":
                new_column = op.get("new_column")
                expression = op.get("expression")
                result_df[new_column] = result_df.eval(expression)

        return result_df

    def _convert_type(self, series: pd.Series, target_type: str) -> pd.Series:
        type_mapping = {
            "int": "int64",
            "float": "float64",
            "string": "str",
            "bool": "bool",
            "datetime": "datetime64"
        }
        target = type_mapping.get(target_type, target_type)
        return series.astype(target)


class AggregateNode(TransformNode):
    def execute(self, df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        group_by = config.get("group_by", [])
        aggregations = config.get("aggregations", [])

        if not group_by or not aggregations:
            return df

        agg_dict = {}
        for agg in aggregations:
            column = agg.get("column")
            function = agg.get("function")
            alias = agg.get("alias", f"{column}_{function}")

            func_map = {
                "sum": "sum",
                "avg": "mean",
                "count": "count",
                "min": "min",
                "max": "max"
            }
            agg_dict[column] = (column, func_map.get(function, function))

        result = df.groupby(group_by).agg(**agg_dict).reset_index()
        return result


class SortNode(TransformNode):
    def execute(self, df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        sort_by = config.get("sort_by", [])
        if not sort_by:
            return df

        columns = [s.get("column") for s in sort_by]
        ascending = [s.get("ascending", True) for s in sort_by]

        return df.sort_values(by=columns, ascending=ascending).reset_index(drop=True)


class DeduplicateNode(TransformNode):
    def execute(self, df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        columns = config.get("columns")
        keep = config.get("keep", "first")

        if columns:
            return df.drop_duplicates(subset=columns, keep=keep).reset_index(drop=True)
        else:
            return df.drop_duplicates(keep=keep).reset_index(drop=True)


class JoinNode(TransformNode):
    def execute(self, dfs: List[pd.DataFrame], config: Dict[str, Any]) -> pd.DataFrame:
        if len(dfs) < 2:
            raise ValueError("Join node requires at least 2 input dataframes")

        join_type = config.get("join_type", "inner")
        left_key = config.get("left_key")
        right_key = config.get("right_key")

        left_df, right_df = dfs[0], dfs[1]

        return pd.merge(
            left_df,
            right_df,
            how=join_type,
            left_on=left_key,
            right_on=right_key,
            suffixes=("_left", "_right")
        )


class BranchNode(TransformNode):
    def execute(self, df: pd.DataFrame, config: Dict[str, Any]) -> List[pd.DataFrame]:
        branches = config.get("branches", [])
        if not branches:
            return [df]

        results = []
        remaining_df = df.copy()

        for i, branch in enumerate(branches):
            condition = branch.get("condition")
            if i == len(branches) - 1 and not condition:
                results.append(remaining_df.copy())
            else:
                condition = self._sanitize_condition(condition)
                matched_df = remaining_df.query(condition)
                results.append(matched_df.copy())
                remaining_df = remaining_df[~remaining_df.index.isin(matched_df.index)]

        return results

    def _sanitize_condition(self, condition: str) -> str:
        condition = condition.replace("&&", "&").replace("||", "|")
        condition = re.sub(r'\bAND\b', '&', condition, flags=re.IGNORECASE)
        condition = re.sub(r'\bOR\b', '|', condition, flags=re.IGNORECASE)
        return condition


class TransformExecutor:
    NODE_TYPES = {
        "filter": FilterNode(),
        "map": MapNode(),
        "aggregate": AggregateNode(),
        "sort": SortNode(),
        "deduplicate": DeduplicateNode(),
        "join": JoinNode(),
        "branch": BranchNode()
    }

    @classmethod
    def execute(cls, node_type: str, inputs: List[pd.DataFrame], config: Dict[str, Any]) -> Any:
        if node_type not in cls.NODE_TYPES:
            raise ValueError(f"Unsupported node type: {node_type}")

        node = cls.NODE_TYPES[node_type]

        if node_type == "join":
            return node.execute(inputs, config)
        elif node_type == "branch":
            return node.execute(inputs[0] if inputs else pd.DataFrame(), config)
        else:
            return node.execute(inputs[0] if inputs else pd.DataFrame(), config)
