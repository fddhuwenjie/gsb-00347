from typing import Dict, Any


def get_csv_cleaning_template() -> Dict[str, Any]:
    return {
        "name": "CSV清洗入库模板",
        "description": "从CSV文件读取数据，经过清洗后写入SQLite数据库",
        "template_type": "csv_cleaning",
        "dag_config": {
            "nodes": [
                {
                    "id": "source_1",
                    "type": "source",
                    "position": {"x": 100, "y": 200},
                    "data": {
                        "label": "CSV数据源",
                        "config": {
                            "source_type": "csv",
                            "source_config": {
                                "file_path": "/data/sample_data/customers.csv",
                                "delimiter": ",",
                                "encoding": "utf-8",
                                "has_header": True
                            }
                        }
                    }
                },
                {
                    "id": "filter_1",
                    "type": "filter",
                    "position": {"x": 350, "y": 200},
                    "data": {
                        "label": "过滤空值",
                        "config": {
                            "condition": "age > 0 and email.notnull()"
                        }
                    }
                },
                {
                    "id": "map_1",
                    "type": "map",
                    "position": {"x": 600, "y": 200},
                    "data": {
                        "label": "字段转换",
                        "config": {
                            "operations": [
                                {
                                    "type": "type_convert",
                                    "column": "age",
                                    "target_type": "int"
                                },
                                {
                                    "type": "rename",
                                    "old_name": "name",
                                    "new_name": "full_name"
                                }
                            ]
                        }
                    }
                },
                {
                    "id": "deduplicate_1",
                    "type": "deduplicate",
                    "position": {"x": 850, "y": 200},
                    "data": {
                        "label": "去重",
                        "config": {
                            "columns": ["email"],
                            "keep": "first"
                        }
                    }
                },
                {
                    "id": "output_1",
                    "type": "output",
                    "position": {"x": 1100, "y": 200},
                    "data": {
                        "label": "写入SQLite",
                        "config": {
                            "output_type": "sqlite",
                            "output_config": {
                                "db_path": "/data/sample_data/etl_output.db",
                                "table_name": "cleaned_customers",
                                "write_mode": "replace",
                                "batch_size": 1000,
                                "error_strategy": "skip"
                            }
                        }
                    }
                }
            ],
            "edges": [
                {"source": "source_1", "target": "filter_1"},
                {"source": "filter_1", "target": "map_1"},
                {"source": "map_1", "target": "deduplicate_1"},
                {"source": "deduplicate_1", "target": "output_1"}
            ]
        }
    }


def get_multi_table_join_template() -> Dict[str, Any]:
    return {
        "name": "多表JOIN聚合报表模板",
        "description": "从多个SQLite表读取数据，JOIN后聚合生成报表",
        "template_type": "multi_table_join",
        "dag_config": {
            "nodes": [
                {
                    "id": "source_1",
                    "type": "source",
                    "position": {"x": 100, "y": 150},
                    "data": {
                        "label": "订单表",
                        "config": {
                            "source_type": "sqlite",
                            "source_config": {
                                "db_path": "/data/sample_data/sales.db",
                                "table_name": "orders"
                            }
                        }
                    }
                },
                {
                    "id": "source_2",
                    "type": "source",
                    "position": {"x": 100, "y": 350},
                    "data": {
                        "label": "客户表",
                        "config": {
                            "source_type": "sqlite",
                            "source_config": {
                                "db_path": "/data/sample_data/sales.db",
                                "table_name": "customers"
                            }
                        }
                    }
                },
                {
                    "id": "join_1",
                    "type": "join",
                    "position": {"x": 350, "y": 250},
                    "data": {
                        "label": "JOIN订单与客户",
                        "config": {
                            "join_type": "inner",
                            "left_key": "customer_id",
                            "right_key": "id"
                        }
                    }
                },
                {
                    "id": "aggregate_1",
                    "type": "aggregate",
                    "position": {"x": 600, "y": 250},
                    "data": {
                        "label": "按地区聚合",
                        "config": {
                            "group_by": ["region"],
                            "aggregations": [
                                {"column": "amount", "function": "sum", "alias": "total_sales"},
                                {"column": "id", "function": "count", "alias": "order_count"},
                                {"column": "amount", "function": "avg", "alias": "avg_order_value"}
                            ]
                        }
                    }
                },
                {
                    "id": "sort_1",
                    "type": "sort",
                    "position": {"x": 850, "y": 250},
                    "data": {
                        "label": "按销售额排序",
                        "config": {
                            "sort_by": [
                                {"column": "total_sales", "ascending": False}
                            ]
                        }
                    }
                },
                {
                    "id": "output_1",
                    "type": "output",
                    "position": {"x": 1100, "y": 250},
                    "data": {
                        "label": "导出CSV报表",
                        "config": {
                            "output_type": "csv",
                            "output_config": {
                                "filename": "sales_report.csv",
                                "delimiter": ",",
                                "encoding": "utf-8",
                                "include_header": True
                            }
                        }
                    }
                }
            ],
            "edges": [
                {"source": "source_1", "target": "join_1"},
                {"source": "source_2", "target": "join_1"},
                {"source": "join_1", "target": "aggregate_1"},
                {"source": "aggregate_1", "target": "sort_1"},
                {"source": "sort_1", "target": "output_1"}
            ]
        }
    }


def get_api_sync_template() -> Dict[str, Any]:
    return {
        "name": "API数据同步模板",
        "description": "从HTTP API获取数据，经过分支处理后同步到不同目标",
        "template_type": "api_sync",
        "dag_config": {
            "nodes": [
                {
                    "id": "source_1",
                    "type": "source",
                    "position": {"x": 100, "y": 300},
                    "data": {
                        "label": "HTTP API",
                        "config": {
                            "source_type": "http_api",
                            "source_config": {
                                "url": "https://jsonplaceholder.typicode.com/users",
                                "headers": {},
                                "params": {},
                                "data_path": ""
                            }
                        }
                    }
                },
                {
                    "id": "map_1",
                    "type": "map",
                    "position": {"x": 350, "y": 300},
                    "data": {
                        "label": "字段映射",
                        "config": {
                            "operations": [
                                {
                                    "type": "rename",
                                    "old_name": "id",
                                    "new_name": "user_id"
                                },
                                {
                                    "type": "compute",
                                    "new_column": "full_address",
                                    "expression": "address.street + ', ' + address.city"
                                }
                            ]
                        }
                    }
                },
                {
                    "id": "branch_1",
                    "type": "branch",
                    "position": {"x": 600, "y": 300},
                    "data": {
                        "label": "按地区分支",
                        "config": {
                            "branches": [
                                {"name": "北美", "condition": "address.city in ['Gwenborough','Wisokyburgh']"},
                                {"name": "其他", "condition": ""}
                            ]
                        }
                    }
                },
                {
                    "id": "output_1",
                    "type": "output",
                    "position": {"x": 900, "y": 150},
                    "data": {
                        "label": "写入北美表",
                        "config": {
                            "output_type": "sqlite",
                            "output_config": {
                                "db_path": "/data/sample_data/api_sync.db",
                                "table_name": "na_users",
                                "write_mode": "append",
                                "batch_size": 100,
                                "error_strategy": "retry"
                            }
                        }
                    }
                },
                {
                    "id": "output_2",
                    "type": "output",
                    "position": {"x": 900, "y": 400},
                    "data": {
                        "label": "写入其他表",
                        "config": {
                            "output_type": "sqlite",
                            "output_config": {
                                "db_path": "/data/sample_data/api_sync.db",
                                "table_name": "other_users",
                                "write_mode": "append",
                                "batch_size": 100,
                                "error_strategy": "retry"
                            }
                        }
                    }
                }
            ],
            "edges": [
                {"source": "source_1", "target": "map_1"},
                {"source": "map_1", "target": "branch_1"},
                {"source": "branch_1", "target": "output_1", "sourceHandle": "branch_0"},
                {"source": "branch_1", "target": "output_2", "sourceHandle": "branch_1"}
            ]
        }
    }


def get_all_templates() -> Dict[str, Dict[str, Any]]:
    return {
        "csv_cleaning": get_csv_cleaning_template(),
        "multi_table_join": get_multi_table_join_template(),
        "api_sync": get_api_sync_template()
    }


def create_pipeline_from_template(template_type: str, name: str) -> Dict[str, Any]:
    templates = get_all_templates()
    if template_type not in templates:
        raise ValueError(f"Unknown template type: {template_type}")

    template = templates[template_type]
    return {
        "name": name,
        "description": template["description"],
        "dag_config": template["dag_config"],
        "is_template": False,
        "template_type": template_type
    }
