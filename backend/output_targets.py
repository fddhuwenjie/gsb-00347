import os
import pandas as pd
import sqlite3
import requests
import json
from typing import Dict, Any, List, Tuple, Optional
from datetime import datetime

EXPORT_DIR = os.getenv("EXPORT_DIR", "./exports")
os.makedirs(EXPORT_DIR, exist_ok=True)

_MARKER_TABLE = "_etl_write_markers"


class OutputTarget:
    def write(self, df: pd.DataFrame, config: Dict[str, Any],
              execution_id: Optional[int] = None,
              node_id: Optional[str] = None) -> Tuple[int, List[str]]:
        raise NotImplementedError


class SQLiteOutput(OutputTarget):
    def _ensure_marker_table(self, conn):
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {_MARKER_TABLE} (
                execution_id INTEGER NOT NULL,
                node_id TEXT NOT NULL,
                rows_written INTEGER NOT NULL DEFAULT 0,
                written_at TEXT NOT NULL,
                PRIMARY KEY (execution_id, node_id)
            )
        """)

    def _check_marker(self, conn, execution_id, node_id):
        if execution_id is None or node_id is None:
            return None
        cur = conn.execute(
            f"SELECT rows_written FROM {_MARKER_TABLE} WHERE execution_id=? AND node_id=?",
            (execution_id, node_id)
        )
        row = cur.fetchone()
        return row[0] if row else None

    def _insert_marker(self, conn, execution_id, node_id, rows_written):
        if execution_id is None or node_id is None:
            return
        conn.execute(
            f"INSERT INTO {_MARKER_TABLE} (execution_id, node_id, rows_written, written_at) VALUES (?, ?, ?, ?)",
            (execution_id, node_id, rows_written, datetime.utcnow().isoformat())
        )

    def write(self, df: pd.DataFrame, config: Dict[str, Any],
              execution_id: Optional[int] = None,
              node_id: Optional[str] = None) -> Tuple[int, List[str]]:
        db_path = config.get("db_path")
        table_name = config.get("table_name")
        write_mode = config.get("write_mode", "append")
        batch_size = config.get("batch_size", 1000)
        error_strategy = config.get("error_strategy", "skip")

        if not db_path or not table_name:
            raise ValueError("db_path and table_name are required")

        errors = []
        total_written = 0

        conn = sqlite3.connect(db_path)
        try:
            self._ensure_marker_table(conn)

            existing_rows = self._check_marker(conn, execution_id, node_id)
            if existing_rows is not None:
                conn.commit()
                return existing_rows, []

            if write_mode == "replace":
                df.head(0).to_sql(table_name, conn, if_exists="replace", index=False)

            for i in range(0, len(df), batch_size):
                batch = df.iloc[i:i + batch_size]
                try:
                    batch.to_sql(table_name, conn, if_exists="append", index=False)
                    total_written += len(batch)
                except Exception as e:
                    error_msg = f"Batch {i//batch_size + 1} error: {str(e)}"
                    errors.append(error_msg)
                    if error_strategy == "abort":
                        raise Exception(error_msg)
                    elif error_strategy == "retry":
                        try:
                            batch.to_sql(table_name, conn, if_exists="append", index=False)
                            total_written += len(batch)
                        except:
                            if error_strategy != "skip":
                                raise

            self._insert_marker(conn, execution_id, node_id, total_written)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

        return total_written, errors


class CSVOutput(OutputTarget):
    def write(self, df: pd.DataFrame, config: Dict[str, Any],
              execution_id: Optional[int] = None,
              node_id: Optional[str] = None) -> Tuple[int, List[str]]:
        filename = config.get("filename")
        delimiter = config.get("delimiter", ",")
        encoding = config.get("encoding", "utf-8")
        include_header = config.get("include_header", True)

        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"export_{timestamp}.csv"

        file_path = os.path.join(EXPORT_DIR, filename)

        if execution_id is not None and node_id is not None:
            marker_path = file_path + f".exec_{execution_id}_{node_id}.done"
            if os.path.exists(marker_path):
                try:
                    with open(marker_path, "r") as f:
                        return int(f.read().strip()), []
                except (ValueError, IOError):
                    return len(df), []

        errors = []
        try:
            df.to_csv(file_path, sep=delimiter, encoding=encoding,
                      header=include_header, index=False)
        except Exception as e:
            errors.append(f"CSV export error: {str(e)}")
            raise

        if execution_id is not None and node_id is not None:
            marker_path = file_path + f".exec_{execution_id}_{node_id}.done"
            with open(marker_path, "w") as f:
                f.write(str(len(df)))

        return len(df), errors


class HTTPAPIOutput(OutputTarget):
    def write(self, df: pd.DataFrame, config: Dict[str, Any],
              execution_id: Optional[int] = None,
              node_id: Optional[str] = None) -> Tuple[int, List[str]]:
        url = config.get("url")
        headers = config.get("headers", {})
        batch_size = config.get("batch_size", 100)
        error_strategy = config.get("error_strategy", "skip")
        max_retries = config.get("max_retries", 3)

        if not url:
            raise ValueError("URL is required")

        if "Content-Type" not in headers:
            headers["Content-Type"] = "application/json"

        if execution_id is not None and node_id is not None:
            headers["X-ETL-Execution-Id"] = str(execution_id)
            headers["X-ETL-Node-Id"] = node_id

        errors = []
        total_written = 0

        for i in range(0, len(df), batch_size):
            batch = df.iloc[i:i + batch_size]
            data = batch.to_dict("records")

            retry_count = 0
            success = False

            while retry_count <= max_retries and not success:
                try:
                    response = requests.post(url, json=data, headers=headers, timeout=30)
                    response.raise_for_status()
                    total_written += len(batch)
                    success = True
                except Exception as e:
                    retry_count += 1
                    if retry_count > max_retries or error_strategy == "abort":
                        error_msg = f"Batch {i//batch_size + 1} error after {retry_count} retries: {str(e)}"
                        errors.append(error_msg)
                        if error_strategy == "abort":
                            raise Exception(error_msg)
                        break

        return total_written, errors


class OutputExecutor:
    TARGET_TYPES = {
        "sqlite": SQLiteOutput(),
        "csv": CSVOutput(),
        "http_api": HTTPAPIOutput()
    }

    @classmethod
    def write(cls, target_type: str, df: pd.DataFrame, config: Dict[str, Any],
              execution_id: Optional[int] = None,
              node_id: Optional[str] = None) -> Tuple[int, List[str]]:
        if target_type not in cls.TARGET_TYPES:
            raise ValueError(f"Unsupported output type: {target_type}")

        target = cls.TARGET_TYPES[target_type]
        return target.write(df, config, execution_id=execution_id, node_id=node_id)
