import os
import pandas as pd
import sqlite3
import requests
import json
from typing import Dict, Any, List, Tuple, Optional
from datetime import datetime

EXPORT_DIR = os.getenv("EXPORT_DIR", "./exports")
os.makedirs(EXPORT_DIR, exist_ok=True)

MARKER_TABLE = "_etl_write_markers"


class OutputTarget:
    def write(self, df: pd.DataFrame, config: Dict[str, Any],
              execution_id: Optional[int] = None,
              node_id: Optional[str] = None) -> Tuple[int, List[str], bool]:
        raise NotImplementedError


class SQLiteOutput(OutputTarget):
    def _ensure_marker_table(self, conn: sqlite3.Connection):
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {MARKER_TABLE} (
                execution_id INTEGER,
                node_id TEXT,
                rows_written INTEGER,
                written_at TEXT,
                PRIMARY KEY (execution_id, node_id)
            )
        """)

    def _check_marker(self, conn: sqlite3.Connection,
                      execution_id: Optional[int],
                      node_id: Optional[str]) -> Optional[int]:
        if execution_id is None or node_id is None:
            return None
        self._ensure_marker_table(conn)
        cur = conn.execute(
            f"SELECT rows_written FROM {MARKER_TABLE} WHERE execution_id = ? AND node_id = ?",
            (execution_id, node_id)
        )
        row = cur.fetchone()
        return row[0] if row else None

    def _insert_marker(self, conn: sqlite3.Connection,
                       execution_id: Optional[int],
                       node_id: Optional[str],
                       rows_written: int):
        if execution_id is None or node_id is None:
            return
        self._ensure_marker_table(conn)
        conn.execute(
            f"INSERT OR REPLACE INTO {MARKER_TABLE} (execution_id, node_id, rows_written, written_at) VALUES (?, ?, ?, ?)",
            (execution_id, node_id, rows_written, datetime.utcnow().isoformat())
        )

    def write(self, df: pd.DataFrame, config: Dict[str, Any],
              execution_id: Optional[int] = None,
              node_id: Optional[str] = None) -> Tuple[int, List[str], bool]:
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
            existing_rows = self._check_marker(conn, execution_id, node_id)
            if existing_rows is not None:
                return existing_rows, [], True

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
        except:
            conn.rollback()
            raise
        finally:
            conn.close()

        return total_written, errors, False


class CSVOutput(OutputTarget):
    def _marker_path(self, file_path: str) -> str:
        return file_path + ".etl_marker"

    def _check_marker(self, file_path: str,
                      execution_id: Optional[int],
                      node_id: Optional[str]) -> Optional[int]:
        if execution_id is None or node_id is None:
            return None
        mpath = self._marker_path(file_path)
        if not os.path.exists(mpath) or not os.path.exists(file_path):
            return None
        try:
            with open(mpath, "r", encoding="utf-8") as f:
                data = json.load(f)
            if data.get("execution_id") == execution_id and data.get("node_id") == node_id:
                return data.get("rows_written", 0)
        except (json.JSONDecodeError, OSError):
            pass
        return None

    def _write_marker(self, file_path: str,
                      execution_id: Optional[int],
                      node_id: Optional[str],
                      rows_written: int):
        if execution_id is None or node_id is None:
            return
        mpath = self._marker_path(file_path)
        tmp_path = mpath + ".tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump({
                "execution_id": execution_id,
                "node_id": node_id,
                "rows_written": rows_written,
                "written_at": datetime.utcnow().isoformat()
            }, f)
        os.replace(tmp_path, mpath)

    def write(self, df: pd.DataFrame, config: Dict[str, Any],
              execution_id: Optional[int] = None,
              node_id: Optional[str] = None) -> Tuple[int, List[str], bool]:
        filename = config.get("filename")
        delimiter = config.get("delimiter", ",")
        encoding = config.get("encoding", "utf-8")
        include_header = config.get("include_header", True)

        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"export_{timestamp}.csv"

        file_path = os.path.join(EXPORT_DIR, filename)

        existing_rows = self._check_marker(file_path, execution_id, node_id)
        if existing_rows is not None:
            return existing_rows, [], True

        errors = []
        try:
            tmp_path = file_path + ".tmp"
            df.to_csv(tmp_path, sep=delimiter, encoding=encoding,
                      header=include_header, index=False)
            os.replace(tmp_path, file_path)
            self._write_marker(file_path, execution_id, node_id, len(df))
        except Exception as e:
            errors.append(f"CSV export error: {str(e)}")
            raise

        return len(df), errors, False


class HTTPAPIOutput(OutputTarget):
    def write(self, df: pd.DataFrame, config: Dict[str, Any],
              execution_id: Optional[int] = None,
              node_id: Optional[str] = None) -> Tuple[int, List[str], bool]:
        url = config.get("url")
        headers = dict(config.get("headers", {}))
        batch_size = config.get("batch_size", 100)
        error_strategy = config.get("error_strategy", "skip")
        max_retries = config.get("max_retries", 3)

        if not url:
            raise ValueError("URL is required")

        if "Content-Type" not in headers:
            headers["Content-Type"] = "application/json"

        if execution_id is not None and node_id is not None:
            headers["X-Idempotency-Key"] = f"{execution_id}-{node_id}"

        errors = []
        total_written = 0
        skipped = False

        for i in range(0, len(df), batch_size):
            batch = df.iloc[i:i + batch_size]
            data = batch.to_dict("records")

            retry_count = 0
            success = False

            while retry_count <= max_retries and not success:
                try:
                    response = requests.post(url, json=data, headers=headers, timeout=30)

                    if response.status_code == 409:
                        skipped = True
                        total_written += len(batch)
                        success = True
                        break

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

        return total_written, errors, skipped


class OutputExecutor:
    TARGET_TYPES = {
        "sqlite": SQLiteOutput(),
        "csv": CSVOutput(),
        "http_api": HTTPAPIOutput()
    }

    @classmethod
    def write(cls, target_type: str, df: pd.DataFrame, config: Dict[str, Any],
              execution_id: Optional[int] = None,
              node_id: Optional[str] = None) -> Tuple[int, List[str], bool]:
        if target_type not in cls.TARGET_TYPES:
            raise ValueError(f"Unsupported output type: {target_type}")

        target = cls.TARGET_TYPES[target_type]
        return target.write(df, config, execution_id=execution_id, node_id=node_id)
