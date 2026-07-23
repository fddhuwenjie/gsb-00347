import os
import pandas as pd
import sqlite3
import requests
import json
from typing import Dict, Any, List, Tuple
from datetime import datetime

EXPORT_DIR = os.getenv("EXPORT_DIR", "./exports")
os.makedirs(EXPORT_DIR, exist_ok=True)


class OutputTarget:
    def write(self, df: pd.DataFrame, config: Dict[str, Any]) -> Tuple[int, List[str]]:
        raise NotImplementedError


class SQLiteOutput(OutputTarget):
    def write(self, df: pd.DataFrame, config: Dict[str, Any]) -> Tuple[int, List[str]]:
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
            conn.commit()
        finally:
            conn.close()

        return total_written, errors


class CSVOutput(OutputTarget):
    def write(self, df: pd.DataFrame, config: Dict[str, Any]) -> Tuple[int, List[str]]:
        filename = config.get("filename")
        delimiter = config.get("delimiter", ",")
        encoding = config.get("encoding", "utf-8")
        include_header = config.get("include_header", True)

        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"export_{timestamp}.csv"

        file_path = os.path.join(EXPORT_DIR, filename)

        errors = []
        try:
            df.to_csv(file_path, sep=delimiter, encoding=encoding,
                      header=include_header, index=False)
        except Exception as e:
            errors.append(f"CSV export error: {str(e)}")
            raise

        return len(df), errors


class HTTPAPIOutput(OutputTarget):
    def write(self, df: pd.DataFrame, config: Dict[str, Any]) -> Tuple[int, List[str]]:
        url = config.get("url")
        headers = config.get("headers", {})
        batch_size = config.get("batch_size", 100)
        error_strategy = config.get("error_strategy", "skip")
        max_retries = config.get("max_retries", 3)

        if not url:
            raise ValueError("URL is required")

        if "Content-Type" not in headers:
            headers["Content-Type"] = "application/json"

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
    def write(cls, target_type: str, df: pd.DataFrame, config: Dict[str, Any]) -> Tuple[int, List[str]]:
        if target_type not in cls.TARGET_TYPES:
            raise ValueError(f"Unsupported output type: {target_type}")

        target = cls.TARGET_TYPES[target_type]
        return target.write(df, config)
