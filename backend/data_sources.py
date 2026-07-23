import os
import pandas as pd
import requests
import sqlite3
from typing import Dict, Any, List, Tuple, Optional
from fastapi import UploadFile
from sqlalchemy.orm import Session
from incremental_sync import IncrementalSyncManager, SqliteCDCTriggerManager
import models


UPLOAD_DIR = os.getenv("UPLOAD_DIR", "./uploads")
os.makedirs(UPLOAD_DIR, exist_ok=True)


class DataSourceHandler:
    @staticmethod
    def preview_csv(config: Dict[str, Any], limit: int = 10) -> Tuple[List[str], List[Dict], int]:
        file_path = config.get("file_path")
        delimiter = config.get("delimiter", ",")
        encoding = config.get("encoding", "utf-8")
        has_header = config.get("has_header", True)

        if not file_path or not os.path.exists(file_path):
            raise FileNotFoundError(f"CSV file not found: {file_path}")

        df = pd.read_csv(file_path, delimiter=delimiter, encoding=encoding,
                         header=0 if has_header else None)

        columns = df.columns.tolist()
        data = df.head(limit).to_dict("records")
        total_count = len(df)

        return columns, data, total_count

    @staticmethod
    def read_csv(config: Dict[str, Any], data_source_id: Optional[int] = None,
                 db_session: Optional[Session] = None) -> pd.DataFrame:
        file_path = config.get("file_path")
        delimiter = config.get("delimiter", ",")
        encoding = config.get("encoding", "utf-8")
        has_header = config.get("has_header", True)

        df = pd.read_csv(file_path, delimiter=delimiter, encoding=encoding,
                         header=0 if has_header else None)

        incremental_config = config.get("incremental", {})
        if incremental_config.get("enabled") and data_source_id and db_session:
            sync_manager = IncrementalSyncManager()
            df = sync_manager.process_incremental_sync(
                data_source_id, "csv", config, df, db_session
            )

        return df

    @staticmethod
    def preview_sqlite(config: Dict[str, Any], limit: int = 10) -> Tuple[List[str], List[Dict], int]:
        db_path = config.get("db_path")
        table_name = config.get("table_name")
        query = config.get("query")

        if not db_path or not os.path.exists(db_path):
            raise FileNotFoundError(f"SQLite database not found: {db_path}")

        conn = sqlite3.connect(db_path)
        try:
            if query:
                df = pd.read_sql_query(query, conn)
            elif table_name:
                df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
            else:
                raise ValueError("Either table_name or query must be provided")

            columns = df.columns.tolist()
            data = df.head(limit).to_dict("records")
            total_count = len(df)

            return columns, data, total_count
        finally:
            conn.close()

    @staticmethod
    def read_sqlite(config: Dict[str, Any], data_source_id: Optional[int] = None,
                    db_session: Optional[Session] = None) -> pd.DataFrame:
        db_path = config.get("db_path")
        table_name = config.get("table_name")
        query = config.get("query")

        incremental_config = config.get("incremental", {})

        if incremental_config.get("enabled") and data_source_id and db_session:
            if incremental_config.get("cdc_enabled") and table_name:
                cdc_manager = SqliteCDCTriggerManager()
                df = cdc_manager.get_changes(data_source_id, db_path, table_name, db_session)
                if len(df) == 0:
                    df = DataSourceHandler._read_sqlite_full(db_path, table_name, query)
                return df

        df = DataSourceHandler._read_sqlite_full(db_path, table_name, query)

        if incremental_config.get("enabled") and data_source_id and db_session:
            sync_manager = IncrementalSyncManager()
            df = sync_manager.process_incremental_sync(
                data_source_id, "sqlite", config, df, db_session
            )

        return df

    @staticmethod
    def _read_sqlite_full(db_path: str, table_name: Optional[str] = None,
                          query: Optional[str] = None) -> pd.DataFrame:
        conn = sqlite3.connect(db_path)
        try:
            if query:
                df = pd.read_sql_query(query, conn)
            elif table_name:
                df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
            else:
                raise ValueError("Either table_name or query must be provided")
            return df
        finally:
            conn.close()

    @staticmethod
    def enable_sqlite_cdc(db_path: str, table_name: str, data_source_id: int) -> bool:
        cdc_manager = SqliteCDCTriggerManager()
        return cdc_manager.enable_cdc(db_path, table_name, data_source_id)

    @staticmethod
    def disable_sqlite_cdc(db_path: str, table_name: str) -> bool:
        cdc_manager = SqliteCDCTriggerManager()
        return cdc_manager.disable_cdc(db_path, table_name)

    @staticmethod
    def preview_http_api(config: Dict[str, Any], limit: int = 10) -> Tuple[List[str], List[Dict], int]:
        url = config.get("url")
        headers = config.get("headers", {})
        params = config.get("params", {})

        if not url:
            raise ValueError("URL must be provided")

        response = requests.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()

        data = response.json()
        if isinstance(data, dict):
            data_path = config.get("data_path", "data")
            for key in data_path.split("."):
                if key in data:
                    data = data[key]
                else:
                    raise ValueError(f"Data path '{data_path}' not found in response")

        if not isinstance(data, list):
            raise ValueError("Response must be a JSON array")

        if len(data) == 0:
            return [], [], 0

        df = pd.DataFrame(data)
        columns = df.columns.tolist()
        preview_data = df.head(limit).to_dict("records")

        return columns, preview_data, len(data)

    @staticmethod
    def read_http_api(config: Dict[str, Any], data_source_id: Optional[int] = None,
                      db_session: Optional[Session] = None) -> pd.DataFrame:
        url = config.get("url")
        headers = config.get("headers", {})
        params = config.get("params", {})

        response = requests.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()

        data = response.json()
        if isinstance(data, dict):
            data_path = config.get("data_path", "data")
            for key in data_path.split("."):
                if key in data:
                    data = data[key]

        if not isinstance(data, list):
            raise ValueError("Response must be a JSON array")

        df = pd.DataFrame(data)

        incremental_config = config.get("incremental", {})
        if incremental_config.get("enabled") and data_source_id and db_session:
            sync_manager = IncrementalSyncManager()
            df = sync_manager.process_incremental_sync(
                data_source_id, "http_api", config, df, db_session
            )

        return df

    @classmethod
    def preview(cls, source_type: str, config: Dict[str, Any], limit: int = 10) -> Tuple[List[str], List[Dict], int]:
        handlers = {
            "csv": cls.preview_csv,
            "sqlite": cls.preview_sqlite,
            "http_api": cls.preview_http_api
        }

        if source_type not in handlers:
            raise ValueError(f"Unsupported source type: {source_type}")

        return handlers[source_type](config, limit)

    @classmethod
    def read(cls, source_type: str, config: Dict[str, Any],
             data_source_id: Optional[int] = None,
             db_session: Optional[Session] = None) -> pd.DataFrame:
        handlers = {
            "csv": cls.read_csv,
            "sqlite": cls.read_sqlite,
            "http_api": cls.read_http_api
        }

        if source_type not in handlers:
            raise ValueError(f"Unsupported source type: {source_type}")

        return handlers[source_type](config, data_source_id, db_session)


async def save_upload_file(file: UploadFile) -> str:
    file_path = os.path.join(UPLOAD_DIR, file.filename)
    with open(file_path, "wb") as buffer:
        content = await file.read()
        buffer.write(content)
    return file_path
