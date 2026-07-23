import sqlite3
import pandas as pd
from typing import Dict, Any, List, Optional
from datetime import datetime
from sqlalchemy.orm import Session
from models import Watermark, ChangeLog, DataSource


class IncrementalSyncManager:
    @staticmethod
    def get_watermark(data_source_id: int, db_session: Session) -> Optional[Watermark]:
        return db_session.query(Watermark).filter(
            Watermark.data_source_id == data_source_id
        ).order_by(Watermark.updated_at.desc()).first()

    @staticmethod
    def update_watermark(data_source_id: int, watermark_type: str, watermark_value: Dict[str, Any], db_session: Session) -> None:
        data_source = db_session.query(DataSource).filter(DataSource.id == data_source_id).first()
        if not data_source:
            return
        existing = IncrementalSyncManager.get_watermark(data_source_id, db_session)
        if existing:
            existing.watermark_type = watermark_type
            existing.watermark_value = watermark_value
            existing.last_sync_time = datetime.utcnow()
        else:
            watermark = Watermark(
                data_source_id=data_source_id,
                source_type=data_source.type,
                watermark_type=watermark_type,
                watermark_value=watermark_value,
                last_sync_time=datetime.utcnow()
            )
            db_session.add(watermark)
        db_session.commit()

    @staticmethod
    def apply_incremental_filter(source_type: str, config: Dict[str, Any], df: pd.DataFrame, watermark: Optional[Watermark]) -> pd.DataFrame:
        if df.empty or not watermark:
            return df

        incremental_config = config.get("incremental", {})
        mode = incremental_config.get("mode")

        if not mode:
            return df

        if mode == "timestamp":
            timestamp_field = incremental_config.get("timestamp_field")
            if not timestamp_field or timestamp_field not in df.columns:
                return df
            last_value = watermark.watermark_value.get("last_timestamp")
            if last_value:
                df = df[df[timestamp_field] > last_value]
        elif mode == "auto_increment_id":
            id_field = incremental_config.get("id_field")
            if not id_field or id_field not in df.columns:
                return df
            last_value = watermark.watermark_value.get("last_id")
            if last_value is not None:
                df = df[df[id_field] > last_value]

        return df.reset_index(drop=True)

    @staticmethod
    def calculate_new_watermark(config: Dict[str, Any], df: pd.DataFrame, current_watermark: Optional[Watermark]) -> Dict[str, Any]:
        incremental_config = config.get("incremental", {})
        mode = incremental_config.get("mode")
        new_value = {}

        if mode == "timestamp":
            timestamp_field = incremental_config.get("timestamp_field")
            if timestamp_field and timestamp_field in df.columns and not df.empty:
                new_value["last_timestamp"] = df[timestamp_field].max()
            elif current_watermark:
                new_value["last_timestamp"] = current_watermark.watermark_value.get("last_timestamp")
        elif mode == "auto_increment_id":
            id_field = incremental_config.get("id_field")
            if id_field and id_field in df.columns and not df.empty:
                new_value["last_id"] = int(df[id_field].max())
            elif current_watermark:
                new_value["last_id"] = current_watermark.watermark_value.get("last_id")

        return new_value

    @staticmethod
    def process_incremental_sync(data_source_id: int, source_type: str, config: Dict[str, Any], df: pd.DataFrame, db_session: Session) -> pd.DataFrame:
        incremental_config = config.get("incremental", {})
        mode = incremental_config.get("mode")

        if not mode:
            return df

        if mode == "cdc":
            db_path = config.get("db_path")
            table_name = config.get("table_name")
            return SqliteCDCTriggerManager.get_changes(
                str(data_source_id), db_path, table_name, db_session
            )

        watermark = IncrementalSyncManager.get_watermark(data_source_id, db_session)
        filtered_df = IncrementalSyncManager.apply_incremental_filter(
            source_type, config, df, watermark
        )

        new_watermark_value = IncrementalSyncManager.calculate_new_watermark(
            config, df, watermark
        )

        if new_watermark_value:
            IncrementalSyncManager.update_watermark(
                data_source_id, mode, new_watermark_value, db_session
            )

        return filtered_df


class SqliteCDCTriggerManager:
    @staticmethod
    def enable_cdc(db_path: str, table_name: str, data_source_id: int) -> None:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        try:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS change_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    data_source_id INTEGER NOT NULL,
                    table_name TEXT NOT NULL,
                    operation_type TEXT NOT NULL,
                    record_id TEXT NOT NULL,
                    record_data TEXT,
                    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    synced INTEGER DEFAULT 0
                )
            """)

            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()
            pk_columns = [col[1] for col in columns if col[5] == 1]
            if not pk_columns:
                pk_columns = [columns[0][1]]

            pk_expr = " || '-' || ".join([f"CAST({col} AS TEXT)" for col in pk_columns])

            json_insert_cols = ", ".join([f"'{col[1]}'" for col in columns])
            json_insert_vals = ", ".join([f"NEW.{col[1]}" for col in columns])
            json_delete_vals = ", ".join([f"OLD.{col[1]}" for col in columns])

            cursor.execute(f"""
                CREATE TRIGGER IF NOT EXISTS {table_name}_insert_trigger
                AFTER INSERT ON {table_name}
                FOR EACH ROW
                BEGIN
                    INSERT INTO change_logs (data_source_id, table_name, operation_type, record_id, record_data)
                    VALUES (
                        {data_source_id},
                        '{table_name}',
                        'INSERT',
                        {pk_expr},
                        json_object({json_insert_cols}, {json_insert_vals})
                    );
                END;
            """)

            cursor.execute(f"""
                CREATE TRIGGER IF NOT EXISTS {table_name}_update_trigger
                AFTER UPDATE ON {table_name}
                FOR EACH ROW
                BEGIN
                    INSERT INTO change_logs (data_source_id, table_name, operation_type, record_id, record_data)
                    VALUES (
                        {data_source_id},
                        '{table_name}',
                        'UPDATE',
                        {pk_expr},
                        json_object({json_insert_cols}, {json_insert_vals})
                    );
                END;
            """)

            cursor.execute(f"""
                CREATE TRIGGER IF NOT EXISTS {table_name}_delete_trigger
                AFTER DELETE ON {table_name}
                FOR EACH ROW
                BEGIN
                    INSERT INTO change_logs (data_source_id, table_name, operation_type, record_id, record_data)
                    VALUES (
                        {data_source_id},
                        '{table_name}',
                        'DELETE',
                        {pk_expr},
                        json_object({json_insert_cols}, {json_delete_vals})
                    );
                END;
            """)

            conn.commit()
        finally:
            conn.close()

    @staticmethod
    def disable_cdc(db_path: str, table_name: str) -> None:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        try:
            cursor.execute(f"DROP TRIGGER IF EXISTS {table_name}_insert_trigger")
            cursor.execute(f"DROP TRIGGER IF EXISTS {table_name}_update_trigger")
            cursor.execute(f"DROP TRIGGER IF EXISTS {table_name}_delete_trigger")
            conn.commit()
        finally:
            conn.close()

    @staticmethod
    def get_changes(data_source_id: str, db_path: str, table_name: str, db_session: Session) -> pd.DataFrame:
        conn = sqlite3.connect(db_path)
        try:
            query = """
                SELECT id, data_source_id, table_name, operation_type, record_id, record_data, changed_at, synced
                FROM change_logs
                WHERE data_source_id = ? AND table_name = ? AND synced = 0
                ORDER BY id ASC
            """
            df = pd.read_sql_query(query, conn, params=(int(data_source_id), table_name))

            if not df.empty:
                change_ids = df["id"].tolist()
                SqliteCDCTriggerManager.mark_changes_synced(change_ids, db_session)

            return df
        finally:
            conn.close()

    @staticmethod
    def mark_changes_synced(change_ids: List[int], db_session: Session) -> None:
        if not change_ids:
            return

        for change_id in change_ids:
            change_log = db_session.query(ChangeLog).filter(ChangeLog.id == change_id).first()
            if change_log:
                change_log.synced = True

        db_session.commit()
