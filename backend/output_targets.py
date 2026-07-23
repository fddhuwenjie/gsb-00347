import os
import re
import pandas as pd
import numpy as np
import sqlite3
import requests
import json
from typing import Dict, Any, List, Tuple, Optional
from datetime import datetime

EXPORT_DIR = os.getenv("EXPORT_DIR", "./exports")
os.makedirs(EXPORT_DIR, exist_ok=True)

# Marker table (inside each target SQLite DB) recording which (execution, node)
# writes have already been committed. Because the marker is inserted in the SAME
# transaction as the data rows, the data + marker are made durable atomically.
COMMIT_MARKER_TABLE = "_etl_output_commits"

# Local commit-log used by targets that write to EXTERNAL systems (CSV files,
# HTTP endpoints) where the marker cannot share a transaction with the data.
# It lets a resumed run detect that an output already committed and skip the
# duplicate side effect.
COMMIT_LOG_DB = os.getenv("ETL_COMMIT_LOG", os.path.join(EXPORT_DIR, "_etl_commit_log.db"))


class LocalCommitLog:
    """Small persistent key/value store of committed output side effects.

    Used by CSV and HTTP outputs. A row is written AFTER the external side
    effect succeeds, so on resume the same key is found and the side effect is
    not repeated. (For SQLite targets the marker instead lives in the target DB
    itself and is committed atomically with the data.)
    """

    def __init__(self, db_path: str = COMMIT_LOG_DB):
        self.db_path = db_path

    def _connect(self):
        conn = sqlite3.connect(self.db_path)
        conn.execute(
            f"CREATE TABLE IF NOT EXISTS {COMMIT_MARKER_TABLE} ("
            "idempotency_key TEXT PRIMARY KEY, "
            "target TEXT, rows_written INTEGER, detail TEXT, created_at TEXT)"
        )
        conn.commit()
        return conn

    def get(self, key: str) -> Optional[Dict[str, Any]]:
        if not key:
            return None
        conn = self._connect()
        try:
            row = conn.execute(
                f"SELECT rows_written, detail FROM {COMMIT_MARKER_TABLE} "
                "WHERE idempotency_key = ?", (key,)
            ).fetchone()
            if row is None:
                return None
            return {"rows_written": int(row[0]), "detail": row[1]}
        finally:
            conn.close()

    def put(self, key: str, target: str, rows_written: int, detail: str = ""):
        if not key:
            return
        conn = self._connect()
        try:
            conn.execute(
                f"INSERT OR REPLACE INTO {COMMIT_MARKER_TABLE} "
                "(idempotency_key, target, rows_written, detail, created_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (key, target, rows_written, detail, datetime.utcnow().isoformat())
            )
            conn.commit()
        finally:
            conn.close()


def _safe_key_fragment(key: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]", "_", key)


class OutputTarget:
    def write(self, df: pd.DataFrame, config: Dict[str, Any],
              idempotency_key: Optional[str] = None) -> Tuple[int, List[str]]:
        raise NotImplementedError


def _to_sqlite_value(value: Any) -> Any:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        return float(value)
    if isinstance(value, np.bool_):
        return bool(value)
    if isinstance(value, np.datetime64):
        return pd.Timestamp(value).to_pydatetime()
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    return value


class SQLiteOutput(OutputTarget):
    def write(self, df: pd.DataFrame, config: Dict[str, Any],
              idempotency_key: Optional[str] = None) -> Tuple[int, List[str]]:
        db_path = config.get("db_path")
        table_name = config.get("table_name")
        write_mode = config.get("write_mode", "append")
        batch_size = config.get("batch_size", 1000)
        error_strategy = config.get("error_strategy", "skip")

        if not db_path or not table_name:
            raise ValueError("db_path and table_name are required")

        errors: List[str] = []

        conn = sqlite3.connect(db_path)
        try:
            # Ensure the commit-marker table exists (idempotent DDL).
            conn.execute(
                f"CREATE TABLE IF NOT EXISTS {COMMIT_MARKER_TABLE} ("
                "idempotency_key TEXT PRIMARY KEY, "
                "table_name TEXT, rows_written INTEGER, created_at TEXT)"
            )
            conn.commit()

            # If this exact (execution, node) write already committed in a prior
            # attempt, skip the physical re-write. This closes the interrupt
            # window between "output committed" and "checkpoint saved": a resumed
            # run will find the marker and NOT write the rows a second time.
            if idempotency_key:
                row = conn.execute(
                    f"SELECT rows_written FROM {COMMIT_MARKER_TABLE} "
                    "WHERE idempotency_key = ?",
                    (idempotency_key,)
                ).fetchone()
                if row is not None:
                    return int(row[0]), []

            # Prepare the table schema (0 rows). Carries no data, so committing
            # it separately from the atomic data section is safe.
            if write_mode == "replace":
                df.head(0).to_sql(table_name, conn, if_exists="replace", index=False)
            else:
                df.head(0).to_sql(table_name, conn, if_exists="append", index=False)
            conn.commit()

            columns = list(df.columns)
            col_names = ", ".join(f'"{c}"' for c in columns)
            placeholders = ", ".join(["?"] * len(columns))
            insert_sql = f'INSERT INTO "{table_name}" ({col_names}) VALUES ({placeholders})'

            records = [
                tuple(_to_sqlite_value(v) for v in row)
                for row in df.itertuples(index=False, name=None)
            ]

            # Atomic section: all data batches AND the commit marker are written
            # under a single explicit transaction and committed exactly once.
            conn.isolation_level = None
            cur = conn.cursor()
            cur.execute("BEGIN")
            try:
                total_written = 0
                for i in range(0, len(records), batch_size):
                    batch = records[i:i + batch_size]
                    try:
                        cur.executemany(insert_sql, batch)
                        total_written += len(batch)
                    except Exception as e:
                        error_msg = f"Batch {i // batch_size + 1} error: {str(e)}"
                        errors.append(error_msg)
                        if error_strategy == "abort":
                            raise Exception(error_msg)
                        elif error_strategy == "retry":
                            try:
                                cur.executemany(insert_sql, batch)
                                total_written += len(batch)
                            except Exception:
                                if error_strategy != "skip":
                                    raise

                if idempotency_key:
                    cur.execute(
                        f"INSERT INTO {COMMIT_MARKER_TABLE} "
                        "(idempotency_key, table_name, rows_written, created_at) "
                        "VALUES (?, ?, ?, ?)",
                        (idempotency_key, table_name, total_written,
                         datetime.utcnow().isoformat())
                    )

                cur.execute("COMMIT")
            except Exception:
                cur.execute("ROLLBACK")
                raise
        finally:
            conn.close()

        return total_written, errors


class CSVOutput(OutputTarget):
    def write(self, df: pd.DataFrame, config: Dict[str, Any],
              idempotency_key: Optional[str] = None) -> Tuple[int, List[str]]:
        filename = config.get("filename")
        delimiter = config.get("delimiter", ",")
        encoding = config.get("encoding", "utf-8")
        include_header = config.get("include_header", True)

        commit_log = LocalCommitLog()

        # If this (execution, node) write already committed, reuse the exact same
        # file (recorded in the commit log) and do NOT create another one. This
        # covers auto-named outputs, which would otherwise emit a fresh
        # timestamped file on every resume.
        if idempotency_key:
            prior = commit_log.get(idempotency_key)
            if prior is not None:
                return prior["rows_written"], []

        if not filename:
            if idempotency_key:
                # Deterministic name derived from the idempotency key, so a resume
                # that somehow missed the marker still targets the same file
                # rather than a new timestamped one.
                filename = f"export_{_safe_key_fragment(idempotency_key)}.csv"
            else:
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

        # Record the commit only after the file was written successfully.
        commit_log.put(idempotency_key, f"csv:{filename}", len(df), detail=file_path)

        return len(df), errors


class HTTPAPIOutput(OutputTarget):
    def write(self, df: pd.DataFrame, config: Dict[str, Any],
              idempotency_key: Optional[str] = None) -> Tuple[int, List[str]]:
        url = config.get("url")
        headers = dict(config.get("headers", {}))
        batch_size = config.get("batch_size", 100)
        error_strategy = config.get("error_strategy", "skip")
        max_retries = config.get("max_retries", 3)

        if not url:
            raise ValueError("URL is required")

        if "Content-Type" not in headers:
            headers["Content-Type"] = "application/json"

        commit_log = LocalCommitLog()

        # Whole-node idempotency: if every batch already POSTed successfully in a
        # prior attempt, don't POST anything again on resume.
        if idempotency_key:
            prior = commit_log.get(idempotency_key)
            if prior is not None:
                return prior["rows_written"], []

        errors = []
        total_written = 0

        for i in range(0, len(df), batch_size):
            batch = df.iloc[i:i + batch_size]
            data = batch.to_dict("records")
            batch_index = i // batch_size

            # Per-batch idempotency key lets a partially-sent node resume without
            # re-POSTing already-delivered batches, and lets the server
            # de-duplicate via the standard Idempotency-Key header.
            batch_headers = dict(headers)
            if idempotency_key:
                batch_key = f"{idempotency_key}:batch:{batch_index}"
                if commit_log.get(batch_key) is not None:
                    total_written += len(batch)
                    continue
                batch_headers["Idempotency-Key"] = batch_key

            retry_count = 0
            success = False

            while retry_count <= max_retries and not success:
                try:
                    response = requests.post(url, json=data, headers=batch_headers, timeout=30)
                    response.raise_for_status()
                    total_written += len(batch)
                    success = True
                    if idempotency_key:
                        commit_log.put(f"{idempotency_key}:batch:{batch_index}",
                                       f"http:{url}", len(batch))
                except Exception as e:
                    retry_count += 1
                    if retry_count > max_retries or error_strategy == "abort":
                        error_msg = f"Batch {batch_index + 1} error after {retry_count} retries: {str(e)}"
                        errors.append(error_msg)
                        if error_strategy == "abort":
                            raise Exception(error_msg)
                        break

        # Mark the whole node committed only when no batch errored, so a resume
        # after a partial failure still retries the missing batches.
        if idempotency_key and not errors:
            commit_log.put(idempotency_key, f"http:{url}", total_written)

        return total_written, errors


class OutputExecutor:
    TARGET_TYPES = {
        "sqlite": SQLiteOutput(),
        "csv": CSVOutput(),
        "http_api": HTTPAPIOutput()
    }

    @classmethod
    def write(cls, target_type: str, df: pd.DataFrame, config: Dict[str, Any],
              idempotency_key: Optional[str] = None) -> Tuple[int, List[str]]:
        if target_type not in cls.TARGET_TYPES:
            raise ValueError(f"Unsupported output type: {target_type}")

        target = cls.TARGET_TYPES[target_type]
        return target.write(df, config, idempotency_key=idempotency_key)
