import pandas as pd
import re
from typing import Dict, Any, List, Tuple, Optional
from schemas import QualityCheckResult
from models import DataQualityRule, QualityReport, QuarantineRecord


class QualityRuleEngine:
    @staticmethod
    def _collect_violation_samples(df: pd.DataFrame, violation_mask: pd.Series, max_samples: int = 20) -> List[Dict[str, Any]]:
        violation_df = df[violation_mask]
        sample_count = min(len(violation_df), max_samples)
        if sample_count == 0:
            return []
        return violation_df.head(sample_count).to_dict("records")

    @staticmethod
    def _build_result(total: int, passed: int, failed: int, violation_samples: List[Dict[str, Any]]) -> QualityCheckResult:
        pass_rate = (passed / total * 100) if total > 0 else 100.0
        return QualityCheckResult(
            passed=failed == 0,
            total_records=total,
            passed_records=passed,
            failed_records=failed,
            pass_rate=round(pass_rate, 2),
            violation_samples=violation_samples
        )

    @classmethod
    def _check_non_null_rate(cls, rule_config: Dict[str, Any], df: pd.DataFrame) -> QualityCheckResult:
        field = rule_config["field"]
        max_null_percent = rule_config["max_null_percent"]

        total = len(df)
        if total == 0:
            return cls._build_result(0, 0, 0, [])

        null_count = df[field].isnull().sum()
        null_percent = (null_count / total) * 100

        if null_percent > max_null_percent:
            violation_mask = df[field].isnull()
            samples = cls._collect_violation_samples(df, violation_mask)
            return cls._build_result(total, total - null_count, null_count, samples)
        else:
            return cls._build_result(total, total, 0, [])

    @classmethod
    def _check_uniqueness(cls, rule_config: Dict[str, Any], df: pd.DataFrame) -> QualityCheckResult:
        fields = rule_config["fields"]

        total = len(df)
        if total == 0:
            return cls._build_result(0, 0, 0, [])

        duplicate_mask = df.duplicated(subset=fields, keep=False)
        duplicate_count = duplicate_mask.sum()

        if duplicate_count > 0:
            samples = cls._collect_violation_samples(df, duplicate_mask)
            return cls._build_result(total, total - duplicate_count, duplicate_count, samples)
        else:
            return cls._build_result(total, total, 0, [])

    @classmethod
    def _check_range(cls, rule_config: Dict[str, Any], df: pd.DataFrame) -> QualityCheckResult:
        field = rule_config["field"]
        min_value = rule_config["min_value"]
        max_value = rule_config["max_value"]

        total = len(df)
        if total == 0:
            return cls._build_result(0, 0, 0, [])

        series = pd.to_numeric(df[field], errors="coerce")
        invalid_mask = series.isnull()
        out_of_range_mask = (series < min_value) | (series > max_value)
        violation_mask = invalid_mask | out_of_range_mask
        violation_count = violation_mask.sum()

        if violation_count > 0:
            samples = cls._collect_violation_samples(df, violation_mask)
            return cls._build_result(total, total - violation_count, violation_count, samples)
        else:
            return cls._build_result(total, total, 0, [])

    @classmethod
    def _check_regex_match(cls, rule_config: Dict[str, Any], df: pd.DataFrame) -> QualityCheckResult:
        field = rule_config["field"]
        pattern = rule_config["pattern"]

        total = len(df)
        if total == 0:
            return cls._build_result(0, 0, 0, [])

        regex = re.compile(pattern)
        series = df[field].astype(str)
        violation_mask = ~series.apply(lambda x: bool(regex.match(x) if pd.notna(x) else False))
        violation_count = violation_mask.sum()

        if violation_count > 0:
            samples = cls._collect_violation_samples(df, violation_mask)
            return cls._build_result(total, total - violation_count, violation_count, samples)
        else:
            return cls._build_result(total, total, 0, [])

    @classmethod
    def check_rule(cls, rule_type: str, rule_config: Dict[str, Any], df: pd.DataFrame) -> QualityCheckResult:
        checkers = {
            "non_null_rate": cls._check_non_null_rate,
            "uniqueness": cls._check_uniqueness,
            "range_check": cls._check_range,
            "regex_match": cls._check_regex_match
        }

        if rule_type not in checkers:
            raise ValueError(f"Unsupported rule type: {rule_type}")

        return checkers[rule_type](rule_config, df)

    @classmethod
    def _apply_mark_strategy(cls, df: pd.DataFrame, check_result: QualityCheckResult,
                             rule_type: str, rule_config: Dict[str, Any]) -> Tuple[pd.DataFrame, bool]:
        result_df = df.copy()

        if "is_dirty" not in result_df.columns:
            result_df["is_dirty"] = False
        if "violation_reason" not in result_df.columns:
            result_df["violation_reason"] = ""

        total = len(result_df)
        if total == 0 or check_result.failed_records == 0:
            return result_df, True

        violation_mask = result_df.index.isin(
            [sample.get("index") for sample in check_result.violation_samples if "index" in sample]
        )
        if violation_mask.sum() != check_result.failed_records:
            if rule_type == "non_null_rate":
                field = rule_config["field"]
                violation_mask = result_df[field].isnull()
            elif rule_type == "uniqueness":
                fields = rule_config["fields"]
                violation_mask = result_df.duplicated(subset=fields, keep=False)
            elif rule_type == "range_check":
                field = rule_config["field"]
                min_value = rule_config["min_value"]
                max_value = rule_config["max_value"]
                series = pd.to_numeric(result_df[field], errors="coerce")
                violation_mask = series.isnull() | (series < min_value) | (series > max_value)
            elif rule_type == "regex_match":
                field = rule_config["field"]
                pattern = rule_config["pattern"]
                regex = re.compile(pattern)
                series = result_df[field].astype(str)
                violation_mask = ~series.apply(lambda x: bool(regex.match(x) if pd.notna(x) else False))

        reason = f"{rule_type}: {str(rule_config)}"
        result_df.loc[violation_mask, "is_dirty"] = True
        result_df.loc[violation_mask, "violation_reason"] = result_df.loc[violation_mask, "violation_reason"].apply(
            lambda x: x + "; " + reason if x else reason
        )

        return result_df, True

    @classmethod
    def _apply_quarantine_strategy(cls, df: pd.DataFrame, check_result: QualityCheckResult,
                                   execution_id: int, node_id: str, rule_id: int,
                                   rule_type: str, rule_config: Dict[str, Any], db_session) -> Tuple[pd.DataFrame, bool]:
        total = len(df)
        if total == 0 or check_result.failed_records == 0:
            return df.copy(), True

        result_df = df.copy()
        violation_mask = pd.Series(False, index=result_df.index)

        if rule_type == "non_null_rate":
            field = rule_config["field"]
            violation_mask = result_df[field].isnull()
        elif rule_type == "uniqueness":
            fields = rule_config["fields"]
            violation_mask = result_df.duplicated(subset=fields, keep=False)
        elif rule_type == "range_check":
            field = rule_config["field"]
            min_value = rule_config["min_value"]
            max_value = rule_config["max_value"]
            series = pd.to_numeric(result_df[field], errors="coerce")
            violation_mask = series.isnull() | (series < min_value) | (series > max_value)
        elif rule_type == "regex_match":
            field = rule_config["field"]
            pattern = rule_config["pattern"]
            regex = re.compile(pattern)
            series = result_df[field].astype(str)
            violation_mask = ~series.apply(lambda x: bool(regex.match(x) if pd.notna(x) else False))

        quarantined_df = result_df[violation_mask].copy()
        clean_df = result_df[~violation_mask].copy()

        reason = f"{rule_type}: {str(rule_config)}"
        for _, row in quarantined_df.iterrows():
            quarantine_record = QuarantineRecord(
                execution_id=execution_id,
                node_id=node_id,
                rule_id=rule_id,
                record_data=row.to_dict(),
                violation_reason=reason
            )
            db_session.add(quarantine_record)
        db_session.commit()

        return clean_df, True

    @classmethod
    def _apply_abort_strategy(cls, check_result: QualityCheckResult) -> Tuple[pd.DataFrame, bool]:
        if check_result.failed_records > 0:
            raise RuntimeError(
                f"Data quality check failed. {check_result.failed_records} records violated the rule. "
                f"Pass rate: {check_result.pass_rate}%"
            )
        return pd.DataFrame(), True

    @classmethod
    def apply_failure_strategy(cls, strategy: str, df: pd.DataFrame, check_result: QualityCheckResult,
                               execution_id: int, node_id: str, rule_id: int, db_session) -> Tuple[pd.DataFrame, bool]:
        rule = db_session.query(DataQualityRule).filter(DataQualityRule.id == rule_id).first()
        if not rule:
            raise ValueError(f"Data quality rule not found: {rule_id}")

        rule_type = rule.rule_type
        rule_config = rule.rule_config

        strategies = {
            "mark": lambda: cls._apply_mark_strategy(df, check_result, rule_type, rule_config),
            "quarantine": lambda: cls._apply_quarantine_strategy(
                df, check_result, execution_id, node_id, rule_id, rule_type, rule_config, db_session
            ),
            "abort": lambda: cls._apply_abort_strategy(check_result)
        }

        if strategy not in strategies:
            raise ValueError(f"Unsupported failure strategy: {strategy}")

        result_df, should_continue = strategies[strategy]()

        if isinstance(result_df, pd.DataFrame) and result_df.empty and strategy != "abort":
            return df.copy(), should_continue

        return result_df, should_continue

    @classmethod
    def generate_report(cls, execution_id: int, node_id: str, rule: DataQualityRule,
                        check_result: QualityCheckResult, db_session) -> QualityReport:
        report = QualityReport(
            execution_id=execution_id,
            rule_id=rule.id,
            node_id=node_id,
            rule_type=rule.rule_type,
            rule_config=rule.rule_config,
            total_records=check_result.total_records,
            passed_records=check_result.passed_records,
            failed_records=check_result.failed_records,
            pass_rate=check_result.pass_rate,
            violation_samples=check_result.violation_samples
        )
        db_session.add(report)
        db_session.commit()
        db_session.refresh(report)
        return report
