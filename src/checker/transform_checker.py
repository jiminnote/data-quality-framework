"""
변환 로직 검증 모듈 (Transform Checker)
========================================
ETL 변환 전후 데이터 정합성을 검증합니다.

기능:
  - 소스/타겟 특정 컬럼 값 일치 여부 (JOIN 기반)
  - 집계 테이블의 합계 = 원본 합계 검증
  - 커스텀 소스/타겟 쿼리 기반 비교
"""

import logging
from decimal import Decimal
from .base_checker import BaseChecker, CheckResult, CheckStatus

logger = logging.getLogger(__name__)


class TransformChecker(BaseChecker):
    """ETL 변환 전후 데이터 정합성 검증"""

    def run_checks(self) -> list[CheckResult]:
        """모든 변환 로직 검증 규칙을 실행합니다."""
        logger.info("=" * 50)
        logger.info("🔄 변환 로직 검증 시작 (%d개 규칙)", len(self.rules))
        logger.info("=" * 50)

        for rule in self.rules:
            try:
                compare_type = rule.get("compare_type", "value")
                if compare_type == "existence":
                    self._run_existence_check(rule)
                elif "join_key" in rule:
                    self._run_join_compare(rule)
                else:
                    self._run_aggregate_compare(rule)
            except Exception as e:
                self._make_error_result(rule, "transform", e)

        return self.results

    def _run_aggregate_compare(self, rule: dict) -> CheckResult:
        """집계 값 비교 (소스 합계 vs 타겟 합계)"""
        rule_id = rule["rule_id"]
        source_query = rule["source_query"]
        target_query = rule["target_query"]
        compare_column = rule["compare_column"]
        tolerance = rule.get("tolerance", 0)

        logger.info("[%s] %s", rule_id, rule["description"])

        source_result = self.db.execute_query(source_query)
        target_result = self.db.execute_query(target_query)

        if not source_result or not target_result:
            return self._make_result(
                rule=rule,
                check_type="transform",
                status=CheckStatus.WARNING,
                details={"message": "소스 또는 타겟 결과가 비어있습니다."},
            )

        source_value = source_result[0].get(compare_column)
        target_value = target_result[0].get(compare_column)

        # Decimal → float 변환
        if isinstance(source_value, Decimal):
            source_value = float(source_value)
        if isinstance(target_value, Decimal):
            target_value = float(target_value)

        if source_value is None or target_value is None:
            status = CheckStatus.WARNING
            diff = None
        elif source_value == 0:
            status = CheckStatus.PASS if target_value == 0 else CheckStatus.FAIL
            diff = abs(target_value)
        else:
            diff = abs(source_value - target_value)
            diff_ratio = diff / abs(source_value)
            status = CheckStatus.PASS if diff_ratio <= tolerance else CheckStatus.FAIL

        result = self._make_result(
            rule=rule,
            check_type="transform",
            status=status,
            details={
                "source_value": source_value,
                "target_value": target_value,
                "difference": diff,
                "tolerance": tolerance,
                "compare_column": compare_column,
            },
        )

        status_icon = "✅" if status == CheckStatus.PASS else "❌"
        logger.info(
            "  %s 소스=%s / 타겟=%s / 차이=%s",
            status_icon, source_value, target_value, diff,
        )
        return result

    def _run_join_compare(self, rule: dict) -> CheckResult:
        """JOIN 기반 행 단위 비교"""
        rule_id = rule["rule_id"]
        source_query = rule["source_query"]
        target_query = rule["target_query"]
        join_key = rule["join_key"]
        compare_column = rule["compare_column"]
        tolerance = rule.get("tolerance", 0)

        logger.info("[%s] %s", rule_id, rule["description"])

        # 소스 결과를 딕셔너리로 변환 (join_key → compare_column)
        source_rows = self.db.execute_query(source_query)
        target_rows = self.db.execute_query(target_query)

        source_map = {}
        for row in source_rows:
            key = str(row[join_key])
            source_map[key] = row[compare_column]

        target_map = {}
        for row in target_rows:
            key = str(row[join_key])
            target_map[key] = row[compare_column]

        total_keys = len(set(source_map.keys()) | set(target_map.keys()))
        mismatch_count = 0
        missing_in_target = 0
        missing_in_source = 0
        value_mismatches = []

        for key in source_map:
            if key not in target_map:
                missing_in_target += 1
                mismatch_count += 1
            else:
                src_val = source_map[key]
                tgt_val = target_map[key]

                # Decimal 변환
                if isinstance(src_val, Decimal):
                    src_val = float(src_val)
                if isinstance(tgt_val, Decimal):
                    tgt_val = float(tgt_val)

                if src_val is None and tgt_val is None:
                    continue
                elif src_val is None or tgt_val is None:
                    mismatch_count += 1
                    value_mismatches.append({"key": key, "source": src_val, "target": tgt_val})
                elif abs(float(src_val) - float(tgt_val)) > tolerance:
                    mismatch_count += 1
                    if len(value_mismatches) < 5:  # 샘플 5건만
                        value_mismatches.append({"key": key, "source": src_val, "target": tgt_val})

        for key in target_map:
            if key not in source_map:
                missing_in_source += 1
                mismatch_count += 1

        status = CheckStatus.PASS if mismatch_count == 0 else CheckStatus.FAIL

        result = self._make_result(
            rule=rule,
            check_type="transform",
            status=status,
            total_rows=total_keys,
            violation_count=mismatch_count,
            details={
                "join_key": join_key,
                "compare_column": compare_column,
                "missing_in_target": missing_in_target,
                "missing_in_source": missing_in_source,
                "value_mismatches_sample": value_mismatches,
            },
        )

        status_icon = "✅" if status == CheckStatus.PASS else "❌"
        logger.info(
            "  %s 전체 %d키 / 불일치 %d건 (타겟누락=%d, 소스누락=%d)",
            status_icon, total_keys, mismatch_count, missing_in_target, missing_in_source,
        )
        return result

    def _run_existence_check(self, rule: dict) -> CheckResult:
        """존재 여부 비교 (소스 레코드가 타겟에 모두 존재하는지)"""
        rule_id = rule["rule_id"]
        source_query = rule["source_query"]
        target_query = rule["target_query"]
        join_key = rule["join_key"]

        logger.info("[%s] %s", rule_id, rule["description"])

        source_rows = self.db.execute_query(source_query)
        target_rows = self.db.execute_query(target_query)

        source_keys = {str(row[join_key]) for row in source_rows}
        target_keys = {str(row[join_key]) for row in target_rows}

        missing_in_target = source_keys - target_keys
        missing_in_source = target_keys - source_keys

        total = len(source_keys)
        violation_count = len(missing_in_target)

        status = CheckStatus.PASS if violation_count == 0 else CheckStatus.FAIL

        result = self._make_result(
            rule=rule,
            check_type="transform",
            status=status,
            total_rows=total,
            violation_count=violation_count,
            details={
                "source_count": len(source_keys),
                "target_count": len(target_keys),
                "missing_in_target": violation_count,
                "missing_in_source": len(missing_in_source),
            },
        )

        status_icon = "✅" if status == CheckStatus.PASS else "❌"
        logger.info(
            "  %s 소스 %d건 / 타겟 %d건 / 타겟누락 %d건",
            status_icon, len(source_keys), len(target_keys), violation_count,
        )
        return result
