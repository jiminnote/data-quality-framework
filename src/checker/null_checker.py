"""
NULL 검증 모듈 (Null Checker)
==============================
필수 컬럼의 NULL 비율을 검사하여 데이터 완전성을 검증합니다.

기능:
  - 컬럼별 NULL 비율 체크
  - 임계치(max_null_ratio) 기반 PASS/FAIL 판정
  - ★ TS-2: NULL vs 빈 문자열('') 구분 이슈 해결
    - include_empty_string 옵션으로 빈 문자열도 NULL로 간주
    - COALESCE(NULLIF(TRIM(col), ''), NULL) IS NULL 패턴 적용
"""

import logging
from .base_checker import BaseChecker, CheckResult, CheckStatus

logger = logging.getLogger(__name__)


class NullChecker(BaseChecker):
    """필수 컬럼 NULL 비율 검증"""

    def run_checks(self) -> list[CheckResult]:
        """모든 NULL 검증 규칙을 실행합니다."""
        logger.info("=" * 50)
        logger.info("🔍 NULL 검증 시작 (%d개 규칙)", len(self.rules))
        logger.info("=" * 50)

        for rule in self.rules:
            try:
                self._run_single_check(rule)
            except Exception as e:
                self._make_error_result(rule, "null", e)

        return self.results

    def _run_single_check(self, rule: dict) -> CheckResult:
        """단일 NULL 검증 규칙을 실행합니다."""
        rule_id = rule["rule_id"]
        table = rule["table"]
        column = rule["column"]
        max_null_ratio = rule.get("max_null_ratio", 0.0)
        include_empty_string = rule.get("include_empty_string", False)

        logger.info("[%s] %s", rule_id, rule["description"])

        # 전체 건수
        total_rows = self.db.execute_count(table)

        if total_rows == 0:
            return self._make_result(
                rule=rule,
                check_type="null",
                status=CheckStatus.WARNING,
                details={"message": "테이블이 비어있습니다."},
            )

        # ★ TS-2: NULL vs 빈 문자열 구분 이슈 해결
        # 기존: IS NULL만 체크 → 빈 문자열 누락
        # 해결: COALESCE(NULLIF(TRIM(col), ''), NULL) IS NULL 패턴
        if include_empty_string:
            null_count_query = f"""
                SELECT COUNT(*) FROM {table}
                WHERE COALESCE(NULLIF(TRIM({column}), ''), NULL) IS NULL
            """
            # 순수 NULL과 빈 문자열 건수를 각각 집계 (상세 리포트용)
            detail_query = f"""
                SELECT
                    SUM(CASE WHEN {column} IS NULL THEN 1 ELSE 0 END) AS pure_null_count,
                    SUM(CASE WHEN {column} IS NOT NULL AND TRIM({column}) = '' THEN 1 ELSE 0 END) AS empty_string_count
                FROM {table}
            """
        else:
            null_count_query = f"""
                SELECT COUNT(*) FROM {table}
                WHERE {column} IS NULL
            """
            detail_query = None

        null_count = self.db.execute_scalar(null_count_query)
        null_ratio = null_count / total_rows

        # 상세 정보 수집
        details = {
            "max_null_ratio": max_null_ratio,
            "actual_null_ratio": round(null_ratio, 6),
            "include_empty_string": include_empty_string,
        }

        if detail_query:
            detail_result = self.db.execute_query(detail_query)
            if detail_result:
                details["pure_null_count"] = detail_result[0].get("pure_null_count", 0)
                details["empty_string_count"] = detail_result[0].get("empty_string_count", 0)

        # 결과 판정
        if null_ratio <= max_null_ratio:
            status = CheckStatus.PASS
        elif null_ratio <= max_null_ratio * 2:
            status = CheckStatus.WARNING
        else:
            status = CheckStatus.FAIL

        result = self._make_result(
            rule=rule,
            check_type="null",
            status=status,
            total_rows=total_rows,
            violation_count=null_count,
            details=details,
        )

        status_icon = "✅" if status == CheckStatus.PASS else ("⚠️" if status == CheckStatus.WARNING else "❌")
        logger.info(
            "  %s %s.%s: NULL %d건 (%.2f%%) / 임계 %.2f%%",
            status_icon, table, column, null_count, null_ratio * 100, max_null_ratio * 100,
        )

        if include_empty_string and details.get("empty_string_count", 0) > 0:
            logger.info(
                "  ℹ️  빈 문자열 %d건 추가 검출 (TS-2 적용)",
                details["empty_string_count"],
            )

        return result
