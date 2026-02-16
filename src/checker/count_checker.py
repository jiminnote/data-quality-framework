"""
건수 검증 모듈 (Count Checker)
===============================
소스 테이블과 타겟 테이블 간 건수를 비교하여 ETL 정합성을 검증합니다.

기능:
  - 소스 vs 타겟 테이블 건수 비교
  - 허용 오차율(threshold) 설정 가능
  - WHERE 조건별 건수 비교 지원
  - ★ TS-1: 대용량 테이블 청크 분할 카운트 지원
"""

import logging
from .base_checker import BaseChecker, CheckResult, CheckStatus

logger = logging.getLogger(__name__)


class CountChecker(BaseChecker):
    """소스/타겟 테이블 건수 비교 검증"""

    # ★ TS-1: 이 건수 이상이면 청크 분할 카운트 사용
    CHUNK_THRESHOLD = 500000

    def run_checks(self) -> list[CheckResult]:
        """모든 건수 검증 규칙을 실행합니다."""
        logger.info("=" * 50)
        logger.info("📊 건수 검증 시작 (%d개 규칙)", len(self.rules))
        logger.info("=" * 50)

        for rule in self.rules:
            try:
                self._run_single_check(rule)
            except Exception as e:
                self._make_error_result(rule, "count", e)

        return self.results

    def _run_single_check(self, rule: dict) -> CheckResult:
        """단일 건수 검증 규칙을 실행합니다."""
        rule_id = rule["rule_id"]
        source_table = rule["source_table"]
        target_table = rule["target_table"]
        threshold = rule.get("threshold", 0.0)
        where_clause = rule.get("where_clause")

        logger.info("[%s] %s", rule_id, rule["description"])

        # 커스텀 쿼리가 있는 경우
        if "source_count_query" in rule and "target_count_query" in rule:
            source_count = self.db.execute_scalar(rule["source_count_query"])
            target_count = self.db.execute_scalar(rule["target_count_query"])
        else:
            # ★ TS-1: 대용량 테이블은 청크 분할 카운트 적용
            source_count = self._get_count(source_table, where_clause)
            target_count = self._get_count(target_table, where_clause)

        # 오차율 계산
        if source_count == 0:
            diff_ratio = 0.0 if target_count == 0 else 1.0
        else:
            diff_ratio = abs(source_count - target_count) / source_count

        # 결과 판정
        if diff_ratio <= threshold:
            status = CheckStatus.PASS
        else:
            status = CheckStatus.FAIL

        violation_count = abs(source_count - target_count)

        result = self._make_result(
            rule=rule,
            check_type="count",
            status=status,
            total_rows=source_count,
            violation_count=violation_count,
            details={
                "source_table": source_table,
                "target_table": target_table,
                "source_count": source_count,
                "target_count": target_count,
                "diff_ratio": round(diff_ratio, 6),
                "threshold": threshold,
                "where_clause": where_clause,
            },
        )

        status_icon = "✅" if status == CheckStatus.PASS else "❌"
        logger.info(
            "  %s 소스=%d / 타겟=%d / 차이율=%.4f%% (임계=%s%%)",
            status_icon, source_count, target_count,
            diff_ratio * 100, threshold * 100,
        )

        return result

    def _get_count(self, table: str, where_clause: str = None) -> int:
        """
        테이블 건수를 조회합니다.

        ★ TS-1: 대용량 테이블(50만 건 이상)은 청크 분할 카운트 사용
        일반 COUNT(*)는 인덱스 없는 테이블에서 타임아웃 위험이 있습니다.
        """
        # 먼저 대략적인 건수 파악 (information_schema 활용)
        approx_count = self.db.execute_scalar(
            "SELECT TABLE_ROWS FROM information_schema.TABLES "
            "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s",
            (table,)
        )

        if approx_count and approx_count > self.CHUNK_THRESHOLD and not where_clause:
            logger.info("  ⚡ 대용량 테이블 감지 (%s ≈ %d건) → 청크 분할 카운트 적용", table, approx_count)
            return self.db.execute_chunked_count(table)
        else:
            return self.db.execute_count(table, where_clause)
