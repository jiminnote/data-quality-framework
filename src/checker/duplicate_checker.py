"""
중복 검증 모듈 (Duplicate Checker)
===================================
복합 키 기반 중복 레코드를 탐지합니다.

기능:
  - 단일/복합 키 기반 중복 탐지
  - 중복 레코드 상세 목록 추출 (리포트용)
  - PK 중복 체크
"""

import logging
from .base_checker import BaseChecker, CheckResult, CheckStatus

logger = logging.getLogger(__name__)

# 중복 검증은 YAML 규칙 대신 직접 호출 방식으로 사용
# (테이블/컬럼 조합이 다양하므로)
DEFAULT_DUPLICATE_RULES = [
    {
        "rule_id": "DUP-001",
        "description": "카드거래 PK 중복 체크",
        "table": "src_card_transactions",
        "columns": ["transaction_id"],
        "check_type": "pk",
    },
    {
        "rule_id": "DUP-002",
        "description": "카드거래 복합키 중복 체크 (고객+가맹점+일시+금액)",
        "table": "src_card_transactions",
        "columns": ["customer_id", "merchant_id", "transaction_date", "transaction_amount"],
        "check_type": "composite",
    },
    {
        "rule_id": "DUP-003",
        "description": "고객 이메일 중복 체크",
        "table": "src_customers",
        "columns": ["email"],
        "check_type": "unique",
    },
    {
        "rule_id": "DUP-004",
        "description": "고객 주민번호 중복 체크",
        "table": "src_customers",
        "columns": ["resident_number"],
        "check_type": "unique",
    },
    {
        "rule_id": "DUP-005",
        "description": "가맹점 코드 중복 체크",
        "table": "src_merchants",
        "columns": ["merchant_code"],
        "check_type": "unique",
    },
]


class DuplicateChecker(BaseChecker):
    """복합 키 기반 중복 레코드 탐지"""

    def __init__(self, db_connector, rules: list[dict] = None):
        super().__init__(db_connector, rules or DEFAULT_DUPLICATE_RULES)

    def run_checks(self) -> list[CheckResult]:
        """모든 중복 검증 규칙을 실행합니다."""
        logger.info("=" * 50)
        logger.info("🔁 중복 검증 시작 (%d개 규칙)", len(self.rules))
        logger.info("=" * 50)

        for rule in self.rules:
            try:
                self._run_single_check(rule)
            except Exception as e:
                self._make_error_result(rule, "duplicate", e)

        return self.results

    def _run_single_check(self, rule: dict) -> CheckResult:
        """단일 중복 검증 규칙을 실행합니다."""
        rule_id = rule["rule_id"]
        table = rule["table"]
        columns = rule["columns"]
        columns_str = ", ".join(columns)

        logger.info("[%s] %s", rule_id, rule["description"])

        # 전체 건수
        total_rows = self.db.execute_count(table)

        # 중복 건수 조회
        duplicate_query = f"""
            SELECT COUNT(*) AS dup_count FROM (
                SELECT {columns_str}, COUNT(*) AS cnt
                FROM {table}
                WHERE {' AND '.join(f'{col} IS NOT NULL' for col in columns)}
                GROUP BY {columns_str}
                HAVING COUNT(*) > 1
            ) AS dup_groups
        """
        dup_group_count = self.db.execute_scalar(duplicate_query)

        # 중복 레코드 총 건수 (그룹 수가 아닌 실제 중복 행 수)
        dup_rows_query = f"""
            SELECT COALESCE(SUM(cnt - 1), 0) AS dup_rows FROM (
                SELECT {columns_str}, COUNT(*) AS cnt
                FROM {table}
                WHERE {' AND '.join(f'{col} IS NOT NULL' for col in columns)}
                GROUP BY {columns_str}
                HAVING COUNT(*) > 1
            ) AS dup_groups
        """
        dup_row_count = self.db.execute_scalar(dup_rows_query) or 0

        # 중복 레코드 샘플 (상위 10건)
        sample_query = f"""
            SELECT {columns_str}, COUNT(*) AS duplicate_count
            FROM {table}
            WHERE {' AND '.join(f'{col} IS NOT NULL' for col in columns)}
            GROUP BY {columns_str}
            HAVING COUNT(*) > 1
            ORDER BY COUNT(*) DESC
            LIMIT 10
        """
        sample_records = self.db.execute_query(sample_query)

        # 결과 판정
        if dup_row_count == 0:
            status = CheckStatus.PASS
        else:
            status = CheckStatus.FAIL

        result = self._make_result(
            rule=rule,
            check_type="duplicate",
            status=status,
            total_rows=total_rows,
            violation_count=dup_row_count,
            details={
                "columns": columns,
                "duplicate_groups": dup_group_count,
                "duplicate_rows": dup_row_count,
                "sample_records": sample_records[:5],  # 리포트용 상위 5건
            },
        )

        status_icon = "✅" if status == CheckStatus.PASS else "❌"
        logger.info(
            "  %s %s [%s]: 중복 그룹 %d개, 중복 행 %d건",
            status_icon, table, columns_str, dup_group_count, dup_row_count,
        )

        return result
