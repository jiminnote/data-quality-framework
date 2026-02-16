"""
범위 검증 모듈 (Range Checker)
================================
숫자/날짜 컬럼의 유효 범위를 검증합니다.

기능:
  - 숫자 컬럼 min/max 범위 체크
  - 날짜 컬럼 유효 범위 체크 (미래 날짜, 1970년 이전 등)
  - 양수 값 체크
  - FK 정합성 체크 (참조 무결성)
"""

import logging
from .base_checker import BaseChecker, CheckResult, CheckStatus

logger = logging.getLogger(__name__)

# 범위 검증 규칙 (금융 도메인 기반)
DEFAULT_RANGE_RULES = [
    {
        "rule_id": "RNG-001",
        "description": "거래 금액 양수 체크",
        "table": "src_card_transactions",
        "column": "transaction_amount",
        "check_type": "positive",
        "min_value": 0,
        "max_value": None,
    },
    {
        "rule_id": "RNG-002",
        "description": "거래 금액 상한 체크 (5억원 이하)",
        "table": "src_card_transactions",
        "column": "transaction_amount",
        "check_type": "range",
        "min_value": 0,
        "max_value": 500000000,
    },
    {
        "rule_id": "RNG-003",
        "description": "거래일시 유효 범위 체크 (2024년)",
        "table": "src_card_transactions",
        "column": "transaction_date",
        "check_type": "date_range",
        "min_date": "2024-01-01",
        "max_date": "2024-12-31",
    },
    {
        "rule_id": "RNG-004",
        "description": "미래 거래일시 체크",
        "table": "src_card_transactions",
        "column": "transaction_date",
        "check_type": "no_future",
    },
    {
        "rule_id": "RNG-005",
        "description": "할부 개월수 유효 범위 (0~36개월)",
        "table": "src_card_transactions",
        "column": "installment_months",
        "check_type": "range",
        "min_value": 0,
        "max_value": 36,
    },
    {
        "rule_id": "RNG-006",
        "description": "고객 생년월일 유효 범위",
        "table": "src_customers",
        "column": "birth_date",
        "check_type": "date_range",
        "min_date": "1920-01-01",
        "max_date": "2010-12-31",
    },
    # FK 정합성 체크
    {
        "rule_id": "FK-001",
        "description": "거래→가맹점 FK 정합성 체크",
        "table": "src_card_transactions",
        "column": "merchant_id",
        "check_type": "foreign_key",
        "parent_table": "src_merchants",
        "parent_column": "merchant_id",
    },
    {
        "rule_id": "FK-002",
        "description": "거래→고객 FK 정합성 체크",
        "table": "src_card_transactions",
        "column": "customer_id",
        "check_type": "foreign_key",
        "parent_table": "src_customers",
        "parent_column": "customer_id",
    },
]


class RangeChecker(BaseChecker):
    """숫자/날짜 컬럼 유효 범위 검증 + FK 정합성 검증"""

    def __init__(self, db_connector, rules: list[dict] = None):
        super().__init__(db_connector, rules or DEFAULT_RANGE_RULES)

    def run_checks(self) -> list[CheckResult]:
        """모든 범위 검증 규칙을 실행합니다."""
        logger.info("=" * 50)
        logger.info("📏 범위/FK 검증 시작 (%d개 규칙)", len(self.rules))
        logger.info("=" * 50)

        for rule in self.rules:
            try:
                check_type = rule.get("check_type", "range")
                if check_type == "foreign_key":
                    self._run_fk_check(rule)
                elif check_type == "date_range":
                    self._run_date_range_check(rule)
                elif check_type == "no_future":
                    self._run_no_future_check(rule)
                else:
                    self._run_numeric_range_check(rule)
            except Exception as e:
                self._make_error_result(rule, "range", e)

        return self.results

    def _run_numeric_range_check(self, rule: dict) -> CheckResult:
        """숫자 범위 검증"""
        rule_id = rule["rule_id"]
        table = rule["table"]
        column = rule["column"]
        min_value = rule.get("min_value")
        max_value = rule.get("max_value")

        logger.info("[%s] %s", rule_id, rule["description"])

        total_rows = self.db.execute_scalar(
            f"SELECT COUNT(*) FROM {table} WHERE {column} IS NOT NULL"
        )

        conditions = []
        if min_value is not None:
            conditions.append(f"{column} < {min_value}")
        if max_value is not None:
            conditions.append(f"{column} > {max_value}")

        condition_str = " OR ".join(conditions) if conditions else "1=0"

        result_query = f"""
            SELECT
                COUNT(CASE WHEN {condition_str} THEN 1 END) AS violation_count,
                MIN({column}) AS actual_min,
                MAX({column}) AS actual_max
            FROM {table}
            WHERE {column} IS NOT NULL
        """
        query_result = self.db.execute_query(result_query)[0]
        violation_count = query_result["violation_count"]

        status = CheckStatus.PASS if violation_count == 0 else CheckStatus.FAIL

        result = self._make_result(
            rule=rule,
            check_type="range",
            status=status,
            total_rows=total_rows,
            violation_count=violation_count,
            details={
                "expected_min": min_value,
                "expected_max": max_value,
                "actual_min": str(query_result["actual_min"]),
                "actual_max": str(query_result["actual_max"]),
            },
        )

        status_icon = "✅" if status == CheckStatus.PASS else "❌"
        logger.info("  %s 범위 위반 %d건", status_icon, violation_count)
        return result

    def _run_date_range_check(self, rule: dict) -> CheckResult:
        """날짜 범위 검증"""
        rule_id = rule["rule_id"]
        table = rule["table"]
        column = rule["column"]
        min_date = rule.get("min_date")
        max_date = rule.get("max_date")

        logger.info("[%s] %s", rule_id, rule["description"])

        total_rows = self.db.execute_scalar(
            f"SELECT COUNT(*) FROM {table} WHERE {column} IS NOT NULL"
        )

        conditions = []
        if min_date:
            conditions.append(f"{column} < '{min_date}'")
        if max_date:
            conditions.append(f"{column} > '{max_date}'")

        condition_str = " OR ".join(conditions) if conditions else "1=0"

        violation_count = self.db.execute_scalar(
            f"SELECT COUNT(*) FROM {table} WHERE {column} IS NOT NULL AND ({condition_str})"
        )

        status = CheckStatus.PASS if violation_count == 0 else CheckStatus.FAIL

        result = self._make_result(
            rule=rule,
            check_type="range",
            status=status,
            total_rows=total_rows,
            violation_count=violation_count,
            details={"min_date": min_date, "max_date": max_date},
        )

        status_icon = "✅" if status == CheckStatus.PASS else "❌"
        logger.info("  %s 날짜 범위 위반 %d건", status_icon, violation_count)
        return result

    def _run_no_future_check(self, rule: dict) -> CheckResult:
        """미래 날짜 검증"""
        rule_id = rule["rule_id"]
        table = rule["table"]
        column = rule["column"]

        logger.info("[%s] %s", rule_id, rule["description"])

        total_rows = self.db.execute_scalar(
            f"SELECT COUNT(*) FROM {table} WHERE {column} IS NOT NULL"
        )
        violation_count = self.db.execute_scalar(
            f"SELECT COUNT(*) FROM {table} WHERE {column} > NOW()"
        )

        status = CheckStatus.PASS if violation_count == 0 else CheckStatus.FAIL

        result = self._make_result(
            rule=rule,
            check_type="range",
            status=status,
            total_rows=total_rows,
            violation_count=violation_count,
            details={"check": "no_future_date"},
        )

        status_icon = "✅" if status == CheckStatus.PASS else "❌"
        logger.info("  %s 미래 날짜 %d건", status_icon, violation_count)
        return result

    def _run_fk_check(self, rule: dict) -> CheckResult:
        """FK 정합성 검증"""
        rule_id = rule["rule_id"]
        table = rule["table"]
        column = rule["column"]
        parent_table = rule["parent_table"]
        parent_column = rule["parent_column"]

        logger.info("[%s] %s", rule_id, rule["description"])

        total_rows = self.db.execute_scalar(
            f"SELECT COUNT(*) FROM {table} WHERE {column} IS NOT NULL"
        )

        # 고아 레코드 (부모 테이블에 없는 FK)
        orphan_query = f"""
            SELECT COUNT(*) FROM {table} c
            WHERE c.{column} IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1 FROM {parent_table} p
                  WHERE p.{parent_column} = c.{column}
              )
        """
        orphan_count = self.db.execute_scalar(orphan_query)

        status = CheckStatus.PASS if orphan_count == 0 else CheckStatus.FAIL

        result = self._make_result(
            rule=rule,
            check_type="foreign_key",
            status=status,
            total_rows=total_rows,
            violation_count=orphan_count,
            details={
                "parent_table": parent_table,
                "parent_column": parent_column,
                "orphan_count": orphan_count,
            },
        )

        status_icon = "✅" if status == CheckStatus.PASS else "❌"
        logger.info("  %s FK 위반 (고아 레코드) %d건", status_icon, orphan_count)
        return result
