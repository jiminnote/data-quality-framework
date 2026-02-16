"""
공통 체커 인터페이스 (Base Checker)
====================================
모든 검증 체커의 기본 클래스를 정의합니다.
"""

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Optional

logger = logging.getLogger(__name__)


class CheckStatus(Enum):
    """검증 결과 상태"""
    PASS = "PASS"
    FAIL = "FAIL"
    WARNING = "WARNING"
    ERROR = "ERROR"


@dataclass
class CheckResult:
    """
    단일 검증 결과를 담는 데이터 클래스

    Attributes:
        rule_id: 검증 규칙 ID (e.g. CNT-001)
        check_type: 검증 유형 (count / null / duplicate / range / transform / masking)
        description: 검증 설명
        table_name: 대상 테이블
        column_name: 대상 컬럼 (없으면 None)
        status: 검증 결과 상태 (PASS / FAIL / WARNING / ERROR)
        total_rows: 전체 행 수
        violation_count: 위반 건수
        violation_ratio: 위반 비율 (0.0 ~ 1.0)
        details: 추가 상세 정보
        executed_at: 실행 시각
    """
    rule_id: str
    check_type: str
    description: str
    table_name: str
    column_name: Optional[str] = None
    status: CheckStatus = CheckStatus.PASS
    total_rows: int = 0
    violation_count: int = 0
    violation_ratio: float = 0.0
    details: dict = field(default_factory=dict)
    executed_at: datetime = field(default_factory=datetime.now)

    def to_dict(self) -> dict:
        """딕셔너리로 변환 (리포트 출력용)"""
        return {
            "rule_id": self.rule_id,
            "check_type": self.check_type,
            "description": self.description,
            "table_name": self.table_name,
            "column_name": self.column_name,
            "status": self.status.value,
            "total_rows": self.total_rows,
            "violation_count": self.violation_count,
            "violation_ratio": round(self.violation_ratio, 6),
            "details": self.details,
            "executed_at": self.executed_at.isoformat(),
        }


class BaseChecker(ABC):
    """
    모든 검증 체커의 기본 클래스

    서브클래스는 run_checks() 메서드를 구현해야 합니다.
    """

    def __init__(self, db_connector, rules: list[dict]):
        """
        Args:
            db_connector: DBConnector 인스턴스
            rules: 해당 유형의 검증 규칙 리스트
        """
        self.db = db_connector
        self.rules = rules
        self.results: list[CheckResult] = []

    @abstractmethod
    def run_checks(self) -> list[CheckResult]:
        """
        모든 규칙에 대해 검증을 수행하고 결과를 반환합니다.

        Returns:
            CheckResult 리스트
        """
        pass

    def _make_result(
        self,
        rule: dict,
        check_type: str,
        status: CheckStatus,
        total_rows: int = 0,
        violation_count: int = 0,
        details: dict = None,
    ) -> CheckResult:
        """CheckResult 객체를 생성하는 헬퍼 메서드"""
        violation_ratio = (
            violation_count / total_rows if total_rows > 0 else 0.0
        )
        result = CheckResult(
            rule_id=rule.get("rule_id", "UNKNOWN"),
            check_type=check_type,
            description=rule.get("description", ""),
            table_name=rule.get("table", rule.get("source_table", "")),
            column_name=rule.get("column"),
            status=status,
            total_rows=total_rows,
            violation_count=violation_count,
            violation_ratio=violation_ratio,
            details=details or {},
        )
        self.results.append(result)
        return result

    def _make_error_result(self, rule: dict, check_type: str, error: Exception) -> CheckResult:
        """에러 발생 시 CheckResult를 생성하는 헬퍼 메서드"""
        logger.error("[%s] %s 실행 오류: %s", rule.get("rule_id"), check_type, error)
        return self._make_result(
            rule=rule,
            check_type=check_type,
            status=CheckStatus.ERROR,
            details={"error": str(error)},
        )

    def get_summary(self) -> dict:
        """검증 결과 요약을 반환합니다."""
        total = len(self.results)
        passed = sum(1 for r in self.results if r.status == CheckStatus.PASS)
        failed = sum(1 for r in self.results if r.status == CheckStatus.FAIL)
        warnings = sum(1 for r in self.results if r.status == CheckStatus.WARNING)
        errors = sum(1 for r in self.results if r.status == CheckStatus.ERROR)

        return {
            "total_checks": total,
            "passed": passed,
            "failed": failed,
            "warnings": warnings,
            "errors": errors,
            "pass_rate": round(passed / total * 100, 2) if total > 0 else 0,
        }
