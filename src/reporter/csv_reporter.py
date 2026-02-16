"""
CSV 검증 리포트 생성기
=======================
검증 결과를 CSV 파일로 출력합니다. (후속 분석용)
"""

import csv
import os
import json
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class CSVReporter:
    """CSV 검증 리포트 생성기"""

    # CSV 컬럼 순서
    COLUMNS = [
        "rule_id",
        "check_type",
        "description",
        "table_name",
        "column_name",
        "status",
        "total_rows",
        "violation_count",
        "violation_ratio",
        "details",
        "executed_at",
    ]

    def __init__(self, report_dir: str = None):
        """
        Args:
            report_dir: 리포트 저장 디렉토리 (기본: reports/)
        """
        if report_dir is None:
            base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            report_dir = os.path.join(base_dir, "..", "reports")
        self.report_dir = os.path.abspath(report_dir)
        os.makedirs(self.report_dir, exist_ok=True)

    def generate(self, results: list, summary: dict = None) -> str:
        """
        검증 결과를 CSV 파일로 생성합니다.

        Args:
            results: CheckResult 객체 리스트 (또는 dict 리스트)
            summary: 요약 정보 (CSV 하단에 추가)

        Returns:
            생성된 CSV 파일 경로
        """
        # dict 리스트로 통일
        result_dicts = []
        for r in results:
            if hasattr(r, "to_dict"):
                result_dicts.append(r.to_dict())
            elif isinstance(r, dict):
                result_dicts.append(r)

        # 파일 저장
        filename = f"dq_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        filepath = os.path.join(self.report_dir, filename)

        with open(filepath, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=self.COLUMNS, extrasaction="ignore")
            writer.writeheader()

            for row in result_dicts:
                # details를 JSON 문자열로 변환
                row_copy = dict(row)
                if isinstance(row_copy.get("details"), dict):
                    row_copy["details"] = json.dumps(
                        row_copy["details"], ensure_ascii=False, default=str
                    )
                writer.writerow(row_copy)

            # 요약 행 추가
            if summary:
                writer.writerow({})  # 빈 행
                writer.writerow({
                    "rule_id": "SUMMARY",
                    "check_type": "-",
                    "description": f"전체 {summary.get('total_checks', 0)}건 | "
                                   f"PASS {summary.get('passed', 0)} | "
                                   f"FAIL {summary.get('failed', 0)} | "
                                   f"WARNING {summary.get('warnings', 0)} | "
                                   f"ERROR {summary.get('errors', 0)}",
                    "status": f"통과율 {summary.get('pass_rate', 0)}%",
                    "executed_at": datetime.now().isoformat(),
                })

        logger.info("📄 CSV 리포트 생성: %s", filepath)
        return filepath
