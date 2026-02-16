"""
통합 실행 엔트리포인트 (main.py)
=================================
전체 데이터 품질 검증 파이프라인을 실행합니다.

실행 방법:
  python -m src.main                          # 기본 (development 환경)
  python -m src.main --env docker             # Docker 환경
  python -m src.main --env production         # Production 환경
  python -m src.main --checks count,null      # 특정 검증만 실행
  python -m src.main --report html            # HTML 리포트만 생성
"""

import argparse
import logging
import sys
import time
from datetime import datetime

from .config_loader import ConfigLoader
from .db_connector import DBConnector
from .checker import (
    CountChecker,
    NullChecker,
    DuplicateChecker,
    RangeChecker,
    TransformChecker,
    MaskingChecker,
    CheckResult,
)
from .reporter import HTMLReporter, CSVReporter

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("validation.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)


def parse_args():
    """커맨드라인 인자를 파싱합니다."""
    parser = argparse.ArgumentParser(
        description="Data Quality Framework - 통합 검증 실행"
    )
    parser.add_argument(
        "--env",
        type=str,
        default="development",
        choices=["development", "docker", "production"],
        help="DB 접속 환경 (기본: development)",
    )
    parser.add_argument(
        "--checks",
        type=str,
        default="all",
        help="실행할 검증 유형 (콤마 구분). 예: count,null,duplicate,range,transform,masking",
    )
    parser.add_argument(
        "--report",
        type=str,
        default="all",
        choices=["all", "html", "csv", "none"],
        help="생성할 리포트 유형 (기본: all)",
    )
    parser.add_argument(
        "--config-dir",
        type=str,
        default=None,
        help="설정 디렉토리 경로 (기본: 프로젝트 루트)",
    )
    return parser.parse_args()


def run_validation(env: str = "development", checks: str = "all",
                   report_type: str = "all", config_dir: str = None):
    """
    전체 검증 파이프라인을 실행합니다.

    Args:
        env: DB 접속 환경
        checks: 실행할 검증 유형 (콤마 구분 또는 "all")
        report_type: 리포트 유형 ("all", "html", "csv", "none")
        config_dir: 설정 디렉토리 경로

    Returns:
        (all_results, summary) 튜플
    """
    start_time = time.time()

    logger.info("=" * 60)
    logger.info("🚀 Data Quality Framework - 검증 시작")
    logger.info("   환경: %s | 검증: %s | 리포트: %s", env, checks, report_type)
    logger.info("=" * 60)

    # 1. 설정 로딩
    logger.info("\n📂 설정 로딩 중...")
    config = ConfigLoader(config_dir)
    db_config = config.load_db_config(env)
    all_rules = config.load_all_rules()
    report_dir = config.get_report_dir()

    logger.info("   DB: %s:%s/%s", db_config["host"], db_config["port"], db_config["database"])
    for rule_type, rules in all_rules.items():
        logger.info("   %s 규칙: %d개", rule_type, len(rules))

    # 2. DB 연결
    logger.info("\n🔌 데이터베이스 연결 중...")
    db = DBConnector(db_config)

    # 3. 검증 실행
    check_list = checks.split(",") if checks != "all" else [
        "count", "null", "duplicate", "range", "transform", "masking"
    ]

    all_results: list[CheckResult] = []

    try:
        if "count" in check_list:
            logger.info("")
            checker = CountChecker(db, all_rules.get("count", []))
            all_results.extend(checker.run_checks())

        if "null" in check_list:
            logger.info("")
            checker = NullChecker(db, all_rules.get("null", []))
            all_results.extend(checker.run_checks())

        if "duplicate" in check_list:
            logger.info("")
            checker = DuplicateChecker(db)
            all_results.extend(checker.run_checks())

        if "range" in check_list:
            logger.info("")
            checker = RangeChecker(db)
            all_results.extend(checker.run_checks())

        if "transform" in check_list:
            logger.info("")
            checker = TransformChecker(db, all_rules.get("transform", []))
            all_results.extend(checker.run_checks())

        if "masking" in check_list:
            logger.info("")
            checker = MaskingChecker(db, all_rules.get("masking", []))
            all_results.extend(checker.run_checks())

    finally:
        db.close()

    # 4. 요약 계산
    total = len(all_results)
    passed = sum(1 for r in all_results if r.status.value == "PASS")
    failed = sum(1 for r in all_results if r.status.value == "FAIL")
    warnings = sum(1 for r in all_results if r.status.value == "WARNING")
    errors = sum(1 for r in all_results if r.status.value == "ERROR")

    summary = {
        "total_checks": total,
        "passed": passed,
        "failed": failed,
        "warnings": warnings,
        "errors": errors,
        "pass_rate": round(passed / total * 100, 2) if total > 0 else 0,
    }

    elapsed = round(time.time() - start_time, 2)

    logger.info("")
    logger.info("=" * 60)
    logger.info("📊 검증 결과 요약")
    logger.info("=" * 60)
    logger.info("   전체: %d건", total)
    logger.info("   ✅ PASS: %d건", passed)
    logger.info("   ❌ FAIL: %d건", failed)
    logger.info("   ⚠️  WARNING: %d건", warnings)
    logger.info("   🔴 ERROR: %d건", errors)
    logger.info("   📈 통과율: %.1f%%", summary["pass_rate"])
    logger.info("   ⏱️  소요 시간: %s초", elapsed)

    # 5. 리포트 생성
    if report_type != "none":
        logger.info("\n📄 리포트 생성 중...")

        if report_type in ("all", "html"):
            html_reporter = HTMLReporter(report_dir)
            html_path = html_reporter.generate(all_results, summary)
            logger.info("   HTML: %s", html_path)

        if report_type in ("all", "csv"):
            csv_reporter = CSVReporter(report_dir)
            csv_path = csv_reporter.generate(all_results, summary)
            logger.info("   CSV: %s", csv_path)

    logger.info("")
    logger.info("✨ 검증 완료! (소요 시간: %s초)", elapsed)

    return all_results, summary


def main():
    """메인 함수"""
    args = parse_args()

    try:
        results, summary = run_validation(
            env=args.env,
            checks=args.checks,
            report_type=args.report,
            config_dir=args.config_dir,
        )

        # FAIL이 있으면 exit code 1
        if summary["failed"] > 0:
            sys.exit(1)

    except ConnectionError as e:
        logger.error("🔴 DB 연결 실패: %s", e)
        sys.exit(2)
    except Exception as e:
        logger.error("🔴 예기치 않은 오류: %s", e, exc_info=True)
        sys.exit(3)


if __name__ == "__main__":
    main()
