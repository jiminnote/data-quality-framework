"""
Data Quality Framework - 통합 프레임워크 사용 예제
================================================

이 스크립트는 data-quality-framework의 핵심 모듈을 활용하여
금융 데이터 품질 검증을 실행하는 예제입니다.

사용법:
  # 프로젝트 루트에서 실행
  python -m examples.sample_validation

  # 또는 직접 실행
  cd examples && python sample_validation.py
"""

import sys
import os

# 프로젝트 루트를 sys.path에 추가 (직접 실행 시)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.checker.base_checker import CheckResult, CheckStatus
from src.checker.count_checker import CountChecker
from src.checker.null_checker import NullChecker
from src.checker.duplicate_checker import DuplicateChecker
from src.checker.range_checker import RangeChecker
from src.checker.masking_checker import MaskingChecker
from src.reporter.html_reporter import HTMLReporter
from src.reporter.csv_reporter import CSVReporter


# ---------------------------------------------------------------------------
# 1) SQLite 기반 Mock DB Connector (MySQL 없이 로컬 테스트용)
# ---------------------------------------------------------------------------
class MockDBConnector:
    """
    SQLite 인메모리 DB를 사용하는 테스트용 커넥터.
    MySQL이 없는 환경에서도 프레임워크 동작을 확인할 수 있습니다.
    """

    def __init__(self):
        import sqlite3
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        self._init_schema()
        self._init_data()

    # -- 스키마 ----------------------------------------------------------
    def _init_schema(self):
        cur = self.conn.cursor()
        cur.executescript("""
            -- 소스 테이블
            CREATE TABLE src_customers (
                customer_id   INTEGER PRIMARY KEY,
                name          TEXT,
                email         TEXT,
                phone         TEXT,
                ssn           TEXT,
                birth_date    TEXT,
                created_at    TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE src_merchants (
                merchant_id   INTEGER PRIMARY KEY,
                merchant_name TEXT,
                category      TEXT
            );

            CREATE TABLE src_card_transactions (
                txn_id        INTEGER PRIMARY KEY,
                customer_id   INTEGER,
                merchant_id   INTEGER,
                txn_amount    REAL,
                txn_date      TEXT,
                status        TEXT DEFAULT 'NORMAL'
            );

            -- 타겟 테이블 (ETL 결과)
            CREATE TABLE tgt_customers (
                customer_id   INTEGER PRIMARY KEY,
                name_hash     TEXT,
                email         TEXT,
                phone_masked  TEXT,
                ssn_masked    TEXT,
                birth_date    TEXT
            );

            CREATE TABLE tgt_card_transactions (
                txn_id        INTEGER PRIMARY KEY,
                customer_id   INTEGER,
                merchant_id   INTEGER,
                txn_amount    REAL,
                txn_date      TEXT,
                status        TEXT
            );
        """)
        self.conn.commit()

    # -- 샘플 데이터 (의도적 품질 이슈 포함) --------------------------------
    def _init_data(self):
        cur = self.conn.cursor()

        # 가맹점 100개
        for i in range(1, 101):
            cur.execute(
                "INSERT INTO src_merchants VALUES (?, ?, ?)",
                (i, f"가맹점_{i:04d}", "음식점" if i % 3 == 0 else "일반"),
            )

        # 고객 500명 (일부 NULL, 빈 문자열 포함)
        for i in range(1, 501):
            name = f"고객_{i:05d}"
            email = f"user{i}@test.com" if i % 20 != 0 else None  # 5% NULL
            phone = f"010-{i:04d}-{(i*7)%10000:04d}"
            ssn = f"{900101+i%999999:06d}-{1000000+i:07d}"
            # TS-2: 10건은 빈 문자열
            if i % 50 == 0:
                email = ""
            cur.execute(
                "INSERT INTO src_customers VALUES (?, ?, ?, ?, ?, ?, datetime('now'))",
                (i, name, email, phone, ssn,
                 f"199{i%10}-{(i%12)+1:02d}-{(i%28)+1:02d}"),
            )

        # 거래 2000건 (NULL 금액, 중복 포함)
        for i in range(1, 2001):
            cid = (i % 500) + 1
            mid = (i % 100) + 1
            amount = round(1000 + (i * 7.3) % 50000, 2)
            # 의도적 NULL 금액 20건
            if i % 100 == 0:
                amount = None
            # 의도적 음수 금액 5건
            if i % 400 == 0:
                amount = -999.99
            txn_date = f"2024-{(i%12)+1:02d}-{(i%28)+1:02d}"
            cur.execute(
                "INSERT INTO src_card_transactions VALUES (?, ?, ?, ?, ?, 'NORMAL')",
                (i, cid, mid, amount, txn_date),
            )

        # 중복 거래 10건 (동일 customer_id + txn_date + txn_amount)
        for i in range(2001, 2011):
            cur.execute(
                "INSERT INTO src_card_transactions VALUES "
                "(?, 1, 1, 5000.00, '2024-01-15', 'NORMAL')",
                (i,),
            )

        # 타겟 고객 (마스킹 적용 — 일부 누락)
        for i in range(1, 501):
            phone_masked = f"010-****-{(i*7)%10000:04d}"
            ssn_masked = f"******-{1000000+i:07d}"
            # 의도적 마스킹 누락 5건
            if i % 100 == 0:
                phone_masked = f"010-{i:04d}-{(i*7)%10000:04d}"  # 원본 그대로
                ssn_masked = f"{900101+i%999999:06d}-{1000000+i:07d}"
            cur.execute(
                "INSERT INTO tgt_customers VALUES (?, ?, ?, ?, ?, ?)",
                (i, f"hash_{i}", f"user{i}@test.com", phone_masked, ssn_masked,
                 f"199{i%10}-{(i%12)+1:02d}-{(i%28)+1:02d}"),
            )

        # 타겟 거래 (의도적 50건 누락 -> 건수 불일치)
        for i in range(1, 1961):
            cid = (i % 500) + 1
            mid = (i % 100) + 1
            amount = round(1000 + (i * 7.3) % 50000, 2)
            if i % 100 == 0:
                amount = None
            txn_date = f"2024-{(i%12)+1:02d}-{(i%28)+1:02d}"
            cur.execute(
                "INSERT INTO tgt_card_transactions VALUES "
                "(?, ?, ?, ?, ?, 'NORMAL')",
                (i, cid, mid, amount, txn_date),
            )

        self.conn.commit()

    # -- DBConnector 인터페이스 구현 ----------------------------------------
    def execute_query(self, query, params=None):
        """SELECT 쿼리 실행 -> list[dict] 반환"""
        cur = self.conn.cursor()
        cur.execute(query, params or ())
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    def execute_count(self, query, params=None):
        """COUNT 쿼리 실행 -> int 반환"""
        rows = self.execute_query(query, params)
        if rows:
            return list(rows[0].values())[0]
        return 0

    def close(self):
        self.conn.close()


# ---------------------------------------------------------------------------
# 2) 메인 실행
# ---------------------------------------------------------------------------
def main():
    print("=" * 65)
    print("  Data Quality Framework - 통합 예제 실행")
    print("=" * 65)
    print()

    # -- DB 준비 -----------------------------------------------------------
    print("[1/6] SQLite 인메모리 DB 초기화 (MySQL 불필요)...")
    db = MockDBConnector()
    print("      소스: 고객 500, 가맹점 100, 거래 2,010")
    print("      타겟: 고객 500, 거래 1,960 (의도적 50건 누락)")
    print()

    all_results: list[CheckResult] = []

    # -- 1. 건수 검증 ------------------------------------------------------
    print("-" * 65)
    print("[2/6] 건수 검증 (CountChecker)")
    print("-" * 65)
    count_rules = [
        {
            "rule_id": "CNT-001",
            "description": "고객 테이블 건수 일치",
            "source_table": "src_customers",
            "target_table": "tgt_customers",
            "threshold_pct": 0.0,
            "enabled": True,
        },
        {
            "rule_id": "CNT-002",
            "description": "거래 테이블 건수 일치",
            "source_table": "src_card_transactions",
            "target_table": "tgt_card_transactions",
            "threshold_pct": 1.0,
            "enabled": True,
        },
    ]
    checker = CountChecker(db, count_rules)
    results = checker.run_checks()
    for r in results:
        tag = "PASS" if r.status == CheckStatus.PASS else "FAIL"
        print(f"   [{tag}] {r.rule_id} | {r.description} | {r.details}")
    all_results.extend(results)
    print()

    # -- 2. NULL 검증 ------------------------------------------------------
    print("-" * 65)
    print("[3/6] NULL 검증 (NullChecker) - TS-2 빈 문자열 통합 검출")
    print("-" * 65)
    null_rules = [
        {
            "rule_id": "NULL-001",
            "description": "고객 이메일 NULL/빈문자열",
            "table": "src_customers",
            "column": "email",
            "threshold_pct": 5.0,
            "include_empty_string": True,
            "enabled": True,
        },
        {
            "rule_id": "NULL-002",
            "description": "거래 금액 NULL",
            "table": "src_card_transactions",
            "column": "txn_amount",
            "threshold_pct": 0.0,
            "include_empty_string": False,
            "enabled": True,
        },
    ]
    checker = NullChecker(db, null_rules)
    results = checker.run_checks()
    for r in results:
        tag = "PASS" if r.status == CheckStatus.PASS else "FAIL"
        print(f"   [{tag}] {r.rule_id} | {r.description} | {r.details}")
    all_results.extend(results)
    print()

    # -- 3. 중복 검증 ------------------------------------------------------
    print("-" * 65)
    print("[4/6] 중복 검증 (DuplicateChecker)")
    print("-" * 65)
    dup_rules = [
        {
            "rule_id": "DUP-001",
            "description": "거래 중복 (customer_id + txn_date + txn_amount)",
            "table": "src_card_transactions",
            "columns": ["customer_id", "txn_date", "txn_amount"],
            "enabled": True,
        },
    ]
    checker = DuplicateChecker(db, dup_rules)
    results = checker.run_checks()
    for r in results:
        tag = "PASS" if r.status == CheckStatus.PASS else "FAIL"
        print(f"   [{tag}] {r.rule_id} | {r.description} | {r.details}")
    all_results.extend(results)
    print()

    # -- 4. 범위 검증 ------------------------------------------------------
    print("-" * 65)
    print("[5/6] 범위 검증 (RangeChecker)")
    print("-" * 65)
    range_rules = [
        {
            "rule_id": "RNG-001",
            "description": "거래 금액 양수 검증",
            "table": "src_card_transactions",
            "column": "txn_amount",
            "min_value": 0,
            "max_value": 100000000,
            "enabled": True,
        },
    ]
    checker = RangeChecker(db, range_rules)
    results = checker.run_checks()
    for r in results:
        tag = "PASS" if r.status == CheckStatus.PASS else "FAIL"
        print(f"   [{tag}] {r.rule_id} | {r.description} | {r.details}")
    all_results.extend(results)
    print()

    # -- 5. 비식별화 검증 --------------------------------------------------
    print("-" * 65)
    print("[6/6] 비식별화 검증 (MaskingChecker) - TS-3 SUBSTRING 최적화")
    print("-" * 65)
    mask_rules = [
        {
            "rule_id": "MASK-001",
            "description": "전화번호 마스킹 확인",
            "table": "tgt_customers",
            "column": "phone_masked",
            "masking_type": "phone",
            "pattern_check": "SUBSTR({column}, 5, 4) = '****'",
            "enabled": True,
        },
    ]
    checker = MaskingChecker(db, mask_rules)
    results = checker.run_checks()
    for r in results:
        tag = "PASS" if r.status == CheckStatus.PASS else "FAIL"
        print(f"   [{tag}] {r.rule_id} | {r.description} | {r.details}")
    all_results.extend(results)
    print()

    # -- 결과 요약 ---------------------------------------------------------
    print("=" * 65)
    print("  검증 결과 요약")
    print("=" * 65)
    total = len(all_results)
    passed = sum(1 for r in all_results if r.status == CheckStatus.PASS)
    failed = sum(1 for r in all_results if r.status == CheckStatus.FAIL)
    errors = sum(1 for r in all_results if r.status == CheckStatus.ERROR)
    pass_rate = round(passed / total * 100, 1) if total > 0 else 0

    print(f"   전체 검증 : {total}건")
    print(f"   PASS     : {passed}건")
    print(f"   FAIL     : {failed}건")
    print(f"   ERROR    : {errors}건")
    print(f"   통과율   : {pass_rate}%")
    print()

    # -- 리포트 생성 -------------------------------------------------------
    reports_dir = os.path.join(os.path.dirname(__file__), '..', 'reports')
    os.makedirs(reports_dir, exist_ok=True)

    # HTML 리포트
    html_reporter = HTMLReporter(all_results)
    html_path = os.path.join(reports_dir, 'example_report.html')
    html_reporter.generate(html_path)
    print(f"   HTML 리포트: {os.path.abspath(html_path)}")

    # CSV 리포트
    csv_reporter = CSVReporter(all_results)
    csv_path = os.path.join(reports_dir, 'example_report.csv')
    csv_reporter.generate(csv_path)
    print(f"   CSV  리포트: {os.path.abspath(csv_path)}")

    print()
    print("검증 완료! 상세 결과는 reports/ 디렉토리를 확인하세요.")

    db.close()
    return 0 if failed == 0 and errors == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
