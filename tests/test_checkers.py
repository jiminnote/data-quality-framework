"""
데이터 품질 검증 모듈 단위 테스트
==================================
SQLite 인메모리 DB를 사용하여 MySQL 없이도 체커 로직을 테스트합니다.

실행:
  pytest tests/test_checkers.py -v
"""

import sqlite3
import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime

# 프로젝트 모듈
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.checker.base_checker import BaseChecker, CheckResult, CheckStatus
from src.config_loader import ConfigLoader


# ============================================
# Fixture: SQLite 기반 Mock DB
# ============================================

class MockDBConnector:
    """
    테스트용 Mock DB Connector
    SQLite 인메모리 DB로 MySQL 커넥터를 시뮬레이션합니다.
    """

    def __init__(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        self._init_tables()

    def _init_tables(self):
        """테스트용 테이블 생성 및 데이터 삽입"""
        cursor = self.conn.cursor()

        # 소스 고객 테이블
        cursor.executescript("""
            CREATE TABLE src_customers (
                customer_id INTEGER PRIMARY KEY,
                customer_name TEXT,
                phone_number TEXT,
                resident_number TEXT,
                email TEXT,
                birth_date DATE,
                gender TEXT,
                customer_grade TEXT
            );

            CREATE TABLE src_merchants (
                merchant_id INTEGER PRIMARY KEY,
                merchant_name TEXT,
                merchant_code TEXT UNIQUE
            );

            CREATE TABLE src_card_transactions (
                transaction_id INTEGER PRIMARY KEY,
                customer_id INTEGER,
                merchant_id INTEGER,
                transaction_date DATETIME,
                transaction_amount DECIMAL,
                approval_status TEXT,
                category TEXT
            );

            CREATE TABLE tgt_customers (
                customer_id INTEGER PRIMARY KEY,
                customer_name_hash TEXT,
                phone_number TEXT,
                resident_number TEXT
            );

            CREATE TABLE tgt_card_transactions (
                transaction_id INTEGER PRIMARY KEY,
                customer_id INTEGER,
                transaction_amount DECIMAL
            );
        """)

        # 소스 고객 데이터 (10건)
        customers = [
            (1, '김민준', '010-1234-5678', '901010-1234567', 'kim@test.com', '1990-10-10', 'M', 'GOLD'),
            (2, '이서윤', '010-2345-6789', '920520-2345678', 'lee@test.com', '1992-05-20', 'F', 'VIP'),
            (3, '박도윤', '', '850315-1111111', 'park@test.com', '1985-03-15', 'M', 'NORMAL'),  # 전화번호 빈 문자열
            (4, '최서연', None, '780101-2222222', None, '1978-01-01', 'F', 'SILVER'),  # 전화번호, 이메일 NULL
            (5, '정시우', '010-5678-9012', '001225-3333333', 'jung@test.com', '2000-12-25', 'M', 'NORMAL'),
        ]
        cursor.executemany(
            "INSERT INTO src_customers VALUES (?,?,?,?,?,?,?,?)", customers
        )

        # 가맹점 데이터
        merchants = [
            (1, '서울 식당', 'MC0001'),
            (2, '강남 마트', 'MC0002'),
            (3, '온라인 쇼핑', 'MC0003'),
        ]
        cursor.executemany("INSERT INTO src_merchants VALUES (?,?,?)", merchants)

        # 거래 데이터 (중복, NULL, FK위반 포함)
        transactions = [
            (1, 1, 1, '2024-06-01 10:00:00', 50000, 'approved', '식비'),
            (2, 2, 2, '2024-06-01 11:00:00', 120000, 'approved', '쇼핑'),
            (3, 1, 1, '2024-06-01 10:00:00', 50000, 'approved', '식비'),  # 중복!
            (4, 3, 999, '2024-06-02 09:00:00', 30000, 'approved', '교통'),  # FK 위반!
            (5, 4, 3, '2024-06-03 14:00:00', None, 'rejected', '기타'),  # NULL 금액
            (6, 5, 2, '2024-06-04 16:00:00', -5000, 'approved', '환불'),  # 음수 금액
        ]
        cursor.executemany(
            "INSERT INTO src_card_transactions VALUES (?,?,?,?,?,?,?)", transactions
        )

        # 타겟 고객 (비식별화 - 일부 누락)
        tgt_customers = [
            (1, 'a' * 64, '010-****-5678', '901010-*******'),  # 정상
            (2, 'b' * 64, '010-****-6789', '920520-*******'),  # 정상
            (3, 'c' * 64, '010-****-0000', '850315-1111111'),  # ★ 비식별화 누락!
            (4, '최서연', '010-1111-2222', '780101-2222222'),  # ★ 전부 누락!
        ]
        cursor.executemany("INSERT INTO tgt_customers VALUES (?,?,?,?)", tgt_customers)

        # 타겟 거래 (건수 불일치 - 5건만)
        tgt_transactions = [
            (1, 1, 50000),
            (2, 2, 120000),
            (3, 1, 50000),
            (4, 3, 30000),
            (5, 4, None),
            # transaction_id 6 누락 → 건수 불일치
        ]
        cursor.executemany("INSERT INTO tgt_card_transactions VALUES (?,?,?)", tgt_transactions)

        self.conn.commit()

    def execute_query(self, query: str, params: tuple = None) -> list[dict]:
        cursor = self.conn.cursor()
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def execute_scalar(self, query: str, params: tuple = None):
        cursor = self.conn.cursor()
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        row = cursor.fetchone()
        return row[0] if row else None

    def execute_count(self, table: str, where_clause: str = None) -> int:
        query = f"SELECT COUNT(*) FROM {table}"
        if where_clause:
            query += f" WHERE {where_clause}"
        return self.execute_scalar(query) or 0

    def execute_chunked_count(self, table: str, chunk_size: int = 100000) -> int:
        return self.execute_count(table)

    def close(self):
        self.conn.close()


@pytest.fixture
def mock_db():
    db = MockDBConnector()
    yield db
    db.close()


# ============================================
# 테스트: CheckResult
# ============================================

class TestCheckResult:
    """CheckResult 데이터 클래스 테스트"""

    def test_to_dict(self):
        result = CheckResult(
            rule_id="TEST-001",
            check_type="test",
            description="테스트",
            table_name="test_table",
            status=CheckStatus.PASS,
            total_rows=100,
            violation_count=0,
        )
        d = result.to_dict()
        assert d["rule_id"] == "TEST-001"
        assert d["status"] == "PASS"
        assert d["violation_ratio"] == 0.0

    def test_fail_result(self):
        result = CheckResult(
            rule_id="TEST-002",
            check_type="test",
            description="실패 테스트",
            table_name="test_table",
            status=CheckStatus.FAIL,
            total_rows=100,
            violation_count=10,
            violation_ratio=0.1,
        )
        d = result.to_dict()
        assert d["status"] == "FAIL"
        assert d["violation_count"] == 10


# ============================================
# 테스트: ConfigLoader
# ============================================

class TestConfigLoader:
    """설정 로더 테스트"""

    def test_load_db_config(self):
        """DB 설정 로딩 테스트"""
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        loader = ConfigLoader(base_dir)
        config = loader.load_db_config("development")
        assert config["host"] == "localhost"
        assert config["port"] == 3306
        assert config["database"] == "data_quality"

    def test_load_null_rules(self):
        """NULL 규칙 로딩 테스트"""
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        loader = ConfigLoader(base_dir)
        rules = loader.load_rules("null")
        assert len(rules) > 0
        assert all("rule_id" in r for r in rules)

    def test_load_all_rules(self):
        """전체 규칙 로딩 테스트"""
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        loader = ConfigLoader(base_dir)
        all_rules = loader.load_all_rules()
        assert "count" in all_rules
        assert "null" in all_rules
        assert "transform" in all_rules
        assert "masking" in all_rules


# ============================================
# 테스트: CountChecker (건수 검증)
# ============================================

class TestCountChecker:
    """건수 검증 테스트"""

    def test_count_mismatch(self, mock_db):
        """소스/타겟 건수 불일치 검출"""
        from src.checker.count_checker import CountChecker

        rules = [{
            "rule_id": "CNT-TEST-001",
            "description": "거래 건수 비교",
            "source_table": "src_card_transactions",
            "target_table": "tgt_card_transactions",
            "threshold": 0.0,
        }]
        checker = CountChecker(mock_db, rules)
        results = checker.run_checks()

        assert len(results) == 1
        assert results[0].status == CheckStatus.FAIL  # 6건 vs 5건
        assert results[0].violation_count == 1

    def test_count_with_threshold(self, mock_db):
        """허용 오차율 적용"""
        from src.checker.count_checker import CountChecker

        rules = [{
            "rule_id": "CNT-TEST-002",
            "description": "오차율 적용 건수 비교",
            "source_table": "src_card_transactions",
            "target_table": "tgt_card_transactions",
            "threshold": 0.2,  # 20% 허용
        }]
        checker = CountChecker(mock_db, rules)
        results = checker.run_checks()

        assert results[0].status == CheckStatus.PASS  # 1/6 ≈ 16.7% < 20%


# ============================================
# 테스트: NullChecker (NULL 검증)
# ============================================

class TestNullChecker:
    """NULL 검증 테스트"""

    def test_null_detection(self, mock_db):
        """NULL 검출"""
        from src.checker.null_checker import NullChecker

        rules = [{
            "rule_id": "NULL-TEST-001",
            "description": "거래 금액 NULL 체크",
            "table": "src_card_transactions",
            "column": "transaction_amount",
            "max_null_ratio": 0.0,
            "include_empty_string": False,
        }]
        checker = NullChecker(mock_db, rules)
        results = checker.run_checks()

        assert len(results) == 1
        assert results[0].status == CheckStatus.FAIL
        assert results[0].violation_count == 1  # 1건 NULL

    def test_null_with_empty_string(self, mock_db):
        """★ TS-2: 빈 문자열 포함 NULL 검출"""
        from src.checker.null_checker import NullChecker

        rules = [{
            "rule_id": "NULL-TEST-002",
            "description": "전화번호 NULL+빈문자열 체크",
            "table": "src_customers",
            "column": "phone_number",
            "max_null_ratio": 0.0,
            "include_empty_string": True,
        }]
        checker = NullChecker(mock_db, rules)
        results = checker.run_checks()

        # NULL 1건 + 빈 문자열 1건 = 2건
        assert results[0].status == CheckStatus.FAIL
        assert results[0].violation_count == 2


# ============================================
# 테스트: DuplicateChecker (중복 검증)
# ============================================

class TestDuplicateChecker:
    """중복 검증 테스트"""

    def test_duplicate_detection(self, mock_db):
        """중복 레코드 검출"""
        from src.checker.duplicate_checker import DuplicateChecker

        rules = [{
            "rule_id": "DUP-TEST-001",
            "description": "거래 복합키 중복 체크",
            "table": "src_card_transactions",
            "columns": ["customer_id", "merchant_id", "transaction_date", "transaction_amount"],
            "check_type": "composite",
        }]
        checker = DuplicateChecker(mock_db, rules)
        results = checker.run_checks()

        assert len(results) == 1
        assert results[0].status == CheckStatus.FAIL
        assert results[0].details["duplicate_rows"] > 0


# ============================================
# 테스트: RangeChecker (범위 검증)
# ============================================

class TestRangeChecker:
    """범위 검증 테스트"""

    def test_negative_amount(self, mock_db):
        """음수 금액 검출"""
        from src.checker.range_checker import RangeChecker

        rules = [{
            "rule_id": "RNG-TEST-001",
            "description": "거래 금액 양수 체크",
            "table": "src_card_transactions",
            "column": "transaction_amount",
            "check_type": "range",
            "min_value": 0,
            "max_value": None,
        }]
        checker = RangeChecker(mock_db, rules)
        results = checker.run_checks()

        assert results[0].status == CheckStatus.FAIL  # -5000 존재

    def test_fk_violation(self, mock_db):
        """FK 위반 검출"""
        from src.checker.range_checker import RangeChecker

        rules = [{
            "rule_id": "FK-TEST-001",
            "description": "거래→가맹점 FK 체크",
            "table": "src_card_transactions",
            "column": "merchant_id",
            "check_type": "foreign_key",
            "parent_table": "src_merchants",
            "parent_column": "merchant_id",
        }]
        checker = RangeChecker(mock_db, rules)
        results = checker.run_checks()

        assert results[0].status == CheckStatus.FAIL
        assert results[0].violation_count == 1  # merchant_id=999


# ============================================
# 테스트: MaskingChecker (비식별화 검증)
# ============================================

class TestMaskingChecker:
    """비식별화 검증 테스트"""

    def test_ssn_masking_violation(self, mock_db):
        """주민번호 마스킹 누락 검출"""
        from src.checker.masking_checker import MaskingChecker

        rules = [{
            "rule_id": "MASK-TEST-001",
            "description": "주민번호 마스킹 체크",
            "table": "tgt_customers",
            "column": "resident_number",
            "masking_type": "ssn",
            "expected_pattern_start": 8,
            "expected_pattern_value": "*******",
            "expected_length": 14,
        }]
        checker = MaskingChecker(mock_db, rules)
        results = checker.run_checks()

        assert results[0].status == CheckStatus.FAIL
        assert results[0].violation_count == 2  # customer 3, 4 누락

    def test_hash_validation(self, mock_db):
        """해싱 적용 검증"""
        from src.checker.masking_checker import MaskingChecker

        rules = [{
            "rule_id": "MASK-TEST-002",
            "description": "이름 해싱 체크",
            "table": "tgt_customers",
            "column": "customer_name_hash",
            "masking_type": "hash",
            "expected_length": 64,
        }]
        checker = MaskingChecker(mock_db, rules)
        results = checker.run_checks()

        # customer 4는 해싱 안 된 원본 이름 ('최서연')
        assert results[0].status == CheckStatus.FAIL
        assert results[0].violation_count >= 1


# ============================================
# 테스트: Reporter
# ============================================

class TestReporter:
    """리포트 생성 테스트"""

    def test_html_report(self, tmp_path):
        """HTML 리포트 생성"""
        from src.reporter.html_reporter import HTMLReporter

        results = [
            CheckResult(
                rule_id="RPT-001", check_type="count",
                description="테스트", table_name="test",
                status=CheckStatus.PASS, total_rows=100,
            ).to_dict(),
            CheckResult(
                rule_id="RPT-002", check_type="null",
                description="테스트2", table_name="test",
                status=CheckStatus.FAIL, total_rows=100,
                violation_count=5, violation_ratio=0.05,
            ).to_dict(),
        ]

        reporter = HTMLReporter(str(tmp_path))
        filepath = reporter.generate(results)

        assert os.path.exists(filepath)
        assert filepath.endswith(".html")

        with open(filepath, "r", encoding="utf-8") as f:
            content = f.read()
        assert "PASS" in content
        assert "FAIL" in content

    def test_csv_report(self, tmp_path):
        """CSV 리포트 생성"""
        from src.reporter.csv_reporter import CSVReporter

        results = [
            CheckResult(
                rule_id="RPT-003", check_type="count",
                description="CSV 테스트", table_name="test",
                status=CheckStatus.PASS, total_rows=50,
            ).to_dict(),
        ]

        reporter = CSVReporter(str(tmp_path))
        filepath = reporter.generate(results)

        assert os.path.exists(filepath)
        assert filepath.endswith(".csv")


# ============================================
# 실행
# ============================================

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
