"""
Data Quality Framework - Sample Validation Script
데이터 품질 검증 실행 예제
"""

import os
import json
from datetime import datetime
from dataclasses import dataclass, field
from typing import Optional
from enum import Enum

# PostgreSQL 사용 시: pip install psycopg2-binary
# MySQL 사용 시: pip install mysql-connector-python
# SQLite 사용 시: 기본 내장


class CheckType(Enum):
    """데이터 품질 체크 유형"""
    DUPLICATE = "duplicate"
    NULL = "null"
    RANGE = "range"
    FOREIGN_KEY = "foreign_key"


class CheckResult(Enum):
    """체크 결과 상태"""
    PASS = "PASS"
    FAIL = "FAIL"
    WARNING = "WARNING"
    ERROR = "ERROR"


@dataclass
class ValidationResult:
    """검증 결과를 담는 데이터 클래스"""
    check_name: str
    check_type: CheckType
    table_name: str
    column_name: Optional[str]
    result: CheckResult
    total_rows: int = 0
    violation_count: int = 0
    violation_percentage: float = 0.0
    details: dict = field(default_factory=dict)
    executed_at: datetime = field(default_factory=datetime.now)
    
    def to_dict(self) -> dict:
        return {
            "check_name": self.check_name,
            "check_type": self.check_type.value,
            "table_name": self.table_name,
            "column_name": self.column_name,
            "result": self.result.value,
            "total_rows": self.total_rows,
            "violation_count": self.violation_count,
            "violation_percentage": self.violation_percentage,
            "details": self.details,
            "executed_at": self.executed_at.isoformat()
        }


class DataQualityChecker:
    """데이터 품질 검증 클래스"""
    
    def __init__(self, connection):
        """
        Args:
            connection: 데이터베이스 연결 객체
        """
        self.connection = connection
        self.results: list[ValidationResult] = []
    
    def execute_query(self, query: str) -> list[dict]:
        """SQL 쿼리를 실행하고 결과를 반환합니다."""
        cursor = self.connection.cursor()
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
        cursor.close()
        return results
    
    def check_duplicates(
        self, 
        table_name: str, 
        columns: list[str],
        check_name: Optional[str] = None
    ) -> ValidationResult:
        """중복 체크를 수행합니다."""
        columns_str = ", ".join(columns)
        check_name = check_name or f"duplicate_check_{table_name}"
        
        query = f"""
        SELECT 
            COUNT(*) AS total_rows,
            COUNT(*) - COUNT(DISTINCT ({columns_str})) AS duplicate_count
        FROM {table_name}
        """
        
        try:
            result = self.execute_query(query)[0]
            total_rows = result["total_rows"]
            duplicate_count = result["duplicate_count"]
            
            validation_result = ValidationResult(
                check_name=check_name,
                check_type=CheckType.DUPLICATE,
                table_name=table_name,
                column_name=columns_str,
                result=CheckResult.PASS if duplicate_count == 0 else CheckResult.FAIL,
                total_rows=total_rows,
                violation_count=duplicate_count,
                violation_percentage=round(duplicate_count / total_rows * 100, 2) if total_rows > 0 else 0
            )
        except Exception as e:
            validation_result = ValidationResult(
                check_name=check_name,
                check_type=CheckType.DUPLICATE,
                table_name=table_name,
                column_name=columns_str,
                result=CheckResult.ERROR,
                details={"error": str(e)}
            )
        
        self.results.append(validation_result)
        return validation_result
    
    def check_null(
        self, 
        table_name: str, 
        column_name: str,
        threshold_percentage: float = 0.0,
        check_name: Optional[str] = None
    ) -> ValidationResult:
        """NULL 체크를 수행합니다."""
        check_name = check_name or f"null_check_{table_name}_{column_name}"
        
        query = f"""
        SELECT 
            COUNT(*) AS total_rows,
            SUM(CASE WHEN {column_name} IS NULL THEN 1 ELSE 0 END) AS null_count
        FROM {table_name}
        """
        
        try:
            result = self.execute_query(query)[0]
            total_rows = result["total_rows"]
            null_count = result["null_count"]
            null_percentage = round(null_count / total_rows * 100, 2) if total_rows > 0 else 0
            
            if null_percentage <= threshold_percentage:
                check_result = CheckResult.PASS
            elif null_percentage <= threshold_percentage * 2:
                check_result = CheckResult.WARNING
            else:
                check_result = CheckResult.FAIL
            
            validation_result = ValidationResult(
                check_name=check_name,
                check_type=CheckType.NULL,
                table_name=table_name,
                column_name=column_name,
                result=check_result,
                total_rows=total_rows,
                violation_count=null_count,
                violation_percentage=null_percentage,
                details={"threshold_percentage": threshold_percentage}
            )
        except Exception as e:
            validation_result = ValidationResult(
                check_name=check_name,
                check_type=CheckType.NULL,
                table_name=table_name,
                column_name=column_name,
                result=CheckResult.ERROR,
                details={"error": str(e)}
            )
        
        self.results.append(validation_result)
        return validation_result
    
    def check_range(
        self, 
        table_name: str, 
        column_name: str,
        min_value: Optional[float] = None,
        max_value: Optional[float] = None,
        check_name: Optional[str] = None
    ) -> ValidationResult:
        """값 범위 체크를 수행합니다."""
        check_name = check_name or f"range_check_{table_name}_{column_name}"
        
        conditions = []
        if min_value is not None:
            conditions.append(f"{column_name} < {min_value}")
        if max_value is not None:
            conditions.append(f"{column_name} > {max_value}")
        
        if not conditions:
            raise ValueError("min_value 또는 max_value 중 하나는 지정해야 합니다.")
        
        condition_str = " OR ".join(conditions)
        
        query = f"""
        SELECT 
            COUNT(*) AS total_rows,
            SUM(CASE WHEN {condition_str} THEN 1 ELSE 0 END) AS out_of_range_count,
            MIN({column_name}) AS actual_min,
            MAX({column_name}) AS actual_max
        FROM {table_name}
        WHERE {column_name} IS NOT NULL
        """
        
        try:
            result = self.execute_query(query)[0]
            total_rows = result["total_rows"]
            out_of_range_count = result["out_of_range_count"]
            
            validation_result = ValidationResult(
                check_name=check_name,
                check_type=CheckType.RANGE,
                table_name=table_name,
                column_name=column_name,
                result=CheckResult.PASS if out_of_range_count == 0 else CheckResult.FAIL,
                total_rows=total_rows,
                violation_count=out_of_range_count,
                violation_percentage=round(out_of_range_count / total_rows * 100, 2) if total_rows > 0 else 0,
                details={
                    "expected_min": min_value,
                    "expected_max": max_value,
                    "actual_min": result["actual_min"],
                    "actual_max": result["actual_max"]
                }
            )
        except Exception as e:
            validation_result = ValidationResult(
                check_name=check_name,
                check_type=CheckType.RANGE,
                table_name=table_name,
                column_name=column_name,
                result=CheckResult.ERROR,
                details={"error": str(e)}
            )
        
        self.results.append(validation_result)
        return validation_result
    
    def check_foreign_key(
        self, 
        child_table: str,
        child_column: str,
        parent_table: str,
        parent_column: str,
        check_name: Optional[str] = None
    ) -> ValidationResult:
        """FK 정합성 체크를 수행합니다."""
        check_name = check_name or f"fk_check_{child_table}_{child_column}"
        
        query = f"""
        SELECT 
            COUNT(*) AS orphan_count
        FROM {child_table} c
        WHERE c.{child_column} IS NOT NULL
          AND NOT EXISTS (
            SELECT 1 
            FROM {parent_table} p 
            WHERE p.{parent_column} = c.{child_column}
          )
        """
        
        try:
            result = self.execute_query(query)[0]
            orphan_count = result["orphan_count"]
            
            # 전체 행 수 조회
            total_query = f"SELECT COUNT(*) AS cnt FROM {child_table} WHERE {child_column} IS NOT NULL"
            total_rows = self.execute_query(total_query)[0]["cnt"]
            
            validation_result = ValidationResult(
                check_name=check_name,
                check_type=CheckType.FOREIGN_KEY,
                table_name=child_table,
                column_name=child_column,
                result=CheckResult.PASS if orphan_count == 0 else CheckResult.FAIL,
                total_rows=total_rows,
                violation_count=orphan_count,
                violation_percentage=round(orphan_count / total_rows * 100, 2) if total_rows > 0 else 0,
                details={
                    "parent_table": parent_table,
                    "parent_column": parent_column
                }
            )
        except Exception as e:
            validation_result = ValidationResult(
                check_name=check_name,
                check_type=CheckType.FOREIGN_KEY,
                table_name=child_table,
                column_name=child_column,
                result=CheckResult.ERROR,
                details={"error": str(e)}
            )
        
        self.results.append(validation_result)
        return validation_result
    
    def get_summary(self) -> dict:
        """검증 결과 요약을 반환합니다."""
        total = len(self.results)
        passed = sum(1 for r in self.results if r.result == CheckResult.PASS)
        failed = sum(1 for r in self.results if r.result == CheckResult.FAIL)
        warnings = sum(1 for r in self.results if r.result == CheckResult.WARNING)
        errors = sum(1 for r in self.results if r.result == CheckResult.ERROR)
        
        return {
            "total_checks": total,
            "passed": passed,
            "failed": failed,
            "warnings": warnings,
            "errors": errors,
            "pass_rate": round(passed / total * 100, 2) if total > 0 else 0,
            "results": [r.to_dict() for r in self.results]
        }
    
    def export_report(self, filepath: str) -> None:
        """검증 결과를 JSON 파일로 내보냅니다."""
        summary = self.get_summary()
        summary["generated_at"] = datetime.now().isoformat()
        
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        
        print(f"Report exported to: {filepath}")


def create_sample_database():
    """샘플 SQLite 데이터베이스를 생성합니다."""
    import sqlite3
    
    conn = sqlite3.connect(":memory:")
    cursor = conn.cursor()
    
    # 테이블 생성
    cursor.executescript("""
        CREATE TABLE customers (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT,
            age INTEGER,
            created_at DATE
        );
        
        CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            customer_id INTEGER,
            amount DECIMAL(10, 2),
            status TEXT,
            order_date DATE
        );
        
        CREATE TABLE order_items (
            id INTEGER PRIMARY KEY,
            order_id INTEGER,
            product_id INTEGER,
            quantity INTEGER,
            price DECIMAL(10, 2)
        );
        
        CREATE TABLE products (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            price DECIMAL(10, 2),
            stock INTEGER
        );
    """)
    
    # 샘플 데이터 삽입
    cursor.executescript("""
        -- 고객 데이터 (일부 NULL, 중복 포함)
        INSERT INTO customers VALUES (1, 'Alice', 'alice@example.com', 25, '2024-01-15');
        INSERT INTO customers VALUES (2, 'Bob', NULL, 30, '2024-02-20');
        INSERT INTO customers VALUES (3, 'Charlie', 'charlie@example.com', -5, '2024-03-10');
        INSERT INTO customers VALUES (4, 'Alice', 'alice2@example.com', 150, '2024-04-05');
        INSERT INTO customers VALUES (5, 'Eve', 'eve@example.com', NULL, '2024-05-12');
        
        -- 제품 데이터
        INSERT INTO products VALUES (1, 'Product A', 100.00, 50);
        INSERT INTO products VALUES (2, 'Product B', 200.00, 30);
        INSERT INTO products VALUES (3, 'Product C', 150.00, 0);
        
        -- 주문 데이터 (일부 고아 레코드 포함)
        INSERT INTO orders VALUES (1, 1, 250.00, 'completed', '2024-06-01');
        INSERT INTO orders VALUES (2, 2, 100.00, 'pending', '2024-06-15');
        INSERT INTO orders VALUES (3, 999, 300.00, 'completed', '2024-06-20');  -- 존재하지 않는 customer_id
        INSERT INTO orders VALUES (4, 3, -50.00, 'invalid_status', '2024-07-01');  -- 음수 금액
        
        -- 주문 상품 데이터
        INSERT INTO order_items VALUES (1, 1, 1, 2, 100.00);
        INSERT INTO order_items VALUES (2, 1, 2, 1, 200.00);
        INSERT INTO order_items VALUES (3, 2, 999, 1, 100.00);  -- 존재하지 않는 product_id
        INSERT INTO order_items VALUES (4, 999, 1, 1, 100.00);  -- 존재하지 않는 order_id
    """)
    
    conn.commit()
    return conn


def main():
    """메인 실행 함수"""
    print("=" * 60)
    print("Data Quality Framework - Validation Example")
    print("=" * 60)
    print()
    
    # 샘플 데이터베이스 생성
    print("📦 Creating sample database...")
    conn = create_sample_database()
    
    # 데이터 품질 체커 초기화
    checker = DataQualityChecker(conn)
    
    print("🔍 Running data quality checks...\n")
    
    # 1. 중복 체크
    print("1️⃣  Duplicate Check - customers.name")
    result = checker.check_duplicates("customers", ["name"])
    print(f"   Result: {result.result.value} (duplicates: {result.violation_count})\n")
    
    # 2. NULL 체크
    print("2️⃣  NULL Check - customers.email")
    result = checker.check_null("customers", "email", threshold_percentage=10)
    print(f"   Result: {result.result.value} (nulls: {result.violation_count}, {result.violation_percentage}%)\n")
    
    print("3️⃣  NULL Check - customers.age")
    result = checker.check_null("customers", "age", threshold_percentage=5)
    print(f"   Result: {result.result.value} (nulls: {result.violation_count}, {result.violation_percentage}%)\n")
    
    # 3. 범위 체크
    print("4️⃣  Range Check - customers.age (0-120)")
    result = checker.check_range("customers", "age", min_value=0, max_value=120)
    print(f"   Result: {result.result.value} (out of range: {result.violation_count})")
    print(f"   Actual range: {result.details['actual_min']} ~ {result.details['actual_max']}\n")
    
    print("5️⃣  Range Check - orders.amount (>= 0)")
    result = checker.check_range("orders", "amount", min_value=0)
    print(f"   Result: {result.result.value} (violations: {result.violation_count})\n")
    
    # 4. FK 정합성 체크
    print("6️⃣  FK Check - orders.customer_id -> customers.id")
    result = checker.check_foreign_key("orders", "customer_id", "customers", "id")
    print(f"   Result: {result.result.value} (orphans: {result.violation_count})\n")
    
    print("7️⃣  FK Check - order_items.product_id -> products.id")
    result = checker.check_foreign_key("order_items", "product_id", "products", "id")
    print(f"   Result: {result.result.value} (orphans: {result.violation_count})\n")
    
    print("8️⃣  FK Check - order_items.order_id -> orders.id")
    result = checker.check_foreign_key("order_items", "order_id", "orders", "id")
    print(f"   Result: {result.result.value} (orphans: {result.violation_count})\n")
    
    # 결과 요약
    print("=" * 60)
    print("📊 VALIDATION SUMMARY")
    print("=" * 60)
    summary = checker.get_summary()
    print(f"   Total Checks: {summary['total_checks']}")
    print(f"   ✅ Passed: {summary['passed']}")
    print(f"   ❌ Failed: {summary['failed']}")
    print(f"   ⚠️  Warnings: {summary['warnings']}")
    print(f"   🔴 Errors: {summary['errors']}")
    print(f"   📈 Pass Rate: {summary['pass_rate']}%")
    print()
    
    # 리포트 파일 저장
    report_path = "validation_report.json"
    checker.export_report(report_path)
    print(f"📄 Full report saved to: {report_path}")
    
    # 연결 종료
    conn.close()
    print("\n✨ Validation completed!")


if __name__ == "__main__":
    main()
