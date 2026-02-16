"""
MySQL 데이터베이스 커넥션 관리
==============================
커넥션 풀 관리 및 쿼리 실행 유틸리티를 제공합니다.

★ TS-1 반영: connect_timeout, read_timeout 설정
★ TS-5 반영: 연결 재시도 로직 (Docker MySQL 초기화 대기)
"""

import time
import logging
import mysql.connector
from mysql.connector import pooling, Error as MySQLError
from typing import Optional
from contextlib import contextmanager

logger = logging.getLogger(__name__)


class DBConnector:
    """MySQL 커넥션 관리 클래스"""

    def __init__(self, db_config: dict, pool_size: int = 5):
        """
        Args:
            db_config: DB 접속 정보 딕셔너리 (config_loader에서 로딩)
            pool_size: 커넥션 풀 크기
        """
        self.db_config = db_config
        self.pool_size = pool_size
        self.pool: Optional[pooling.MySQLConnectionPool] = None
        self._init_pool()

    def _init_pool(self):
        """
        커넥션 풀을 초기화합니다.

        ★ TS-5: Docker MySQL 초기화 순서 이슈 대응
        컨테이너 시작 ≠ MySQL 서비스 준비 완료이므로, 재시도 로직 포함
        """
        max_retries = 10
        retry_interval = 3  # 초

        for attempt in range(1, max_retries + 1):
            try:
                self.pool = pooling.MySQLConnectionPool(
                    pool_name="dq_pool",
                    pool_size=self.pool_size,
                    pool_reset_session=True,
                    host=self.db_config["host"],
                    port=self.db_config.get("port", 3306),
                    user=self.db_config["user"],
                    password=self.db_config["password"],
                    database=self.db_config["database"],
                    charset=self.db_config.get("charset", "utf8mb4"),
                    connect_timeout=self.db_config.get("connect_timeout", 30),
                    # ★ TS-1: 대용량 쿼리를 위한 read_timeout 설정
                    read_timeout=self.db_config.get("read_timeout", 60),
                    autocommit=True,
                )
                logger.info("✅ MySQL 커넥션 풀 초기화 성공 (시도 %d/%d)", attempt, max_retries)
                return

            except MySQLError as e:
                logger.warning(
                    "⚠️  MySQL 연결 실패 (시도 %d/%d): %s", attempt, max_retries, e
                )
                if attempt < max_retries:
                    logger.info("   %d초 후 재시도...", retry_interval)
                    time.sleep(retry_interval)
                else:
                    raise ConnectionError(
                        f"MySQL 연결에 실패했습니다. {max_retries}회 시도 후 포기.\n"
                        f"호스트: {self.db_config['host']}:{self.db_config.get('port', 3306)}\n"
                        f"에러: {e}"
                    )

    @contextmanager
    def get_connection(self):
        """
        커넥션 풀에서 연결을 가져오는 컨텍스트 매니저

        Usage:
            with connector.get_connection() as conn:
                cursor = conn.cursor(dictionary=True)
                cursor.execute("SELECT ...")
        """
        conn = None
        try:
            conn = self.pool.get_connection()
            yield conn
        except MySQLError as e:
            logger.error("DB 에러: %s", e)
            raise
        finally:
            if conn and conn.is_connected():
                conn.close()

    def execute_query(self, query: str, params: tuple = None) -> list[dict]:
        """
        SELECT 쿼리를 실행하고 결과를 딕셔너리 리스트로 반환합니다.

        Args:
            query: SQL 쿼리
            params: 쿼리 파라미터 (선택)

        Returns:
            결과 행의 딕셔너리 리스트
        """
        with self.get_connection() as conn:
            cursor = conn.cursor(dictionary=True)
            try:
                cursor.execute(query, params)
                results = cursor.fetchall()
                return results
            finally:
                cursor.close()

    def execute_scalar(self, query: str, params: tuple = None):
        """
        스칼라 값을 반환하는 쿼리를 실행합니다.

        Returns:
            첫 번째 행의 첫 번째 값
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(query, params)
                row = cursor.fetchone()
                return row[0] if row else None
            finally:
                cursor.close()

    def execute_count(self, table: str, where_clause: str = None) -> int:
        """
        테이블의 건수를 조회합니다.

        Args:
            table: 테이블명
            where_clause: WHERE 조건 (선택)

        Returns:
            건수 (int)
        """
        query = f"SELECT COUNT(*) FROM {table}"
        if where_clause:
            query += f" WHERE {where_clause}"
        return self.execute_scalar(query) or 0

    def execute_chunked_count(self, table: str, chunk_size: int = 100000) -> int:
        """
        ★ TS-1: 대용량 테이블 건수 검증 시 청크 분할 카운트

        전체 COUNT(*) 대신 id 범위 기반으로 분할하여 집계합니다.
        인덱스를 활용하므로 풀스캔을 피할 수 있습니다.

        Args:
            table: 테이블명
            chunk_size: 청크 크기 (기본 10만)

        Returns:
            총 건수
        """
        # PK 범위 파악
        min_id = self.execute_scalar(f"SELECT MIN(CAST(transaction_id AS SIGNED)) FROM {table}") or 0
        max_id = self.execute_scalar(f"SELECT MAX(CAST(transaction_id AS SIGNED)) FROM {table}") or 0

        if max_id == 0:
            return 0

        total_count = 0
        current_start = min_id

        while current_start <= max_id:
            current_end = current_start + chunk_size - 1
            chunk_count = self.execute_scalar(
                f"SELECT COUNT(*) FROM {table} "
                f"WHERE transaction_id BETWEEN %s AND %s",
                (current_start, current_end)
            )
            total_count += (chunk_count or 0)
            current_start = current_end + 1

        logger.info(
            "✅ 청크 분할 카운트 완료: %s = %d건 (청크 크기: %d)",
            table, total_count, chunk_size
        )
        return total_count

    def close(self):
        """커넥션 풀을 종료합니다."""
        if self.pool:
            logger.info("MySQL 커넥션 풀 종료")
            # mysql.connector 풀은 명시적 close가 없으므로 참조 해제
            self.pool = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
