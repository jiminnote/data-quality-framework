-- ============================================
-- 건수 검증 (Count Check)
-- ============================================
-- 소스 테이블과 타겟 테이블 간 건수를 비교합니다.
-- ★ TS-1: 대용량 테이블은 청크 분할 카운트 적용
-- ============================================

-- 1. 기본 소스/타겟 건수 비교
SELECT
    '소스/타겟 건수 비교' AS check_name,
    '${SOURCE_TABLE}' AS source_table,
    '${TARGET_TABLE}' AS target_table,
    (SELECT COUNT(*) FROM ${SOURCE_TABLE}) AS source_count,
    (SELECT COUNT(*) FROM ${TARGET_TABLE}) AS target_count,
    ABS(
        (SELECT COUNT(*) FROM ${SOURCE_TABLE}) -
        (SELECT COUNT(*) FROM ${TARGET_TABLE})
    ) AS diff_count,
    CASE
        WHEN (SELECT COUNT(*) FROM ${SOURCE_TABLE}) = 0 THEN 'WARNING'
        WHEN ABS(
            (SELECT COUNT(*) FROM ${SOURCE_TABLE}) -
            (SELECT COUNT(*) FROM ${TARGET_TABLE})
        ) / (SELECT COUNT(*) FROM ${SOURCE_TABLE}) <= ${THRESHOLD}
        THEN 'PASS'
        ELSE 'FAIL'
    END AS check_result;


-- 2. WHERE 조건별 건수 비교
SELECT
    '조건부 건수 비교' AS check_name,
    (SELECT COUNT(*) FROM ${SOURCE_TABLE} WHERE ${WHERE_CLAUSE}) AS source_count,
    (SELECT COUNT(*) FROM ${TARGET_TABLE} WHERE ${WHERE_CLAUSE}) AS target_count,
    CASE
        WHEN (SELECT COUNT(*) FROM ${SOURCE_TABLE} WHERE ${WHERE_CLAUSE}) =
             (SELECT COUNT(*) FROM ${TARGET_TABLE} WHERE ${WHERE_CLAUSE})
        THEN 'PASS'
        ELSE 'FAIL'
    END AS check_result;


-- 3. ★ TS-1: 청크 분할 건수 검증 (대용량 테이블용)
-- 인덱스 활용을 위해 PK 범위 기반으로 분할 집계
-- 이 쿼리를 반복 실행하여 합산합니다.
SELECT COUNT(*) AS chunk_count
FROM ${TABLE_NAME}
WHERE ${PK_COLUMN} BETWEEN ${START_ID} AND ${END_ID};


-- 4. 날짜 파티션별 건수 비교
SELECT
    DATE(s.transaction_date) AS partition_date,
    s.src_count,
    COALESCE(t.tgt_count, 0) AS tgt_count,
    CASE
        WHEN s.src_count = COALESCE(t.tgt_count, 0) THEN 'PASS'
        ELSE 'FAIL'
    END AS check_result
FROM (
    SELECT DATE(transaction_date) AS transaction_date, COUNT(*) AS src_count
    FROM src_card_transactions
    GROUP BY DATE(transaction_date)
) s
LEFT JOIN (
    SELECT DATE(transaction_date) AS transaction_date, COUNT(*) AS tgt_count
    FROM tgt_card_transactions
    GROUP BY DATE(transaction_date)
) t ON DATE(s.transaction_date) = DATE(t.transaction_date)
ORDER BY partition_date;
