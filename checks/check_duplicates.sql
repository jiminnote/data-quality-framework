-- ============================================
-- 중복 체크 (Duplicate Check)
-- ============================================
-- 설명: 테이블 내 중복 레코드를 탐지합니다.
-- 사용법: ${TABLE_NAME}, ${COLUMNS}를 실제 값으로 대체하세요.
-- ============================================

-- 1. 단일 컬럼 중복 체크
-- 특정 컬럼에서 중복된 값을 찾습니다.
SELECT 
    ${COLUMN_NAME} AS duplicate_value,
    COUNT(*) AS duplicate_count
FROM ${TABLE_NAME}
GROUP BY ${COLUMN_NAME}
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;


-- 2. 복합 컬럼 중복 체크
-- 여러 컬럼 조합에서 중복된 레코드를 찾습니다.
SELECT 
    ${COLUMN_1},
    ${COLUMN_2},
    ${COLUMN_3},
    COUNT(*) AS duplicate_count
FROM ${TABLE_NAME}
GROUP BY ${COLUMN_1}, ${COLUMN_2}, ${COLUMN_3}
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;


-- 3. 전체 중복 레코드 상세 조회
-- 중복된 레코드의 전체 내용을 확인합니다.
WITH duplicates AS (
    SELECT 
        ${COLUMN_NAME},
        COUNT(*) OVER (PARTITION BY ${COLUMN_NAME}) AS dup_count
    FROM ${TABLE_NAME}
)
SELECT *
FROM ${TABLE_NAME} t
WHERE EXISTS (
    SELECT 1 
    FROM duplicates d 
    WHERE d.${COLUMN_NAME} = t.${COLUMN_NAME} 
    AND d.dup_count > 1
)
ORDER BY ${COLUMN_NAME};


-- 4. 중복률 통계
-- 테이블의 전체 중복률을 계산합니다.
SELECT 
    COUNT(*) AS total_rows,
    COUNT(DISTINCT ${COLUMN_NAME}) AS unique_values,
    COUNT(*) - COUNT(DISTINCT ${COLUMN_NAME}) AS duplicate_rows,
    ROUND(
        (COUNT(*) - COUNT(DISTINCT ${COLUMN_NAME}))::NUMERIC / COUNT(*) * 100, 
        2
    ) AS duplicate_percentage
FROM ${TABLE_NAME};


-- 5. Primary Key 중복 체크
-- PK가 중복되어서는 안 되는 경우 검증합니다.
SELECT 
    '${TABLE_NAME}' AS table_name,
    '${PK_COLUMN}' AS pk_column,
    CASE 
        WHEN COUNT(*) = COUNT(DISTINCT ${PK_COLUMN}) THEN 'PASS'
        ELSE 'FAIL'
    END AS check_result,
    COUNT(*) - COUNT(DISTINCT ${PK_COLUMN}) AS duplicate_count
FROM ${TABLE_NAME};
