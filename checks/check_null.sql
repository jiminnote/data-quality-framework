-- ============================================
-- NULL 체크 (Null Check)
-- ============================================
-- 설명: 필수 컬럼의 NULL 값을 검증합니다.
-- 사용법: ${TABLE_NAME}, ${COLUMN_NAME}를 실제 값으로 대체하세요.
-- ============================================

-- 1. 단일 컬럼 NULL 체크
-- 특정 컬럼의 NULL 값 개수를 확인합니다.
SELECT 
    '${TABLE_NAME}' AS table_name,
    '${COLUMN_NAME}' AS column_name,
    COUNT(*) AS total_rows,
    SUM(CASE WHEN ${COLUMN_NAME} IS NULL THEN 1 ELSE 0 END) AS null_count,
    ROUND(
        SUM(CASE WHEN ${COLUMN_NAME} IS NULL THEN 1 ELSE 0 END)::NUMERIC / COUNT(*) * 100,
        2
    ) AS null_percentage,
    CASE 
        WHEN SUM(CASE WHEN ${COLUMN_NAME} IS NULL THEN 1 ELSE 0 END) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END AS check_result
FROM ${TABLE_NAME};


-- 2. 다중 컬럼 NULL 체크
-- 여러 컬럼의 NULL 상태를 한번에 확인합니다.
SELECT 
    '${TABLE_NAME}' AS table_name,
    COUNT(*) AS total_rows,
    SUM(CASE WHEN ${COLUMN_1} IS NULL THEN 1 ELSE 0 END) AS column_1_nulls,
    SUM(CASE WHEN ${COLUMN_2} IS NULL THEN 1 ELSE 0 END) AS column_2_nulls,
    SUM(CASE WHEN ${COLUMN_3} IS NULL THEN 1 ELSE 0 END) AS column_3_nulls
FROM ${TABLE_NAME};


-- 3. 테이블 전체 컬럼 NULL 분석 (PostgreSQL)
-- 테이블의 모든 컬럼에 대한 NULL 통계를 생성합니다.
SELECT 
    column_name,
    data_type,
    is_nullable,
    CASE 
        WHEN is_nullable = 'NO' THEN 'NOT NULL 제약조건 있음'
        ELSE 'NULL 허용'
    END AS constraint_info
FROM information_schema.columns
WHERE table_name = '${TABLE_NAME}'
    AND table_schema = '${SCHEMA_NAME}'
ORDER BY ordinal_position;


-- 4. NULL 레코드 상세 조회
-- NULL 값을 포함한 레코드를 상세히 조회합니다.
SELECT *
FROM ${TABLE_NAME}
WHERE ${COLUMN_NAME} IS NULL
LIMIT 100;


-- 5. 빈 문자열 포함 NULL 체크
-- NULL과 빈 문자열('')을 모두 체크합니다.
SELECT 
    '${TABLE_NAME}' AS table_name,
    '${COLUMN_NAME}' AS column_name,
    COUNT(*) AS total_rows,
    SUM(CASE WHEN ${COLUMN_NAME} IS NULL THEN 1 ELSE 0 END) AS null_count,
    SUM(CASE WHEN TRIM(${COLUMN_NAME}) = '' THEN 1 ELSE 0 END) AS empty_string_count,
    SUM(CASE WHEN ${COLUMN_NAME} IS NULL OR TRIM(${COLUMN_NAME}) = '' THEN 1 ELSE 0 END) AS total_blank_count
FROM ${TABLE_NAME};


-- 6. 조건부 NULL 체크
-- 특정 조건에서만 NULL이 허용되지 않는 경우를 체크합니다.
SELECT 
    '${TABLE_NAME}' AS table_name,
    COUNT(*) AS violation_count,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS check_result
FROM ${TABLE_NAME}
WHERE ${CONDITION_COLUMN} = '${CONDITION_VALUE}'
    AND ${REQUIRED_COLUMN} IS NULL;


-- 7. NULL 비율 임계값 체크
-- NULL 비율이 허용 임계값을 초과하는지 확인합니다.
WITH null_stats AS (
    SELECT 
        COUNT(*) AS total_rows,
        SUM(CASE WHEN ${COLUMN_NAME} IS NULL THEN 1 ELSE 0 END) AS null_count
    FROM ${TABLE_NAME}
)
SELECT 
    '${TABLE_NAME}' AS table_name,
    '${COLUMN_NAME}' AS column_name,
    total_rows,
    null_count,
    ROUND(null_count::NUMERIC / total_rows * 100, 2) AS null_percentage,
    ${THRESHOLD_PERCENTAGE} AS threshold,
    CASE 
        WHEN (null_count::NUMERIC / total_rows * 100) <= ${THRESHOLD_PERCENTAGE} THEN 'PASS'
        ELSE 'FAIL'
    END AS check_result
FROM null_stats;
