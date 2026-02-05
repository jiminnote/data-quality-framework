-- ============================================
-- 값 범위 체크 (Range Check)
-- ============================================
-- 설명: 숫자/날짜 컬럼의 유효 범위를 검증합니다.
-- 사용법: 변수들을 실제 값으로 대체하세요.
-- ============================================

-- 1. 숫자 범위 체크
-- 숫자 컬럼이 지정된 범위 내에 있는지 확인합니다.
SELECT 
    '${TABLE_NAME}' AS table_name,
    '${COLUMN_NAME}' AS column_name,
    COUNT(*) AS total_rows,
    SUM(CASE WHEN ${COLUMN_NAME} < ${MIN_VALUE} OR ${COLUMN_NAME} > ${MAX_VALUE} THEN 1 ELSE 0 END) AS out_of_range_count,
    MIN(${COLUMN_NAME}) AS actual_min,
    MAX(${COLUMN_NAME}) AS actual_max,
    CASE 
        WHEN MIN(${COLUMN_NAME}) >= ${MIN_VALUE} AND MAX(${COLUMN_NAME}) <= ${MAX_VALUE} THEN 'PASS'
        ELSE 'FAIL'
    END AS check_result
FROM ${TABLE_NAME}
WHERE ${COLUMN_NAME} IS NOT NULL;


-- 2. 날짜 범위 체크
-- 날짜 컬럼이 유효한 범위 내에 있는지 확인합니다.
SELECT 
    '${TABLE_NAME}' AS table_name,
    '${DATE_COLUMN}' AS column_name,
    COUNT(*) AS total_rows,
    SUM(CASE 
        WHEN ${DATE_COLUMN} < '${START_DATE}'::DATE 
          OR ${DATE_COLUMN} > '${END_DATE}'::DATE THEN 1 
        ELSE 0 
    END) AS out_of_range_count,
    MIN(${DATE_COLUMN}) AS actual_min_date,
    MAX(${DATE_COLUMN}) AS actual_max_date,
    CASE 
        WHEN MIN(${DATE_COLUMN}) >= '${START_DATE}'::DATE 
         AND MAX(${DATE_COLUMN}) <= '${END_DATE}'::DATE THEN 'PASS'
        ELSE 'FAIL'
    END AS check_result
FROM ${TABLE_NAME}
WHERE ${DATE_COLUMN} IS NOT NULL;


-- 3. 미래 날짜 체크
-- 현재 날짜보다 미래인 날짜가 있는지 확인합니다.
SELECT 
    '${TABLE_NAME}' AS table_name,
    '${DATE_COLUMN}' AS column_name,
    COUNT(*) AS future_date_count,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS check_result
FROM ${TABLE_NAME}
WHERE ${DATE_COLUMN} > CURRENT_DATE;


-- 4. 범위 외 레코드 상세 조회
-- 범위를 벗어난 레코드를 상세히 조회합니다.
SELECT *
FROM ${TABLE_NAME}
WHERE ${COLUMN_NAME} < ${MIN_VALUE} 
   OR ${COLUMN_NAME} > ${MAX_VALUE}
ORDER BY ${COLUMN_NAME}
LIMIT 100;


-- 5. 양수 값 체크
-- 양수여야 하는 컬럼에 음수나 0이 있는지 확인합니다.
SELECT 
    '${TABLE_NAME}' AS table_name,
    '${COLUMN_NAME}' AS column_name,
    COUNT(*) AS total_rows,
    SUM(CASE WHEN ${COLUMN_NAME} <= 0 THEN 1 ELSE 0 END) AS non_positive_count,
    CASE 
        WHEN SUM(CASE WHEN ${COLUMN_NAME} <= 0 THEN 1 ELSE 0 END) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END AS check_result
FROM ${TABLE_NAME}
WHERE ${COLUMN_NAME} IS NOT NULL;


-- 6. 백분율 범위 체크 (0-100)
-- 백분율 값이 0~100 범위 내에 있는지 확인합니다.
SELECT 
    '${TABLE_NAME}' AS table_name,
    '${PERCENTAGE_COLUMN}' AS column_name,
    COUNT(*) AS total_rows,
    SUM(CASE WHEN ${PERCENTAGE_COLUMN} < 0 OR ${PERCENTAGE_COLUMN} > 100 THEN 1 ELSE 0 END) AS invalid_percentage_count,
    CASE 
        WHEN SUM(CASE WHEN ${PERCENTAGE_COLUMN} < 0 OR ${PERCENTAGE_COLUMN} > 100 THEN 1 ELSE 0 END) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END AS check_result
FROM ${TABLE_NAME}
WHERE ${PERCENTAGE_COLUMN} IS NOT NULL;


-- 7. Enum/허용값 체크
-- 컬럼 값이 허용된 값 목록에 포함되어 있는지 확인합니다.
SELECT 
    '${TABLE_NAME}' AS table_name,
    '${COLUMN_NAME}' AS column_name,
    COUNT(*) AS total_rows,
    SUM(CASE WHEN ${COLUMN_NAME} NOT IN (${ALLOWED_VALUES}) THEN 1 ELSE 0 END) AS invalid_value_count,
    CASE 
        WHEN SUM(CASE WHEN ${COLUMN_NAME} NOT IN (${ALLOWED_VALUES}) THEN 1 ELSE 0 END) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END AS check_result
FROM ${TABLE_NAME}
WHERE ${COLUMN_NAME} IS NOT NULL;


-- 8. 통계 기반 이상치 체크
-- 평균에서 3 표준편차 이상 벗어난 값을 이상치로 탐지합니다.
WITH stats AS (
    SELECT 
        AVG(${COLUMN_NAME}) AS mean_val,
        STDDEV(${COLUMN_NAME}) AS stddev_val
    FROM ${TABLE_NAME}
    WHERE ${COLUMN_NAME} IS NOT NULL
)
SELECT 
    '${TABLE_NAME}' AS table_name,
    '${COLUMN_NAME}' AS column_name,
    COUNT(*) AS outlier_count,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'WARNING' END AS check_result
FROM ${TABLE_NAME} t, stats s
WHERE t.${COLUMN_NAME} IS NOT NULL
  AND (t.${COLUMN_NAME} < s.mean_val - 3 * s.stddev_val 
       OR t.${COLUMN_NAME} > s.mean_val + 3 * s.stddev_val);


-- 9. 날짜 순서 체크
-- 시작일이 종료일보다 이전인지 확인합니다.
SELECT 
    '${TABLE_NAME}' AS table_name,
    COUNT(*) AS invalid_date_order_count,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS check_result
FROM ${TABLE_NAME}
WHERE ${START_DATE_COLUMN} > ${END_DATE_COLUMN};
