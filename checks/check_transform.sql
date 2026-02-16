-- ============================================
-- 변환 로직 검증 (Transform Check)
-- ============================================
-- ETL 변환 전후 데이터 정합성을 SQL로 검증합니다.
-- JOIN 기반으로 소스/타겟 값을 비교합니다.
-- ============================================

-- 1. 거래 금액 합계 정합성 (소스 vs 타겟)
SELECT
    '거래 금액 합계 정합성' AS check_name,
    s.source_total,
    t.target_total,
    ABS(s.source_total - t.target_total) AS diff_amount,
    CASE
        WHEN ABS(s.source_total - COALESCE(t.target_total, 0)) < 0.01 THEN 'PASS'
        ELSE 'FAIL'
    END AS check_result
FROM
    (SELECT SUM(transaction_amount) AS source_total FROM src_card_transactions WHERE transaction_amount IS NOT NULL) s,
    (SELECT SUM(transaction_amount) AS target_total FROM tgt_card_transactions) t;


-- 2. 일별 거래 집계 합계 정합성
SELECT
    src.tx_date,
    src.source_daily_total,
    tgt.target_daily_total,
    ABS(src.source_daily_total - COALESCE(tgt.target_daily_total, 0)) AS diff_amount,
    CASE
        WHEN ABS(src.source_daily_total - COALESCE(tgt.target_daily_total, 0)) < 0.01 THEN 'PASS'
        ELSE 'FAIL'
    END AS check_result
FROM (
    SELECT DATE(transaction_date) AS tx_date, SUM(transaction_amount) AS source_daily_total
    FROM src_card_transactions
    WHERE transaction_amount IS NOT NULL
    GROUP BY DATE(transaction_date)
) src
LEFT JOIN (
    SELECT summary_date AS tx_date, total_amount AS target_daily_total
    FROM tgt_daily_summary
) tgt ON src.tx_date = tgt.tx_date
HAVING check_result = 'FAIL'
ORDER BY src.tx_date;


-- 3. 고객 ID 매핑 정합성 (소스에 있고 타겟에 없는 고객)
SELECT
    '고객 ID 매핑 누락' AS check_name,
    COUNT(*) AS missing_count,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS check_result
FROM src_customers sc
WHERE NOT EXISTS (
    SELECT 1 FROM tgt_customers tc WHERE tc.customer_id = sc.customer_id
);


-- 4. 거래 건수 집계 정합성
SELECT
    src.tx_date,
    src.source_tx_count,
    tgt.target_tx_count,
    CASE
        WHEN src.source_tx_count = COALESCE(tgt.target_tx_count, 0) THEN 'PASS'
        ELSE 'FAIL'
    END AS check_result
FROM (
    SELECT DATE(transaction_date) AS tx_date, COUNT(*) AS source_tx_count
    FROM src_card_transactions
    GROUP BY DATE(transaction_date)
) src
LEFT JOIN (
    SELECT summary_date AS tx_date, transaction_count AS target_tx_count
    FROM tgt_daily_summary
) tgt ON src.tx_date = tgt.tx_date
HAVING check_result = 'FAIL';
