-- ============================================
-- ETL 타겟 테이블 데이터 생성
-- ============================================
-- 소스 데이터를 변환하여 타겟 테이블에 적재합니다.
--
-- ★ 의도적 품질 이슈 삽입:
--   - tgt_customers에 비식별화 누락 100건
--   - tgt_card_transactions에 건수 불일치 (ETL 오류 시뮬레이션)
-- ============================================

USE data_quality;

-- ------------------------------------------
-- 1. 타겟 고객 테이블 적재 (비식별화 적용)
-- ------------------------------------------
INSERT INTO tgt_customers
    (customer_id, customer_name_hash, phone_number, resident_number,
     email, address, birth_date, gender, customer_grade)
SELECT
    customer_id,
    SHA2(customer_name, 256) AS customer_name_hash,
    -- 전화번호 마스킹: 010-****-5678
    CONCAT(
        SUBSTRING(phone_number, 1, 4),
        '****',
        SUBSTRING(phone_number, 9)
    ) AS phone_number,
    -- 주민번호 마스킹: 123456-*******
    CONCAT(
        SUBSTRING(resident_number, 1, 7),
        '*******'
    ) AS resident_number,
    email,
    address,
    birth_date,
    gender,
    customer_grade
FROM src_customers;


-- ★ 이슈 4: 비식별화 누락 레코드 100건 (원본 주민번호 노출)
UPDATE tgt_customers tc
JOIN src_customers sc ON tc.customer_id = sc.customer_id
SET tc.resident_number = sc.resident_number,
    tc.phone_number = sc.phone_number,
    tc.customer_name_hash = sc.customer_name  -- 해싱 안 된 원본 이름
WHERE tc.customer_id IN (
    SELECT customer_id FROM (
        SELECT customer_id
        FROM src_customers
        ORDER BY RAND()
        LIMIT 100
    ) AS tmp
);


-- ------------------------------------------
-- 2. 타겟 거래 테이블 적재 (카드번호 마스킹)
-- ------------------------------------------
-- ★ 이슈 5: 의도적 건수 불일치 — 1,000건 누락 (ETL 오류 시뮬레이션)
INSERT INTO tgt_card_transactions
    (transaction_id, customer_id, merchant_id, card_number_masked,
     transaction_date, transaction_amount, currency, approval_status,
     approval_number, installment_months, category)
SELECT
    transaction_id,
    customer_id,
    merchant_id,
    -- 카드번호 마스킹: 5412-****-****-1234
    CONCAT(
        SUBSTRING(card_number, 1, 5),
        '****-****',
        SUBSTRING(card_number, 16)
    ) AS card_number_masked,
    transaction_date,
    transaction_amount,
    currency,
    approval_status,
    approval_number,
    installment_months,
    category
FROM src_card_transactions
WHERE transaction_id <= (SELECT MAX(transaction_id) - 1000 FROM src_card_transactions);
-- ↑ 마지막 1,000건 의도적 누락


-- ------------------------------------------
-- 3. 일별 거래 집계 테이블 적재
-- ------------------------------------------
INSERT INTO tgt_daily_summary
    (summary_date, transaction_count, total_amount, avg_amount,
     max_amount, min_amount, approved_count, rejected_count, unique_customers)
SELECT
    DATE(transaction_date) AS summary_date,
    COUNT(*) AS transaction_count,
    SUM(transaction_amount) AS total_amount,
    AVG(transaction_amount) AS avg_amount,
    MAX(transaction_amount) AS max_amount,
    MIN(transaction_amount) AS min_amount,
    SUM(CASE WHEN approval_status = 'approved' THEN 1 ELSE 0 END) AS approved_count,
    SUM(CASE WHEN approval_status = 'rejected' THEN 1 ELSE 0 END) AS rejected_count,
    COUNT(DISTINCT customer_id) AS unique_customers
FROM src_card_transactions
WHERE transaction_amount IS NOT NULL
GROUP BY DATE(transaction_date)
ORDER BY summary_date;
