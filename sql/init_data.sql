-- ============================================
-- 금융 샘플 데이터 생성
-- ============================================
-- 프로시저를 사용하여 대량 데이터를 자동 생성합니다.
-- - src_customers: 10만 건
-- - src_merchants: 5천 건
-- - src_card_transactions: 100만 건
--
-- ★ 의도적 품질 이슈 삽입:
--   - NULL 거래금액 500건
--   - 중복 거래 200건
--   - FK 위반 (존재하지 않는 가맹점 코드) 300건
-- ============================================

USE data_quality;

-- ------------------------------------------
-- 1. 가맹점 데이터 생성 (5,000건)
-- ------------------------------------------
DELIMITER //

DROP PROCEDURE IF EXISTS generate_merchants//
CREATE PROCEDURE generate_merchants()
BEGIN
    DECLARE i INT DEFAULT 1;
    DECLARE v_biz_type VARCHAR(50);
    DECLARE v_region VARCHAR(100);

    WHILE i <= 5000 DO
        SET v_biz_type = ELT(1 + FLOOR(RAND() * 8),
            '식음료', '유통', '온라인', '의료', '교통', '교육', '숙박', '문화');
        SET v_region = ELT(1 + FLOOR(RAND() * 8),
            '서울', '경기', '부산', '대구', '인천', '광주', '대전', '제주');

        INSERT INTO src_merchants (merchant_name, business_type, region, merchant_code, status)
        VALUES (
            CONCAT(v_region, ' ', v_biz_type, ' ', LPAD(i, 5, '0')),
            v_biz_type,
            v_region,
            CONCAT('MC', LPAD(i, 6, '0')),
            IF(RAND() < 0.95, 'ACTIVE', 'INACTIVE')
        );

        SET i = i + 1;
    END WHILE;
END//

DELIMITER ;
CALL generate_merchants();


-- ------------------------------------------
-- 2. 고객 데이터 생성 (100,000건)
-- ------------------------------------------
DELIMITER //

DROP PROCEDURE IF EXISTS generate_customers//
CREATE PROCEDURE generate_customers()
BEGIN
    DECLARE i INT DEFAULT 1;
    DECLARE v_name VARCHAR(100);
    DECLARE v_gender CHAR(1);
    DECLARE v_grade VARCHAR(10);
    DECLARE v_birth DATE;
    DECLARE v_ssn_front VARCHAR(6);
    DECLARE v_ssn_back VARCHAR(7);

    WHILE i <= 100000 DO
        SET v_gender = IF(RAND() < 0.5, 'M', 'F');
        SET v_grade = ELT(1 + FLOOR(RAND() * 4), 'NORMAL', 'SILVER', 'GOLD', 'VIP');
        SET v_birth = DATE_ADD('1960-01-01', INTERVAL FLOOR(RAND() * 20000) DAY);
        SET v_ssn_front = DATE_FORMAT(v_birth, '%y%m%d');
        SET v_ssn_back = LPAD(FLOOR(RAND() * 10000000), 7, '0');

        SET v_name = CONCAT(
            ELT(1 + FLOOR(RAND() * 10), '김', '이', '박', '최', '정', '강', '조', '윤', '장', '임'),
            ELT(1 + FLOOR(RAND() * 10), '민준', '서윤', '도윤', '서연', '시우', '하은', '주원', '지유', '지호', '수아')
        );

        INSERT INTO src_customers (customer_name, phone_number, resident_number, email, address, birth_date, gender, customer_grade)
        VALUES (
            v_name,
            CONCAT('010-', LPAD(FLOOR(RAND() * 10000), 4, '0'), '-', LPAD(FLOOR(RAND() * 10000), 4, '0')),
            CONCAT(v_ssn_front, '-', v_ssn_back),
            CONCAT('user', LPAD(i, 6, '0'), '@example.com'),
            CONCAT(
                ELT(1 + FLOOR(RAND() * 5), '서울시 강남구', '서울시 서초구', '경기도 성남시', '부산시 해운대구', '대전시 유성구'),
                ' ', FLOOR(RAND() * 999) + 1, '번지'
            ),
            v_birth,
            v_gender,
            v_grade
        );

        SET i = i + 1;
    END WHILE;
END//

DELIMITER ;
CALL generate_customers();


-- ------------------------------------------
-- 3. 카드 거래 데이터 생성 (1,000,000건)
-- ------------------------------------------
DELIMITER //

DROP PROCEDURE IF EXISTS generate_transactions//
CREATE PROCEDURE generate_transactions()
BEGIN
    DECLARE i INT DEFAULT 1;
    DECLARE v_cust_id INT;
    DECLARE v_merch_id INT;
    DECLARE v_amount DECIMAL(15,2);
    DECLARE v_tx_date DATETIME;
    DECLARE v_status VARCHAR(20);
    DECLARE v_category VARCHAR(50);
    DECLARE batch_size INT DEFAULT 5000;

    -- 배치 단위 인서트를 위한 autocommit 비활성화
    SET autocommit = 0;

    WHILE i <= 1000000 DO
        SET v_cust_id = FLOOR(RAND() * 100000) + 1;
        SET v_merch_id = FLOOR(RAND() * 5000) + 1;
        SET v_amount = ROUND(RAND() * 500000 + 100, 2);  -- 100 ~ 500,100원
        SET v_tx_date = DATE_ADD('2024-01-01', INTERVAL FLOOR(RAND() * 365) DAY);
        SET v_tx_date = DATE_ADD(v_tx_date, INTERVAL FLOOR(RAND() * 86400) SECOND);
        SET v_status = ELT(1 + FLOOR(RAND() * 3), 'approved', 'approved', 'rejected');  -- 승인 비율 높게
        SET v_category = ELT(1 + FLOOR(RAND() * 8),
            '식비', '교통', '쇼핑', '의료', '교육', '여행', '문화', '기타');

        INSERT INTO src_card_transactions
            (customer_id, merchant_id, card_number, transaction_date,
             transaction_amount, currency, approval_status, approval_number,
             installment_months, category)
        VALUES (
            v_cust_id,
            v_merch_id,
            CONCAT('5412-', LPAD(FLOOR(RAND()*10000),4,'0'), '-', LPAD(FLOOR(RAND()*10000),4,'0'), '-', LPAD(FLOOR(RAND()*10000),4,'0')),
            v_tx_date,
            v_amount,
            'KRW',
            v_status,
            CONCAT('AP', LPAD(i, 8, '0')),
            ELT(1 + FLOOR(RAND() * 4), 0, 0, 3, 6),
            v_category
        );

        -- 배치 단위 커밋
        IF i % batch_size = 0 THEN
            COMMIT;
        END IF;

        SET i = i + 1;
    END WHILE;

    COMMIT;
    SET autocommit = 1;
END//

DELIMITER ;
CALL generate_transactions();


-- ------------------------------------------
-- 4. 의도적 품질 이슈 삽입
-- ------------------------------------------

-- ★ 이슈 1: NULL 거래금액 500건 삽입
UPDATE src_card_transactions
SET transaction_amount = NULL
WHERE transaction_id IN (
    SELECT transaction_id FROM (
        SELECT transaction_id
        FROM src_card_transactions
        ORDER BY RAND()
        LIMIT 500
    ) AS tmp
);

-- ★ 이슈 2: 중복 거래 200건 삽입
-- 기존 거래를 복제하여 중복 레코드 생성
INSERT INTO src_card_transactions
    (customer_id, merchant_id, card_number, transaction_date,
     transaction_amount, currency, approval_status, approval_number,
     installment_months, category)
SELECT
    customer_id, merchant_id, card_number, transaction_date,
    transaction_amount, currency, approval_status, approval_number,
    installment_months, category
FROM src_card_transactions
ORDER BY RAND()
LIMIT 200;

-- ★ 이슈 3: FK 위반 — 존재하지 않는 가맹점 코드 300건
UPDATE src_card_transactions
SET merchant_id = 99999  -- 존재하지 않는 merchant_id
WHERE transaction_id IN (
    SELECT transaction_id FROM (
        SELECT transaction_id
        FROM src_card_transactions
        WHERE merchant_id != 99999
        ORDER BY RAND()
        LIMIT 300
    ) AS tmp
);


-- ------------------------------------------
-- 5. 프로시저 정리
-- ------------------------------------------
DROP PROCEDURE IF EXISTS generate_merchants;
DROP PROCEDURE IF EXISTS generate_customers;
DROP PROCEDURE IF EXISTS generate_transactions;
