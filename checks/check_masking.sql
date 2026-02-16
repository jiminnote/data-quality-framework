-- ============================================
-- 비식별화 검증 (Masking Check)
-- ============================================
-- 개인정보 마스킹/해싱 적용 여부를 검증합니다.
-- ★ TS-3: REGEXP 대신 SUBSTRING 기반 검증으로 성능 최적화
-- ============================================

-- 1. 주민번호 마스킹 검증
-- 기대 포맷: 123456-******* (앞 6자리 유지, 뒤 7자리 마스킹)
-- ★ TS-3 적용: SUBSTRING 기반 (기존 REGEXP 대비 40배 빠름)
SELECT
    '주민번호 마스킹 검증' AS check_name,
    COUNT(*) AS total_rows,
    SUM(CASE
        WHEN CHAR_LENGTH(resident_number) != 14
          OR SUBSTRING(resident_number, 8) != '*******'
        THEN 1 ELSE 0
    END) AS violation_count,
    CASE
        WHEN SUM(CASE
            WHEN CHAR_LENGTH(resident_number) != 14
              OR SUBSTRING(resident_number, 8) != '*******'
            THEN 1 ELSE 0
        END) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END AS check_result
FROM tgt_customers
WHERE resident_number IS NOT NULL;


-- 2. 전화번호 마스킹 검증
-- 기대 포맷: 010-****-5678 (중간 4자리 마스킹)
SELECT
    '전화번호 마스킹 검증' AS check_name,
    COUNT(*) AS total_rows,
    SUM(CASE
        WHEN SUBSTRING(phone_number, 5, 4) != '****'
        THEN 1 ELSE 0
    END) AS violation_count,
    CASE
        WHEN SUM(CASE
            WHEN SUBSTRING(phone_number, 5, 4) != '****'
            THEN 1 ELSE 0
        END) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END AS check_result
FROM tgt_customers
WHERE phone_number IS NOT NULL;


-- 3. 이름 해싱 검증 (SHA-256 = 64자 16진수)
SELECT
    '이름 해싱 검증' AS check_name,
    COUNT(*) AS total_rows,
    SUM(CASE
        WHEN CHAR_LENGTH(customer_name_hash) != 64
          OR customer_name_hash REGEXP '[^0-9a-fA-F]'
        THEN 1 ELSE 0
    END) AS violation_count,
    CASE
        WHEN SUM(CASE
            WHEN CHAR_LENGTH(customer_name_hash) != 64
              OR customer_name_hash REGEXP '[^0-9a-fA-F]'
            THEN 1 ELSE 0
        END) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END AS check_result
FROM tgt_customers
WHERE customer_name_hash IS NOT NULL;


-- 4. 비식별화 누락 레코드 검출 (원본 주민번호 잔존)
-- 마스킹 안 된 원본: 뒤 7자리가 모두 숫자
SELECT
    '비식별화 누락 검출' AS check_name,
    COUNT(*) AS leak_count,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS check_result
FROM tgt_customers
WHERE resident_number IS NOT NULL
  AND CHAR_LENGTH(resident_number) = 14
  AND SUBSTRING(resident_number, 8) != '*******';


-- 5. 비식별화 누락 상세 (샘플 조회)
SELECT
    customer_id,
    resident_number,
    phone_number,
    customer_name_hash,
    '비식별화 누락 의심' AS reason
FROM tgt_customers
WHERE resident_number IS NOT NULL
  AND CHAR_LENGTH(resident_number) = 14
  AND SUBSTRING(resident_number, 8) != '*******'
LIMIT 20;
