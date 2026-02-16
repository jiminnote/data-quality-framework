-- ============================================
-- 금융 샘플 데이터 스키마 정의
-- ============================================
-- 소스 테이블 (ETL 전) + 타겟 테이블 (ETL 후)
-- 카드거래 도메인 기반 가상 데이터
-- ============================================

USE data_quality;

-- ------------------------------------------
-- 소스 테이블 (ETL 전 원본 데이터)
-- ------------------------------------------

-- 고객 정보 테이블 (10만 건 목표)
CREATE TABLE IF NOT EXISTS src_customers (
    customer_id       INT PRIMARY KEY AUTO_INCREMENT,
    customer_name     VARCHAR(100) NOT NULL COMMENT '고객 이름',
    phone_number      VARCHAR(20) COMMENT '전화번호 (010-XXXX-XXXX)',
    resident_number   VARCHAR(14) COMMENT '주민등록번호 (XXXXXX-XXXXXXX)',
    email             VARCHAR(100) COMMENT '이메일',
    address           VARCHAR(300) COMMENT '주소',
    birth_date        DATE COMMENT '생년월일',
    gender            CHAR(1) COMMENT '성별 (M/F)',
    customer_grade    VARCHAR(10) DEFAULT 'NORMAL' COMMENT '고객 등급',
    created_at        DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at        DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_customer_name (customer_name),
    INDEX idx_phone (phone_number),
    INDEX idx_grade (customer_grade)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='소스: 고객 정보';


-- 가맹점 정보 테이블 (5천 건 목표)
CREATE TABLE IF NOT EXISTS src_merchants (
    merchant_id       INT PRIMARY KEY AUTO_INCREMENT,
    merchant_name     VARCHAR(200) NOT NULL COMMENT '가맹점명',
    business_type     VARCHAR(50) COMMENT '업종 (식음료/유통/온라인 등)',
    region            VARCHAR(100) COMMENT '지역',
    merchant_code     VARCHAR(20) UNIQUE COMMENT '가맹점 코드',
    status            VARCHAR(10) DEFAULT 'ACTIVE' COMMENT '상태 (ACTIVE/INACTIVE)',
    created_at        DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_business_type (business_type),
    INDEX idx_region (region),
    INDEX idx_merchant_code (merchant_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='소스: 가맹점 정보';


-- 카드 거래 테이블 (100만 건 목표)
CREATE TABLE IF NOT EXISTS src_card_transactions (
    transaction_id      BIGINT PRIMARY KEY AUTO_INCREMENT,
    customer_id         INT NOT NULL COMMENT '고객 ID (FK)',
    merchant_id         INT COMMENT '가맹점 ID (FK)',
    card_number         VARCHAR(20) COMMENT '카드번호 (마스킹 전)',
    transaction_date    DATETIME NOT NULL COMMENT '거래 일시',
    transaction_amount  DECIMAL(15, 2) COMMENT '거래 금액',
    currency            VARCHAR(3) DEFAULT 'KRW' COMMENT '통화',
    approval_status     VARCHAR(20) DEFAULT 'approved' COMMENT '승인 상태',
    approval_number     VARCHAR(20) COMMENT '승인 번호',
    installment_months  INT DEFAULT 0 COMMENT '할부 개월수',
    category            VARCHAR(50) COMMENT '거래 카테고리',
    created_at          DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_customer (customer_id),
    INDEX idx_merchant (merchant_id),
    INDEX idx_tx_date (transaction_date),
    INDEX idx_approval (approval_status),
    INDEX idx_amount (transaction_amount)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='소스: 카드 거래';


-- ------------------------------------------
-- 타겟 테이블 (ETL 후 — 검증 대상)
-- ------------------------------------------

-- 비식별화 적용된 고객 정보
CREATE TABLE IF NOT EXISTS tgt_customers (
    customer_id         INT PRIMARY KEY,
    customer_name_hash  VARCHAR(64) COMMENT '이름 SHA-256 해시',
    phone_number        VARCHAR(20) COMMENT '전화번호 (중간 4자리 마스킹)',
    resident_number     VARCHAR(14) COMMENT '주민번호 (뒤 7자리 마스킹)',
    email               VARCHAR(100) COMMENT '이메일',
    address             VARCHAR(300) COMMENT '주소',
    birth_date          DATE COMMENT '생년월일',
    gender              CHAR(1) COMMENT '성별',
    customer_grade      VARCHAR(10) COMMENT '고객 등급',
    etl_loaded_at       DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT 'ETL 적재 시각',
    INDEX idx_grade (customer_grade)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='타겟: 비식별화 고객 정보';


-- 변환·적재된 거래 데이터
CREATE TABLE IF NOT EXISTS tgt_card_transactions (
    transaction_id      BIGINT PRIMARY KEY,
    customer_id         INT NOT NULL,
    merchant_id         INT,
    card_number_masked  VARCHAR(20) COMMENT '카드번호 (앞6뒤4 외 마스킹)',
    transaction_date    DATETIME NOT NULL,
    transaction_amount  DECIMAL(15, 2),
    currency            VARCHAR(3),
    approval_status     VARCHAR(20),
    approval_number     VARCHAR(20),
    installment_months  INT,
    category            VARCHAR(50),
    etl_loaded_at       DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_customer (customer_id),
    INDEX idx_merchant (merchant_id),
    INDEX idx_tx_date (transaction_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='타겟: 카드 거래';


-- 일별 거래 집계
CREATE TABLE IF NOT EXISTS tgt_daily_summary (
    summary_date        DATE PRIMARY KEY COMMENT '집계 날짜',
    transaction_count   INT NOT NULL COMMENT '거래 건수',
    total_amount        DECIMAL(18, 2) NOT NULL COMMENT '총 거래 금액',
    avg_amount          DECIMAL(15, 2) COMMENT '평균 거래 금액',
    max_amount          DECIMAL(15, 2) COMMENT '최대 거래 금액',
    min_amount          DECIMAL(15, 2) COMMENT '최소 거래 금액',
    approved_count      INT COMMENT '승인 건수',
    rejected_count      INT COMMENT '거절 건수',
    unique_customers    INT COMMENT '고유 고객수',
    etl_loaded_at       DATETIME DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='타겟: 일별 거래 집계';


-- ------------------------------------------
-- 검증 결과 저장 테이블 (프레임워크 내부용)
-- ------------------------------------------
CREATE TABLE IF NOT EXISTS dq_validation_results (
    result_id           BIGINT PRIMARY KEY AUTO_INCREMENT,
    rule_id             VARCHAR(20) NOT NULL COMMENT '검증 규칙 ID',
    check_type          VARCHAR(30) NOT NULL COMMENT '검증 유형',
    table_name          VARCHAR(100) COMMENT '대상 테이블',
    column_name         VARCHAR(100) COMMENT '대상 컬럼',
    result_status       VARCHAR(10) NOT NULL COMMENT 'PASS/FAIL/WARNING/ERROR',
    total_rows          BIGINT DEFAULT 0,
    violation_count     BIGINT DEFAULT 0,
    violation_ratio     DECIMAL(10, 6) DEFAULT 0,
    details             JSON COMMENT '상세 정보 (JSON)',
    executed_at         DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_rule (rule_id),
    INDEX idx_status (result_status),
    INDEX idx_executed (executed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='검증 결과 이력';
