# Data Quality Framework

데이터 품질을 검증하기 위한 SQL 기반 프레임워크입니다.

## 📁 프로젝트 구조

```
data-quality-framework/
├── README.md
├── checks/
│   ├── check_duplicates.sql      # 중복 체크
│   ├── check_null.sql            # NULL 체크  
│   ├── check_range.sql           # 값 범위 체크
│   └── check_foreign_key.sql     # FK 정합성 체크
└── examples/
    └── sample_validation.py      # 실행 예제
```

## 🔍 체크 항목

### 1. 중복 체크 (check_duplicates.sql)
테이블 내 중복 레코드를 탐지합니다.

### 2. NULL 체크 (check_null.sql)
필수 컬럼의 NULL 값을 검증합니다.

### 3. 값 범위 체크 (check_range.sql)
숫자/날짜 컬럼의 유효 범위를 검증합니다.

### 4. FK 정합성 체크 (check_foreign_key.sql)
외래 키 참조 무결성을 검증합니다.

## 사용법

### Python 예제 실행

```bash
cd examples
python sample_validation.py
```

## 설정

### 환경 변수

```bash
export DB_HOST=localhost
export DB_PORT=5432
export DB_NAME=your_database
export DB_USER=your_username
export DB_PASSWORD=your_password
```

## 요구사항

- Python 3.8+
- psycopg2 (PostgreSQL) 또는 해당 DB 드라이버
- 대상 데이터베이스 접근 권한
