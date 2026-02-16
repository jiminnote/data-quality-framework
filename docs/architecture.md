# 아키텍처 문서
## Data Quality Framework — Architecture

---

## 1. 시스템 개요

Data Quality Framework는 금융 데이터의 품질을 자동으로 검증하는 Python 기반 프레임워크입니다.
ETL 파이프라인의 소스/타겟 테이블 간 데이터 정합성을 6가지 검증 유형으로 확인하고,
결과를 HTML/CSV 리포트로 자동 생성합니다.

```
┌──────────────────────────────────────────────────────────────┐
│                    Data Quality Framework                     │
│                                                              │
│  ┌─────────┐   ┌──────────────┐   ┌────────────┐            │
│  │ Config  │──▶│   Checker    │──▶│  Reporter  │            │
│  │ (YAML)  │   │  (6 types)   │   │ (HTML/CSV) │            │
│  └─────────┘   └──────┬───────┘   └────────────┘            │
│                        │                                      │
│                 ┌──────▼───────┐                              │
│                 │ DB Connector │                              │
│                 │   (MySQL)    │                              │
│                 └──────┬───────┘                              │
│                        │                                      │
└────────────────────────┼─────────────────────────────────────┘
                         │
              ┌──────────▼──────────┐
              │   MySQL (Docker)    │
              │  ┌───────────────┐  │
              │  │ src_customers │  │
              │  │ src_merchants │  │
              │  │ src_card_tx   │  │
              │  │ tgt_customers │  │
              │  │ tgt_card_tx   │  │
              │  │ tgt_daily_sum │  │
              │  └───────────────┘  │
              └─────────────────────┘
```

---

## 2. 디렉토리 구조

```
data-quality-framework/
├── config/
│   ├── db_config.yml          # DB 접속 정보 (환경별)
│   └── rules/                 # 검증 규칙 정의 (YAML)
│       ├── count_rules.yml
│       ├── null_rules.yml
│       ├── transform_rules.yml
│       └── masking_rules.yml
├── sql/                        # MySQL 스키마 + 데이터
│   ├── init_schema.sql
│   ├── init_data.sql
│   └── init_target.sql
├── checks/                     # SQL 검증 쿼리 템플릿
├── src/                        # Python 소스 코드
│   ├── config_loader.py       # YAML 로더
│   ├── db_connector.py        # MySQL 커넥션 관리
│   ├── checker/               # 검증 모듈 (6개)
│   │   ├── base_checker.py
│   │   ├── count_checker.py
│   │   ├── null_checker.py
│   │   ├── duplicate_checker.py
│   │   ├── range_checker.py
│   │   ├── transform_checker.py
│   │   └── masking_checker.py
│   ├── reporter/              # 리포트 생성
│   │   ├── html_reporter.py
│   │   └── csv_reporter.py
│   └── main.py                # 통합 실행 엔트리포인트
├── scripts/                    # 자동화 스크립트
│   ├── run_validation.sh
│   └── setup_crontab.sh
├── tests/                      # 단위 테스트
├── reports/                    # 생성된 리포트 저장
├── docker-compose.yml          # MySQL 컨테이너
└── requirements.txt
```

---

## 3. 핵심 컴포넌트

### 3.1 Config Layer (`config_loader.py`)
- YAML 기반 설정 관리
- 환경별 DB 접속 정보 분리 (development / docker / production)
- 환경변수 치환 지원 (`${DB_HOST}` 형식)
- 검증 규칙을 YAML로 정의하여 코드 변경 없이 규칙 추가/수정 가능

### 3.2 Database Layer (`db_connector.py`)
- MySQL 커넥션 풀 관리
- 컨텍스트 매니저 기반 자동 연결 해제
- **★ TS-1**: 대용량 테이블을 위한 청크 분할 카운트
- **★ TS-5**: Docker MySQL 초기화 대기를 위한 재시도 로직

### 3.3 Checker Layer (`src/checker/`)
모든 체커는 `BaseChecker`를 상속하며, 공통 인터페이스 `run_checks()` 구현

| 체커 | 설명 | 핵심 기능 |
|------|------|----------|
| `CountChecker` | 건수 검증 | 소스/타겟 건수 비교, 오차율 설정, 청크 분할 |
| `NullChecker` | NULL 검증 | NULL 비율 체크, 빈 문자열 통합 검출 (TS-2) |
| `DuplicateChecker` | 중복 검증 | 복합키 기반 중복 탐지, 상세 목록 추출 |
| `RangeChecker` | 범위/FK 검증 | 숫자/날짜 범위, FK 정합성 체크 |
| `TransformChecker` | 변환 로직 검증 | JOIN 기반 값 비교, 집계 정합성 |
| `MaskingChecker` | 비식별화 검증 | 마스킹/해싱 검증, SUBSTRING 기반 (TS-3) |

### 3.4 Reporter Layer (`src/reporter/`)
- **HTML Reporter**: 시각적 대시보드형 리포트 (PASS/FAIL 색상 표시)
- **CSV Reporter**: 후속 분석용 표 형식 데이터

### 3.5 Execution Layer
- `main.py`: 통합 파이프라인 (Config → DB → Checker → Reporter)
- `run_validation.sh`: 배치 실행 스크립트
- `setup_crontab.sh`: crontab 자동 등록

---

## 4. 실행 흐름

```
1. main.py 실행
   ├─ ConfigLoader: YAML 설정/규칙 로딩
   ├─ DBConnector: MySQL 연결 (풀 초기화)
   │
   ├─ CountChecker.run_checks()
   ├─ NullChecker.run_checks()
   ├─ DuplicateChecker.run_checks()
   ├─ RangeChecker.run_checks()
   ├─ TransformChecker.run_checks()
   ├─ MaskingChecker.run_checks()
   │
   ├─ 결과 집계 (PASS/FAIL/WARNING/ERROR)
   │
   ├─ HTMLReporter.generate()
   ├─ CSVReporter.generate()
   │
   └─ exit code: 0 (all pass) / 1 (any fail)
```

---

## 5. 데이터 모델

### 소스 테이블 (ETL 전)
- `src_customers` (10만 건): 고객 정보 (이름, 전화번호, 주민번호, 주소)
- `src_card_transactions` (100만 건): 카드 거래 (거래일시, 가맹점, 금액)
- `src_merchants` (5천 건): 가맹점 정보 (가맹점명, 업종, 지역)

### 타겟 테이블 (ETL 후)
- `tgt_customers`: 비식별화 적용된 고객 정보
- `tgt_card_transactions`: 변환·적재된 거래 데이터
- `tgt_daily_summary`: 일별 거래 집계

### 의도적 품질 이슈
- NULL 거래금액 500건
- 중복 거래 200건
- 비식별화 누락 100건
- ETL 건수 불일치 1,000건
- FK 위반 300건

---

## 6. 기술 스택

| 영역 | 기술 |
|------|------|
| 언어 | Python 3.9+ |
| 데이터베이스 | MySQL 8.0 |
| 컨테이너 | Docker / Docker Compose |
| 설정 관리 | PyYAML |
| DB 드라이버 | mysql-connector-python |
| 리포트 | Jinja2 (HTML), csv (CSV) |
| 테스트 | pytest |
| 자동화 | Shell Script, crontab |

---

## 7. 확장 포인트

- **새 검증 유형 추가**: `BaseChecker` 상속 후 `run_checks()` 구현
- **새 DB 지원**: `DBConnector` 인터페이스 유지하며 드라이버만 교체
- **새 리포트 형식**: `Reporter` 패턴에 맞춰 추가 (Excel, Slack 알림 등)
- **규칙 추가**: YAML 파일에 규칙만 추가하면 코드 변경 없이 적용
