# 📊 Data Quality Framework

**SQL 기반 데이터 품질 검증 프레임워크** — 금융 데이터의 ETL 파이프라인 정합성을 자동으로 검증합니다.

[![Python 3.9+](https://img.shields.io/badge/Python-3.9%2B-blue.svg)](https://www.python.org/)
[![MySQL 8.0](https://img.shields.io/badge/MySQL-8.0-orange.svg)](https://www.mysql.com/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED.svg)](https://www.docker.com/)

---

## 🎯 프로젝트 배경

금융 IT 실무에서 겪은 데이터 품질 이슈를 체계적으로 해결하기 위해 개발한 프레임워크입니다.

- **수동 SQL 검증** → 휴먼에러, 시간 소모 → **자동화된 검증 파이프라인**
- **사후 대응** → **사전 예방 체계** (crontab 기반 배치 실행)
- **비식별화 검증 누락** → **금융권 컴플라이언스 준수** (마스킹/해싱 자동 검증)

---

## ⚡ 핵심 기능

| # | 검증 유형 | 설명 |
|---|----------|------|
| 1 | **건수 검증** | 소스/타겟 테이블 건수 비교, 허용 오차율 설정, 청크 분할 |
| 2 | **NULL 검증** | 필수 컬럼 NULL 비율 체크, 빈 문자열 통합 검출 (TS-2) |
| 3 | **중복 검증** | 복합키 기반 중복 탐지, 상세 목록 추출 |
| 4 | **범위 검증** | 숫자/날짜 범위 체크, FK 정합성 검증 |
| 5 | **변환 로직 검증** | ETL 전후 JOIN 기반 값 비교, 집계 정합성 |
| 6 | **비식별화 검증** | 주민번호/전화번호 마스킹, 이름 해싱 적용 확인 |

---

## 📂 프로젝트 구조

```
data-quality-framework/
├── config/
│   ├── db_config.yml                 # DB 접속 정보 (환경별)
│   └── rules/                        # 검증 규칙 정의 (YAML)
│       ├── count_rules.yml
│       ├── null_rules.yml
│       ├── transform_rules.yml
│       └── masking_rules.yml
├── sql/
│   ├── init_schema.sql               # 금융 테이블 스키마
│   ├── init_data.sql                 # 샘플 데이터 (고객 10만/거래 100만)
│   └── init_target.sql               # ETL 타겟 테이블
├── checks/                           # SQL 검증 쿼리 템플릿
│   ├── check_duplicates.sql
│   ├── check_null.sql
│   ├── check_range.sql
│   ├── check_foreign_key.sql
│   ├── check_count.sql
│   ├── check_transform.sql
│   └── check_masking.sql
├── src/
│   ├── config_loader.py              # YAML 설정/규칙 로더
│   ├── db_connector.py               # MySQL 커넥션 관리
│   ├── checker/                      # 검증 모듈 (6개)
│   │   ├── base_checker.py
│   │   ├── count_checker.py
│   │   ├── null_checker.py
│   │   ├── duplicate_checker.py
│   │   ├── range_checker.py
│   │   ├── transform_checker.py
│   │   └── masking_checker.py
│   ├── reporter/                     # 리포트 생성
│   │   ├── html_reporter.py
│   │   └── csv_reporter.py
│   └── main.py                       # 통합 실행 엔트리포인트
├── scripts/
│   ├── run_validation.sh             # 배치 실행 스크립트
│   └── setup_crontab.sh             # crontab 등록
├── tests/
│   └── test_checkers.py              # 단위 테스트
├── docs/
│   ├── architecture.md               # 아키텍처 문서
│   └── troubleshooting.md           # 트러블슈팅 기록
├── reports/                          # 생성된 리포트 저장
├── docker-compose.yml                # MySQL 컨테이너
└── requirements.txt
```

---

## 🚀 빠른 시작

### 1. 환경 설정

```bash
# 저장소 클론
git clone https://github.com/jiminnote/data-quality-framework.git
cd data-quality-framework

# Python 가상환경 생성 및 활성화
python3 -m venv venv
source venv/bin/activate

# 의존성 설치
pip install -r requirements.txt
```

### 2. Docker MySQL 실행

```bash
# MySQL 컨테이너 시작 (스키마 + 데이터 자동 초기화)
docker-compose up -d

# 초기화 완료 확인 (healthcheck)
docker-compose logs -f mysql
```

> ⚠️ 초기 데이터 생성(고객 10만, 거래 100만)에 수 분이 소요될 수 있습니다.

### 3. 검증 실행

```bash
# 전체 검증 실행
python -m src.main --env development

# Docker 환경
python -m src.main --env docker

# 특정 검증만 실행
python -m src.main --checks count,null,masking

# HTML 리포트만 생성
python -m src.main --report html
```

### 4. 리포트 확인

```bash
# reports/ 디렉토리에 HTML/CSV 리포트 생성
open reports/dq_report_*.html
```

---

## 🔧 트러블슈팅 하이라이트

### TS-1: 대용량 건수 검증 타임아웃 → 청크 분할 검증
- **문제**: 100만 건 `COUNT(*)` 시 30초 타임아웃
- **해결**: PK 범위 기반 청크 분할 + 인덱스 활용
- **결과**: 검증 시간 **70% 단축** (45초 → 13초)

### TS-2: NULL vs 빈 문자열 구분 이슈
- **문제**: `IS NULL`로 빈 문자열(`''`) 미검출
- **해결**: `COALESCE(NULLIF(TRIM(col), ''), NULL) IS NULL` 패턴
- **결과**: 빈 문자열 **500건 추가 검출**

### TS-3: 비식별화 정규식 성능 저하
- **문제**: 10만 건 `REGEXP` 매칭 3분 소요
- **해결**: `SUBSTRING` + 고정 위치 체크로 전환
- **결과**: 3분 → **5초** (40배 개선)

### TS-4: crontab 환경변수 미인식
- **문제**: crontab 실행 시 `ModuleNotFoundError`
- **해결**: `run_validation.sh`에서 venv 활성화 + 절대경로 사용
- **결과**: crontab **안정적 실행** 확인

### TS-5: Docker MySQL 초기화 순서 이슈
- **문제**: `docker-compose up` 직후 `Connection refused`
- **해결**: `healthcheck` (mysqladmin ping) + Python 재시도 로직
- **결과**: MySQL 준비 완료 후 **자동 실행**

> 상세 내용: [docs/troubleshooting.md](docs/troubleshooting.md)

---

## 🧪 테스트

```bash
# 전체 테스트 실행 (SQLite 인메모리 DB 사용, MySQL 불필요)
pytest tests/ -v

# 특정 테스트
pytest tests/test_checkers.py::TestNullChecker -v
```

---

## ⏰ 배치 자동화

```bash
# 배치 실행
./scripts/run_validation.sh --env docker

# crontab 등록 (매일 오전 6시)
./scripts/setup_crontab.sh

# crontab 상태 확인
./scripts/setup_crontab.sh --status
```

---

## 🏗️ 기술 스택

| 영역 | 기술 |
|------|------|
| Language | Python 3.9+ |
| Database | MySQL 8.0 |
| Container | Docker / Docker Compose |
| Config | PyYAML |
| Report | HTML (Jinja2), CSV |
| Test | pytest |
| Automation | Shell Script, crontab |

---

## 📊 금융 샘플 데이터

| 테이블 | 건수 | 설명 |
|--------|------|------|
| `src_customers` | 10만 | 고객 정보 (이름, 전화번호, 주민번호) |
| `src_card_transactions` | 100만 | 카드 거래 (일시, 가맹점, 금액) |
| `src_merchants` | 5천 | 가맹점 정보 (가맹점명, 업종) |
| `tgt_customers` | 10만 | 비식별화 적용 고객 정보 |
| `tgt_card_transactions` | ~99만 | 변환·적재 거래 (의도적 1천 건 누락) |
| `tgt_daily_summary` | 365 | 일별 거래 집계 |

### 의도적 품질 이슈 (검증 대상)
- NULL 거래금액 500건
- 중복 거래 200건
- 비식별화 누락 100건
- 건수 불일치 1,000건
- FK 위반 300건

---

## 📖 문서

- [아키텍처](docs/architecture.md) — 시스템 구조, 컴포넌트 설명
- [트러블슈팅](docs/troubleshooting.md) — 5건의 이슈 해결 과정

---

## 📜 라이센스

MIT License
