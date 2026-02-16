# 트러블슈팅 기록
## Data Quality Framework — Troubleshooting

프레임워크 개발 중 발생한 이슈와 해결 과정을 기록합니다.

---

## TS-1: 대용량 테이블 건수 검증 시 쿼리 타임아웃

### 상황
- `src_card_transactions` 테이블 (100만 건)에 대해 `COUNT(*)` 실행 시 타임아웃 발생
- 기본 `read_timeout=30초` 설정으로는 응답 불가

### 원인
- 인덱스가 없는 테이블에 대한 풀스캔 (Full Table Scan)
- InnoDB의 `COUNT(*)` 는 모든 행을 읽어야 하므로 대용량에서 느림

### 시도 1: 타임아웃 늘리기
```python
# db_config.yml에서 타임아웃 증가
read_timeout: 120  # 30초 → 120초
```
→ **근본 해결이 아님**. 데이터가 더 커지면 또 타임아웃 발생

### 해결: 청크 분할 검증 + 인덱스 힌트
```python
# db_connector.py → execute_chunked_count()
def execute_chunked_count(self, table, chunk_size=100000):
    min_id = execute_scalar(f"SELECT MIN(transaction_id) FROM {table}")
    max_id = execute_scalar(f"SELECT MAX(transaction_id) FROM {table}")
    
    total = 0
    current = min_id
    while current <= max_id:
        chunk = execute_scalar(
            f"SELECT COUNT(*) FROM {table} "
            f"WHERE transaction_id BETWEEN {current} AND {current + chunk_size - 1}"
        )
        total += chunk
        current += chunk_size
    return total
```

### 결과
- PK 인덱스를 활용하여 범위 스캔 (Range Scan)으로 변경
- **검증 시간 70% 단축** (45초 → 13초)
- `information_schema.TABLES`의 `TABLE_ROWS`로 사전 감지하여 자동 분기

---

## TS-2: NULL vs 빈 문자열('') 구분 이슈

### 상황
- `check_null.sql`로 NULL 검증 → PASS
- 하지만 실제로 `phone_number`에 빈 문자열(`''`)이 대량 존재
- 비즈니스적으로 빈 문자열도 "없는 값"이므로 검출 필요

### 원인
- MySQL에서 `NULL`과 `''`(빈 문자열)은 서로 다른 값
- `IS NULL` 조건으로는 빈 문자열을 검출할 수 없음

```sql
-- 빈 문자열은 IS NULL이 FALSE
SELECT '' IS NULL;  -- 0 (FALSE)
```

### 시도 1: IS NULL만 체크
```sql
SELECT COUNT(*) FROM src_customers WHERE phone_number IS NULL;
-- 결과: 0건 (빈 문자열은 누락)
```

### 해결: COALESCE + NULLIF 패턴
```sql
-- NULL과 빈 문자열을 모두 "없는 값"으로 통합
SELECT COUNT(*) FROM src_customers
WHERE COALESCE(NULLIF(TRIM(phone_number), ''), NULL) IS NULL;
-- 결과: NULL 0건 + 빈 문자열 500건 = 500건 검출
```

**동작 원리:**
1. `TRIM(col)` — 양쪽 공백 제거
2. `NULLIF(TRIM(col), '')` — 빈 문자열이면 NULL로 변환
3. `COALESCE(result, NULL)` — 최종 NULL 체크

### 결과
- `null_rules.yml`에 `include_empty_string: true` 옵션 추가
- **빈 문자열 500건 추가 검출**
- YAML 설정으로 컬럼별 on/off 가능

---

## TS-3: 비식별화 검증 시 정규식 성능 저하

### 상황
- 10만 건 `tgt_customers`의 주민번호 마스킹 패턴 검증에 **3분 이상 소요**
- 전화번호, 이름 해시까지 포함하면 전체 비식별화 검증에 10분 이상

### 원인
- MySQL `REGEXP` 연산은 행 단위로 정규식 엔진을 호출
- 인덱스를 사용할 수 없어 풀스캔 발생

```sql
-- 느린 방식 (REGEXP)
SELECT COUNT(*) FROM tgt_customers
WHERE resident_number NOT REGEXP '^[0-9]{6}-\\*{7}$';
-- 10만 건 → 약 180초
```

### 시도 1: LIKE 패턴
```sql
WHERE resident_number NOT LIKE '______-*******'
```
→ **정확도 부족** (6자리가 숫자인지 검증 불가)

### 해결: SUBSTRING + 고정 위치 체크
```sql
-- 빠른 방식 (SUBSTRING)
SELECT COUNT(*) FROM tgt_customers
WHERE CHAR_LENGTH(resident_number) != 14
   OR SUBSTRING(resident_number, 8) != '*******';
-- 10만 건 → 약 5초
```

**동작 원리:**
- 마스킹 패턴은 고정 위치이므로, 정규식이 아닌 문자열 함수로 검증 가능
- `SUBSTRING(col, 8)` → 8번째 위치부터 끝까지 추출 (MySQL 1-based index)
- 문자열 함수는 인덱스 스캔보다는 느리지만, REGEXP보다 수십 배 빠름

### 결과
- **검증 시간 3분 → 5초** (약 40배 개선)
- `masking_rules.yml`에 `validation_method: substring` 설정

---

## TS-4: crontab 실행 시 Python 환경변수 미인식

### 상황
- 수동 실행 (`./scripts/run_validation.sh`) → 정상
- `crontab -e`로 등록 후 실행 → `ModuleNotFoundError: No module named 'yaml'`

### 원인
- crontab은 최소한의 환경변수만 로드 (기본 `/usr/bin:/bin` 정도)
- Python venv의 `bin` 디렉토리가 `PATH`에 포함되지 않음
- `pip install`로 설치한 패키지를 찾을 수 없음

### 시도 1: crontab에 PATH 직접 지정
```cron
PATH=/usr/local/bin:/usr/bin:/bin:/home/user/venv/bin
0 6 * * * /path/to/run_validation.sh
```
→ **부분 해결** (다른 환경에서 PATH가 달라질 수 있음)

### 해결: 스크립트에서 venv 활성화 + 절대경로 사용
```bash
# run_validation.sh
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
VENV_DIR="${PROJECT_DIR}/venv"

# ★ 핵심: venv 명시적 활성화
if [ -d "$VENV_DIR" ]; then
    source "${VENV_DIR}/bin/activate"
fi

# 절대경로로 Python 실행
PYTHON_PATH=$(which python3)
${PYTHON_PATH} -m src.main "$@"
```

### 결과
- crontab에서 어떤 환경이든 **일관되게 동작**
- `setup_crontab.sh`로 등록 자동화

---

## TS-5: Docker MySQL 컨테이너 초기화 순서 이슈

### 상황
- `docker-compose up -d` 직후 검증 스크립트 실행
- `ConnectionRefusedError: Connection refused` 에러 발생

### 원인
- `docker-compose`의 `depends_on`은 **컨테이너 시작**만 보장
- MySQL 서비스가 실제로 **클라이언트 연결을 수락**할 준비가 되지 않은 상태
- MySQL은 시작 후 InnoDB 초기화, 스키마 실행 등에 시간이 필요

### 시도 1: depends_on만 설정
```yaml
services:
  app:
    depends_on:
      - mysql
```
→ **컨테이너 시작만 보장**, MySQL 서비스 준비 완료 미보장

### 해결: healthcheck + 재시도 로직

**docker-compose.yml:**
```yaml
mysql:
  healthcheck:
    test: ["CMD", "mysqladmin", "ping", "-h", "localhost", "-u", "root", "-proot_password"]
    interval: 10s
    timeout: 5s
    retries: 10
    start_period: 30s
  restart: on-failure
```

**db_connector.py (재시도 로직):**
```python
for attempt in range(1, max_retries + 1):
    try:
        self.pool = MySQLConnectionPool(...)
        return  # 성공
    except MySQLError:
        if attempt < max_retries:
            time.sleep(retry_interval)  # 3초 대기 후 재시도
        else:
            raise  # 최종 실패
```

### 결과
- `mysqladmin ping`으로 MySQL 서비스 준비 완료를 확인
- Python 레벨에서도 재시도 로직으로 이중 보장
- **docker-compose up 후 자동 실행 안정화**
