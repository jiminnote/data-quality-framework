#!/bin/bash
# ============================================
# Data Quality Framework - 배치 실행 스크립트
# ============================================
# 사용법:
#   ./scripts/run_validation.sh                    # 기본 실행
#   ./scripts/run_validation.sh --env docker       # Docker 환경
#   ./scripts/run_validation.sh --checks count,null  # 특정 검증만
#
# ★ TS-4: crontab 실행 시 Python 환경변수 미인식 이슈 해결
#   - 절대경로 사용
#   - source venv/bin/activate 명시적 실행
# ============================================

set -euo pipefail

# ---- 경로 설정 (절대경로) ----
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
VENV_DIR="${PROJECT_DIR}/venv"
LOG_DIR="${PROJECT_DIR}/reports"
LOG_FILE="${LOG_DIR}/validation_$(date +%Y%m%d_%H%M%S).log"

# ---- 로그 디렉토리 생성 ----
mkdir -p "$LOG_DIR"

# ---- 타임스탬프 함수 ----
timestamp() {
    date "+%Y-%m-%d %H:%M:%S"
}

echo "======================================"
echo "🚀 Data Quality Framework - 배치 실행"
echo "   시작: $(timestamp)"
echo "   프로젝트: ${PROJECT_DIR}"
echo "======================================"

# ---- Python 가상환경 활성화 ----
# ★ TS-4: crontab에서 실행 시 PATH에 venv가 없으므로 명시적 activate 필요
if [ -d "$VENV_DIR" ]; then
    echo "📦 가상환경 활성화: ${VENV_DIR}"
    source "${VENV_DIR}/bin/activate"
else
    echo "⚠️  가상환경이 없습니다. 시스템 Python을 사용합니다."
    echo "   가상환경 생성: python3 -m venv ${VENV_DIR}"
fi

# ---- Python 경로 확인 ----
PYTHON_PATH=$(which python3 2>/dev/null || which python 2>/dev/null)
echo "🐍 Python: ${PYTHON_PATH}"
echo "   버전: $(${PYTHON_PATH} --version)"

# ---- 의존성 확인 ----
cd "$PROJECT_DIR"
if [ -f "requirements.txt" ]; then
    pip install -r requirements.txt --quiet 2>/dev/null || true
fi

# ---- 검증 실행 ----
echo ""
echo "🔍 검증 실행 중... (로그: ${LOG_FILE})"
echo ""

${PYTHON_PATH} -m src.main "$@" 2>&1 | tee "$LOG_FILE"
EXIT_CODE=${PIPESTATUS[0]}

echo ""
echo "======================================"
if [ $EXIT_CODE -eq 0 ]; then
    echo "✅ 검증 완료 - 모든 검증 통과"
elif [ $EXIT_CODE -eq 1 ]; then
    echo "❌ 검증 완료 - FAIL 항목 존재"
elif [ $EXIT_CODE -eq 2 ]; then
    echo "🔴 DB 연결 실패"
else
    echo "🔴 실행 오류 (exit code: ${EXIT_CODE})"
fi
echo "   종료: $(timestamp)"
echo "   로그: ${LOG_FILE}"
echo "======================================"

exit $EXIT_CODE
