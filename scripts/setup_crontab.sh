#!/bin/bash
# ============================================
# crontab 자동 등록 스크립트
# ============================================
# 사용법:
#   ./scripts/setup_crontab.sh              # crontab 등록
#   ./scripts/setup_crontab.sh --remove     # crontab 제거
#
# ★ TS-4: crontab 환경변수 미인식 이슈 해결
#   - run_validation.sh에서 venv 활성화 + 절대경로 사용
#   - crontab 엔트리에 SHELL, PATH 명시
# ============================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
RUN_SCRIPT="${SCRIPT_DIR}/run_validation.sh"
CRON_IDENTIFIER="# DATA_QUALITY_FRAMEWORK"

# ---- 실행 권한 설정 ----
chmod +x "$RUN_SCRIPT"

# ---- 도움말 ----
show_help() {
    echo "사용법: $0 [옵션]"
    echo ""
    echo "옵션:"
    echo "  (없음)     crontab에 검증 스케줄 등록"
    echo "  --remove   crontab에서 검증 스케줄 제거"
    echo "  --status   현재 등록 상태 확인"
    echo "  --help     도움말 표시"
    echo ""
    echo "기본 스케줄: 매일 오전 6시 실행"
}

# ---- 등록 ----
register_crontab() {
    # 기존 항목 제거
    remove_crontab 2>/dev/null || true

    # ★ TS-4: crontab 환경에서도 올바른 PATH를 사용하도록 설정
    # crontab은 최소한의 환경변수만 로드하므로, 스크립트 내에서 처리
    CRON_ENTRY="0 6 * * * ${RUN_SCRIPT} --env docker >> ${PROJECT_DIR}/reports/cron.log 2>&1 ${CRON_IDENTIFIER}"

    (crontab -l 2>/dev/null; echo "$CRON_ENTRY") | crontab -

    echo "✅ crontab 등록 완료"
    echo "   스케줄: 매일 오전 6:00"
    echo "   스크립트: ${RUN_SCRIPT}"
    echo "   로그: ${PROJECT_DIR}/reports/cron.log"
    echo ""
    echo "📋 현재 crontab:"
    crontab -l | grep -A1 "DATA_QUALITY" || echo "   (등록된 항목 없음)"
}

# ---- 제거 ----
remove_crontab() {
    crontab -l 2>/dev/null | grep -v "${CRON_IDENTIFIER}" | crontab - 2>/dev/null || true
    echo "✅ crontab 제거 완료"
}

# ---- 상태 확인 ----
check_status() {
    echo "📋 Data Quality Framework crontab 상태:"
    echo ""
    if crontab -l 2>/dev/null | grep -q "DATA_QUALITY_FRAMEWORK"; then
        crontab -l | grep "DATA_QUALITY_FRAMEWORK"
        echo ""
        echo "상태: ✅ 등록됨"
    else
        echo "상태: ❌ 미등록"
    fi
}

# ---- 메인 ----
case "${1:-}" in
    --remove)
        remove_crontab
        ;;
    --status)
        check_status
        ;;
    --help|-h)
        show_help
        ;;
    *)
        register_crontab
        ;;
esac
