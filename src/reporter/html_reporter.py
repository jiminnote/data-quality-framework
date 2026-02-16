"""
HTML 검증 리포트 생성기
========================
검증 결과를 시각적인 HTML 리포트로 생성합니다.
PASS/FAIL 시각화 + 상세 테이블 포함

색상 코드:
  - PASS: 초록 (#27ae60)
  - FAIL: 빨강 (#e74c3c)
  - WARNING: 주황 (#f39c12)
  - ERROR: 회색 (#95a5a6)
"""

import os
import json
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>데이터 품질 검증 리포트 - {{ generated_at }}</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: 'Malgun Gothic', 'Apple SD Gothic Neo', sans-serif;
            background: #f5f6fa;
            color: #2c3e50;
            padding: 20px;
        }
        .container { max-width: 1200px; margin: 0 auto; }

        /* 헤더 */
        .header {
            background: linear-gradient(135deg, #1B4F72, #2E86C1);
            color: white;
            padding: 30px;
            border-radius: 10px;
            margin-bottom: 20px;
        }
        .header h1 { font-size: 24px; margin-bottom: 5px; }
        .header .subtitle { font-size: 14px; opacity: 0.8; }

        /* 요약 카드 */
        .summary-cards {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
            gap: 15px;
            margin-bottom: 25px;
        }
        .card {
            background: white;
            border-radius: 8px;
            padding: 20px;
            text-align: center;
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        }
        .card .number { font-size: 36px; font-weight: bold; }
        .card .label { font-size: 13px; color: #7f8c8d; margin-top: 5px; }
        .card.pass .number { color: #27ae60; }
        .card.fail .number { color: #e74c3c; }
        .card.warning .number { color: #f39c12; }
        .card.error .number { color: #95a5a6; }
        .card.total .number { color: #2c3e50; }
        .card.rate .number { color: #2E86C1; }

        /* 진행 바 */
        .progress-bar {
            background: #ecf0f1;
            border-radius: 10px;
            height: 30px;
            overflow: hidden;
            margin-bottom: 25px;
        }
        .progress-bar .fill {
            height: 100%;
            display: flex;
            align-items: center;
            justify-content: center;
            color: white;
            font-size: 13px;
            font-weight: bold;
        }
        .progress-bar .pass-fill { background: #27ae60; }
        .progress-bar .fail-fill { background: #e74c3c; }
        .progress-bar .warn-fill { background: #f39c12; }

        /* 섹션 */
        .section {
            background: white;
            border-radius: 8px;
            padding: 20px;
            margin-bottom: 20px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        }
        .section h2 {
            font-size: 18px;
            margin-bottom: 15px;
            padding-bottom: 10px;
            border-bottom: 2px solid #ecf0f1;
        }

        /* 테이블 */
        table {
            width: 100%;
            border-collapse: collapse;
            font-size: 13px;
        }
        th {
            background: #1B4F72;
            color: white;
            padding: 10px 12px;
            text-align: left;
            font-weight: 500;
        }
        td {
            padding: 8px 12px;
            border-bottom: 1px solid #ecf0f1;
        }
        tr:hover { background: #f8f9fa; }

        /* 상태 뱃지 */
        .badge {
            display: inline-block;
            padding: 3px 10px;
            border-radius: 12px;
            font-size: 12px;
            font-weight: bold;
            color: white;
        }
        .badge-pass { background: #27ae60; }
        .badge-fail { background: #e74c3c; }
        .badge-warning { background: #f39c12; }
        .badge-error { background: #95a5a6; }

        /* 푸터 */
        .footer {
            text-align: center;
            padding: 20px;
            color: #95a5a6;
            font-size: 12px;
        }
    </style>
</head>
<body>
<div class="container">
    <!-- 헤더 -->
    <div class="header">
        <h1>📊 데이터 품질 검증 리포트</h1>
        <div class="subtitle">생성 시각: {{ generated_at }} | Data Quality Framework v1.0</div>
    </div>

    <!-- 요약 카드 -->
    <div class="summary-cards">
        <div class="card total">
            <div class="number">{{ total_checks }}</div>
            <div class="label">전체 검증</div>
        </div>
        <div class="card pass">
            <div class="number">{{ passed }}</div>
            <div class="label">✅ PASS</div>
        </div>
        <div class="card fail">
            <div class="number">{{ failed }}</div>
            <div class="label">❌ FAIL</div>
        </div>
        <div class="card warning">
            <div class="number">{{ warnings }}</div>
            <div class="label">⚠️ WARNING</div>
        </div>
        <div class="card error">
            <div class="number">{{ errors }}</div>
            <div class="label">🔴 ERROR</div>
        </div>
        <div class="card rate">
            <div class="number">{{ pass_rate }}%</div>
            <div class="label">통과율</div>
        </div>
    </div>

    <!-- 진행 바 -->
    <div class="progress-bar">
        {{ progress_bar_html }}
    </div>

    <!-- 검증 유형별 결과 -->
    {{ sections_html }}

    <!-- 푸터 -->
    <div class="footer">
        Data Quality Framework | Generated by html_reporter.py
    </div>
</div>
</body>
</html>"""


class HTMLReporter:
    """HTML 검증 리포트 생성기"""

    def __init__(self, report_dir: str = None):
        """
        Args:
            report_dir: 리포트 저장 디렉토리 (기본: reports/)
        """
        if report_dir is None:
            base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            report_dir = os.path.join(base_dir, "..", "reports")
        self.report_dir = os.path.abspath(report_dir)
        os.makedirs(self.report_dir, exist_ok=True)

    def generate(self, results: list, summary: dict = None) -> str:
        """
        검증 결과를 HTML 리포트로 생성합니다.

        Args:
            results: CheckResult 객체 리스트 (또는 dict 리스트)
            summary: 요약 정보 (없으면 results에서 자동 계산)

        Returns:
            생성된 HTML 파일 경로
        """
        # dict 리스트로 통일
        result_dicts = []
        for r in results:
            if hasattr(r, "to_dict"):
                result_dicts.append(r.to_dict())
            elif isinstance(r, dict):
                result_dicts.append(r)

        # 요약 계산
        if summary is None:
            summary = self._calculate_summary(result_dicts)

        # HTML 생성
        generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        html = HTML_TEMPLATE
        html = html.replace("{{ generated_at }}", generated_at)
        html = html.replace("{{ total_checks }}", str(summary.get("total_checks", 0)))
        html = html.replace("{{ passed }}", str(summary.get("passed", 0)))
        html = html.replace("{{ failed }}", str(summary.get("failed", 0)))
        html = html.replace("{{ warnings }}", str(summary.get("warnings", 0)))
        html = html.replace("{{ errors }}", str(summary.get("errors", 0)))
        html = html.replace("{{ pass_rate }}", str(summary.get("pass_rate", 0)))
        html = html.replace("{{ progress_bar_html }}", self._make_progress_bar(summary))
        html = html.replace("{{ sections_html }}", self._make_sections(result_dicts))

        # 파일 저장
        filename = f"dq_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
        filepath = os.path.join(self.report_dir, filename)

        with open(filepath, "w", encoding="utf-8") as f:
            f.write(html)

        logger.info("📄 HTML 리포트 생성: %s", filepath)
        return filepath

    def _calculate_summary(self, results: list[dict]) -> dict:
        """결과에서 요약을 계산합니다."""
        total = len(results)
        passed = sum(1 for r in results if r.get("status") == "PASS")
        failed = sum(1 for r in results if r.get("status") == "FAIL")
        warnings = sum(1 for r in results if r.get("status") == "WARNING")
        errors = sum(1 for r in results if r.get("status") == "ERROR")

        return {
            "total_checks": total,
            "passed": passed,
            "failed": failed,
            "warnings": warnings,
            "errors": errors,
            "pass_rate": round(passed / total * 100, 2) if total > 0 else 0,
        }

    def _make_progress_bar(self, summary: dict) -> str:
        """진행 바 HTML 생성"""
        total = summary.get("total_checks", 0)
        if total == 0:
            return '<div class="fill pass-fill" style="width:100%">검증 없음</div>'

        passed = summary.get("passed", 0)
        failed = summary.get("failed", 0)
        warnings = summary.get("warnings", 0)

        pass_pct = round(passed / total * 100, 1)
        fail_pct = round(failed / total * 100, 1)
        warn_pct = round(warnings / total * 100, 1)

        parts = []
        if pass_pct > 0:
            parts.append(f'<div class="fill pass-fill" style="width:{pass_pct}%">PASS {pass_pct}%</div>')
        if fail_pct > 0:
            parts.append(f'<div class="fill fail-fill" style="width:{fail_pct}%">FAIL {fail_pct}%</div>')
        if warn_pct > 0:
            parts.append(f'<div class="fill warn-fill" style="width:{warn_pct}%">WARN {warn_pct}%</div>')

        return "".join(parts)

    def _make_sections(self, results: list[dict]) -> str:
        """검증 유형별 섹션 HTML 생성"""
        # 유형별 그룹핑
        groups = {}
        type_labels = {
            "count": "📊 건수 검증",
            "null": "🔍 NULL 검증",
            "duplicate": "🔁 중복 검증",
            "range": "📏 범위 검증",
            "foreign_key": "🔗 FK 정합성 검증",
            "transform": "🔄 변환 로직 검증",
            "masking": "🔒 비식별화 검증",
        }

        for r in results:
            ctype = r.get("check_type", "unknown")
            if ctype not in groups:
                groups[ctype] = []
            groups[ctype].append(r)

        sections = []
        for ctype, items in groups.items():
            label = type_labels.get(ctype, f"기타 ({ctype})")
            rows_html = ""
            for item in items:
                status = item.get("status", "UNKNOWN")
                badge_class = f"badge-{status.lower()}"
                details_str = json.dumps(item.get("details", {}), ensure_ascii=False, default=str)
                if len(details_str) > 200:
                    details_str = details_str[:200] + "..."

                rows_html += f"""
                <tr>
                    <td>{item.get('rule_id', '-')}</td>
                    <td>{item.get('description', '-')}</td>
                    <td>{item.get('table_name', '-')}</td>
                    <td>{item.get('column_name', '-') or '-'}</td>
                    <td><span class="badge {badge_class}">{status}</span></td>
                    <td>{item.get('total_rows', 0):,}</td>
                    <td>{item.get('violation_count', 0):,}</td>
                    <td>{round(item.get('violation_ratio', 0) * 100, 2)}%</td>
                    <td style="max-width:300px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;" title='{details_str}'>{details_str}</td>
                </tr>"""

            section_html = f"""
            <div class="section">
                <h2>{label} ({len(items)}건)</h2>
                <table>
                    <thead>
                        <tr>
                            <th>규칙ID</th>
                            <th>설명</th>
                            <th>테이블</th>
                            <th>컬럼</th>
                            <th>결과</th>
                            <th>전체 행수</th>
                            <th>위반 건수</th>
                            <th>위반율</th>
                            <th>상세</th>
                        </tr>
                    </thead>
                    <tbody>
                        {rows_html}
                    </tbody>
                </table>
            </div>"""
            sections.append(section_html)

        return "\n".join(sections)
