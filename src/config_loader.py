"""
YAML 규칙 로더
==============
config/rules/ 디렉토리의 YAML 파일을 읽어 검증 규칙을 로딩합니다.
config/db_config.yml에서 DB 접속 정보를 로딩합니다.
"""

import os
import yaml
from typing import Any


class ConfigLoader:
    """YAML 기반 설정 및 검증 규칙 로더"""

    def __init__(self, base_dir: str = None):
        """
        Args:
            base_dir: 프로젝트 루트 디렉토리 경로.
                      None이면 이 파일 기준 상위 2단계 (프로젝트 루트)를 사용.
        """
        if base_dir is None:
            base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.base_dir = base_dir
        self.config_dir = os.path.join(base_dir, "config")
        self.rules_dir = os.path.join(self.config_dir, "rules")

    def _load_yaml(self, filepath: str) -> dict:
        """YAML 파일을 읽어 딕셔너리로 반환"""
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"설정 파일을 찾을 수 없습니다: {filepath}")

        with open(filepath, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)

        if data is None:
            return {}
        return data

    def load_db_config(self, env: str = "development") -> dict:
        """
        DB 접속 정보를 로딩합니다.

        Args:
            env: 환경 이름 (development / docker / production)

        Returns:
            DB 접속 정보 딕셔너리
        """
        filepath = os.path.join(self.config_dir, "db_config.yml")
        config = self._load_yaml(filepath)

        if env not in config:
            raise KeyError(f"환경 '{env}'이 db_config.yml에 정의되어 있지 않습니다.")

        db_config = config[env]

        # 환경변수 치환 (production 환경)
        for key, value in db_config.items():
            if isinstance(value, str) and value.startswith("${") and value.endswith("}"):
                env_var = value[2:-1]
                env_value = os.environ.get(env_var)
                if env_value is None:
                    raise EnvironmentError(
                        f"환경변수 '{env_var}'가 설정되지 않았습니다."
                    )
                # 포트는 정수로 변환
                if key == "port":
                    db_config[key] = int(env_value)
                else:
                    db_config[key] = env_value

        return db_config

    def load_rules(self, rule_type: str) -> list[dict]:
        """
        특정 유형의 검증 규칙을 로딩합니다.

        Args:
            rule_type: 규칙 유형 (count / null / transform / masking)

        Returns:
            활성화된 규칙 리스트
        """
        filename = f"{rule_type}_rules.yml"
        filepath = os.path.join(self.rules_dir, filename)
        data = self._load_yaml(filepath)

        # 첫 번째 키의 값을 규칙 리스트로 사용
        rules_key = f"{rule_type}_rules"
        if rules_key not in data:
            raise KeyError(f"'{rules_key}' 키를 {filename}에서 찾을 수 없습니다.")

        rules = data[rules_key]

        # 활성화된 규칙만 필터링
        enabled_rules = [r for r in rules if r.get("enabled", True)]
        return enabled_rules

    def load_all_rules(self) -> dict[str, list[dict]]:
        """
        모든 검증 규칙을 로딩합니다.

        Returns:
            {"count": [...], "null": [...], "transform": [...], "masking": [...]}
        """
        all_rules = {}
        rule_types = ["count", "null", "transform", "masking"]

        for rule_type in rule_types:
            try:
                all_rules[rule_type] = self.load_rules(rule_type)
            except FileNotFoundError:
                print(f"⚠️  {rule_type}_rules.yml 파일이 없습니다. 건너뜁니다.")
                all_rules[rule_type] = []

        return all_rules

    def get_report_dir(self) -> str:
        """리포트 저장 디렉토리 경로를 반환합니다."""
        report_dir = os.path.join(self.base_dir, "reports")
        os.makedirs(report_dir, exist_ok=True)
        return report_dir
