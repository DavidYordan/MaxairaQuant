from __future__ import annotations
from pathlib import Path
import yaml
from .schema import AppConfig

_CFG: AppConfig | None = None

def load_config(path: str | Path) -> AppConfig:
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    global _CFG
    _CFG = AppConfig.model_validate(raw["app"])
    return _CFG

def get_config() -> AppConfig:
    global _CFG
    if _CFG is None:
        default_path = Path(__file__).parents[2] / "config" / "app.yaml"
        _CFG = load_config(default_path)
    return _CFG