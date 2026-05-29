from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

_SAFE_LOADER: type[yaml.SafeLoader] = getattr(yaml, "CSafeLoader", yaml.SafeLoader)


def safe_load_yaml_text(text: str) -> Any:
    return yaml.load(text, Loader=_SAFE_LOADER)


def safe_load_yaml_path(path: Path) -> Any:
    return safe_load_yaml_text(path.read_text(encoding="utf-8"))
