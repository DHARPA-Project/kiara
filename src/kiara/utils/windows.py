import os
from pathlib import Path
import platform

is_windows = any(platform.win32_ver())

def fix_windows_longpath(path: Path) -> Path:
    if not is_windows:
        return path

    normalized = os.fspath(path.resolve())
    if not normalized.startswith('\\\\?\\'):
        normalized = '\\\\?\\' + normalized
    return Path(normalized)
