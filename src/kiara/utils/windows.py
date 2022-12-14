# -*- coding: utf-8 -*-
import os
import platform
import shutil
import tempfile
from functools import lru_cache
from pathlib import Path

is_windows = any(platform.win32_ver())


def fix_windows_longpath(path: Path) -> Path:
    if not is_windows:
        return path

    normalized = os.fspath(path.resolve())
    if not normalized.startswith("\\\\?\\"):
        normalized = "\\\\?\\" + normalized
    return Path(normalized)


def fix_windows_symlink(source: Path, target: Path) -> None:

    if not is_windows:
        target.symlink_to(source)
        return
    else:
        try:
            target.symlink_to(source)
        except OSError:
            import traceback

            raise Exception(
                "Operating system does not support symbolic " "links.",
                "link",
                (source, target),
                traceback.format_exc(),
            )


@lru_cache
def check_symlink_works() -> bool:

    dirname = tempfile.mkdtemp()

    source = Path(dirname) / "source"
    target = Path(dirname) / "target"

    source.touch()
    try:
        target.symlink_to(source)
        return True
    except OSError:
        return False
    finally:
        shutil.rmtree(dirname, ignore_errors=True)
