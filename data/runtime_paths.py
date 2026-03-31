import shutil
from pathlib import Path

from astrbot.api import logger
from astrbot.api.star import StarTools

PLUGIN_NAME = "astrbot_plugin_deltaforce_loot_broadcast"
LEGACY_PLUGIN_NAMES = (
    "astrbot_plugin_df_red",
)
PLUGIN_ROOT = Path(__file__).resolve().parent.parent
FALLBACK_RUNTIME_DIR = PLUGIN_ROOT / ".runtime_data"
_FRAMEWORK_RUNTIME_DIR = None
_FRAMEWORK_RUNTIME_DIR_FAILURE_LOGGED = False


def _coerce_path(value):
    if value is None:
        return None
    return Path(value).resolve()


def _normalize_runtime_relative_path(value, *, label):
    relative_path = Path(str(value or ""))
    if not relative_path.parts:
        raise ValueError(f"{label} must not be empty")
    if relative_path.is_absolute():
        raise ValueError(f"{label} must be a relative path")
    if any(part in ("", ".", "..") for part in relative_path.parts):
        raise ValueError(f"{label} must stay within the runtime directory")
    return relative_path


def _get_framework_runtime_dir():
    global _FRAMEWORK_RUNTIME_DIR, _FRAMEWORK_RUNTIME_DIR_FAILURE_LOGGED
    if _FRAMEWORK_RUNTIME_DIR is not None:
        return _FRAMEWORK_RUNTIME_DIR
    try:
        runtime_dir = _coerce_path(StarTools.get_data_dir(PLUGIN_NAME))
    except Exception as exc:
        if not _FRAMEWORK_RUNTIME_DIR_FAILURE_LOGGED:
            logger.warning(
                "Failed to resolve AstrBot plugin data dir via StarTools, "
                f"falling back to local runtime dir: {type(exc).__name__}: {exc}"
            )
            _FRAMEWORK_RUNTIME_DIR_FAILURE_LOGGED = True
        return None
    if runtime_dir is not None:
        _FRAMEWORK_RUNTIME_DIR = runtime_dir
        _FRAMEWORK_RUNTIME_DIR_FAILURE_LOGGED = False
    return runtime_dir


def get_runtime_data_dir():
    runtime_dir = _get_framework_runtime_dir() or FALLBACK_RUNTIME_DIR
    runtime_dir.mkdir(parents=True, exist_ok=True)
    return runtime_dir.resolve()


def _get_legacy_runtime_dirs():
    legacy_dirs = []
    framework_runtime_dir = _get_framework_runtime_dir()
    if framework_runtime_dir is not None:
        plugin_data_root = framework_runtime_dir.parent
        for legacy_name in LEGACY_PLUGIN_NAMES:
            legacy_dirs.append((plugin_data_root / legacy_name).resolve())
    legacy_dirs.append(FALLBACK_RUNTIME_DIR.resolve())

    unique_dirs = []
    seen = set()
    for path in legacy_dirs:
        key = str(path)
        if key in seen:
            continue
        seen.add(key)
        unique_dirs.append(path)
    return unique_dirs


def get_runtime_debug_dir():
    debug_dir = get_runtime_data_dir() / "debug"
    debug_dir.mkdir(parents=True, exist_ok=True)
    return debug_dir.resolve()


def get_plugin_root():
    return PLUGIN_ROOT


def _copy_legacy_file_if_needed(target_path, legacy_paths):
    target_path = Path(target_path).resolve()
    if target_path.exists():
        return target_path

    for legacy_path in legacy_paths:
        normalized_legacy_path = Path(legacy_path).resolve()
        if normalized_legacy_path == target_path:
            continue
        if not normalized_legacy_path.exists():
            continue
        target_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(normalized_legacy_path, target_path)
        logger.info(
            f"Migrated runtime data to {target_path} from legacy file "
            f"{normalized_legacy_path}"
        )
        return target_path

    return target_path


def get_runtime_file_path(filename, legacy_relative_paths=None):
    runtime_relative_path = _normalize_runtime_relative_path(
        filename,
        label="filename",
    )
    runtime_path = get_runtime_data_dir() / runtime_relative_path
    legacy_relative_paths = legacy_relative_paths or [runtime_relative_path]
    normalized_legacy_paths = [
        _normalize_runtime_relative_path(relative_path, label="legacy_relative_paths item")
        for relative_path in legacy_relative_paths
    ]
    legacy_paths = [
        PLUGIN_ROOT / relative_path
        for relative_path in normalized_legacy_paths
    ]
    for legacy_dir in _get_legacy_runtime_dirs():
        for relative_path in normalized_legacy_paths:
            legacy_paths.append(legacy_dir / relative_path)
    return _copy_legacy_file_if_needed(runtime_path, legacy_paths)
