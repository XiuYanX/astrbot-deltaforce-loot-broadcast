import logging
import os
import shutil

PLUGIN_NAME = "astrbot_plugin_deltaforce_loot_broadcast"
LEGACY_PLUGIN_NAMES = (
    "astrbot_plugin_df_red",
)
PLUGIN_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
FALLBACK_RUNTIME_DIR = os.path.join(PLUGIN_ROOT, ".runtime_data")


try:
    from astrbot.api import logger
except ImportError:
    logger = logging.getLogger(__name__)


def _find_astrbot_data_dir():
    current = PLUGIN_ROOT
    while True:
        parent = os.path.dirname(current)
        if parent == current:
            return ""
        if os.path.basename(current) == "plugins" and os.path.basename(parent) == "data":
            return parent
        current = parent


def get_runtime_data_dir():
    env_dir = os.environ.get("ASTRBOT_PLUGIN_DATA_DIR", "").strip()
    if env_dir:
        runtime_dir = os.path.abspath(env_dir)
    else:
        astrbot_data_dir = _find_astrbot_data_dir()
        if astrbot_data_dir:
            runtime_dir = os.path.join(astrbot_data_dir, "plugin_data", PLUGIN_NAME)
        else:
            runtime_dir = FALLBACK_RUNTIME_DIR
    os.makedirs(runtime_dir, exist_ok=True)
    return runtime_dir


def _get_legacy_runtime_dirs():
    legacy_dirs = []
    astrbot_data_dir = _find_astrbot_data_dir()
    if astrbot_data_dir:
        for legacy_name in LEGACY_PLUGIN_NAMES:
            legacy_dirs.append(os.path.join(astrbot_data_dir, "plugin_data", legacy_name))
    legacy_dirs.append(FALLBACK_RUNTIME_DIR)
    return legacy_dirs


def get_runtime_debug_dir():
    debug_dir = os.path.join(get_runtime_data_dir(), "debug")
    os.makedirs(debug_dir, exist_ok=True)
    return debug_dir


def get_plugin_root():
    return PLUGIN_ROOT


def _copy_legacy_file_if_needed(target_path, legacy_paths):
    if os.path.exists(target_path):
        return target_path

    for legacy_path in legacy_paths:
        normalized_legacy_path = os.path.abspath(legacy_path)
        if normalized_legacy_path == os.path.abspath(target_path):
            continue
        if not os.path.exists(normalized_legacy_path):
            continue
        os.makedirs(os.path.dirname(target_path), exist_ok=True)
        shutil.copy2(normalized_legacy_path, target_path)
        logger.info(f"Migrated runtime data to {target_path} from legacy file {normalized_legacy_path}")
        return target_path

    return target_path


def get_runtime_file_path(filename, legacy_relative_paths=None):
    runtime_path = os.path.join(get_runtime_data_dir(), filename)
    legacy_relative_paths = legacy_relative_paths or [filename]
    legacy_paths = [
        os.path.join(PLUGIN_ROOT, relative_path)
        for relative_path in legacy_relative_paths
    ]
    for legacy_dir in _get_legacy_runtime_dirs():
        legacy_paths.append(os.path.join(legacy_dir, filename))
    return _copy_legacy_file_if_needed(runtime_path, legacy_paths)
