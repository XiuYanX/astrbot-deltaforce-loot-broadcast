import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest import mock


REPO_PARENT = Path(__file__).resolve().parents[2]
if str(REPO_PARENT) not in sys.path:
    sys.path.insert(0, str(REPO_PARENT))
TEST_TMP_ROOT = Path(__file__).resolve().parent / ".tmp"
TEST_TMP_ROOT.mkdir(parents=True, exist_ok=True)


class _DummyLogger:
    def __init__(self):
        self.warning_messages = []

    def info(self, *args, **kwargs):
        return None

    def warning(self, message, *args, **kwargs):
        self.warning_messages.append(str(message))

    def error(self, *args, **kwargs):
        return None

    def debug(self, *args, **kwargs):
        return None


class _DummyCookieJar:
    def filter_cookies(self, url):
        return {}


class _DummyClientSession:
    def __init__(self, *args, **kwargs):
        self.closed = False
        self.cookie_jar = _DummyCookieJar()

    async def close(self):
        self.closed = True


class _DummyClientTimeout:
    def __init__(self, *args, **kwargs):
        return None


class _DummyClientError(Exception):
    pass


class _DummyContentTypeError(Exception):
    pass


class _DummyPlain:
    def __init__(self, text):
        self.text = text


class _DummyStarTools:
    @staticmethod
    def get_data_dir(name):
        return Path.cwd() / ".runtime_data"


if "aiohttp" not in sys.modules:
    aiohttp = types.ModuleType("aiohttp")
    aiohttp.ClientSession = _DummyClientSession
    aiohttp.ClientTimeout = _DummyClientTimeout
    aiohttp.CookieJar = _DummyCookieJar
    aiohttp.ClientError = _DummyClientError
    aiohttp.ContentTypeError = _DummyContentTypeError
    sys.modules["aiohttp"] = aiohttp

if "astrbot" not in sys.modules:
    astrbot = types.ModuleType("astrbot")
    api = types.ModuleType("astrbot.api")
    event = types.ModuleType("astrbot.api.event")
    message_components = types.ModuleType("astrbot.api.message_components")
    star = types.ModuleType("astrbot.api.star")

    api.logger = _DummyLogger()
    event.MessageChain = object
    message_components.Plain = _DummyPlain
    star.StarTools = _DummyStarTools

    sys.modules["astrbot"] = astrbot
    sys.modules["astrbot.api"] = api
    sys.modules["astrbot.api.event"] = event
    sys.modules["astrbot.api.message_components"] = message_components
    sys.modules["astrbot.api.star"] = star


from astrbot_plugin_df_red.data import runtime_paths, secret_store
from astrbot_plugin_df_red.monitor.red_detector import RedDetector


class RuntimePathsRegressionTests(unittest.TestCase):
    def test_framework_runtime_dir_retries_after_initial_failure(self):
        resolved_runtime_dir = Path(tempfile.gettempdir()) / "df-red-framework-runtime"
        calls = {"count": 0}

        def fake_get_data_dir(_plugin_name):
            calls["count"] += 1
            if calls["count"] == 1:
                raise RuntimeError("framework not ready")
            return resolved_runtime_dir

        with mock.patch.object(runtime_paths.StarTools, "get_data_dir", side_effect=fake_get_data_dir):
            runtime_paths._FRAMEWORK_RUNTIME_DIR = None
            runtime_paths._FRAMEWORK_RUNTIME_DIR_FAILURE_LOGGED = False

            self.assertIsNone(runtime_paths._get_framework_runtime_dir())
            self.assertEqual(
                runtime_paths._get_framework_runtime_dir(),
                resolved_runtime_dir.resolve(),
            )
            self.assertEqual(calls["count"], 2)

    def test_custom_legacy_relative_paths_are_migrated_from_legacy_dirs(self):
        runtime_dir = TEST_TMP_ROOT / "runtime"
        plugin_root = TEST_TMP_ROOT / "plugin_root"
        legacy_dir = TEST_TMP_ROOT / "legacy_plugin"
        captured = {}

        def fake_copy(target_path, legacy_paths):
            captured["target_path"] = Path(target_path)
            captured["legacy_paths"] = [Path(path) for path in legacy_paths]
            return Path(target_path)

        with (
            mock.patch.object(runtime_paths, "PLUGIN_ROOT", plugin_root),
            mock.patch.object(runtime_paths, "FALLBACK_RUNTIME_DIR", TEST_TMP_ROOT / "fallback"),
            mock.patch.object(runtime_paths, "get_runtime_data_dir", return_value=runtime_dir),
            mock.patch.object(runtime_paths, "_get_legacy_runtime_dirs", return_value=[legacy_dir]),
            mock.patch.object(runtime_paths, "_copy_legacy_file_if_needed", side_effect=fake_copy),
        ):
            target_path = runtime_paths.get_runtime_file_path(
                "new.json",
                legacy_relative_paths=["nested/old.json"],
            )

        self.assertEqual(Path(target_path), runtime_dir / "new.json")
        self.assertIn(
            (legacy_dir / "nested" / "old.json").resolve(),
            [path.resolve() for path in captured["legacy_paths"]],
        )


class SecretProtectorRegressionTests(unittest.TestCase):
    def test_plaintext_warning_is_logged_once_per_instance(self):
        logger = _DummyLogger()
        with mock.patch.object(secret_store, "logger", logger):
            protector = secret_store.SecretProtector()
            self.assertEqual(protector.unprotect("legacy-openid"), "legacy-openid")
            self.assertEqual(protector.unprotect("legacy-openid"), "legacy-openid")

        self.assertEqual(len(logger.warning_messages), 1)
        self.assertIn("legacy plaintext secret value", logger.warning_messages[0])

    def test_windows_dpapi_failure_falls_back_to_plaintext(self):
        logger = _DummyLogger()
        with (
            mock.patch.object(secret_store, "logger", logger),
            mock.patch.object(secret_store.os, "name", "nt"),
            mock.patch.object(
                secret_store.SecretProtector,
                "_protect_with_dpapi",
                side_effect=OSError("dpapi unavailable"),
            ),
        ):
            protector = secret_store.SecretProtector()
            self.assertEqual(protector.protect("token-value"), "token-value")
            self.assertEqual(protector.protect("token-value"), "token-value")

        self.assertEqual(len(logger.warning_messages), 1)
        self.assertIn("fallback activated", logger.warning_messages[0])


class RedDetectorRegressionTests(unittest.TestCase):
    def test_flow_key_includes_additional_fields_without_breaking_legacy_matching(self):
        first_item = {
            "dtEventTime": "2026-03-31 12:00:00",
            "iGoodsId": "1001",
            "AddOrReduce": "+1",
            "Reason": "撤离带出",
            "Name": "样本A",
            "AfterCount": 1,
        }
        second_item = {
            **first_item,
            "Name": "样本B",
            "AfterCount": 2,
        }

        legacy_key = RedDetector._build_legacy_flow_key(first_item)
        self.assertEqual(legacy_key, RedDetector._build_legacy_flow_key(second_item))
        self.assertNotEqual(RedDetector._build_flow_key(first_item), RedDetector._build_flow_key(second_item))
        self.assertIn(legacy_key, RedDetector._build_flow_key_variants(first_item))


if __name__ == "__main__":
    unittest.main()
