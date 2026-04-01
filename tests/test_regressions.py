import asyncio
import enum
import importlib
import json
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


class _DummyImage:
    @staticmethod
    def fromBase64(value):
        return value


class _DummyMessageType(enum.Enum):
    GROUP_MESSAGE = "GroupMessage"
    FRIEND_MESSAGE = "FriendMessage"
    OTHER_MESSAGE = "OtherMessage"


class _DummyFilter:
    @staticmethod
    def command(_name):
        def decorator(func):
            return func

        return decorator


class _DummyStar:
    def __init__(self, context):
        self.context = context


class _DummyContext:
    def __init__(self, config=None):
        self._config = dict(config or {"admins_id": []})

    async def send_message(self, origin, message):
        return None

    def get_config(self, umo=None):
        return self._config


def _make_origin(session_id, message_type=_DummyMessageType.FRIEND_MESSAGE, platform_id="test-platform"):
    return f"{platform_id}:{message_type.value}:{session_id}"


def _dummy_register(*args, **kwargs):
    def decorator(cls):
        return cls

    return decorator


class _DummyStarTools:
    @staticmethod
    def get_data_dir(name):
        return Path.cwd() / ".runtime_data"


def _build_import_stubs():
    aiohttp = types.ModuleType("aiohttp")
    aiohttp.ClientSession = _DummyClientSession
    aiohttp.ClientTimeout = _DummyClientTimeout
    aiohttp.CookieJar = _DummyCookieJar
    aiohttp.ClientError = _DummyClientError
    aiohttp.ContentTypeError = _DummyContentTypeError

    astrbot = types.ModuleType("astrbot")
    api = types.ModuleType("astrbot.api")
    event = types.ModuleType("astrbot.api.event")
    message_components = types.ModuleType("astrbot.api.message_components")
    platform = types.ModuleType("astrbot.api.platform")
    star = types.ModuleType("astrbot.api.star")

    api.logger = _DummyLogger()
    event.filter = _DummyFilter()
    event.AstrMessageEvent = object
    event.MessageChain = object
    message_components.Plain = _DummyPlain
    message_components.Image = _DummyImage
    platform.MessageType = _DummyMessageType
    star.Context = _DummyContext
    star.Star = _DummyStar
    star.StarTools = _DummyStarTools
    star.register = _dummy_register

    return {
        "aiohttp": aiohttp,
        "astrbot": astrbot,
        "astrbot.api": api,
        "astrbot.api.event": event,
        "astrbot.api.message_components": message_components,
        "astrbot.api.platform": platform,
        "astrbot.api.star": star,
    }

PACKAGE_NAME = Path(__file__).resolve().parents[1].name
with mock.patch.dict(sys.modules, _build_import_stubs(), clear=False):
    runtime_paths = importlib.import_module(f"{PACKAGE_NAME}.data.runtime_paths")
    secret_store = importlib.import_module(f"{PACKAGE_NAME}.data.secret_store")
    storage_module = importlib.import_module(f"{PACKAGE_NAME}.data.storage")
    game_api_module = importlib.import_module(f"{PACKAGE_NAME}.api.game_api")
    red_detector_module = importlib.import_module(f"{PACKAGE_NAME}.monitor.red_detector")
    main_module = importlib.import_module(f"{PACKAGE_NAME}.main")
GameAPI = game_api_module.GameAPI
Storage = storage_module.Storage
RedDetector = red_detector_module.RedDetector
DeltaForceRedPlugin = main_module.DeltaForceRedPlugin


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

    def test_runtime_data_dir_uses_conventional_fallback_when_framework_lookup_fails(self):
        fallback_dir = TEST_TMP_ROOT / "data" / "plugin_data" / runtime_paths.PLUGIN_NAME
        with (
            mock.patch.object(runtime_paths, "FALLBACK_RUNTIME_DIR", fallback_dir),
            mock.patch.object(
                runtime_paths.StarTools,
                "get_data_dir",
                side_effect=RuntimeError("framework not ready"),
            ),
        ):
            runtime_paths._FRAMEWORK_RUNTIME_DIR = None
            runtime_paths._FRAMEWORK_RUNTIME_DIR_FAILURE_LOGGED = False

            self.assertEqual(
                runtime_paths.get_runtime_data_dir(),
                fallback_dir.resolve(),
            )

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

    def test_windows_dpapi_failure_refuses_plaintext_storage(self):
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
            with self.assertRaises(secret_store.SecretProtectionError):
                protector.protect("token-value")
            with self.assertRaises(secret_store.SecretProtectionError):
                protector.protect("token-value")

        self.assertEqual(len(logger.warning_messages), 1)
        self.assertIn("refusing to store sensitive values", logger.warning_messages[0])


class StorageRegressionTests(unittest.IsolatedAsyncioTestCase):
    async def test_group_origin_is_normalized_to_string(self):
        with mock.patch.object(Storage, "_load_from_disk", return_value=({"group_origins": [], "users": {}}, False)):
            storage = Storage(filepath=TEST_TMP_ROOT / "storage-normalize.json")

        async def fake_persist(new_data=None):
            if new_data is not None:
                storage.data = json.loads(json.dumps(new_data))

        with mock.patch.object(storage, "_persist_locked", side_effect=fake_persist):
            self.assertTrue(await storage.add_group(123))
            self.assertFalse(await storage.add_group("123"))
            self.assertEqual(await storage.get_groups(), ["123"])
            self.assertTrue(await storage.remove_group(123))
            self.assertEqual(await storage.get_groups(), [])

    async def test_hydration_marks_secret_decryption_failures(self):
        persisted = {
            "group_origins": [],
            "users": {
                "sender-1": {
                    "openid_secret": "v1:fernet:broken",
                }
            },
        }
        with mock.patch.object(Storage, "_load_from_disk", return_value=(persisted, False)):
            storage = Storage(filepath=TEST_TMP_ROOT / "storage-secret-error.json")

        with mock.patch.object(
            storage.secret_protector,
            "unprotect",
            side_effect=secret_store.SecretDecryptionError("decrypt failed"),
        ):
            user_data = await storage.get_user("sender-1")

        self.assertNotIn("openid", user_data)
        self.assertEqual(user_data.get("_secret_errors"), {"openid": "decrypt failed"})

    async def test_add_user_resets_account_specific_runtime_state_on_rebind(self):
        persisted = {
            "group_origins": [],
            "users": {
                "sender-1": {
                    "name": "legacy",
                    "platform": "qq",
                    "role_id": "old-role",
                    "last_match_time": "2026-03-31 10:00:00",
                    "last_room_id": "room-old",
                    "last_item_flow_keys": ["legacy-flow"],
                    "pending_broadcasts": [{"message": "old", "origins": ["group:1"]}],
                    "assets": ["legacy-asset"],
                    "openid_secret": "old-openid",
                    "access_token_secret": "old-token",
                }
            },
        }
        with mock.patch.object(Storage, "_load_from_disk", return_value=(persisted, False)):
            storage = Storage(filepath=TEST_TMP_ROOT / "storage-rebind-reset.json")

        async def fake_persist(new_data=None):
            if new_data is not None:
                storage.data = json.loads(json.dumps(new_data))

        with (
            mock.patch.object(
                storage.secret_protector,
                "protect",
                side_effect=lambda value: f"enc:{value}",
            ),
            mock.patch.object(storage, "_persist_locked", side_effect=fake_persist),
        ):
            await storage.add_user(
                "sender-1",
                "new-openid",
                "new-token",
                name="tester",
                platform="qq",
                role_id="new-role",
            )

        user_state = storage.data["users"]["sender-1"]
        self.assertEqual(user_state["name"], "tester")
        self.assertEqual(user_state["platform"], "qq")
        self.assertEqual(user_state["role_id"], "new-role")
        self.assertEqual(user_state["binding_status"], "active")
        self.assertEqual(user_state["binding_status_reason"], "")
        self.assertEqual(user_state["last_match_time"], "")
        self.assertEqual(user_state["last_room_id"], "")
        self.assertEqual(user_state["last_item_flow_keys"], [])
        self.assertEqual(user_state["pending_broadcasts"], [])
        self.assertEqual(user_state["assets"], [])
        self.assertEqual(user_state["openid_secret"], "enc:new-openid")
        self.assertEqual(user_state["access_token_secret"], "enc:new-token")

    async def test_add_user_persists_notify_origin_when_available(self):
        with mock.patch.object(Storage, "_load_from_disk", return_value=({"group_origins": [], "users": {}}, False)):
            storage = Storage(filepath=TEST_TMP_ROOT / "storage-notify-origin.json")

        async def fake_persist(new_data=None):
            if new_data is not None:
                storage.data = json.loads(json.dumps(new_data))

        with (
            mock.patch.object(
                storage.secret_protector,
                "protect",
                side_effect=lambda value: f"enc:{value}",
            ),
            mock.patch.object(storage, "_persist_locked", side_effect=fake_persist),
        ):
            await storage.add_user(
                "sender-1",
                "openid",
                "token",
                notify_origin=_make_origin("123"),
            )

        self.assertEqual(storage.data["users"]["sender-1"]["notify_origin"], _make_origin("123"))
        self.assertEqual(storage.data["users"]["sender-1"]["interaction_origin"], _make_origin("123"))

    async def test_add_user_moves_group_notify_origin_to_interaction_origin(self):
        with mock.patch.object(Storage, "_load_from_disk", return_value=({"group_origins": [], "users": {}}, False)):
            storage = Storage(filepath=TEST_TMP_ROOT / "storage-group-notify-origin.json")

        async def fake_persist(new_data=None):
            if new_data is not None:
                storage.data = json.loads(json.dumps(new_data))

        with (
            mock.patch.object(
                storage.secret_protector,
                "protect",
                side_effect=lambda value: f"enc:{value}",
            ),
            mock.patch.object(storage, "_persist_locked", side_effect=fake_persist),
        ):
            await storage.add_user(
                "sender-1",
                "openid",
                "token",
                notify_origin=_make_origin("group-1", _DummyMessageType.GROUP_MESSAGE),
            )

        self.assertNotIn("notify_origin", storage.data["users"]["sender-1"])
        self.assertEqual(
            storage.data["users"]["sender-1"]["interaction_origin"],
            _make_origin("group-1", _DummyMessageType.GROUP_MESSAGE),
        )


class GameAPIRegressionTests(unittest.IsolatedAsyncioTestCase):
    def test_merge_cookies_accepts_json_and_cookie_header_strings(self):
        merged = GameAPI._merge_cookies(
            {"qrsig": "alpha"},
            json.dumps({"pt_login_sig": "beta"}),
            "p_skey=gamma; pt4_token=delta",
            json.dumps(json.dumps({"double": "encoded"})),
        )

        self.assertEqual(
            merged,
            {
                "qrsig": "alpha",
                "pt_login_sig": "beta",
                "p_skey": "gamma",
                "pt4_token": "delta",
                "double": "encoded",
            },
        )

    async def test_get_login_token_returns_cookie_payload(self):
        api = GameAPI()
        with mock.patch.object(
            api,
            "_request_text",
            mock.AsyncMock(
                return_value=(
                    {
                        "status": 200,
                        "headers": {},
                        "cookies": {"pt_login_sig": "sig-1", "foo": "bar"},
                    },
                    "",
                )
            ),
        ):
            result = await api.get_login_token()

        self.assertTrue(result["status"])
        self.assertEqual(
            result["data"],
            {
                "cookie": {"pt_login_sig": "sig-1", "foo": "bar"},
                "loginSig": "sig-1",
            },
        )

    async def test_get_qq_login_qr_reuses_login_token_cookie_and_sig(self):
        api = GameAPI()
        request_bytes = mock.AsyncMock(
            return_value=(
                {
                    "status": 200,
                    "headers": {},
                    "cookies": {"qrsig": "alpha"},
                },
                b"qr-image",
            )
        )
        with (
            mock.patch.object(
                api,
                "get_login_token",
                mock.AsyncMock(
                    return_value={
                        "status": True,
                        "data": {
                            "cookie": {"pt_login_sig": "sig-1", "ptdrvs": "token-1"},
                            "loginSig": "sig-1",
                        },
                    }
                ),
            ),
            mock.patch.object(api, "_request_bytes", request_bytes),
        ):
            result = await api.get_qq_login_qr()

        self.assertTrue(result["status"])
        request_bytes.assert_awaited_once()
        self.assertEqual(
            request_bytes.await_args.kwargs["cookies"],
            {"pt_login_sig": "sig-1", "ptdrvs": "token-1"},
        )
        self.assertEqual(result["data"]["loginSig"], "sig-1")
        self.assertEqual(result["data"]["cookie"]["pt_login_sig"], "sig-1")
        self.assertEqual(result["data"]["cookie"]["qrsig"], "alpha")

    async def test_access_token_exchange_rejects_untrusted_redirect_host(self):
        api = GameAPI()
        request_mock = mock.AsyncMock(
            return_value=(
                {
                    "status": 302,
                    "headers": {"Location": "https://evil.example/callback?code=abc"},
                    "cookies": {},
                },
                "",
            )
        )

        with mock.patch.object(api, "_request_text", request_mock):
            result = await api.get_access_token_by_cookie({"p_skey": "token"})

        self.assertFalse(result["status"])
        self.assertEqual(request_mock.await_count, 1)

    async def test_access_token_exchange_rejects_untrusted_second_hop_redirect_host(self):
        api = GameAPI()
        request_mock = mock.AsyncMock(
            side_effect=[
                (
                    {
                        "status": 302,
                        "headers": {"Location": "https://milo.qq.com/callback?code=abc"},
                        "cookies": {},
                    },
                    "",
                ),
                (
                    {
                        "status": 302,
                        "headers": {"Location": "https://evil.example/callback?code=abc"},
                        "cookies": {},
                    },
                    "",
                ),
            ]
        )

        with mock.patch.object(api, "_request_text", request_mock):
            result = await api.get_access_token_by_cookie({"p_skey": "token"})

        self.assertFalse(result["status"])
        self.assertEqual(request_mock.await_count, 2)

    async def test_access_token_exchange_does_not_swallow_value_error(self):
        api = GameAPI()
        with mock.patch.object(api, "_request_text", mock.AsyncMock(side_effect=ValueError("boom"))):
            with self.assertRaises(ValueError):
                await api.get_access_token_by_cookie({"p_skey": "token"})

    async def test_session_uses_dummy_cookie_jar_when_available(self):
        created = {}

        class _ObservedDummyCookieJar:
            pass

        class _ObservedSession:
            def __init__(self, *args, **kwargs):
                self.closed = False
                self.cookie_jar = kwargs.get("cookie_jar")
                created["cookie_jar"] = self.cookie_jar

            async def close(self):
                self.closed = True

        api = GameAPI()
        with (
            mock.patch.object(game_api_module.aiohttp, "ClientSession", _ObservedSession),
            mock.patch.object(game_api_module.aiohttp, "DummyCookieJar", _ObservedDummyCookieJar, create=True),
        ):
            session = await api._get_session()

        self.assertIs(session.cookie_jar, created["cookie_jar"])
        self.assertIsInstance(session.cookie_jar, _ObservedDummyCookieJar)

    async def test_refresh_item_catalog_reports_cache_fallback_status(self):
        api = GameAPI()
        with (
            mock.patch.object(api, "_fetch_item_catalog_from_remote", mock.AsyncMock(return_value=None)),
            mock.patch.object(
                api,
                "_load_item_catalog_cache",
                return_value={"items": [{"objectID": "1001"}]},
            ),
        ):
            result = await api.refresh_item_catalog("openid", "token", platform="qq")

        self.assertFalse(result["status"])
        self.assertEqual(result["source"], "cache")
        self.assertEqual(result["items"], [{"objectID": "1001"}])

    async def test_fetch_item_catalog_uses_fresh_cache_without_remote_refresh(self):
        api = GameAPI()
        remote_fetch = mock.AsyncMock(return_value=[{"objectID": "2002"}])
        current_time = 1_700_000_000
        with (
            mock.patch.object(game_api_module.time, "time", return_value=current_time),
            mock.patch.object(
                api,
                "_load_item_catalog_cache",
                return_value={
                    "updated_at": current_time,
                    "items": [{"objectID": "1001"}],
                },
            ),
            mock.patch.object(api, "_fetch_item_catalog_from_remote", remote_fetch),
        ):
            result = await api.fetch_item_catalog("openid", "token", platform="qq")

        self.assertEqual(result, [{"objectID": "1001"}])
        remote_fetch.assert_not_awaited()

    async def test_fetch_item_catalog_metadata_marks_stale_fallback(self):
        api = GameAPI()
        current_time = 1_700_000_000
        stale_updated_at = current_time - game_api_module.ITEM_CATALOG_CACHE_TTL_SECONDS - 1
        with (
            mock.patch.object(game_api_module.time, "time", return_value=current_time),
            mock.patch.object(
                api,
                "_load_item_catalog_cache",
                return_value={
                    "updated_at": stale_updated_at,
                    "items": [{"objectID": "1001"}],
                },
            ),
            mock.patch.object(api, "_fetch_item_catalog_from_remote", mock.AsyncMock(return_value=None)),
        ):
            result = await api.fetch_item_catalog(
                "openid",
                "token",
                platform="qq",
                return_metadata=True,
            )

        self.assertEqual(result["items"], [{"objectID": "1001"}])
        self.assertEqual(result["cache_status"], "stale_fallback")
        self.assertTrue(result["used_stale_cache"])

    async def test_fetch_item_catalog_refreshes_stale_cache_from_remote(self):
        api = GameAPI()
        current_time = 1_700_000_000
        stale_updated_at = current_time - game_api_module.ITEM_CATALOG_CACHE_TTL_SECONDS - 1
        remote_fetch = mock.AsyncMock(return_value=[{"objectID": "2002"}])
        with (
            mock.patch.object(game_api_module.time, "time", return_value=current_time),
            mock.patch.object(
                api,
                "_load_item_catalog_cache",
                return_value={
                    "updated_at": stale_updated_at,
                    "items": [{"objectID": "1001"}],
                },
            ),
            mock.patch.object(api, "_fetch_item_catalog_from_remote", remote_fetch),
        ):
            result = await api.fetch_item_catalog("openid", "token", platform="qq")

        self.assertEqual(result, [{"objectID": "2002"}])
        remote_fetch.assert_awaited_once()

    async def test_fetch_item_catalog_falls_back_to_stale_cache_when_remote_refresh_fails(self):
        api = GameAPI()
        current_time = 1_700_000_000
        stale_updated_at = current_time - game_api_module.ITEM_CATALOG_CACHE_TTL_SECONDS - 1
        remote_fetch = mock.AsyncMock(return_value=None)
        with (
            mock.patch.object(game_api_module.time, "time", return_value=current_time),
            mock.patch.object(
                api,
                "_load_item_catalog_cache",
                return_value={
                    "updated_at": stale_updated_at,
                    "items": [{"objectID": "1001"}],
                },
            ),
            mock.patch.object(api, "_fetch_item_catalog_from_remote", remote_fetch),
        ):
            result = await api.fetch_item_catalog("openid", "token", platform="qq")

        self.assertEqual(result, [{"objectID": "1001"}])
        remote_fetch.assert_awaited_once()

    async def test_fetch_item_catalog_returns_none_when_remote_and_cache_missing(self):
        api = GameAPI()
        with (
            mock.patch.object(api, "_fetch_item_catalog_from_remote", mock.AsyncMock(return_value=None)),
            mock.patch.object(api, "_load_item_catalog_cache", return_value=None),
        ):
            result = await api.fetch_item_catalog("openid", "token", platform="qq")

        self.assertIsNone(result)

    async def test_bind_account_preserves_non_auth_upstream_failure_message(self):
        api = GameAPI()
        with mock.patch.object(
            api,
            "_post_base_json",
            mock.AsyncMock(return_value=({}, {"ret": 101, "msg": "系统繁忙"})),
        ):
            result = await api.bind_account("token", "openid", "qq")

        self.assertFalse(result["status"])
        self.assertEqual(result["message"], "系统繁忙")
        self.assertEqual(result["error_kind"], "upstream_error")

    async def test_bind_account_marks_explicit_auth_expiration(self):
        api = GameAPI()
        with mock.patch.object(
            api,
            "_post_base_json",
            mock.AsyncMock(return_value=({}, {"ret": 101, "msg": "鉴权已过期，请重新扫码登录"})),
        ):
            result = await api.bind_account("token", "openid", "qq")

        self.assertFalse(result["status"])
        self.assertEqual(result["message"], "鉴权已过期，请重新扫码登录")
        self.assertEqual(result["error_kind"], "credential_expired")


class RedDetectorRegressionTests(unittest.IsolatedAsyncioTestCase):
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

    async def test_build_latest_broadcast_payload_handles_malformed_records_payload(self):
        storage = mock.AsyncMock()
        api = mock.Mock()
        api.fetch_records_v2 = mock.AsyncMock(return_value={"data": [{"dtEventTime": "bad"}]})
        api.fetch_records = mock.AsyncMock(return_value=["bad-record"])
        detector = RedDetector(storage, context=mock.Mock(), api=api)

        result = await detector.build_latest_broadcast_payload("openid", "token")

        self.assertEqual(result, {"error": "未获取到最近一局战绩"})

    async def test_build_latest_broadcast_payload_reports_item_catalog_unavailable(self):
        storage = mock.AsyncMock()
        api = mock.Mock()
        api.fetch_records_v2 = mock.AsyncMock(
            return_value=[{"dtEventTime": "2026-03-31 12:00:00", "roomId": "room-1"}]
        )
        api.fetch_records = mock.AsyncMock(return_value=[])
        api.fetch_all_item_flows = mock.AsyncMock(
            return_value=[
                {
                    "dtEventTime": "2026-03-31 12:00:05",
                    "iGoodsId": "1001",
                    "AddOrReduce": "+1",
                    "Reason": "撤离带出",
                    "Name": "样本A",
                }
            ]
        )
        api.fetch_item_catalog = mock.AsyncMock(return_value=None)
        detector = RedDetector(
            storage,
            context=_DummyContext({"admins_id": ["admin-1"]}),
            api=api,
        )

        result = await detector.build_latest_broadcast_payload("openid", "token")

        self.assertEqual(
            result,
            {"error": "未获取到物品目录，请稍后重试或先执行 df刷新物品缓存。"},
        )

    async def test_check_all_users_keeps_other_tasks_running_after_one_failure(self):
        storage = mock.AsyncMock()
        storage.get_users = mock.AsyncMock(return_value={"user-a": {}, "user-b": {}})
        detector = RedDetector(storage, context=mock.Mock(), api=mock.Mock())
        started = asyncio.Event()
        completed = []

        async def fake_check_user(sender_id, user_data):
            if sender_id == "user-a":
                started.set()
                raise RuntimeError("boom")
            await started.wait()
            await asyncio.sleep(0)
            completed.append(sender_id)

        detector.check_user = fake_check_user

        await detector.check_all_users()

        self.assertEqual(completed, ["user-b"])

    async def test_check_user_marks_binding_invalid_and_sends_notice_immediately(self):
        storage = mock.AsyncMock()
        storage.update_user_state = mock.AsyncMock(return_value=True)
        api = mock.Mock()
        api.fetch_records_v2 = mock.AsyncMock(return_value=[])
        api.fetch_records = mock.AsyncMock(return_value=[])
        api.bind_account = mock.AsyncMock(
            return_value={"status": False, "message": "获取失败,检查鉴权是否过期", "data": {}}
        )
        detector = RedDetector(
            storage,
            context=_DummyContext({"admins_id": ["admin-1"]}),
            api=api,
        )
        detector._send_message_to_origin = mock.AsyncMock(return_value="RawText")
        user_data = {
            "openid": "openid",
            "access_token": "token",
            "platform": "qq",
            "name": "tester",
            "notify_origin": _make_origin("123"),
        }

        await detector._check_user_impl("user-a", user_data)

        detector._send_message_to_origin.assert_awaited_once()
        detector._send_message_to_origin.assert_awaited_with(
            _make_origin("123"),
            mock.ANY,
        )
        self.assertEqual(user_data["binding_status"], "invalid")
        self.assertEqual(user_data["binding_status_reason"], "获取失败,检查鉴权是否过期")
        self.assertIsNone(user_data["pending_notice"])
        self.assertIn("请重新执行 df绑定", detector._send_message_to_origin.await_args.args[1])
        self.assertNotIn("df解绑", detector._send_message_to_origin.await_args.args[1])
        storage.update_user_state.assert_has_awaits(
            [
                mock.call(
                    "user-a",
                    binding_status="invalid",
                    binding_status_reason="获取失败,检查鉴权是否过期",
                ),
                mock.call(
                    "user-a",
                    pending_notice={
                        "type": "binding_invalid",
                        "message": (
                            "检测到你当前保存的三角洲绑定可能已失效，后台监测已暂停。\n"
                            "请重新执行 df绑定 以覆盖旧绑定。\n"
                            "原因：获取失败,检查鉴权是否过期"
                        ),
                    },
                ),
                mock.call("user-a", pending_notice=None),
            ]
        )

    async def test_check_user_retries_pending_invalid_notice_after_send_failure(self):
        storage = mock.AsyncMock()
        storage.update_user_state = mock.AsyncMock(return_value=True)
        api = mock.Mock()
        api.fetch_records_v2 = mock.AsyncMock(return_value=[])
        api.fetch_records = mock.AsyncMock(return_value=[])
        api.bind_account = mock.AsyncMock(
            return_value={"status": False, "message": "获取失败,检查鉴权是否过期", "data": {}}
        )
        detector = RedDetector(storage, context=mock.Mock(), api=api)
        detector._send_message_to_origin = mock.AsyncMock(
            side_effect=[RuntimeError("send failed"), "RawText"]
        )
        user_data = {
            "openid": "openid",
            "access_token": "token",
            "platform": "qq",
            "name": "tester",
            "notify_origin": _make_origin("123"),
        }

        await detector._check_user_impl("user-a", user_data)

        self.assertEqual(user_data["binding_status"], "invalid")
        self.assertEqual(
            user_data["pending_notice"],
            {
                "type": "binding_invalid",
                "message": (
                    "检测到你当前保存的三角洲绑定可能已失效，后台监测已暂停。\n"
                    "请重新执行 df绑定 以覆盖旧绑定。\n"
                    "原因：获取失败,检查鉴权是否过期"
                ),
            },
        )
        self.assertEqual(detector._send_message_to_origin.await_count, 1)

        await detector._check_user_impl("user-a", user_data)

        self.assertEqual(detector._send_message_to_origin.await_count, 2)
        self.assertIsNone(user_data["pending_notice"])
        self.assertEqual(api.bind_account.await_count, 1)
        storage.update_user_state.assert_has_awaits(
            [
                mock.call(
                    "user-a",
                    binding_status="invalid",
                    binding_status_reason="获取失败,检查鉴权是否过期",
                ),
                mock.call(
                    "user-a",
                    pending_notice={
                        "type": "binding_invalid",
                        "message": (
                            "检测到你当前保存的三角洲绑定可能已失效，后台监测已暂停。\n"
                            "请重新执行 df绑定 以覆盖旧绑定。\n"
                            "原因：获取失败,检查鉴权是否过期"
                        ),
                    },
                ),
                mock.call("user-a", pending_notice=None),
            ]
        )

    async def test_flush_pending_notice_uses_derived_private_origin_for_binding_notice(self):
        storage = mock.AsyncMock()
        storage.update_user_state = mock.AsyncMock(return_value=True)
        detector = RedDetector(storage, context=mock.Mock(), api=mock.Mock())
        detector._send_message_to_origin = mock.AsyncMock(return_value="RawText")
        user_data = {
            "interaction_origin": _make_origin("group-1", _DummyMessageType.GROUP_MESSAGE),
            "pending_notice": {
                "type": "binding_invalid",
                "message": "notice-message",
            },
        }

        result = await detector._flush_pending_notice("user-a", user_data)

        self.assertTrue(result)
        detector._send_message_to_origin.assert_awaited_once_with(
            _make_origin("user-a"),
            "notice-message",
        )

    async def test_flush_pending_notice_routes_admin_notice_to_astrbot_admin_private(self):
        storage = mock.AsyncMock()
        storage.update_user_state = mock.AsyncMock(return_value=True)
        detector = RedDetector(
            storage,
            context=_DummyContext({"admins_id": ["admin-1"]}),
            api=mock.Mock(),
        )
        detector._send_message_to_origin = mock.AsyncMock(return_value="RawText")
        user_data = {
            "notify_origin": _make_origin("123"),
            "interaction_origin": _make_origin("group-1", _DummyMessageType.GROUP_MESSAGE),
            "pending_notice": {
                "type": "item_catalog_stale_fallback",
                "message": "notice-message",
                "target": red_detector_module.NOTICE_TARGET_ADMIN,
            },
        }

        result = await detector._flush_pending_notice("user-a", user_data)

        self.assertTrue(result)
        detector._send_message_to_origin.assert_awaited_once_with(
            _make_origin("admin-1"),
            "notice-message",
        )

    async def test_check_user_does_not_invalidate_binding_on_non_auth_validation_failure(self):
        storage = mock.AsyncMock()
        storage.update_user_state = mock.AsyncMock(return_value=True)
        api = mock.Mock()
        api.fetch_records_v2 = mock.AsyncMock(return_value=[])
        api.fetch_records = mock.AsyncMock(return_value=[])
        api.bind_account = mock.AsyncMock(
            return_value={
                "status": False,
                "message": "系统繁忙",
                "error_kind": "upstream_error",
                "data": {},
            }
        )
        detector = RedDetector(storage, context=mock.Mock(), api=api)
        detector._send_message_to_origin = mock.AsyncMock(return_value="RawText")
        user_data = {
            "openid": "openid",
            "access_token": "token",
            "platform": "qq",
            "notify_origin": "friend:123",
        }

        await detector._check_user_impl("user-a", user_data)

        detector._send_message_to_origin.assert_not_awaited()
        storage.update_user_state.assert_not_awaited()
        self.assertNotIn("binding_status", user_data)

    async def test_check_user_routes_repeated_non_auth_validation_failures_to_astrbot_admin_private(self):
        storage = mock.AsyncMock()
        storage.update_user_state = mock.AsyncMock(return_value=True)
        api = mock.Mock()
        api.fetch_records_v2 = mock.AsyncMock(return_value=[])
        api.fetch_records = mock.AsyncMock(return_value=[])
        api.bind_account = mock.AsyncMock(
            return_value={
                "status": False,
                "message": "系统繁忙",
                "error_kind": "upstream_error",
                "data": {},
            }
        )
        detector = RedDetector(
            storage,
            context=_DummyContext({"admins_id": ["admin-1"]}),
            api=api,
        )
        detector._send_message_to_origin = mock.AsyncMock(return_value="RawText")
        user_data = {
            "openid": "openid",
            "access_token": "token",
            "platform": "qq",
            "name": "tester",
            "notify_origin": _make_origin("123"),
        }

        for _ in range(red_detector_module.TRANSIENT_FAILURE_NOTICE_THRESHOLD):
            await detector._check_user_impl("user-a", user_data)

        detector._send_message_to_origin.assert_awaited_once_with(
            _make_origin("admin-1"),
            mock.ANY,
        )
        self.assertNotIn("binding_status", user_data)
        self.assertIsNone(user_data["pending_notice"])
        self.assertIn("tester(user-a)", detector._send_message_to_origin.await_args.args[1])
        storage.update_user_state.assert_has_awaits(
            [
                mock.call("user-a", pending_notice=mock.ANY),
                mock.call("user-a", pending_notice=None),
            ]
        )
        first_notice = storage.update_user_state.await_args_list[0].kwargs["pending_notice"]
        self.assertEqual(first_notice["type"], "transient_upstream_error")
        self.assertEqual(first_notice["target"], red_detector_module.NOTICE_TARGET_ADMIN)
        self.assertIn("tester(user-a)", first_notice["message"])

    async def test_check_user_resets_transient_failure_counter_after_successful_match(self):
        storage = mock.AsyncMock()
        storage.update_user_state = mock.AsyncMock(return_value=True)
        api = mock.Mock()
        api.fetch_records_v2 = mock.AsyncMock(
            side_effect=[
                [],
                [],
                [{"dtEventTime": "2026-03-31 12:00:00", "roomId": "room-1"}],
                [],
                [],
            ]
        )
        api.fetch_records = mock.AsyncMock(return_value=[])
        api.fetch_all_item_flows = mock.AsyncMock(return_value=[])
        api.bind_account = mock.AsyncMock(
            return_value={
                "status": False,
                "message": "系统繁忙",
                "error_kind": "upstream_error",
                "data": {},
            }
        )
        detector = RedDetector(storage, context=mock.Mock(), api=api)
        detector._send_message_to_origin = mock.AsyncMock(return_value="RawText")
        user_data = {
            "openid": "openid",
            "access_token": "token",
            "platform": "qq",
            "notify_origin": "friend:123",
            "last_item_flow_keys": ["legacy-key"],
            "last_match_time": "2026-03-30 11:59:59",
            "last_room_id": "room-0",
        }

        await detector._check_user_impl("user-a", user_data)
        await detector._check_user_impl("user-a", user_data)
        self.assertEqual(detector.transient_failure_counters["user-a"], 2)

        await detector._check_user_impl("user-a", user_data)
        self.assertNotIn("user-a", detector.transient_failure_counters)

        await detector._check_user_impl("user-a", user_data)
        await detector._check_user_impl("user-a", user_data)

        detector._send_message_to_origin.assert_not_awaited()
        storage.update_user_state.assert_not_awaited()

    async def test_check_user_does_not_advance_baseline_when_item_catalog_unavailable(self):
        storage = mock.AsyncMock()
        storage.update_user_state = mock.AsyncMock(return_value=True)
        api = mock.Mock()
        api.fetch_records_v2 = mock.AsyncMock(
            return_value=[{"dtEventTime": "2026-03-31 12:00:00", "roomId": "room-1"}]
        )
        api.fetch_records = mock.AsyncMock(return_value=[])
        api.fetch_all_item_flows = mock.AsyncMock(
            return_value=[
                {
                    "dtEventTime": "2026-03-31 12:00:05",
                    "iGoodsId": "1001",
                    "AddOrReduce": "+1",
                    "Reason": "撤离带出",
                    "Name": "样本A",
                    "AfterCount": 1,
                }
            ]
        )
        api.fetch_item_catalog = mock.AsyncMock(return_value=None)
        detector = RedDetector(storage, context=mock.Mock(), api=api)
        detector.broadcast = mock.AsyncMock()
        user_data = {
            "openid": "openid",
            "access_token": "token",
            "platform": "qq",
            "name": "tester",
            "last_match_time": "2026-03-30 11:59:59",
            "last_room_id": "room-0",
            "last_item_flow_keys": ["legacy-key"],
        }

        await detector._check_user_impl("user-a", user_data)

        storage.update_user_state.assert_not_awaited()
        detector.broadcast.assert_not_awaited()

    async def test_check_user_routes_stale_item_catalog_notice_to_astrbot_admin_private(self):
        storage = mock.AsyncMock()
        storage.update_user_state = mock.AsyncMock(return_value=True)
        detector = RedDetector(
            storage,
            context=_DummyContext({"admins_id": ["admin-1"]}),
            api=mock.Mock(),
        )
        detector._send_message_to_origin = mock.AsyncMock(return_value="RawText")
        user_data = {
            "name": "tester",
            "notify_origin": _make_origin("123"),
            "interaction_origin": _make_origin("group-1", _DummyMessageType.GROUP_MESSAGE),
        }

        result = await detector._maybe_notify_item_catalog_fallback("user-a", user_data)

        self.assertTrue(result)
        detector._send_message_to_origin.assert_awaited_once_with(
            _make_origin("admin-1"),
            mock.ANY,
        )
        storage.update_user_state.assert_has_awaits(
            [
                mock.call("user-a", pending_notice=mock.ANY),
                mock.call("user-a", pending_notice=None),
            ]
        )
        first_notice = storage.update_user_state.await_args_list[0].kwargs["pending_notice"]
        self.assertEqual(first_notice["type"], "item_catalog_stale_fallback")
        self.assertEqual(first_notice["target"], red_detector_module.NOTICE_TARGET_ADMIN)
        self.assertIn("tester(user-a)", first_notice["message"])
        self.assertIn("user-a", detector.item_catalog_fallback_notified_users)

    async def test_retry_pending_broadcasts_keeps_only_still_failing_targets(self):
        storage = mock.AsyncMock()
        storage.get_groups = mock.AsyncMock(return_value=["group:1", "group:2", "group:3"])
        storage.update_user_state = mock.AsyncMock(return_value=True)
        detector = RedDetector(storage, context=mock.Mock(), api=mock.Mock())
        detector.broadcast_message = mock.AsyncMock(
            return_value={
                "message": "msg",
                "total_groups": 2,
                "success_groups": [{"origin": "group:1"}],
                "failed_groups": [{"origin": "group:2", "error": "boom"}],
            }
        )
        user_data = {
            "pending_broadcasts": [
                {"message": "msg", "origins": ["group:1", "group:2", "group:removed"]}
            ]
        }

        still_pending = await detector.retry_pending_broadcasts("user-a", user_data)

        self.assertTrue(still_pending)
        detector.broadcast_message.assert_awaited_with(
            "msg",
            origins=["group:1", "group:2"],
            write_debug_snapshot=False,
            log_prefix="Retrying pending broadcast",
        )
        storage.update_user_state.assert_awaited_with(
            "user-a",
            pending_broadcasts=[{"message": "msg", "origins": ["group:2"]}],
        )

    async def test_check_user_queues_failed_groups_and_advances_flow_baseline(self):
        storage = mock.AsyncMock()
        events = []

        async def fake_update_user_state(sender_id, **fields):
            events.append(("save", sender_id, dict(fields)))
            return True

        storage.update_user_state = mock.AsyncMock(side_effect=fake_update_user_state)
        api = mock.Mock()
        api.fetch_records_v2 = mock.AsyncMock(
            return_value=[{"dtEventTime": "2026-03-31 12:00:00", "roomId": "room-1"}]
        )
        api.fetch_records = mock.AsyncMock(return_value=[])
        api.fetch_all_item_flows = mock.AsyncMock(
            return_value=[
                {
                    "dtEventTime": "2026-03-31 12:00:05",
                    "iGoodsId": "1001",
                    "AddOrReduce": "+1",
                    "Reason": "撤离带出",
                    "Name": "样本A",
                    "AfterCount": 1,
                }
            ]
        )
        detector = RedDetector(storage, context=mock.Mock(), api=api)
        detector._get_item_catalog_map_with_meta = mock.AsyncMock(
            return_value=(
                {
                    "1001": {
                        "primaryClass": "props",
                        "secondClass": "collection",
                        "grade": 6,
                    }
                },
                {},
            )
        )
        detector._enrich_match_info = mock.AsyncMock(
            return_value={"dtEventTime": "2026-03-31 12:00:00", "roomId": "room-1"}
        )
        detector.ensure_user_role_id = mock.AsyncMock(return_value="role-1")
        async def fake_broadcast(*args, **kwargs):
            events.append(("broadcast", args, kwargs))
            return {
                "message": "msg",
                "total_groups": 2,
                "success_groups": [{"origin": "group:1"}],
                "failed_groups": [{"origin": "group:2", "error": "boom"}],
            }

        detector.broadcast = mock.AsyncMock(side_effect=fake_broadcast)
        user_data = {
            "openid": "openid",
            "access_token": "token",
            "platform": "qq",
            "name": "tester",
            "last_match_time": "2026-03-30 11:59:59",
            "last_room_id": "room-0",
            "last_item_flow_keys": ["legacy-key"],
        }

        await detector._check_user_impl("user-a", user_data)

        expected_flow_key = RedDetector._build_flow_key(
            {
                "dtEventTime": "2026-03-31 12:00:05",
                "iGoodsId": "1001",
                "AddOrReduce": "+1",
                "Reason": "撤离带出",
                "Name": "样本A",
                "AfterCount": 1,
            }
        )
        expected_pending = [
            {
                "message": "msg",
                "origins": ["group:2"],
                "event_time": "2026-03-31 12:00:00",
                "room_id": "room-1",
            }
        ]
        self.assertEqual(
            events,
            [
                (
                    "save",
                    "user-a",
                    {
                        "last_item_flow_keys": [expected_flow_key],
                        "last_match_time": "2026-03-31 12:00:00",
                        "last_room_id": "room-1",
                    },
                ),
                ("broadcast", ("tester", mock.ANY, {"dtEventTime": "2026-03-31 12:00:00", "roomId": "room-1"},), {"role_id": "role-1"}),
                (
                    "save",
                    "user-a",
                    {
                        "pending_broadcasts": expected_pending,
                    },
                ),
            ],
        )

    async def test_check_user_does_not_broadcast_when_baseline_persist_fails(self):
        storage = mock.AsyncMock()
        storage.update_user_state = mock.AsyncMock(side_effect=OSError("disk full"))
        api = mock.Mock()
        api.fetch_records_v2 = mock.AsyncMock(
            return_value=[{"dtEventTime": "2026-03-31 12:00:00", "roomId": "room-1"}]
        )
        api.fetch_records = mock.AsyncMock(return_value=[])
        api.fetch_all_item_flows = mock.AsyncMock(
            return_value=[
                {
                    "dtEventTime": "2026-03-31 12:00:05",
                    "iGoodsId": "1001",
                    "AddOrReduce": "+1",
                    "Reason": "撤离带出",
                    "Name": "样本A",
                    "AfterCount": 1,
                }
            ]
        )
        detector = RedDetector(storage, context=mock.Mock(), api=api)
        detector._get_item_catalog_map_with_meta = mock.AsyncMock(
            return_value=(
                {
                    "1001": {
                        "primaryClass": "props",
                        "secondClass": "collection",
                        "grade": 6,
                    }
                },
                {},
            )
        )
        detector.broadcast = mock.AsyncMock()
        user_data = {
            "openid": "openid",
            "access_token": "token",
            "platform": "qq",
            "name": "tester",
            "last_match_time": "2026-03-30 11:59:59",
            "last_room_id": "room-0",
            "last_item_flow_keys": ["legacy-key"],
        }

        with self.assertRaises(OSError):
            await detector._check_user_impl("user-a", user_data)

        detector.broadcast.assert_not_awaited()


class MainRegressionTests(unittest.IsolatedAsyncioTestCase):
    async def test_finish_bind_handles_storage_write_failure(self):
        storage = mock.AsyncMock()
        command_api = mock.AsyncMock()
        detector = mock.Mock()
        command_api.bind_account = mock.AsyncMock(
            return_value={"status": True, "data": {"role_id": "role-1"}}
        )
        storage.get_user = mock.AsyncMock(return_value=None)
        storage.add_user = mock.AsyncMock(side_effect=OSError("disk full"))

        with (
            mock.patch.object(main_module, "Storage", return_value=storage),
            mock.patch.object(main_module, "GameAPI", return_value=command_api),
            mock.patch.object(main_module, "RedDetector", return_value=detector),
        ):
            plugin = DeltaForceRedPlugin(_DummyContext())

        success, message = await plugin._finish_bind(
            "sender-1",
            "tester",
            "qq",
            "openid",
            "token",
        )

        self.assertFalse(success)
        self.assertIn("保存失败", message)

    async def test_finish_bind_clears_runtime_state_after_success(self):
        storage = mock.AsyncMock()
        command_api = mock.AsyncMock()
        detector = mock.Mock()
        command_api.bind_account = mock.AsyncMock(
            return_value={"status": True, "data": {"role_id": "role-1"}}
        )
        storage.get_user = mock.AsyncMock(return_value=None)
        storage.add_user = mock.AsyncMock(return_value=None)

        with (
            mock.patch.object(main_module, "Storage", return_value=storage),
            mock.patch.object(main_module, "GameAPI", return_value=command_api),
            mock.patch.object(main_module, "RedDetector", return_value=detector),
        ):
            plugin = DeltaForceRedPlugin(_DummyContext())

        success, _message = await plugin._finish_bind(
            "sender-1",
            "tester",
            "qq",
            "openid",
            "token",
        )

        self.assertTrue(success)
        detector.clear_user_runtime_state.assert_called_once_with("sender-1")

    async def test_finish_bind_persists_notify_origin(self):
        storage = mock.AsyncMock()
        command_api = mock.AsyncMock()
        detector = mock.Mock()
        command_api.bind_account = mock.AsyncMock(
            return_value={"status": True, "data": {"role_id": "role-1"}}
        )
        storage.get_user = mock.AsyncMock(return_value=None)
        storage.add_user = mock.AsyncMock(return_value=None)

        with (
            mock.patch.object(main_module, "Storage", return_value=storage),
            mock.patch.object(main_module, "GameAPI", return_value=command_api),
            mock.patch.object(main_module, "RedDetector", return_value=detector),
        ):
            plugin = DeltaForceRedPlugin(_DummyContext())

        success, _message = await plugin._finish_bind(
            "sender-1",
            "tester",
            "qq",
            "openid",
            "token",
            notify_origin=_make_origin("123"),
        )

        self.assertTrue(success)
        storage.add_user.assert_awaited_with(
            "sender-1",
            "openid",
            "token",
            name="tester",
            platform="qq",
            role_id="role-1",
            notify_origin=_make_origin("123"),
            interaction_origin=_make_origin("123"),
        )

    async def test_finish_bind_preserves_existing_private_notify_origin_on_group_rebind(self):
        storage = mock.AsyncMock()
        command_api = mock.AsyncMock()
        detector = mock.Mock()
        command_api.bind_account = mock.AsyncMock(
            return_value={"status": True, "data": {"role_id": "role-1"}}
        )
        storage.get_user = mock.AsyncMock(
            return_value={
                "notify_origin": _make_origin("123"),
                "interaction_origin": _make_origin("123"),
            }
        )
        storage.add_user = mock.AsyncMock(return_value=None)

        with (
            mock.patch.object(main_module, "Storage", return_value=storage),
            mock.patch.object(main_module, "GameAPI", return_value=command_api),
            mock.patch.object(main_module, "RedDetector", return_value=detector),
        ):
            plugin = DeltaForceRedPlugin(_DummyContext())

        success, _message = await plugin._finish_bind(
            "sender-1",
            "tester",
            "qq",
            "openid",
            "token",
            notify_origin=_make_origin("group-1", _DummyMessageType.GROUP_MESSAGE),
        )

        self.assertTrue(success)
        storage.add_user.assert_awaited_with(
            "sender-1",
            "openid",
            "token",
            name="tester",
            platform="qq",
            role_id="role-1",
            notify_origin=_make_origin("123"),
            interaction_origin=_make_origin("group-1", _DummyMessageType.GROUP_MESSAGE),
        )

    async def test_remember_user_origin_updates_interaction_origin_without_overwriting_private(self):
        storage = mock.AsyncMock()
        command_api = mock.AsyncMock()
        detector = mock.Mock()

        with (
            mock.patch.object(main_module, "Storage", return_value=storage),
            mock.patch.object(main_module, "GameAPI", return_value=command_api),
            mock.patch.object(main_module, "RedDetector", return_value=detector),
        ):
            plugin = DeltaForceRedPlugin(_DummyContext())

        class _DummyEvent:
            unified_msg_origin = _make_origin("group-1", _DummyMessageType.GROUP_MESSAGE)

            def is_private_chat(self):
                return False

        user_data = {
            "notify_origin": _make_origin("123"),
            "interaction_origin": _make_origin("123"),
        }
        await plugin._remember_user_origin("sender-1", _DummyEvent(), user_data=user_data)

        storage.update_user_state.assert_awaited_once_with(
            "sender-1",
            interaction_origin=_make_origin("group-1", _DummyMessageType.GROUP_MESSAGE),
        )
        self.assertEqual(user_data["notify_origin"], _make_origin("123"))
        self.assertEqual(
            user_data["interaction_origin"],
            _make_origin("group-1", _DummyMessageType.GROUP_MESSAGE),
        )

    async def test_bind_with_qq_qr_retries_transient_login_status_failures(self):
        storage = mock.AsyncMock()
        command_api = mock.AsyncMock()
        detector = mock.Mock()
        command_api.get_qq_login_qr = mock.AsyncMock(
            return_value={
                "status": True,
                "data": {
                    "image_base64": "base64-qr",
                    "cookie": {"qrsig": "sig"},
                    "qrSig": "sig",
                    "qrToken": 123,
                    "loginSig": "login-sig",
                },
            }
        )
        command_api.get_login_status = mock.AsyncMock(
            side_effect=[
                {"code": -4, "message": "获取登录状态失败", "data": {}},
                {"code": 1, "message": "等待扫码", "data": {}},
                {"code": 0, "message": "登录成功", "data": {"cookie": {"p_skey": "cookie"}}},
            ]
        )
        command_api.get_access_token_by_cookie = mock.AsyncMock(
            return_value={
                "status": True,
                "data": {"openid": "openid", "access_token": "token"},
            }
        )
        command_api.bind_account = mock.AsyncMock(
            return_value={"status": True, "data": {"role_id": "role-1"}}
        )
        storage.add_user = mock.AsyncMock(return_value=None)

        with (
            mock.patch.object(main_module, "Storage", return_value=storage),
            mock.patch.object(main_module, "GameAPI", return_value=command_api),
            mock.patch.object(main_module, "RedDetector", return_value=detector),
            mock.patch.object(main_module.asyncio, "sleep", mock.AsyncMock()),
        ):
            plugin = DeltaForceRedPlugin(_DummyContext())

        class _DummyEvent:
            @staticmethod
            def get_sender_id():
                return "sender-1"

            @staticmethod
            def get_sender_name():
                return "tester"

            @staticmethod
            def plain_result(message):
                return message

            @staticmethod
            def chain_result(message):
                return ("chain", message)

        messages = []
        async for result in plugin._bind_with_qq_qr(_DummyEvent()):
            messages.append(result)

        self.assertEqual(command_api.get_login_status.await_count, 3)
        self.assertEqual(messages[1], "请打开手机QQ使用摄像头扫码，等待自动绑定。")
        self.assertIn("绑定成功", messages[-1])

    async def test_set_group_reports_duplicate_binding(self):
        storage = mock.AsyncMock()
        command_api = mock.AsyncMock()
        detector = mock.Mock()
        storage.add_group = mock.AsyncMock(return_value=False)

        with (
            mock.patch.object(main_module, "Storage", return_value=storage),
            mock.patch.object(main_module, "GameAPI", return_value=command_api),
            mock.patch.object(main_module, "RedDetector", return_value=detector),
        ):
            plugin = DeltaForceRedPlugin(_DummyContext())

        class _DummyEvent:
            unified_msg_origin = "group:1"

            @staticmethod
            def plain_result(message):
                return message

        messages = []
        async for result in plugin.set_group(_DummyEvent()):
            messages.append(result)

        self.assertEqual(messages, ["当前群已经设置为播报群，无需重复设置。"])

    async def test_status_reports_local_record_not_fresh_login_success(self):
        storage = mock.AsyncMock()
        command_api = mock.AsyncMock()
        detector = mock.Mock()
        storage.get_user = mock.AsyncMock(
            return_value={
                "openid": "openid",
                "access_token": "token",
                "platform": "qq",
            }
        )
        storage.get_groups = mock.AsyncMock(return_value=["group:1", "group:2"])

        with (
            mock.patch.object(main_module, "Storage", return_value=storage),
            mock.patch.object(main_module, "GameAPI", return_value=command_api),
            mock.patch.object(main_module, "RedDetector", return_value=detector),
        ):
            plugin = DeltaForceRedPlugin(_DummyContext())

        class _DummyEvent:
            @staticmethod
            def get_sender_id():
                return "sender-1"

            @staticmethod
            def plain_result(message):
                return message

        messages = []
        async for result in plugin.status(_DummyEvent()):
            messages.append(result)

        self.assertEqual(
            messages,
            [
                "【账号记录】：已保存绑定记录\n【绑定状态】：有效\n【当前共有播报群数量】：2 个"
            ],
        )

    async def test_status_reports_invalid_binding_reason(self):
        storage = mock.AsyncMock()
        command_api = mock.AsyncMock()
        detector = mock.Mock()
        storage.get_user = mock.AsyncMock(
            return_value={
                "binding_status": "invalid",
                "binding_status_reason": "鉴权已过期",
            }
        )
        storage.get_groups = mock.AsyncMock(return_value=["group:1"])

        with (
            mock.patch.object(main_module, "Storage", return_value=storage),
            mock.patch.object(main_module, "GameAPI", return_value=command_api),
            mock.patch.object(main_module, "RedDetector", return_value=detector),
        ):
            plugin = DeltaForceRedPlugin(_DummyContext())

        class _DummyEvent:
            @staticmethod
            def get_sender_id():
                return "sender-1"

            @staticmethod
            def plain_result(message):
                return message

        messages = []
        async for result in plugin.status(_DummyEvent()):
            messages.append(result)

        self.assertEqual(
            messages,
            [
                "【账号记录】：已保存绑定记录\n【绑定状态】：已失效\n【失效原因】：鉴权已过期\n【当前共有播报群数量】：1 个"
            ],
        )

    async def test_unbind_account_handles_storage_write_failure(self):
        storage = mock.AsyncMock()
        command_api = mock.AsyncMock()
        detector = mock.Mock()
        storage.remove_user = mock.AsyncMock(side_effect=OSError("disk full"))

        with (
            mock.patch.object(main_module, "Storage", return_value=storage),
            mock.patch.object(main_module, "GameAPI", return_value=command_api),
            mock.patch.object(main_module, "RedDetector", return_value=detector),
        ):
            plugin = DeltaForceRedPlugin(_DummyContext())

        class _DummyEvent:
            @staticmethod
            def get_sender_id():
                return "sender-1"

            @staticmethod
            def plain_result(message):
                return message

        messages = []
        async for result in plugin.unbind_account(_DummyEvent()):
            messages.append(result)

        self.assertEqual(messages, ["解绑失败，请检查插件运行目录写入权限后重试。"])

    async def test_unset_group_handles_storage_write_failure(self):
        storage = mock.AsyncMock()
        command_api = mock.AsyncMock()
        detector = mock.Mock()
        storage.remove_group = mock.AsyncMock(side_effect=OSError("disk full"))

        with (
            mock.patch.object(main_module, "Storage", return_value=storage),
            mock.patch.object(main_module, "GameAPI", return_value=command_api),
            mock.patch.object(main_module, "RedDetector", return_value=detector),
        ):
            plugin = DeltaForceRedPlugin(_DummyContext())

        class _DummyEvent:
            unified_msg_origin = "group:1"

            @staticmethod
            def plain_result(message):
                return message

        messages = []
        async for result in plugin.unset_group(_DummyEvent()):
            messages.append(result)

        self.assertEqual(messages, ["取消群绑定失败，请检查插件运行目录写入权限后重试。"])

    async def test_refresh_item_catalog_reports_cache_fallback_as_failure(self):
        storage = mock.AsyncMock()
        command_api = mock.AsyncMock()
        detector = mock.Mock()
        detector.api = command_api
        storage.get_user = mock.AsyncMock(
            return_value={"openid": "openid", "access_token": "token", "platform": "qq"}
        )
        command_api.refresh_item_catalog = mock.AsyncMock(
            return_value={
                "status": False,
                "items": [{"objectID": "1001"}],
                "source": "cache",
            }
        )

        with (
            mock.patch.object(main_module, "Storage", return_value=storage),
            mock.patch.object(main_module, "GameAPI", return_value=command_api),
            mock.patch.object(main_module, "RedDetector", return_value=detector),
        ):
            plugin = DeltaForceRedPlugin(_DummyContext())

        class _DummyEvent:
            @staticmethod
            def get_sender_id():
                return "sender-1"

            @staticmethod
            def plain_result(message):
                return message

        messages = []
        async for result in plugin.refresh_item_catalog(_DummyEvent()):
            messages.append(result)

        self.assertEqual(messages, ["❌ 远程刷新失败，当前仍在使用本地缓存，共 1 条。"])

    async def test_check_now_sanitizes_failed_group_error_details(self):
        storage = mock.AsyncMock()
        command_api = mock.AsyncMock()
        detector = mock.Mock()
        storage.get_user = mock.AsyncMock(
            return_value={
                "openid": "openid",
                "access_token": "token",
                "platform": "qq",
                "name": "tester",
            }
        )
        storage.get_groups = mock.AsyncMock(return_value=["group:1"])
        detector.build_latest_broadcast_payload = mock.AsyncMock(
            return_value={
                "detected_items": [{"name": "样本A", "change": "+1"}],
                "match_info": {"dtEventTime": "2026-03-31 12:00:00", "roomId": "room-1"},
            }
        )
        detector.ensure_user_role_id = mock.AsyncMock(return_value="role-1")
        detector.broadcast = mock.AsyncMock(
            return_value={
                "message": "msg",
                "total_groups": 1,
                "success_groups": [],
                "failed_groups": [{"origin": "group:1", "error": "C:\\secret\\path"}],
            }
        )
        detector.persist_failed_broadcasts = mock.AsyncMock(return_value=None)

        with (
            mock.patch.object(main_module, "Storage", return_value=storage),
            mock.patch.object(main_module, "GameAPI", return_value=command_api),
            mock.patch.object(main_module, "RedDetector", return_value=detector),
        ):
            plugin = DeltaForceRedPlugin(_DummyContext())

        class _DummyEvent:
            @staticmethod
            def get_sender_id():
                return "sender-1"

            @staticmethod
            def plain_result(message):
                return message

        messages = []
        async for result in plugin.check_now(_DummyEvent()):
            messages.append(result)

        joined = "\n".join(messages)
        self.assertIn("发送失败，请查看日志。", joined)
        self.assertNotIn("C:\\secret\\path", joined)

    async def test_check_now_secret_error_short_circuits_before_progress_message(self):
        storage = mock.AsyncMock()
        command_api = mock.AsyncMock()
        detector = mock.Mock()
        storage.get_user = mock.AsyncMock(
            return_value={
                "_secret_errors": {"openid": "decrypt failed"},
            }
        )

        with (
            mock.patch.object(main_module, "Storage", return_value=storage),
            mock.patch.object(main_module, "GameAPI", return_value=command_api),
            mock.patch.object(main_module, "RedDetector", return_value=detector),
        ):
            plugin = DeltaForceRedPlugin(_DummyContext())

        class _DummyEvent:
            @staticmethod
            def get_sender_id():
                return "sender-1"

            @staticmethod
            def plain_result(message):
                return message

        messages = []
        async for result in plugin.check_now(_DummyEvent()):
            messages.append(result)

        self.assertEqual(
            messages,
            ["已保存的账号凭证无法解密，请重新执行 df绑定 以覆盖旧绑定；若仍失败，再执行 df解绑 后重试。"],
        )

    async def test_check_now_invalid_binding_short_circuits_before_progress_message(self):
        storage = mock.AsyncMock()
        command_api = mock.AsyncMock()
        detector = mock.Mock()
        storage.get_user = mock.AsyncMock(
            return_value={
                "binding_status": "invalid",
                "binding_status_reason": "鉴权已过期",
            }
        )

        with (
            mock.patch.object(main_module, "Storage", return_value=storage),
            mock.patch.object(main_module, "GameAPI", return_value=command_api),
            mock.patch.object(main_module, "RedDetector", return_value=detector),
        ):
            plugin = DeltaForceRedPlugin(_DummyContext())

        class _DummyEvent:
            @staticmethod
            def get_sender_id():
                return "sender-1"

            @staticmethod
            def plain_result(message):
                return message

        messages = []
        async for result in plugin.check_now(_DummyEvent()):
            messages.append(result)

        self.assertEqual(
            messages,
            ["当前保存的绑定已失效，后台监测已暂停。\n请重新执行 df绑定 以覆盖旧绑定。\n原因：鉴权已过期"],
        )

    async def test_check_now_continues_when_role_id_cache_persist_fails(self):
        storage = mock.AsyncMock()
        command_api = mock.AsyncMock()
        storage.get_user = mock.AsyncMock(
            return_value={
                "openid": "openid",
                "access_token": "token",
                "platform": "qq",
                "name": "tester",
            }
        )
        storage.get_groups = mock.AsyncMock(return_value=["group:1"])
        storage.update_user_state = mock.AsyncMock(side_effect=OSError("disk full"))
        command_api.fetch_records_v2 = mock.AsyncMock(
            return_value=[
                {
                    "dtEventTime": "2026-03-31 12:00:00",
                    "roomId": "room-1",
                    "roleId": "role-1",
                }
            ]
        )
        command_api.fetch_records = mock.AsyncMock(return_value=[])
        command_api.fetch_all_item_flows = mock.AsyncMock(
            return_value=[
                {
                    "dtEventTime": "2026-03-31 12:00:05",
                    "iGoodsId": "1001",
                    "AddOrReduce": "+1",
                    "Reason": "撤离带出",
                    "Name": "样本A",
                    "AfterCount": 1,
                }
            ]
        )
        command_api.fetch_item_catalog = mock.AsyncMock(
            return_value=[
                {
                    "objectID": "1001",
                    "primaryClass": "props",
                    "secondClass": "collection",
                    "grade": 6,
                }
            ]
        )
        command_api.fetch_room_info = mock.AsyncMock(return_value=[])
        command_api.fetch_room_flow = mock.AsyncMock(return_value=None)

        with (
            mock.patch.object(main_module, "Storage", return_value=storage),
            mock.patch.object(main_module, "GameAPI", return_value=command_api),
        ):
            plugin = DeltaForceRedPlugin(_DummyContext())

        class _DummyEvent:
            @staticmethod
            def get_sender_id():
                return "sender-1"

            @staticmethod
            def plain_result(message):
                return message

        messages = []
        async for result in plugin.check_now(_DummyEvent()):
            messages.append(result)

        self.assertEqual(
            messages,
            [
                "正在检查最近一局，请稍候...",
                "最近一局已成功播报到 1/1 个播报群。",
            ],
        )


if __name__ == "__main__":
    unittest.main()
