import asyncio
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
    async def send_message(self, origin, message):
        return None


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
    star = types.ModuleType("astrbot.api.star")

    api.logger = _DummyLogger()
    event.filter = _DummyFilter()
    event.AstrMessageEvent = object
    event.MessageChain = object
    message_components.Plain = _DummyPlain
    message_components.Image = _DummyImage
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
        self.assertEqual(user_state["last_match_time"], "")
        self.assertEqual(user_state["last_room_id"], "")
        self.assertEqual(user_state["last_item_flow_keys"], [])
        self.assertEqual(user_state["pending_broadcasts"], [])
        self.assertEqual(user_state["assets"], [])
        self.assertEqual(user_state["openid_secret"], "enc:new-openid")
        self.assertEqual(user_state["access_token_secret"], "enc:new-token")


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
        detector._get_item_catalog_map = mock.AsyncMock(
            return_value={
                "1001": {
                    "primaryClass": "props",
                    "secondClass": "collection",
                    "grade": 6,
                }
            }
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
        detector._get_item_catalog_map = mock.AsyncMock(
            return_value={
                "1001": {
                    "primaryClass": "props",
                    "secondClass": "collection",
                    "grade": 6,
                }
            }
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

        self.assertEqual(messages, ["已保存的账号凭证无法解密，请先解绑后重新绑定。"])


if __name__ == "__main__":
    unittest.main()
