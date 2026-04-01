import asyncio
from datetime import datetime
from pathlib import Path

from astrbot.api import logger
from astrbot.api.message_components import Plain

from ..api.game_api import GameAPI
from ..data.runtime_paths import get_runtime_debug_dir
from ..data.storage import Storage

try:
    from astrbot.api.event import MessageChain
except Exception:
    MessageChain = None

ITEM_FLOW_BASELINE_LIMIT = 200
MAX_PARALLEL_USER_CHECKS = 4
MAX_PARALLEL_BROADCASTS = 4
CHECK_USER_TIMEOUT_SECONDS = 45
MAX_PENDING_BROADCASTS = 20
ITEM_CATALOG_UNAVAILABLE_ERROR = "未获取到物品目录，请稍后重试或先执行 df刷新物品缓存。"
TRANSIENT_FAILURE_NOTICE_THRESHOLD = 3
NOTICE_TARGET_INTERACTION = "interaction"
NOTICE_TARGET_ADMIN = "admin"


class RedDetector:
    def __init__(self, storage: Storage, context, api=None):
        self.storage = storage
        self.api = api or GameAPI()
        self._owns_api = api is None
        self.context = context
        self.check_counters = {}
        self.credential_error_users = set()
        self.transient_failure_counters = {}
        self.transient_failure_notified_users = set()
        self.item_catalog_fallback_notified_users = set()
        self.debug_dir = Path(get_runtime_debug_dir())
        self.max_parallel_user_checks = MAX_PARALLEL_USER_CHECKS
        self.max_parallel_broadcasts = MAX_PARALLEL_BROADCASTS

    async def close(self):
        if self._owns_api:
            await self.api.close()

    def write_debug_file(self, filename, content):
        path = self.debug_dir / filename
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
        return str(path)

    def get_runtime_debug_dir(self):
        return str(self.debug_dir)

    def clear_user_runtime_state(self, sender_id):
        sender_id = str(sender_id)
        self.check_counters.pop(sender_id, None)
        self.credential_error_users.discard(sender_id)
        self.transient_failure_counters.pop(sender_id, None)
        self.transient_failure_notified_users.discard(sender_id)
        self.item_catalog_fallback_notified_users.discard(sender_id)

    @staticmethod
    def _normalize_origin(origin):
        return str(origin or "").strip()

    @classmethod
    def _normalize_origins(cls, origins):
        normalized = []
        for origin in origins or []:
            normalized_origin = cls._normalize_origin(origin)
            if normalized_origin and normalized_origin not in normalized:
                normalized.append(normalized_origin)
        return normalized

    @staticmethod
    def _get_flow_window(item_flows, limit=ITEM_FLOW_BASELINE_LIMIT):
        return list(item_flows[:limit])

    @staticmethod
    def _normalize_text_value(value):
        if value is None or isinstance(value, (dict, list, tuple, set)):
            return ""
        text = str(value).strip()
        if not text or text.lower() in {"none", "null", "unknown"}:
            return ""
        return text

    @staticmethod
    def _is_binding_invalid_message(message):
        message_text = str(message or "").strip()
        if not message_text:
            return False
        invalid_tokens = (
            "鉴权",
            "过期",
            "重新扫码登录",
            "cookie无效",
            "cookie过期",
            "登录失效",
        )
        lowered_message = message_text.lower()
        return any(token in message_text for token in invalid_tokens) or any(
            token in lowered_message for token in ("cookie invalid", "cookie expired")
        )

    @staticmethod
    def _normalize_binding_status(value):
        return "invalid" if str(value or "").strip().lower() == "invalid" else "active"

    @staticmethod
    def _normalize_failure_reason(message):
        normalized = str(message or "").strip()
        return normalized or "未获取到明确错误信息"

    @classmethod
    def _normalize_pending_notice(cls, notice):
        if not isinstance(notice, dict):
            return {}
        message = cls._normalize_text_value(notice.get("message"))
        if not message:
            return {}
        normalized_notice = {"message": message}
        notice_type = cls._normalize_text_value(notice.get("type"))
        if notice_type:
            normalized_notice["type"] = notice_type
        notice_target = cls._normalize_text_value(notice.get("target"))
        if notice_target in {NOTICE_TARGET_INTERACTION, NOTICE_TARGET_ADMIN}:
            normalized_notice["target"] = notice_target
        return normalized_notice

    @staticmethod
    def _format_notice_subject(sender_id, user_data):
        display_name = str(user_data.get("name", "")).strip()
        if display_name:
            return f"{display_name}({sender_id})"
        return f"用户 {sender_id}"

    def _get_admin_ids(self):
        try:
            config = self.context.get_config()
        except Exception as exc:
            logger.warning(
                "Failed to load AstrBot admin config while routing pending notices: "
                f"{type(exc).__name__}: {exc}"
            )
            return []

        raw_admin_ids = []
        if hasattr(config, "get"):
            raw_admin_ids = config.get("admins_id", []) or []

        if isinstance(raw_admin_ids, (str, int)):
            raw_admin_ids = [raw_admin_ids]
        elif not isinstance(raw_admin_ids, (list, tuple, set)):
            return []

        normalized_admin_ids = []
        for admin_id in raw_admin_ids:
            normalized_admin_id = self._normalize_text_value(admin_id)
            if normalized_admin_id and normalized_admin_id not in normalized_admin_ids:
                normalized_admin_ids.append(normalized_admin_id)
        return normalized_admin_ids

    @staticmethod
    def _resolve_user_private_origin(sender_id, user_data):
        interaction_origin = Storage.normalize_interaction_origin(
            user_data.get("interaction_origin", ""),
        )
        private_origin = Storage.derive_private_origin(
            sender_id,
            primary_origin=user_data.get("notify_origin", ""),
            fallback_origin=interaction_origin,
        )
        return private_origin, interaction_origin

    def _resolve_admin_notice_origins(self, user_data):
        platform_id = ""
        for origin in (
            user_data.get("interaction_origin", ""),
            user_data.get("notify_origin", ""),
        ):
            platform_id = Storage.extract_platform_id(origin)
            if platform_id:
                break

        if not platform_id:
            return []

        admin_origins = []
        for admin_id in self._get_admin_ids():
            admin_origin = Storage.build_private_origin(platform_id, admin_id)
            if admin_origin and admin_origin not in admin_origins:
                admin_origins.append(admin_origin)
        return admin_origins

    def _resolve_pending_notice_origins(self, sender_id, user_data, pending_notice):
        private_origin, interaction_origin = self._resolve_user_private_origin(
            sender_id,
            user_data,
        )
        notice_target = pending_notice.get("target")
        if notice_target == NOTICE_TARGET_ADMIN:
            admin_origins = self._resolve_admin_notice_origins(user_data)
            if admin_origins:
                return admin_origins, NOTICE_TARGET_ADMIN

            fallback_origin = interaction_origin or private_origin
            if fallback_origin:
                logger.warning(
                    f"Falling back to non-admin notice routing for user {sender_id} "
                    "because no official AstrBot admin private target could be resolved."
                )
                return [fallback_origin], f"{NOTICE_TARGET_ADMIN}_fallback"
            return [], NOTICE_TARGET_ADMIN

        if notice_target == NOTICE_TARGET_INTERACTION:
            target_origin = interaction_origin or private_origin
            return ([target_origin], NOTICE_TARGET_INTERACTION) if target_origin else ([], NOTICE_TARGET_INTERACTION)

        return ([private_origin], "private") if private_origin else ([], "private")

    @classmethod
    def _normalize_pending_broadcasts(cls, pending_broadcasts):
        normalized = []
        if not isinstance(pending_broadcasts, list):
            return normalized

        for entry in pending_broadcasts:
            if not isinstance(entry, dict):
                continue
            message = str(entry.get("message", "")).strip()
            origins = cls._normalize_origins(entry.get("origins", []))
            if not message or not origins:
                continue

            normalized_entry = {
                "message": message,
                "origins": origins,
            }
            event_time = cls._normalize_text_value(entry.get("event_time"))
            room_id = cls._normalize_text_value(entry.get("room_id"))
            if event_time:
                normalized_entry["event_time"] = event_time
            if room_id:
                normalized_entry["room_id"] = room_id
            normalized.append(normalized_entry)

        return normalized

    @classmethod
    def _merge_pending_broadcasts(cls, pending_broadcasts, message, failed_groups, match_info=None):
        normalized_pending = cls._normalize_pending_broadcasts(pending_broadcasts)
        failed_origins = cls._normalize_origins(
            item.get("origin", "")
            for item in failed_groups
            if isinstance(item, dict)
        )
        message_text = str(message or "").strip()
        if not message_text or not failed_origins:
            return normalized_pending

        new_entry = {
            "message": message_text,
            "origins": failed_origins,
        }
        if isinstance(match_info, dict):
            event_time = cls._normalize_text_value(
                match_info.get("dtEventTime") or match_info.get("event_time")
            )
            room_id = cls._normalize_text_value(
                match_info.get("roomId") or match_info.get("RoomId")
            )
            if event_time:
                new_entry["event_time"] = event_time
            if room_id:
                new_entry["room_id"] = room_id

        for existing in normalized_pending:
            same_room = new_entry.get("room_id") and new_entry.get("room_id") == existing.get("room_id")
            same_time = new_entry.get("event_time") and new_entry.get("event_time") == existing.get("event_time")
            if existing.get("message") == message_text or (same_room and same_time):
                existing["origins"] = cls._normalize_origins(
                    [*existing.get("origins", []), *failed_origins]
                )
                if new_entry.get("event_time") and not existing.get("event_time"):
                    existing["event_time"] = new_entry["event_time"]
                if new_entry.get("room_id") and not existing.get("room_id"):
                    existing["room_id"] = new_entry["room_id"]
                break
        else:
            normalized_pending.append(new_entry)

        if len(normalized_pending) > MAX_PENDING_BROADCASTS:
            normalized_pending = normalized_pending[-MAX_PENDING_BROADCASTS:]
        return normalized_pending

    @classmethod
    def _deep_find_text(cls, source, exact_keys=None, fuzzy_tokens=None):
        exact_keys = tuple(exact_keys or ())
        fuzzy_tokens = tuple(fuzzy_tokens or ())

        if isinstance(source, dict):
            for key in exact_keys:
                value = cls._normalize_text_value(source.get(key))
                if value:
                    return value

            for key, value in source.items():
                key_text = str(key).lower()
                if fuzzy_tokens and any(token in key_text for token in fuzzy_tokens) and not key_text.endswith("id"):
                    text = cls._normalize_text_value(value)
                    if text:
                        return text
                nested = cls._deep_find_text(value, exact_keys=exact_keys, fuzzy_tokens=fuzzy_tokens)
                if nested:
                    return nested

        elif isinstance(source, list):
            for item in source:
                nested = cls._deep_find_text(item, exact_keys=exact_keys, fuzzy_tokens=fuzzy_tokens)
                if nested:
                    return nested

        return ""

    @classmethod
    def _extract_map_name(cls, *sources):
        exact_keys = (
            "map_name",
            "MapName",
            "mapName",
            "sMapName",
            "Map",
            "map",
            "BattlefieldName",
            "battlefieldName",
            "SceneName",
            "sceneName",
            "PlaceName",
            "placeName",
        )
        fuzzy_tokens = ("map", "scene", "place", "battlefield")
        for source in sources:
            value = cls._deep_find_text(source, exact_keys=exact_keys, fuzzy_tokens=fuzzy_tokens)
            if value:
                return value
        return ""

    @classmethod
    def _extract_role_id(cls, *sources):
        exact_keys = (
            "role_id",
            "roleId",
            "RoleId",
            "sRoleId",
            "charId",
            "CharId",
        )
        for source in sources:
            value = cls._deep_find_text(source, exact_keys=exact_keys)
            if value:
                return value
        return ""

    @staticmethod
    def _coerce_dict_list(value, *, label):
        if not isinstance(value, list):
            if value not in (None, "", {}):
                logger.warning(
                    f"Expected {label} to be a list, got {type(value).__name__}."
                )
            return []

        normalized = [item for item in value if isinstance(item, dict)]
        if len(normalized) != len(value):
            logger.warning(
                f"Dropped {len(value) - len(normalized)} non-dict entries from {label}."
            )
        return normalized

    async def _fetch_latest_match(self, openid, access_token, platform="qq"):
        for label, fetcher in (
            ("records_v2", self.api.fetch_records_v2),
            ("records", self.api.fetch_records),
        ):
            records = self._coerce_dict_list(
                await fetcher(openid, access_token, type_id=4, platform=platform),
                label=label,
            )
            if records:
                return records[0]
        return None

    @staticmethod
    def _format_item_names(detected_items, limit=3):
        names = [
            str(item.get("name", "")).strip()
            for item in detected_items
            if str(item.get("name", "")).strip()
        ]
        if not names:
            return "未知物品"
        if len(names) <= limit:
            return "、".join(names)
        return f"{'、'.join(names[:limit])} 等 {len(names)} 件物品"

    async def _persist_role_id_hint(self, sender_id, user_data, role_id):
        role_id = str(role_id or "").strip()
        if not role_id:
            return ""
        if str(user_data.get("role_id", "")).strip() == role_id:
            user_data["role_id"] = role_id
            return role_id

        try:
            await self.storage.update_user_state(sender_id, role_id=role_id)
        except OSError as exc:
            logger.warning(
                f"Failed to persist role_id for sender {sender_id}: "
                f"{type(exc).__name__}: {exc}"
            )
        user_data["role_id"] = role_id
        return role_id

    async def _queue_pending_notice(self, sender_id, user_data, notice_type, message, target=None):
        pending_notice = {
            "type": str(notice_type or "").strip() or "generic",
            "message": str(message or "").strip(),
        }
        normalized_target = str(target or "").strip()
        if normalized_target in {NOTICE_TARGET_INTERACTION, NOTICE_TARGET_ADMIN}:
            pending_notice["target"] = normalized_target
        normalized_pending_notice = self._normalize_pending_notice(pending_notice)
        if not normalized_pending_notice:
            return False

        current_notice = self._normalize_pending_notice(user_data.get("pending_notice"))
        if current_notice == normalized_pending_notice:
            return False

        try:
            await self.storage.update_user_state(
                sender_id,
                pending_notice=normalized_pending_notice,
            )
        except OSError as exc:
            logger.warning(
                f"Failed to persist pending notice for user {sender_id}: "
                f"{type(exc).__name__}: {exc}"
            )
            return False
        user_data["pending_notice"] = normalized_pending_notice
        return True

    async def _flush_pending_notice(self, sender_id, user_data):
        pending_notice = self._normalize_pending_notice(user_data.get("pending_notice"))
        if not pending_notice:
            return False

        notify_origins, route_mode = self._resolve_pending_notice_origins(
            sender_id,
            user_data,
            pending_notice,
        )
        if not notify_origins:
            logger.warning(
                f"Failed to resolve a valid notice target for user {sender_id} "
                f"(route={route_mode})."
            )
            return False

        successful_origins = []
        failed_origins = []
        for notify_origin in notify_origins:
            try:
                await self._send_message_to_origin(notify_origin, pending_notice["message"])
            except Exception as exc:
                failed_origins.append(
                    f"{notify_origin}: {type(exc).__name__}: {exc}"
                )
                continue
            successful_origins.append(notify_origin)

        if not successful_origins:
            logger.warning(
                f"Failed to send pending notice to user {sender_id} via "
                f"{route_mode}: {' | '.join(failed_origins) if failed_origins else 'unknown send error'}"
            )
            return False
        if failed_origins:
            logger.warning(
                f"Partially failed to send pending notice for user {sender_id}: "
                f"{' | '.join(failed_origins)}"
            )

        try:
            await self.storage.update_user_state(sender_id, pending_notice=None)
        except OSError as exc:
            logger.warning(
                f"Failed to clear pending notice for user {sender_id}: "
                f"{type(exc).__name__}: {exc}"
            )
        user_data["pending_notice"] = None
        logger.info(
            f"Sent pending notice to user {sender_id} via {route_mode}: "
            f"{', '.join(successful_origins)}."
        )
        return True

    async def _set_binding_invalid(self, sender_id, user_data, reason, notice_type, notice_message):
        reason = str(reason or "").strip()
        fields = {}
        if self._normalize_binding_status(user_data.get("binding_status")) != "invalid":
            fields["binding_status"] = "invalid"
        if str(user_data.get("binding_status_reason", "")).strip() != reason:
            fields["binding_status_reason"] = reason
        if fields:
            try:
                await self.storage.update_user_state(sender_id, **fields)
            except OSError as exc:
                logger.warning(
                    f"Failed to persist invalid binding status for user {sender_id}: "
                    f"{type(exc).__name__}: {exc}"
                )
            else:
                user_data.update(fields)

        await self._queue_pending_notice(
            sender_id,
            user_data,
            notice_type,
            notice_message,
        )
        await self._flush_pending_notice(sender_id, user_data)

    def _clear_transient_failure_state(self, sender_id):
        sender_id = str(sender_id)
        self.transient_failure_counters.pop(sender_id, None)
        self.transient_failure_notified_users.discard(sender_id)

    def _clear_item_catalog_fallback_state(self, sender_id):
        self.item_catalog_fallback_notified_users.discard(str(sender_id))

    async def _register_transient_failure(self, sender_id, user_data, error_message):
        sender_id = str(sender_id)
        normalized_error = self._normalize_failure_reason(error_message)
        failure_count = self.transient_failure_counters.get(sender_id, 0) + 1
        self.transient_failure_counters[sender_id] = failure_count

        if failure_count < TRANSIENT_FAILURE_NOTICE_THRESHOLD:
            return False
        if sender_id in self.transient_failure_notified_users:
            return False

        notice_subject = self._format_notice_subject(sender_id, user_data)
        notice_message = (
            f"[系统告警] {notice_subject} 后台连续 {failure_count} 次获取三角洲数据失败，"
            "但暂未判定绑定已失效。\n"
            f"最近错误：{normalized_error}\n"
            "系统仍会自动重试；若长时间持续，请查看日志。"
        )
        current_notice = self._normalize_pending_notice(user_data.get("pending_notice"))
        queued = await self._queue_pending_notice(
            sender_id,
            user_data,
            "transient_upstream_error",
            notice_message,
            target=NOTICE_TARGET_ADMIN,
        )
        if not queued:
            expected_notice = {
                "type": "transient_upstream_error",
                "message": notice_message,
                "target": NOTICE_TARGET_ADMIN,
            }
            if current_notice != expected_notice:
                return False

        self.transient_failure_notified_users.add(sender_id)
        await self._flush_pending_notice(sender_id, user_data)
        return True

    async def _maybe_notify_item_catalog_fallback(self, sender_id, user_data):
        sender_id = str(sender_id)
        if sender_id in self.item_catalog_fallback_notified_users:
            return False

        notice_subject = self._format_notice_subject(sender_id, user_data)
        notice_message = (
            f"[系统告警] {notice_subject} 的物品目录自动刷新失败，"
            "当前暂时沿用旧缓存继续检测。\n"
            "这通常不会中断播报，但新版本物品分类可能存在延迟。\n"
            "若持续出现，请查看日志，必要时手动执行 df刷新物品缓存。"
        )
        current_notice = self._normalize_pending_notice(user_data.get("pending_notice"))
        queued = await self._queue_pending_notice(
            sender_id,
            user_data,
            "item_catalog_stale_fallback",
            notice_message,
            target=NOTICE_TARGET_ADMIN,
        )
        if not queued:
            expected_notice = {
                "type": "item_catalog_stale_fallback",
                "message": notice_message,
                "target": NOTICE_TARGET_ADMIN,
            }
            if current_notice != expected_notice:
                return False

        self.item_catalog_fallback_notified_users.add(sender_id)
        await self._flush_pending_notice(sender_id, user_data)
        return True

    async def _maybe_notify_invalid_binding(self, sender_id, user_data, openid, access_token, platform="qq"):
        try:
            bind_res = await self.api.bind_account(access_token, openid, platform)
        except Exception as exc:
            logger.warning(
                f"Failed to validate binding for user {sender_id}: "
                f"{type(exc).__name__}: {exc}"
            )
            await self._register_transient_failure(
                sender_id,
                user_data,
                f"绑定状态校验异常: {type(exc).__name__}: {exc}",
            )
            return False

        if not isinstance(bind_res, dict):
            logger.warning(
                f"Expected bind_account validation result to be a dict for user {sender_id}, "
                f"got {type(bind_res).__name__}."
            )
            await self._register_transient_failure(
                sender_id,
                user_data,
                f"绑定状态校验返回格式异常: {type(bind_res).__name__}",
            )
            return False

        if bind_res.get("status"):
            self._clear_transient_failure_state(sender_id)
            role_id = str(bind_res.get("data", {}).get("role_id", "")).strip()
            if role_id:
                await self._persist_role_id_hint(sender_id, user_data, role_id)
            return False

        error_message = str(bind_res.get("message", "")).strip()
        error_kind = str(bind_res.get("error_kind", "")).strip().lower()
        if error_kind:
            if error_kind != "credential_expired":
                await self._register_transient_failure(
                    sender_id,
                    user_data,
                    error_message or f"绑定状态校验失败({error_kind})",
                )
                return False
        elif not self._is_binding_invalid_message(error_message):
            await self._register_transient_failure(
                sender_id,
                user_data,
                error_message or "绑定状态校验失败",
            )
            return False

        self._clear_transient_failure_state(sender_id)
        logger.warning(
            f"Detected invalid binding for user {sender_id}: {error_message}"
        )
        await self._set_binding_invalid(
            sender_id,
            user_data,
            error_message,
            "binding_invalid",
            "检测到你当前保存的三角洲绑定可能已失效，后台监测已暂停。\n"
            "请重新执行 df绑定 以覆盖旧绑定。\n"
            f"原因：{error_message}",
        )
        return True

    async def ensure_user_role_id(self, sender_id, user_data, match_info=None):
        role_id = str(user_data.get("role_id", "")).strip()
        if not role_id and match_info:
            role_id = self._extract_role_id(match_info)

        if role_id:
            return await self._persist_role_id_hint(sender_id, user_data, role_id)

        openid = user_data.get("openid")
        access_token = user_data.get("access_token")
        platform = (user_data.get("platform", "qq") or "qq").strip().lower()
        if not openid or not access_token:
            return ""

        bind_res = await self.api.bind_account(access_token, openid, platform)
        bind_data = bind_res.get("data", {}) if isinstance(bind_res, dict) else {}
        if bind_res and not isinstance(bind_res, dict):
            logger.warning(
                f"Expected bind_account result to be a dict for sender {sender_id}, "
                f"got {type(bind_res).__name__}."
            )
        role_id = str(bind_data.get("role_id", "")).strip()
        if role_id:
            return await self._persist_role_id_hint(sender_id, user_data, role_id)
        return ""

    async def _enrich_match_info(self, openid, access_token, match_info, platform="qq"):
        if not isinstance(match_info, dict):
            return match_info

        enriched = dict(match_info)
        map_name = self._extract_map_name(enriched)
        role_id = self._extract_role_id(enriched)
        if map_name:
            enriched["map_name"] = map_name
        if role_id:
            enriched["role_id"] = role_id
        if map_name and role_id:
            return enriched

        room_id = self._extract_room_id(enriched)
        if not room_id:
            return enriched

        room_info_result, room_flow_result = await asyncio.gather(
            self.api.fetch_room_info(openid, access_token, room_id, platform=platform),
            self.api.fetch_room_flow(
                openid,
                access_token,
                room_id,
                type_id=1,
                platform=platform,
            ),
            return_exceptions=True,
        )

        room_info = []
        room_flow = None
        if isinstance(room_info_result, Exception):
            logger.warning(
                f"Failed to enrich room info for room {room_id}: "
                f"{type(room_info_result).__name__}: {room_info_result}"
            )
        else:
            room_info = room_info_result

        if isinstance(room_flow_result, Exception):
            logger.warning(
                f"Failed to enrich room flow for room {room_id}: "
                f"{type(room_flow_result).__name__}: {room_flow_result}"
            )
        else:
            room_flow = room_flow_result

        if room_info:
            enriched["room_info"] = room_info
        if room_flow:
            enriched["room_flow"] = room_flow

        map_name = self._extract_map_name(enriched, room_info, room_flow)
        role_id = self._extract_role_id(enriched, room_info, room_flow)
        if map_name:
            enriched["map_name"] = map_name
        if role_id:
            enriched["role_id"] = role_id
        return enriched

    def _build_broadcast_message(self, user_name, detected_items, match_info=None, role_id=""):
        display_name = str(user_name or "").strip() or "未知玩家"
        display_role_id = str(role_id or self._extract_role_id(match_info) or "").strip() or "未知角色ID"
        event_time = "未知时间"
        if isinstance(match_info, dict):
            event_time = match_info.get("dtEventTime") or match_info.get("event_time") or event_time
        map_name = self._extract_map_name(match_info) or "未知地图"
        item_names = self._format_item_names(detected_items)
        return (
            "【天降洪福大红播报】\n"
            f"恭喜本群玩家 {display_name} / {display_role_id} 在 {event_time} 的 {map_name} 中带出了 {item_names}！"
        )

    async def persist_failed_broadcasts(self, sender_id, user_data, broadcast_result, match_info=None):
        current_pending = self._normalize_pending_broadcasts(user_data.get("pending_broadcasts", []))
        updated_pending = self._merge_pending_broadcasts(
            current_pending,
            broadcast_result.get("message", ""),
            broadcast_result.get("failed_groups", []),
            match_info=match_info,
        )
        if updated_pending == current_pending:
            return updated_pending

        await self.storage.update_user_state(sender_id, pending_broadcasts=updated_pending)
        user_data["pending_broadcasts"] = updated_pending
        return updated_pending

    async def retry_pending_broadcasts(self, sender_id, user_data):
        pending_broadcasts = self._normalize_pending_broadcasts(user_data.get("pending_broadcasts", []))
        if not pending_broadcasts:
            return False

        active_groups = set(self._normalize_origins(await self.storage.get_groups()))
        remaining_pending = []
        changed = False

        for entry in pending_broadcasts:
            retry_origins = [origin for origin in entry.get("origins", []) if origin in active_groups]
            if not retry_origins:
                changed = True
                continue

            result = await self.broadcast_message(
                entry["message"],
                origins=retry_origins,
                write_debug_snapshot=False,
                log_prefix="Retrying pending broadcast",
            )
            failed_origins = self._normalize_origins(
                item.get("origin", "")
                for item in result.get("failed_groups", [])
                if isinstance(item, dict)
            )
            if failed_origins:
                if failed_origins != retry_origins:
                    changed = True
                updated_entry = dict(entry)
                updated_entry["origins"] = failed_origins
                remaining_pending.append(updated_entry)
                continue

            changed = True
            logger.info(
                f"Pending broadcast retry completed for user {sender_id} "
                f"({len(retry_origins)} groups)"
            )

        if changed or remaining_pending != pending_broadcasts:
            await self.storage.update_user_state(sender_id, pending_broadcasts=remaining_pending)
            user_data["pending_broadcasts"] = remaining_pending

        return bool(remaining_pending)

    async def _send_message_to_origin(self, origin, msg):
        errors = []

        if MessageChain is not None:
            try:
                chain = MessageChain().message(msg)
                await self.context.send_message(origin, chain)
                return "MessageChain"
            except Exception as exc:
                errors.append(f"MessageChain: {type(exc).__name__}: {exc}")

        try:
            await self.context.send_message(origin, [Plain(msg)])
            return "PlainList"
        except Exception as exc:
            errors.append(f"PlainList: {type(exc).__name__}: {exc}")

        try:
            await self.context.send_message(origin, msg)
            return "RawText"
        except Exception as exc:
            errors.append(f"RawText: {type(exc).__name__}: {exc}")

        raise RuntimeError(" | ".join(errors) if errors else "unknown send error")

    @staticmethod
    def _parse_time(value):
        if not value:
            return None
        try:
            return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
        except Exception:
            return None

    @staticmethod
    def _safe_int(value, default=0):
        try:
            return int(float(str(value)))
        except Exception:
            return default

    @staticmethod
    def _is_positive_change(change_value):
        try:
            return float(str(change_value)) > 0
        except Exception:
            return str(change_value).startswith("+")

    def _match_time_window(self, match_time, item_time, seconds=300):
        match_dt = self._parse_time(match_time)
        item_dt = self._parse_time(item_time)
        if not match_dt or not item_dt:
            return False
        return abs((item_dt - match_dt).total_seconds()) <= seconds

    @staticmethod
    def _extract_category_fields(info):
        if not isinstance(info, dict):
            return []
        fields = []
        props_detail = info.get("propsDetail") if isinstance(info.get("propsDetail"), dict) else {}
        for key in [
            "primary",
            "second",
            "type",
            "subType",
            "category",
            "objectType",
            "itemType",
            "primaryClass",
            "secondClass",
            "secondClassCN",
            "thirdClass",
            "thirdClassCN",
        ]:
            value = info.get(key)
            if value is not None:
                fields.append(str(value).lower())
        for key in ["type", "propsSource", "useMap", "usePlace"]:
            value = props_detail.get(key)
            if value is not None:
                fields.append(str(value).lower())
        return fields

    def _is_collection_item(self, info):
        if not isinstance(info, dict):
            return False
        primary_class = str(info.get("primaryClass", "")).lower()
        second_class = str(info.get("secondClass", "")).lower()
        grade = self._safe_int(info.get("grade", 0))
        return primary_class == "props" and second_class == "collection" and grade == 6

    @staticmethod
    def _summarize_flow_buckets(item_flows):
        summary = {
            "撤离带出+": 0,
            "撤离带出-": 0,
            "带入局内+": 0,
            "带入局内-": 0,
            "其他+": 0,
            "其他-": 0,
        }
        for item in item_flows:
            reason = str(item.get("Reason", ""))
            is_positive = RedDetector._is_positive_change(item.get("AddOrReduce", "0"))
            if "撤离带出" in reason:
                summary["撤离带出+" if is_positive else "撤离带出-"] += 1
            elif "带入局内" in reason:
                summary["带入局内+" if is_positive else "带入局内-"] += 1
            else:
                summary["其他+" if is_positive else "其他-"] += 1
        return summary

    @staticmethod
    def _build_legacy_flow_key(item):
        return "|".join(
            [
                str(item.get("dtEventTime", "")),
                str(item.get("iGoodsId", "")),
                str(item.get("AddOrReduce", "")),
                str(item.get("Reason", "")),
            ]
        )

    @classmethod
    def _build_flow_key(cls, item):
        return "|".join(
            [
                cls._build_legacy_flow_key(item),
                str(item.get("Name", "")),
                str(item.get("AfterCount", "")),
            ]
        )

    @classmethod
    def _build_flow_key_variants(cls, item):
        return {
            cls._build_flow_key(item),
            cls._build_legacy_flow_key(item),
        }

    def _collect_match_window_items(self, item_flows, match_time, reason_keyword, positive_change, seconds=1800):
        result = []
        for item in item_flows:
            reason = str(item.get("Reason", ""))
            if reason_keyword not in reason:
                continue
            is_positive = self._is_positive_change(item.get("AddOrReduce", "0"))
            if is_positive != positive_change:
                continue
            if match_time and not self._match_time_window(match_time, item.get("dtEventTime", ""), seconds=seconds):
                continue
            result.append(item)
        return result

    async def broadcast(self, user_name, detected_items, match_info=None, role_id=""):
        message = self._build_broadcast_message(
            user_name,
            detected_items,
            match_info,
            role_id=role_id,
        )
        return await self.broadcast_message(
            message,
            write_debug_snapshot=True,
            log_prefix="Triggered collection broadcast",
        )

    def _collect_reason_items(self, item_flows, reason_keyword, positive_change):
        result = []
        for item in item_flows:
            reason = str(item.get("Reason", ""))
            if reason_keyword not in reason:
                continue
            is_positive = self._is_positive_change(item.get("AddOrReduce", "0"))
            if is_positive != positive_change:
                continue
            result.append(item)
        return result

    def _extract_room_id(self, match):
        if not isinstance(match, dict):
            return ""
        return str(match.get("roomId") or match.get("RoomId") or "")

    @staticmethod
    def _build_item_catalog_map(items):
        if not items:
            return None
        info_map = {}
        for info in items:
            if not isinstance(info, dict):
                continue
            key = str(info.get("objectID") or info.get("item_id") or info.get("id") or "")
            if key:
                info_map[key] = info
        return info_map or None

    async def _get_item_catalog_map(self, openid, access_token, platform="qq"):
        items = await self.api.fetch_item_catalog(openid, access_token, platform=platform)
        return self._build_item_catalog_map(items)

    async def _get_item_catalog_map_with_meta(self, openid, access_token, platform="qq"):
        result = await self.api.fetch_item_catalog(
            openid,
            access_token,
            platform=platform,
            return_metadata=True,
        )
        if result is None:
            return None, {}
        if isinstance(result, dict):
            items = result.get("items", [])
            metadata = {key: value for key, value in result.items() if key != "items"}
            return self._build_item_catalog_map(items), metadata
        return self._build_item_catalog_map(result), {}

    async def build_debug_report(self, openid, access_token, platform="qq"):
        latest_match = await self._fetch_latest_match(
            openid,
            access_token,
            platform=platform,
        )
        if not latest_match:
            return {"error": "未获取到最新战绩"}

        current_match_time = latest_match.get("dtEventTime", "")
        current_room_id = self._extract_room_id(latest_match)
        item_flows = self._coerce_dict_list(
            await self.api.fetch_all_item_flows(openid, access_token, platform=platform),
            label="item_flows",
        )

        all_carry_out_items = self._collect_reason_items(
            item_flows,
            reason_keyword="撤离带出",
            positive_change=True,
        )
        all_carry_in_items = self._collect_reason_items(
            item_flows,
            reason_keyword="带入局内",
            positive_change=True,
        )
        carry_out_items = self._collect_match_window_items(
            item_flows,
            current_match_time,
            reason_keyword="撤离带出",
            positive_change=True,
            seconds=420,
        )

        collection_candidates = []
        if carry_out_items:
            info_map = await self._get_item_catalog_map(openid, access_token, platform=platform)
            if info_map is None:
                return {"error": ITEM_CATALOG_UNAVAILABLE_ERROR}

            for item in carry_out_items:
                item_id = str(item.get("iGoodsId", ""))
                info = info_map.get(item_id, {})
                collection_candidates.append(
                    {
                        "id": item_id,
                        "name": item.get("Name", ""),
                        "time": item.get("dtEventTime", ""),
                        "change": item.get("AddOrReduce", ""),
                        "reason": item.get("Reason", ""),
                        "is_collection": self._is_collection_item(info),
                        "grade": self._safe_int(info.get("grade", 0)),
                        "category_fields": self._extract_category_fields(info),
                    }
                )

        return {
            "match": {
                "room_id": current_room_id,
                "event_time": current_match_time,
                "final_price": latest_match.get("FinalPrice", "0"),
                "escape_reason": latest_match.get("EscapeFailReason", ""),
            },
            "flow_summary": self._summarize_flow_buckets(item_flows),
            "total_item_flows": len(item_flows),
            "all_carry_out_items": all_carry_out_items,
            "all_carry_in_items": all_carry_in_items,
            "carry_out_items": carry_out_items,
            "collection_candidates": collection_candidates,
        }

    async def build_latest_broadcast_payload(self, openid, access_token, platform="qq"):
        latest_match = await self._fetch_latest_match(
            openid,
            access_token,
            platform=platform,
        )
        if not latest_match:
            return {"error": "未获取到最近一局战绩"}

        current_match_time = latest_match.get("dtEventTime", "")
        item_flows = self._coerce_dict_list(
            await self.api.fetch_all_item_flows(openid, access_token, platform=platform),
            label="item_flows",
        )
        if not item_flows:
            return {"error": "未获取到道具流水"}

        carry_out_items = self._collect_match_window_items(
            item_flows,
            current_match_time,
            reason_keyword="撤离带出",
            positive_change=True,
            seconds=420,
        )

        detected_items = []
        if carry_out_items:
            info_map = await self._get_item_catalog_map(openid, access_token, platform=platform)
            if info_map is None:
                return {"error": ITEM_CATALOG_UNAVAILABLE_ERROR}

            for item in carry_out_items:
                item_id = str(item.get("iGoodsId", ""))
                info = info_map.get(item_id, {})
                if not self._is_collection_item(info):
                    continue
                detected_items.append(
                    {
                        "id": item_id,
                        "name": item.get("Name") or info.get("name") or f"未知物品({item_id})",
                        "time": item.get("dtEventTime", ""),
                        "change": item.get("AddOrReduce", "+0"),
                        "reason": item.get("Reason", ""),
                    }
                )

        detected_items.sort(key=lambda item: item.get("name", ""))
        latest_match = await self._enrich_match_info(openid, access_token, latest_match, platform=platform)
        return {
            "match_info": latest_match,
            "detected_items": detected_items,
        }

    async def check_all_users(self):
        users = await self.storage.get_users()
        if not users:
            self.check_counters.clear()
            return

        active_sender_ids = {str(sender_id) for sender_id in users}
        for sender_id in list(self.check_counters):
            if sender_id not in active_sender_ids:
                self.check_counters.pop(sender_id, None)

        semaphore = asyncio.Semaphore(self.max_parallel_user_checks)

        async def run_check(sender_id, user_data):
            async with semaphore:
                await self.check_user(sender_id, user_data)

        sender_entries = [
            (str(sender_id), user_data)
            for sender_id, user_data in users.items()
        ]
        outcomes = await asyncio.gather(
            *(run_check(sender_id, user_data) for sender_id, user_data in sender_entries),
            return_exceptions=True,
        )
        for (sender_id, _), outcome in zip(sender_entries, outcomes):
            if isinstance(outcome, asyncio.CancelledError):
                logger.warning(f"User check for {sender_id} was cancelled before completion.")
            elif isinstance(outcome, Exception):
                logger.error(
                    f"Unexpected failure escaped user check for {sender_id}: "
                    f"{type(outcome).__name__}: {outcome}"
                )

    async def check_user(self, sender_id, user_data):
        sender_id = str(sender_id)
        try:
            await asyncio.wait_for(
                self._check_user_impl(sender_id, user_data),
                timeout=CHECK_USER_TIMEOUT_SECONDS,
            )
        except asyncio.TimeoutError:
            logger.warning(
                f"Timed out while checking user {sender_id} after "
                f"{CHECK_USER_TIMEOUT_SECONDS}s"
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error(f"Failed to check user {sender_id}: {type(exc).__name__}: {exc}")

    async def _check_user_impl(self, sender_id, user_data):
        await self._flush_pending_notice(sender_id, user_data)

        if self._normalize_pending_broadcasts(user_data.get("pending_broadcasts", [])):
            await self.retry_pending_broadcasts(sender_id, user_data)

        if user_data.get("_secret_errors"):
            if sender_id not in self.credential_error_users:
                logger.warning(
                    f"Skipping user {sender_id} because stored credentials could not be decrypted."
                )
                self.credential_error_users.add(sender_id)
                await self._set_binding_invalid(
                    sender_id,
                    user_data,
                    "已保存的账号凭证无法解密",
                    "secret_error",
                    "检测到你当前保存的账号凭证无法解密，后台监测已暂停。\n"
                    "请重新执行 df绑定 以覆盖旧绑定；若仍失败，再执行 df解绑 后重试。",
                )
            return
        self.credential_error_users.discard(sender_id)

        if self._normalize_binding_status(user_data.get("binding_status")) == "invalid":
            self._clear_transient_failure_state(sender_id)
            return

        openid = user_data.get("openid")
        access_token = user_data.get("access_token")
        platform = (user_data.get("platform", "qq") or "qq").strip().lower()
        if not openid or not access_token:
            return

        counter = self.check_counters.get(sender_id, 0)
        self.check_counters[sender_id] = counter + 1

        latest_match = await self._fetch_latest_match(
            openid,
            access_token,
            platform=platform,
        )
        if not latest_match:
            await self._maybe_notify_invalid_binding(
                sender_id,
                user_data,
                openid,
                access_token,
                platform=platform,
            )
            return

        self._clear_transient_failure_state(sender_id)

        current_match_time = latest_match.get("dtEventTime", "")
        current_room_id = self._extract_room_id(latest_match)
        last_match_time = user_data.get("last_match_time", "")
        last_room_id = str(user_data.get("last_room_id", "") or "")
        last_item_flow_keys = set(user_data.get("last_item_flow_keys", []))

        match_updated = False
        if current_room_id:
            match_updated = current_room_id != last_room_id
        elif current_match_time:
            match_updated = current_match_time != last_match_time

        should_check_flow = match_updated or counter % 15 == 0 or not last_item_flow_keys
        if not should_check_flow:
            return

        item_flows = self._coerce_dict_list(
            await self.api.fetch_all_item_flows(
                openid,
                access_token,
                platform=platform,
            ),
            label="item_flows",
        )
        if not item_flows:
            return

        flow_window = self._get_flow_window(item_flows)
        current_flow_keys = list(
            dict.fromkeys(self._build_flow_key(item) for item in flow_window)
        )

        if not last_item_flow_keys:
            await self.storage.update_user_state(
                sender_id,
                last_item_flow_keys=current_flow_keys,
                last_match_time=current_match_time,
                last_room_id=current_room_id,
            )
            logger.info(
                f"玩家 {sender_id} 首次加载道具流水基线完成 "
                f"({len(current_flow_keys)} 项)"
            )
            return

        new_flow_items = [
            item
            for item in flow_window
            if last_item_flow_keys.isdisjoint(self._build_flow_key_variants(item))
        ]
        carry_out_items = self._collect_match_window_items(
            new_flow_items,
            current_match_time,
            reason_keyword="撤离带出",
            positive_change=True,
            seconds=420,
        )

        detected_items = []
        if carry_out_items:
            info_map, catalog_meta = await self._get_item_catalog_map_with_meta(
                openid,
                access_token,
                platform=platform,
            )
            if info_map is None:
                logger.warning(
                    f"Skipping collection detection for user {sender_id} because the item catalog is unavailable."
                )
                return
            if catalog_meta.get("used_stale_cache"):
                await self._maybe_notify_item_catalog_fallback(sender_id, user_data)
            else:
                self._clear_item_catalog_fallback_state(sender_id)

            for item in carry_out_items:
                item_id = str(item.get("iGoodsId", ""))
                info = info_map.get(item_id, {})
                display_name = item.get("Name") or info.get("name") or f"未知物品({item_id})"
                if not self._is_collection_item(info):
                    continue
                detected_items.append(
                    {
                        "id": item_id,
                        "name": display_name,
                        "time": item.get("dtEventTime", ""),
                        "change": item.get("AddOrReduce", "+0"),
                        "reason": item.get("Reason", ""),
                    }
                )

        detected_items.sort(key=lambda item: item.get("name", ""))
        update_fields = {
            "last_item_flow_keys": current_flow_keys,
            "last_match_time": current_match_time,
            "last_room_id": current_room_id,
        }
        await self.storage.update_user_state(
            sender_id,
            **update_fields,
        )
        user_data.update(update_fields)
        if detected_items:
            latest_match = await self._enrich_match_info(
                openid,
                access_token,
                latest_match,
                platform=platform,
            )
            role_id = await self.ensure_user_role_id(sender_id, user_data, match_info=latest_match)
            broadcast_result = await self.broadcast(
                user_data.get("name", sender_id),
                detected_items,
                latest_match,
                role_id=role_id,
            )
            if not broadcast_result.get("success_groups"):
                failed_origins = ", ".join(
                    item.get("origin", "")
                    for item in broadcast_result.get("failed_groups", [])
                    if item.get("origin", "")
                )
                logger.warning(
                    f"User {sender_id} detected collection items but all broadcasts failed. "
                    f"Targets: {failed_origins or 'unknown'}"
                )
            if broadcast_result.get("failed_groups"):
                pending_broadcasts = self._merge_pending_broadcasts(
                    user_data.get("pending_broadcasts", []),
                    broadcast_result.get("message", ""),
                    broadcast_result.get("failed_groups", []),
                    match_info=latest_match,
                )
                try:
                    await self.storage.update_user_state(
                        sender_id,
                        pending_broadcasts=pending_broadcasts,
                    )
                except OSError as exc:
                    logger.error(
                        f"Failed to persist pending broadcast retries for user {sender_id}: "
                        f"{type(exc).__name__}: {exc}"
                    )
                else:
                    user_data["pending_broadcasts"] = pending_broadcasts
                    logger.warning(
                        f"Queued retry for {len(broadcast_result.get('failed_groups', []))} "
                        f"failed broadcast targets for user {sender_id}."
                    )

    async def broadcast_message(self, message, origins=None, *, write_debug_snapshot=False, log_prefix="Broadcasting message"):
        if origins is None:
            groups = await self.storage.get_groups()
        else:
            groups = origins
        groups = self._normalize_origins(groups)
        result = {
            "message": message,
            "total_groups": len(groups),
            "success_groups": [],
            "failed_groups": [],
        }
        msg = message

        logger.info(f"{log_prefix}:\n{msg}")
        if write_debug_snapshot:
            try:
                self.write_debug_file("debug_last_broadcast.txt", msg)
            except Exception as exc:
                logger.error(f"Failed to write latest broadcast snapshot: {exc}")

        if not groups:
            return result

        semaphore = asyncio.Semaphore(self.max_parallel_broadcasts)

        async def send_to_group(origin):
            async with semaphore:
                try:
                    send_mode = await self._send_message_to_origin(origin, msg)
                    return True, {
                        "origin": origin,
                        "mode": send_mode,
                    }
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    error_message = str(exc)
                    logger.error(f"Broadcast to group {origin} failed: {error_message}")
                    return False, {
                        "origin": origin,
                        "error": "发送失败，请查看日志。",
                    }

        outcomes = await asyncio.gather(*(send_to_group(origin) for origin in groups))
        for success, payload in outcomes:
            if success:
                result["success_groups"].append(payload)
            else:
                result["failed_groups"].append(payload)

        logger.info(
            f"{log_prefix} result: success {len(result['success_groups'])}/{len(groups)}, "
            f"failed {len(result['failed_groups'])}/{len(groups)}"
        )
        return result
